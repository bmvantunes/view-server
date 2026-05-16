import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import {
  rowKeyByField,
  type DeltaEvent,
  type DeltaOperation,
  type RuntimeRow,
  type RuntimeRowKeyFn,
  type SubscriptionEvent,
} from "../protocol/index.ts";
import { versionGap, type ViewServerError } from "../errors.ts";

export type LiveQueryStatus = "connecting" | "live" | "reconnecting" | "stale";

export type LiveQueryLifecycle = "connecting" | "syncing" | "live" | "closed";

export type LiveQueryInitialData<TRow> = {
  readonly rows: readonly TRow[];
  readonly totalRows: number;
};

export type LiveQueryValue<TRow> = LiveQueryInitialData<TRow> & {
  readonly status: LiveQueryStatus;
  readonly connection: LiveQueryConnection;
};

export type LiveQueryResult<TRow> = AsyncResult.AsyncResult<LiveQueryValue<TRow>, ViewServerError>;

export type LiveQueryConnection = {
  readonly connected: boolean;
  readonly attempt: number;
  readonly lastConnectedAt?: number | undefined;
  readonly lastDisconnectedAt?: number | undefined;
};

export type LiveQueryListener = (state: LiveQueryResult<RuntimeRow>) => void;

export class LiveQueryStore {
  #state: LiveQueryResult<RuntimeRow>;
  #value: LiveQueryValue<RuntimeRow>;
  #hasValue: boolean;
  #listeners = new Set<LiveQueryListener>();
  #version: bigint | undefined;
  #rowKey: RuntimeRowKeyFn;

  constructor(
    initialData: LiveQueryInitialData<RuntimeRow> | undefined,
    rowKey: RuntimeRowKeyFn = (row) => rowKeyByField(row, "id"),
  ) {
    this.#hasValue = initialData !== undefined;
    this.#value = {
      rows: initialData?.rows ?? [],
      totalRows: initialData?.totalRows ?? 0,
      status: this.#hasValue ? "stale" : "connecting",
      connection: {
        connected: false,
        attempt: 0,
      },
    };
    this.#state = this.#waitingState();
    this.#rowKey = rowKey;
  }

  get snapshot(): LiveQueryResult<RuntimeRow> {
    return this.#state;
  }

  subscribe(listener: LiveQueryListener): () => void {
    this.#listeners.add(listener);
    listener(this.#state);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  setStatus(status: LiveQueryLifecycle): void {
    this.#value = {
      ...this.#value,
      status: this.#statusFromLifecycle(status),
      connection:
        status === "closed"
          ? {
              ...this.#value.connection,
              connected: false,
              lastDisconnectedAt: Date.now(),
            }
          : this.#value.connection,
    };
    this.#update(status === "live" ? this.#successState(false) : this.#waitingState());
  }

  setError(error: ViewServerError): void {
    this.#value = {
      ...this.#value,
      status: this.#value.connection.attempt > 1 ? "reconnecting" : "stale",
      connection: {
        ...this.#value.connection,
        connected: false,
        lastDisconnectedAt: Date.now(),
      },
    };
    this.#update(
      this.#hasValue
        ? AsyncResult.failWithPrevious<LiveQueryValue<RuntimeRow>, ViewServerError>(error, {
            previous: Option.some(
              AsyncResult.success<LiveQueryValue<RuntimeRow>, ViewServerError>(this.#value, {
                waiting: true,
              }),
            ),
            waiting: true,
          })
        : AsyncResult.fail<ViewServerError, LiveQueryValue<RuntimeRow>>(error),
    );
  }

  setRowKey(rowKey: RuntimeRowKeyFn): void {
    this.#rowKey = rowKey;
  }

  beginAttempt(attempt: number, now = Date.now()): void {
    this.#value = {
      ...this.#value,
      status: this.#hasValue ? "stale" : "connecting",
      connection: {
        ...this.#value.connection,
        connected: false,
        attempt,
        ...(this.#value.connection.connected ? { lastDisconnectedAt: now } : {}),
      },
    };
    this.#update(this.#waitingState());
  }

  retryAttempt(attempt: number, now = Date.now()): void {
    this.#value = {
      ...this.#value,
      status: this.#hasValue ? "reconnecting" : "connecting",
      connection: {
        ...this.#value.connection,
        connected: false,
        attempt,
        lastDisconnectedAt: now,
      },
    };
    this.#update(this.#waitingState());
  }

  apply(event: SubscriptionEvent<readonly RuntimeRow[]>): void {
    if (event.type === "snapshot") {
      this.#version = BigInt(event.meta.version);
      this.#hasValue = true;
      this.#value = {
        rows: event.rows,
        totalRows: event.meta.totalRows,
        status: "live",
        connection: {
          ...this.#value.connection,
          connected: true,
          lastConnectedAt: Date.now(),
        },
      };
      this.#update(this.#successState(false));
      return;
    }
    if (event.type === "status") {
      this.#hasValue = true;
      this.#value = {
        ...this.#value,
        totalRows: event.meta.totalRows,
        status: event.status,
        connection: {
          ...this.#value.connection,
          connected: true,
        },
      };
      this.#update(this.#waitingState());
      return;
    }

    const fromVersion = BigInt(event.meta.fromVersion);
    const toVersion = BigInt(event.meta.toVersion);
    if (this.#version !== undefined && this.#version !== fromVersion) {
      throw versionGap("client", this.#version, fromVersion);
    }
    this.#version = toVersion;
    this.#hasValue = true;
    this.#value = {
      rows:
        event.ops.length === 0
          ? this.#value.rows
          : applyDeltaOperations(this.#value.rows, event, this.#rowKey),
      totalRows: event.meta.totalRows,
      status: "live",
      connection: {
        ...this.#value.connection,
        connected: true,
        lastConnectedAt: Date.now(),
      },
    };
    this.#update(this.#successState(false));
  }

  #update(next: LiveQueryResult<RuntimeRow>): void {
    this.#state = next;
    for (const listener of this.#listeners) {
      listener(next);
    }
  }

  #waitingState(): LiveQueryResult<RuntimeRow> {
    return this.#hasValue ? this.#successState(true) : AsyncResult.initial(true);
  }

  #successState(waiting: boolean): LiveQueryResult<RuntimeRow> {
    return AsyncResult.success<LiveQueryValue<RuntimeRow>, ViewServerError>(this.#value, {
      waiting,
    });
  }

  #statusFromLifecycle(status: LiveQueryLifecycle): LiveQueryStatus {
    if (status === "live" && this.#value.connection.connected) {
      return "live";
    }
    if (this.#value.connection.attempt > 1) {
      return "reconnecting";
    }
    if (this.#hasValue) {
      return "stale";
    }
    return "connecting";
  }
}

export function applyDeltaOperations(
  rows: readonly RuntimeRow[],
  event: DeltaEvent<readonly RuntimeRow[]>,
  rowKeyOrIdField: RuntimeRowKeyFn | string = "id",
): readonly RuntimeRow[] {
  const rowKey =
    typeof rowKeyOrIdField === "string"
      ? (row: RuntimeRow) => rowKeyByField(row, rowKeyOrIdField)
      : rowKeyOrIdField;
  if (shouldUseIndexedDeltaApplication(rows.length, event.ops.length)) {
    const indexed = tryApplyIndexedDeltaOperations(rows, event, rowKey);
    if (indexed !== undefined) {
      return indexed;
    }
  }
  const next = rows.map((row) => ({ ...row }));
  for (const operation of event.ops) {
    applyDeltaOperationSequential(next, operation, rowKey);
  }
  return next;
}

function shouldUseIndexedDeltaApplication(rowCount: number, operationCount: number): boolean {
  return rowCount >= 500 && operationCount >= 64;
}

function tryApplyIndexedDeltaOperations(
  rows: readonly RuntimeRow[],
  event: DeltaEvent<readonly RuntimeRow[]>,
  rowKey: RuntimeRowKeyFn,
): readonly RuntimeRow[] | undefined {
  const originalRowsByKey = new Map<string | number, RuntimeRow>();
  for (const row of rows) {
    const key = rowKey(row);
    if (originalRowsByKey.has(key)) {
      return undefined;
    }
    originalRowsByKey.set(key, row);
  }

  const removedKeys = new Set<string | number>();
  const placedRowsByIndex = new Map<number, RuntimeRow>();
  const placedKeys = new Set<string | number>();
  let sawPlacement = false;
  for (const operation of event.ops) {
    if (operation.type === "remove") {
      if (sawPlacement) {
        return undefined;
      }
      removedKeys.add(operation.key);
      continue;
    }

    sawPlacement = true;
    if (operation.index === undefined || !Number.isFinite(operation.index)) {
      return undefined;
    }
    const index = Math.trunc(operation.index);
    if (index < 0 || placedRowsByIndex.has(index)) {
      return undefined;
    }
    const key =
      operation.type === "patch" ? operation.key : (operation.key ?? rowKey(operation.row));
    if (placedKeys.has(key)) {
      return undefined;
    }
    if (operation.type === "patch") {
      const previous = originalRowsByKey.get(operation.key);
      if (previous === undefined) {
        continue;
      }
      placedRowsByIndex.set(index, { ...previous, ...operation.changes });
    } else {
      placedRowsByIndex.set(index, operation.row);
    }
    placedKeys.add(key);
  }

  const remainingRows: RuntimeRow[] = [];
  let removedExistingCount = 0;
  for (const row of rows) {
    const key = rowKey(row);
    if (removedKeys.has(key)) {
      removedExistingCount++;
      continue;
    }
    if (!placedKeys.has(key)) {
      remainingRows.push({ ...row });
    }
  }
  let insertedCount = 0;
  for (const key of placedKeys) {
    if (!originalRowsByKey.has(key) || removedKeys.has(key)) {
      insertedCount++;
    }
  }
  const finalLength = rows.length - removedExistingCount + insertedCount;
  for (const index of placedRowsByIndex.keys()) {
    if (index >= finalLength) {
      return undefined;
    }
  }
  const next: RuntimeRow[] = [];
  let remainingIndex = 0;
  for (let index = 0; index < finalLength; index++) {
    const placed = placedRowsByIndex.get(index);
    if (placed !== undefined) {
      next.push(placed);
      continue;
    }
    const row = remainingRows[remainingIndex];
    if (row !== undefined) {
      next.push(row);
      remainingIndex++;
    }
  }
  return next;
}

function applyDeltaOperationSequential(
  rows: RuntimeRow[],
  operation: DeltaOperation<RuntimeRow>,
  rowKey: RuntimeRowKeyFn,
): void {
  if (operation.type === "remove") {
    const index = rows.findIndex((row) => rowKey(row) === operation.key);
    if (index >= 0) {
      rows.splice(index, 1);
    }
    return;
  }

  if (operation.type === "patch") {
    const index = rows.findIndex((row) => rowKey(row) === operation.key);
    if (index >= 0) {
      const patched = { ...rows[index], ...operation.changes };
      rows.splice(index, 1);
      rows.splice(normalizeIndex(operation.index, rows.length, index), 0, patched);
    }
    return;
  }

  const key = operation.key ?? rowKey(operation.row);
  const index = rows.findIndex((row) => rowKey(row) === key);
  if (index >= 0) {
    rows.splice(index, 1);
  }
  rows.splice(
    normalizeIndex(operation.index, rows.length, index >= 0 ? index : rows.length),
    0,
    operation.row,
  );
}

function normalizeIndex(index: number | undefined, length: number, fallback: number): number {
  if (index === undefined || !Number.isFinite(index)) {
    return Math.max(0, Math.min(length, fallback));
  }
  return Math.max(0, Math.min(length, Math.trunc(index)));
}
