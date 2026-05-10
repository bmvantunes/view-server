import {
  rowKeyByField,
  type DeltaEvent,
  type DeltaOperation,
  type RuntimeRow,
  type RuntimeRowKeyFn,
  type SubscriptionEvent,
  type SubscriptionStatus,
} from "../protocol/index.ts";
import { versionGap, type ViewServerError } from "../errors.ts";

export type SubscriptionState<TData extends readonly RuntimeRow[]> = {
  readonly data: TData;
  readonly totalRows: number;
  readonly status: SubscriptionStatus;
  readonly error?: ViewServerError | undefined;
};

export type SubscriptionListener<TData extends readonly RuntimeRow[]> = (
  state: SubscriptionState<TData>,
) => void;

export class SubscriptionStore<TData extends readonly RuntimeRow[]> {
  #state: SubscriptionState<TData>;
  #listeners = new Set<SubscriptionListener<TData>>();
  #version: bigint | undefined;
  #rowKey: RuntimeRowKeyFn;

  constructor(initialData: TData, rowKey: RuntimeRowKeyFn = (row) => rowKeyByField(row, "id")) {
    this.#state = {
      data: initialData,
      totalRows: initialData.length,
      status: "connecting",
    };
    this.#rowKey = rowKey;
  }

  get snapshot(): SubscriptionState<TData> {
    return this.#state;
  }

  subscribe(listener: SubscriptionListener<TData>): () => void {
    this.#listeners.add(listener);
    listener(this.#state);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  setStatus(status: SubscriptionStatus): void {
    this.#update({ ...this.#state, status });
  }

  setError(error: ViewServerError): void {
    this.#update({ ...this.#state, status: "error", error });
  }

  setRowKey(rowKey: RuntimeRowKeyFn): void {
    this.#rowKey = rowKey;
  }

  apply(event: SubscriptionEvent<readonly RuntimeRow[]>): void {
    if (event.type === "snapshot") {
      this.#version = BigInt(event.meta.version);
      this.#update({
        data: event.rows as TData,
        totalRows: event.meta.totalRows,
        status: "live",
        error: undefined,
      });
      return;
    }

    const fromVersion = BigInt(event.meta.fromVersion);
    const toVersion = BigInt(event.meta.toVersion);
    if (this.#version !== undefined && this.#version !== fromVersion) {
      throw versionGap("client", this.#version, fromVersion);
    }
    this.#version = toVersion;
    this.#update({
      data:
        event.ops.length === 0
          ? this.#state.data
          : (applyDeltaOperations(this.#state.data, event, this.#rowKey) as TData),
      totalRows: event.meta.totalRows,
      status: "live",
      error: undefined,
    });
  }

  #update(next: SubscriptionState<TData>): void {
    this.#state = next;
    for (const listener of this.#listeners) {
      listener(next);
    }
  }
}

export function applyDeltaOperations<TData extends readonly RuntimeRow[]>(
  rows: TData,
  event: DeltaEvent<readonly RuntimeRow[]>,
  rowKeyOrIdField: RuntimeRowKeyFn | string = "id",
): readonly RuntimeRow[] {
  const rowKey =
    typeof rowKeyOrIdField === "string"
      ? (row: RuntimeRow) => rowKeyByField(row, rowKeyOrIdField)
      : rowKeyOrIdField;
  const next = rows.map((row) => ({ ...row }));
  for (const operation of event.ops) {
    applyDeltaOperation(next, operation, rowKey);
  }
  return next;
}

function applyDeltaOperation(
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
