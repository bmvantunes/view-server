import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import {
  rowKeyByField,
  type RuntimeRow,
  type RuntimeRowKeyFn,
  type SubscriptionEvent,
} from "../protocol/index.ts";
import { versionGap, type ViewServerError } from "../errors.ts";
import { applyDeltaOperations, applySnapshot, applyStatus } from "./visible-rows.ts";

export { applyDeltaOperations } from "./visible-rows.ts";

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
      const snapshot = applySnapshot(event);
      this.#version = snapshot.version;
      this.#hasValue = true;
      this.#value = {
        rows: snapshot.rows,
        totalRows: snapshot.totalRows,
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
      const status = applyStatus(this.#value.rows, event);
      this.#hasValue = true;
      this.#value = {
        ...this.#value,
        rows: status.rows,
        totalRows: status.totalRows,
        status: status.status,
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
