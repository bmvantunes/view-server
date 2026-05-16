import {
  rowKeyByField,
  type RuntimeRow,
  type RuntimeRowKeyFn,
  type SubscriptionEvent,
} from "../protocol/index.ts";
import { versionGap, type ViewServerError } from "../errors.ts";
import {
  initialLiveQueryLifecycle,
  transitionBeginAttempt,
  transitionDelta,
  transitionError,
  transitionLifecycleStatus,
  transitionRetryAttempt,
  transitionSnapshot,
  transitionStatusEvent,
  type LiveQueryConnection,
  type LiveQueryInitialData,
  type LiveQueryLifecycle,
  type LiveQueryLifecycleState,
  type LiveQueryResult,
  type LiveQueryStatus,
  type LiveQueryValue,
} from "./live-query-lifecycle.ts";
import { applyDeltaOperations, applySnapshot, applyStatus } from "./visible-rows.ts";

export { applyDeltaOperations } from "./visible-rows.ts";
export type {
  LiveQueryConnection,
  LiveQueryInitialData,
  LiveQueryLifecycle,
  LiveQueryResult,
  LiveQueryStatus,
  LiveQueryValue,
} from "./live-query-lifecycle.ts";

export type LiveQueryListener = (state: LiveQueryResult<RuntimeRow>) => void;

export class LiveQueryStore {
  #state: LiveQueryResult<RuntimeRow>;
  #lifecycle: LiveQueryLifecycleState<RuntimeRow>;
  #listeners = new Set<LiveQueryListener>();
  #version: bigint | undefined;
  #rowKey: RuntimeRowKeyFn;

  constructor(
    initialData: LiveQueryInitialData<RuntimeRow> | undefined,
    rowKey: RuntimeRowKeyFn = (row) => rowKeyByField(row, "id"),
  ) {
    const initial = initialLiveQueryLifecycle(initialData);
    this.#lifecycle = initial.lifecycle;
    this.#state = initial.result;
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
    this.#applyTransition(transitionLifecycleStatus(this.#lifecycle, status));
  }

  setError(error: ViewServerError): void {
    this.#applyTransition(transitionError(this.#lifecycle, error));
  }

  setRowKey(rowKey: RuntimeRowKeyFn): void {
    this.#rowKey = rowKey;
  }

  beginAttempt(attempt: number, now = Date.now()): void {
    this.#applyTransition(transitionBeginAttempt(this.#lifecycle, attempt, now));
  }

  retryAttempt(attempt: number, now = Date.now()): void {
    this.#applyTransition(transitionRetryAttempt(this.#lifecycle, attempt, now));
  }

  apply(event: SubscriptionEvent<readonly RuntimeRow[]>): void {
    if (event.type === "snapshot") {
      const snapshot = applySnapshot(event);
      this.#version = snapshot.version;
      this.#applyTransition(transitionSnapshot(this.#lifecycle, snapshot.rows, snapshot.totalRows));
      return;
    }
    if (event.type === "status") {
      const status = applyStatus(this.#lifecycle.value.rows, event);
      this.#applyTransition(
        transitionStatusEvent(this.#lifecycle, status.rows, status.totalRows, status.status),
      );
      return;
    }

    const fromVersion = BigInt(event.meta.fromVersion);
    const toVersion = BigInt(event.meta.toVersion);
    if (this.#version !== undefined && this.#version !== fromVersion) {
      throw versionGap("client", this.#version, fromVersion);
    }
    this.#version = toVersion;
    this.#applyTransition(
      transitionDelta(
        this.#lifecycle,
        event.ops.length === 0
          ? this.#lifecycle.value.rows
          : applyDeltaOperations(this.#lifecycle.value.rows, event, this.#rowKey),
        event.meta.totalRows,
      ),
    );
  }

  #applyTransition(transition: {
    readonly lifecycle: LiveQueryLifecycleState<RuntimeRow>;
    readonly result: LiveQueryResult<RuntimeRow>;
  }): void {
    this.#lifecycle = transition.lifecycle;
    this.#update(transition.result);
  }

  #update(next: LiveQueryResult<RuntimeRow>): void {
    this.#state = next;
    for (const listener of this.#listeners) {
      listener(next);
    }
  }
}
