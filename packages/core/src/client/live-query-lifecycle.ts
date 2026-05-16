import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import type { ViewServerError } from "../errors.ts";

export type LiveQueryStatus = "connecting" | "live" | "reconnecting" | "stale";

export type LiveQueryLifecycle = "connecting" | "syncing" | "live" | "closed";

export type LiveQueryInitialData<TRow> = {
  readonly rows: readonly TRow[];
  readonly totalRows: number;
};

export type LiveQueryConnection = {
  readonly connected: boolean;
  readonly attempt: number;
  readonly lastConnectedAt?: number | undefined;
  readonly lastDisconnectedAt?: number | undefined;
};

export type LiveQueryValue<TRow> = LiveQueryInitialData<TRow> & {
  readonly status: LiveQueryStatus;
  readonly connection: LiveQueryConnection;
};

export type LiveQueryResult<TRow> = AsyncResult.AsyncResult<LiveQueryValue<TRow>, ViewServerError>;

export type LiveQueryLifecycleState<TRow> = {
  readonly value: LiveQueryValue<TRow>;
  readonly hasValue: boolean;
};

export type LiveQueryTransition<TRow> = {
  readonly lifecycle: LiveQueryLifecycleState<TRow>;
  readonly result: LiveQueryResult<TRow>;
};

export function initialLiveQueryLifecycle<TRow>(
  initialData: LiveQueryInitialData<TRow> | undefined,
): LiveQueryTransition<TRow> {
  const hasValue = initialData !== undefined;
  const lifecycle: LiveQueryLifecycleState<TRow> = {
    hasValue,
    value: {
      rows: initialData?.rows ?? [],
      totalRows: initialData?.totalRows ?? 0,
      status: hasValue ? "stale" : "connecting",
      connection: {
        connected: false,
        attempt: 0,
      },
    },
  };
  return {
    lifecycle,
    result: waitingResult(lifecycle),
  };
}

export function transitionLifecycleStatus<TRow>(
  lifecycle: LiveQueryLifecycleState<TRow>,
  status: LiveQueryLifecycle,
  now = Date.now(),
): LiveQueryTransition<TRow> {
  const next: LiveQueryLifecycleState<TRow> = {
    ...lifecycle,
    value: {
      ...lifecycle.value,
      status: statusFromLifecycle(lifecycle, status),
      connection:
        status === "closed"
          ? {
              ...lifecycle.value.connection,
              connected: false,
              lastDisconnectedAt: now,
            }
          : lifecycle.value.connection,
    },
  };
  return {
    lifecycle: next,
    result: status === "live" ? successResult(next, false) : waitingResult(next),
  };
}

export function transitionError<TRow>(
  lifecycle: LiveQueryLifecycleState<TRow>,
  error: ViewServerError,
  now = Date.now(),
): LiveQueryTransition<TRow> {
  const next: LiveQueryLifecycleState<TRow> = {
    ...lifecycle,
    value: {
      ...lifecycle.value,
      status: lifecycle.value.connection.attempt > 1 ? "reconnecting" : "stale",
      connection: {
        ...lifecycle.value.connection,
        connected: false,
        lastDisconnectedAt: now,
      },
    },
  };
  return {
    lifecycle: next,
    result: next.hasValue
      ? AsyncResult.failWithPrevious<LiveQueryValue<TRow>, ViewServerError>(error, {
          previous: Option.some(
            AsyncResult.success<LiveQueryValue<TRow>, ViewServerError>(next.value, {
              waiting: true,
            }),
          ),
          waiting: true,
        })
      : AsyncResult.fail<ViewServerError, LiveQueryValue<TRow>>(error),
  };
}

export function transitionBeginAttempt<TRow>(
  lifecycle: LiveQueryLifecycleState<TRow>,
  attempt: number,
  now = Date.now(),
): LiveQueryTransition<TRow> {
  const next: LiveQueryLifecycleState<TRow> = {
    ...lifecycle,
    value: {
      ...lifecycle.value,
      status: lifecycle.hasValue ? "stale" : "connecting",
      connection: {
        ...lifecycle.value.connection,
        connected: false,
        attempt,
        ...(lifecycle.value.connection.connected ? { lastDisconnectedAt: now } : {}),
      },
    },
  };
  return {
    lifecycle: next,
    result: waitingResult(next),
  };
}

export function transitionRetryAttempt<TRow>(
  lifecycle: LiveQueryLifecycleState<TRow>,
  attempt: number,
  now = Date.now(),
): LiveQueryTransition<TRow> {
  const next: LiveQueryLifecycleState<TRow> = {
    ...lifecycle,
    value: {
      ...lifecycle.value,
      status: lifecycle.hasValue ? "reconnecting" : "connecting",
      connection: {
        ...lifecycle.value.connection,
        connected: false,
        attempt,
        lastDisconnectedAt: now,
      },
    },
  };
  return {
    lifecycle: next,
    result: waitingResult(next),
  };
}

export function transitionSnapshot<TRow>(
  lifecycle: LiveQueryLifecycleState<TRow>,
  rows: readonly TRow[],
  totalRows: number,
  now = Date.now(),
): LiveQueryTransition<TRow> {
  const next: LiveQueryLifecycleState<TRow> = {
    hasValue: true,
    value: {
      rows,
      totalRows,
      status: "live" as const,
      connection: {
        ...lifecycle.value.connection,
        connected: true,
        lastConnectedAt: now,
      },
    },
  };
  return {
    lifecycle: next,
    result: successResult(next, false),
  };
}

export function transitionStatusEvent<TRow>(
  lifecycle: LiveQueryLifecycleState<TRow>,
  rows: readonly TRow[],
  totalRows: number,
  status: LiveQueryStatus,
): LiveQueryTransition<TRow> {
  const next: LiveQueryLifecycleState<TRow> = {
    hasValue: true,
    value: {
      ...lifecycle.value,
      rows,
      totalRows,
      status,
      connection: {
        ...lifecycle.value.connection,
        connected: true,
      },
    },
  };
  return {
    lifecycle: next,
    result: waitingResult(next),
  };
}

export function transitionDelta<TRow>(
  lifecycle: LiveQueryLifecycleState<TRow>,
  rows: readonly TRow[],
  totalRows: number,
  now = Date.now(),
): LiveQueryTransition<TRow> {
  const next: LiveQueryLifecycleState<TRow> = {
    hasValue: true,
    value: {
      rows,
      totalRows,
      status: "live" as const,
      connection: {
        ...lifecycle.value.connection,
        connected: true,
        lastConnectedAt: now,
      },
    },
  };
  return {
    lifecycle: next,
    result: successResult(next, false),
  };
}

export function waitingResult<TRow>(
  lifecycle: LiveQueryLifecycleState<TRow>,
): LiveQueryResult<TRow> {
  return lifecycle.hasValue ? successResult(lifecycle, true) : AsyncResult.initial(true);
}

export function successResult<TRow>(
  lifecycle: LiveQueryLifecycleState<TRow>,
  waiting: boolean,
): LiveQueryResult<TRow> {
  return AsyncResult.success<LiveQueryValue<TRow>, ViewServerError>(lifecycle.value, {
    waiting,
  });
}

function statusFromLifecycle<TRow>(
  lifecycle: LiveQueryLifecycleState<TRow>,
  status: LiveQueryLifecycle,
): LiveQueryStatus {
  if (status === "live" && lifecycle.value.connection.connected) {
    return "live";
  }
  if (lifecycle.value.connection.attempt > 1) {
    return "reconnecting";
  }
  if (lifecycle.hasValue) {
    return "stale";
  }
  return "connecting";
}
