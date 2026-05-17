import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import type * as Cause from "effect/Cause";
import type { ViewServerError } from "../errors.ts";
import type {
  DeltaEvent,
  DeltaOperation,
  LiveQueryStatusEvent,
  RuntimeRow,
  SnapshotEvent,
  SubscriptionEvent,
} from "../protocol/index.ts";

export type FanoutQueueState = {
  pendingLagVersions: bigint;
  pendingDeltaEvent?: MutableDeltaEvent | undefined;
  pendingDeltaOps?: DeltaOperation<RuntimeRow>[] | undefined;
};

export type FanoutQueue = {
  readonly offerDelta: (
    queue: SubscriptionEventQueue,
    state: FanoutQueueState,
    event: DeltaEvent<readonly RuntimeRow[]>,
  ) => Effect.Effect<boolean, ViewServerError>;
  readonly offerStatus: (
    queue: SubscriptionEventQueue,
    state: FanoutQueueState,
    event: LiveQueryStatusEvent,
  ) => Effect.Effect<boolean, ViewServerError>;
  readonly offerSnapshot: (
    queue: SubscriptionEventQueue,
    state: FanoutQueueState,
    event: SnapshotEvent<readonly RuntimeRow[]>,
  ) => Effect.Effect<boolean, ViewServerError>;
  readonly wouldExceedQueueLimit: (depth: number) => boolean;
  readonly isQueueAtLimit: (depth: number) => boolean;
  readonly lagForDepth: (queueDepth: number, pendingLagVersions: bigint) => bigint;
};

export type SubscriptionEventQueue = Queue.Queue<
  SubscriptionEvent<readonly RuntimeRow[]>,
  ViewServerError | Cause.Done
>;

type MutableDeltaEvent = {
  type: "delta";
  requestId: string;
  ops: DeltaOperation<RuntimeRow>[];
  meta: {
    fromVersion: string;
    toVersion: string;
    totalRows: number;
    serverTime: number;
  };
};

export function makeFanoutQueue(options: {
  readonly maxQueueDepth: number;
  readonly deltaCoalescing: boolean;
}): FanoutQueue {
  const wouldExceedQueueLimit = (depth: number): boolean =>
    options.maxQueueDepth <= 0 ? depth >= 0 : depth >= options.maxQueueDepth;

  const isQueueAtLimit = (depth: number): boolean =>
    options.maxQueueDepth <= 0 ? depth > 0 : depth >= options.maxQueueDepth;

  return {
    offerDelta: Effect.fnUntraced(function* (queue, state, event) {
      if (!options.deltaCoalescing) {
        const depth = yield* Queue.size(queue);
        if (wouldExceedQueueLimit(depth)) {
          return false;
        }
        yield* Queue.offer(queue, event);
        return true;
      }
      const depth = yield* Queue.size(queue);
      if (depth === 0) {
        clearPendingDelta(state);
      }
      if (state.pendingDeltaEvent !== undefined && state.pendingDeltaOps !== undefined) {
        const nextLag = state.pendingLagVersions + deltaVersionSpan(event);
        if (options.maxQueueDepth > 0 && nextLag > BigInt(options.maxQueueDepth)) {
          return false;
        }
        state.pendingDeltaOps.push(...event.ops);
        state.pendingDeltaEvent.requestId = event.requestId;
        state.pendingDeltaEvent.meta.toVersion = event.meta.toVersion;
        state.pendingDeltaEvent.meta.totalRows = event.meta.totalRows;
        state.pendingDeltaEvent.meta.serverTime = event.meta.serverTime;
        state.pendingLagVersions = nextLag;
        return true;
      }
      if (wouldExceedQueueLimit(depth)) {
        return false;
      }
      if (options.maxQueueDepth > 0 && deltaVersionSpan(event) > BigInt(options.maxQueueDepth)) {
        return false;
      }
      const pendingDelta = mutableDeltaEvent(event);
      yield* Queue.offer(queue, pendingDelta);
      state.pendingDeltaEvent = pendingDelta;
      state.pendingDeltaOps = pendingDelta.ops;
      state.pendingLagVersions = deltaVersionSpan(pendingDelta);
      return true;
    }),

    offerStatus: Effect.fnUntraced(function* (queue, state, event) {
      const queued = yield* drainQueuedEvents(queue);
      const nextQueued = [...queued.filter((queuedEvent) => queuedEvent.type !== "status"), event];
      if (nextQueued.length > options.maxQueueDepth) {
        yield* offerQueuedEvents(queue, queued);
        return false;
      }
      yield* offerQueuedEvents(queue, nextQueued);
      clearPendingDelta(state);
      state.pendingLagVersions = queueEventsVersionLag(nextQueued);
      return true;
    }),

    offerSnapshot: Effect.fnUntraced(function* (queue, state, event) {
      const queued = yield* drainQueuedEvents(queue);
      const nextQueued = [...queued.filter((queuedEvent) => queuedEvent.type !== "status"), event];
      if (nextQueued.length > options.maxQueueDepth) {
        yield* offerQueuedEvents(queue, queued);
        return false;
      }
      yield* offerQueuedEvents(queue, nextQueued);
      clearPendingDelta(state);
      state.pendingLagVersions = queueEventsVersionLag(nextQueued);
      return true;
    }),

    wouldExceedQueueLimit,
    isQueueAtLimit,
    lagForDepth: (queueDepth, pendingLagVersions) =>
      subscriptionLagVersionsForQueueDepth(queueDepth, pendingLagVersions, options.deltaCoalescing),
  };
}

function mutableDeltaEvent(event: DeltaEvent<readonly RuntimeRow[]>): MutableDeltaEvent {
  return {
    type: "delta",
    requestId: event.requestId,
    ops: [...event.ops],
    meta: {
      fromVersion: event.meta.fromVersion,
      toVersion: event.meta.toVersion,
      totalRows: event.meta.totalRows,
      serverTime: event.meta.serverTime,
    },
  };
}

function clearPendingDelta(state: FanoutQueueState): void {
  state.pendingDeltaEvent = undefined;
  state.pendingDeltaOps = undefined;
  state.pendingLagVersions = 0n;
}

export function coalescedQueueEvents(
  prefix: readonly SubscriptionEvent<readonly RuntimeRow[]>[],
  queuedDeltas: readonly DeltaEvent<readonly RuntimeRow[]>[],
  nextDelta: DeltaEvent<readonly RuntimeRow[]>,
): readonly SubscriptionEvent<readonly RuntimeRow[]>[] {
  return [...prefix, coalesceDeltas([...queuedDeltas, nextDelta])];
}

export function coalesceDeltas(
  deltas: readonly DeltaEvent<readonly RuntimeRow[]>[],
): DeltaEvent<readonly RuntimeRow[]> {
  const first = deltas[0];
  const last = deltas[deltas.length - 1];
  if (first === undefined || last === undefined) {
    throw new Error("Cannot coalesce an empty delta list");
  }
  return {
    type: "delta",
    requestId: last.requestId,
    ops: deltas.flatMap((delta) => delta.ops),
    meta: {
      fromVersion: first.meta.fromVersion,
      toVersion: last.meta.toVersion,
      totalRows: last.meta.totalRows,
      serverTime: last.meta.serverTime,
    },
  };
}

export function deltaVersionSpan(delta: DeltaEvent<readonly RuntimeRow[]>): bigint {
  return BigInt(delta.meta.toVersion) - BigInt(delta.meta.fromVersion);
}

export function queueEventsVersionLag(
  events: readonly SubscriptionEvent<readonly RuntimeRow[]>[],
): bigint {
  return events.reduce(
    (total, event) => (event.type === "delta" ? total + deltaVersionSpan(event) : total),
    0n,
  );
}

export function subscriptionLagVersionsForQueueDepth(
  queueDepth: number,
  pendingLagVersions: bigint,
  deltaCoalescing: boolean,
): bigint {
  if (queueDepth <= 0) {
    return 0n;
  }
  return deltaCoalescing ? pendingLagVersions : BigInt(queueDepth);
}

const drainQueuedEvents = Effect.fnUntraced(function* (queue: SubscriptionEventQueue) {
  const events: SubscriptionEvent<readonly RuntimeRow[]>[] = [];
  let polling = true;
  while (polling) {
    const next = yield* Queue.poll(queue);
    if (Option.isSome(next)) {
      events.push(next.value);
    } else {
      polling = false;
    }
  }
  return events;
});

const offerQueuedEvents = Effect.fnUntraced(function* (
  queue: SubscriptionEventQueue,
  events: readonly SubscriptionEvent<readonly RuntimeRow[]>[],
) {
  yield* Effect.forEach(events, (event) => Queue.offer(queue, event), { discard: true });
});
