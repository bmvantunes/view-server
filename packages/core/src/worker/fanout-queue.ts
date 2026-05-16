import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import type * as Cause from "effect/Cause";
import type { ViewServerError } from "../errors.ts";
import type {
  DeltaEvent,
  LiveQueryStatusEvent,
  RuntimeRow,
  SnapshotEvent,
  SubscriptionEvent,
} from "../protocol/index.ts";

export type FanoutQueueState = {
  pendingLagVersions: bigint;
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
      const queued = yield* drainQueuedEvents(queue);
      const queuedPrefix = queued.filter((queuedEvent) => queuedEvent.type !== "delta");
      const queuedDeltas = queued.filter((queuedEvent) => queuedEvent.type === "delta");
      const nextQueued = coalescedQueueEvents(queuedPrefix, queuedDeltas, event);
      if (
        nextQueued.length > options.maxQueueDepth ||
        (queuedDeltas.length === 0 && wouldExceedQueueLimit(queuedPrefix.length))
      ) {
        yield* offerQueuedEvents(queue, queued);
        return false;
      }
      const coalesced = nextQueued[nextQueued.length - 1];
      if (
        coalesced?.type === "delta" &&
        options.maxQueueDepth > 0 &&
        deltaVersionSpan(coalesced) > BigInt(options.maxQueueDepth)
      ) {
        yield* offerQueuedEvents(queue, queued);
        return false;
      }
      yield* offerQueuedEvents(queue, nextQueued);
      state.pendingLagVersions = queueEventsVersionLag(nextQueued);
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
      state.pendingLagVersions = queueEventsVersionLag(nextQueued);
      return true;
    }),

    wouldExceedQueueLimit,
    isQueueAtLimit,
    lagForDepth: (queueDepth, pendingLagVersions) =>
      subscriptionLagVersionsForQueueDepth(queueDepth, pendingLagVersions, options.deltaCoalescing),
  };
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
