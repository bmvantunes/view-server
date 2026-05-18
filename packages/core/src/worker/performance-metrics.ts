export type WorkerMutationTiming = {
  readonly batchSize: number;
  readonly totalMs: number;
  readonly gateWaitMs: number;
  readonly applyMemoryMs: number;
  readonly activeGroupedViewUpdateMs: number;
  readonly fanoutLoopMs: number;
  readonly deltaConstructionMs: number;
  readonly streamOfferMs: number;
  readonly subscriptionsTouched: number;
  readonly deltasGenerated: number;
  readonly statusGenerated: number;
  readonly snapshotsGenerated: number;
};

export type WorkerMutationTimingAccumulator = {
  readonly batchSize: number;
  readonly acceptedAtMs: number;
  gateWaitMs: number;
  applyMemoryMs: number;
  activeGroupedViewUpdateMs: number;
  fanoutLoopMs: number;
  deltaConstructionMs: number;
  streamOfferMs: number;
  subscriptionsTouched: number;
  deltasGenerated: number;
  statusGenerated: number;
  snapshotsGenerated: number;
};

export type WorkerPerformanceMetricsSnapshot = {
  readonly totalDeltasGenerated: number;
  readonly totalStatusGenerated: number;
  readonly totalSnapshotsGenerated: number;
  readonly maxGateWaitMs: number;
  readonly maxApplyMemoryMs: number;
  readonly maxActiveGroupedViewUpdateMs: number;
  readonly maxFanoutLoopMs: number;
  readonly maxDeltaConstructionMs: number;
  readonly maxStreamOfferMs: number;
  readonly maxSubscriptionsTouchedPerMutation: number;
  readonly lastMutationTiming?: WorkerMutationTiming | undefined;
};

export class WorkerPerformanceTracker {
  #totalDeltasGenerated = 0;
  #totalStatusGenerated = 0;
  #totalSnapshotsGenerated = 0;
  #maxGateWaitMs = 0;
  #maxApplyMemoryMs = 0;
  #maxActiveGroupedViewUpdateMs = 0;
  #maxFanoutLoopMs = 0;
  #maxDeltaConstructionMs = 0;
  #maxStreamOfferMs = 0;
  #maxSubscriptionsTouchedPerMutation = 0;
  #lastMutationTiming: WorkerMutationTiming | undefined;

  beginMutation(input: {
    readonly batchSize: number;
    readonly acceptedAtMs: number;
    readonly gateWaitMs: number;
  }): WorkerMutationTimingAccumulator {
    return {
      batchSize: input.batchSize,
      acceptedAtMs: input.acceptedAtMs,
      gateWaitMs: input.gateWaitMs,
      applyMemoryMs: 0,
      activeGroupedViewUpdateMs: 0,
      fanoutLoopMs: 0,
      deltaConstructionMs: 0,
      streamOfferMs: 0,
      subscriptionsTouched: 0,
      deltasGenerated: 0,
      statusGenerated: 0,
      snapshotsGenerated: 0,
    };
  }

  finishMutation(timing: WorkerMutationTimingAccumulator, finishedAtMs: number): void {
    const snapshot = {
      batchSize: timing.batchSize,
      totalMs: roundMetric(finishedAtMs - timing.acceptedAtMs),
      gateWaitMs: roundMetric(timing.gateWaitMs),
      applyMemoryMs: roundMetric(timing.applyMemoryMs),
      activeGroupedViewUpdateMs: roundMetric(timing.activeGroupedViewUpdateMs),
      fanoutLoopMs: roundMetric(timing.fanoutLoopMs),
      deltaConstructionMs: roundMetric(timing.deltaConstructionMs),
      streamOfferMs: roundMetric(timing.streamOfferMs),
      subscriptionsTouched: timing.subscriptionsTouched,
      deltasGenerated: timing.deltasGenerated,
      statusGenerated: timing.statusGenerated,
      snapshotsGenerated: timing.snapshotsGenerated,
    };
    this.#lastMutationTiming = snapshot;
    this.#maxGateWaitMs = Math.max(this.#maxGateWaitMs, snapshot.gateWaitMs);
    this.#maxApplyMemoryMs = Math.max(this.#maxApplyMemoryMs, snapshot.applyMemoryMs);
    this.#maxActiveGroupedViewUpdateMs = Math.max(
      this.#maxActiveGroupedViewUpdateMs,
      snapshot.activeGroupedViewUpdateMs,
    );
    this.#maxFanoutLoopMs = Math.max(this.#maxFanoutLoopMs, snapshot.fanoutLoopMs);
    this.#maxDeltaConstructionMs = Math.max(
      this.#maxDeltaConstructionMs,
      snapshot.deltaConstructionMs,
    );
    this.#maxStreamOfferMs = Math.max(this.#maxStreamOfferMs, snapshot.streamOfferMs);
    this.#maxSubscriptionsTouchedPerMutation = Math.max(
      this.#maxSubscriptionsTouchedPerMutation,
      snapshot.subscriptionsTouched,
    );
  }

  recordDeltaGenerated(timing: WorkerMutationTimingAccumulator | undefined): void {
    this.#totalDeltasGenerated += 1;
    if (timing !== undefined) {
      timing.deltasGenerated += 1;
    }
  }

  recordStatusGenerated(timing: WorkerMutationTimingAccumulator | undefined): void {
    this.#totalStatusGenerated += 1;
    if (timing !== undefined) {
      timing.statusGenerated += 1;
    }
  }

  recordSnapshotGenerated(timing: WorkerMutationTimingAccumulator | undefined): void {
    this.#totalSnapshotsGenerated += 1;
    if (timing !== undefined) {
      timing.snapshotsGenerated += 1;
    }
  }

  snapshot(): WorkerPerformanceMetricsSnapshot {
    return {
      totalDeltasGenerated: this.#totalDeltasGenerated,
      totalStatusGenerated: this.#totalStatusGenerated,
      totalSnapshotsGenerated: this.#totalSnapshotsGenerated,
      maxGateWaitMs: this.#maxGateWaitMs,
      maxApplyMemoryMs: this.#maxApplyMemoryMs,
      maxActiveGroupedViewUpdateMs: this.#maxActiveGroupedViewUpdateMs,
      maxFanoutLoopMs: this.#maxFanoutLoopMs,
      maxDeltaConstructionMs: this.#maxDeltaConstructionMs,
      maxStreamOfferMs: this.#maxStreamOfferMs,
      maxSubscriptionsTouchedPerMutation: this.#maxSubscriptionsTouchedPerMutation,
      ...(this.#lastMutationTiming === undefined
        ? {}
        : { lastMutationTiming: this.#lastMutationTiming }),
    };
  }
}

export function emptyWorkerPerformanceMetricsSnapshot(): WorkerPerformanceMetricsSnapshot {
  return {
    totalDeltasGenerated: 0,
    totalStatusGenerated: 0,
    totalSnapshotsGenerated: 0,
    maxGateWaitMs: 0,
    maxApplyMemoryMs: 0,
    maxActiveGroupedViewUpdateMs: 0,
    maxFanoutLoopMs: 0,
    maxDeltaConstructionMs: 0,
    maxStreamOfferMs: 0,
    maxSubscriptionsTouchedPerMutation: 0,
  };
}

export function addTiming(
  timing: WorkerMutationTimingAccumulator | undefined,
  field:
    | "applyMemoryMs"
    | "activeGroupedViewUpdateMs"
    | "fanoutLoopMs"
    | "deltaConstructionMs"
    | "streamOfferMs",
  startedAtMs: number,
  finishedAtMs: number,
): void {
  if (timing !== undefined) {
    timing[field] += finishedAtMs - startedAtMs;
  }
}

function roundMetric(value: number): number {
  return Math.round(value * 1_000) / 1_000;
}
