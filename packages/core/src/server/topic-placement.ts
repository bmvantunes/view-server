import * as Effect from "effect/Effect";
import type { NormalizedViewServerConfig, TopicConfig } from "../config/index.ts";
import { VIEW_SERVER_HEALTH_TOPIC } from "../config/index.ts";
import { snapshotBackendFailed, type ViewServerError } from "../errors.ts";
import type { RuntimeRow } from "../protocol/index.ts";
import { createMemorySnapshotBackend, type SnapshotBackend } from "../snapshot/snapshot-backend.ts";
import {
  makeInProcessTopicWorkerHost,
  type TopicWorkerHost,
  type TopicWorkerHostFactory,
} from "../worker/index.ts";

export type TopicPlacementOptions = {
  readonly initialRows?: Readonly<Record<string, readonly RuntimeRow[]>> | undefined;
  readonly topicWorkerFactory?: TopicWorkerHostFactory | undefined;
  /** @internal Test-only backend injection for fault and fallback coverage. */
  readonly __testingSnapshotBackends?: Readonly<Record<string, SnapshotBackend>> | undefined;
  /** @internal Test-only backend factory for fault and fallback coverage. */
  readonly __testingSnapshotBackendFactory?:
    | ((topic: string, config: TopicConfig) => SnapshotBackend)
    | undefined;
  /** @internal Browser/package tests only. Production runtime must use chDB. */
  readonly __testingUseMemorySnapshotBackend?: boolean | undefined;
};

export type TopicPlacement = {
  readonly topic: string;
  readonly config: TopicConfig;
  readonly worker: TopicWorkerHost;
  readonly snapshotBackend: SnapshotBackend | undefined;
};

export type TopicPlacementSet = {
  readonly placements: readonly TopicPlacement[];
  readonly workers: ReadonlyMap<string, TopicWorkerHost>;
};

export function createTopicPlacements(
  config: NormalizedViewServerConfig,
  options: TopicPlacementOptions,
): Effect.Effect<TopicPlacementSet, ViewServerError, import("effect/Scope").Scope> {
  return Effect.fn("view-server.topic_placement.create")(function* () {
    const makeTopicWorker = options.topicWorkerFactory ?? makeInProcessTopicWorkerHost;
    const placements: TopicPlacement[] = [];
    const workers = new Map<string, TopicWorkerHost>();

    for (const [topic, topicConfig] of Object.entries(config.topics)) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": topic,
      });
      const snapshotBackend = yield* shouldResolveSnapshotBackend(options)
        ? resolveSnapshotBackend(topic, topicConfig, options)
        : Effect.succeed(undefined);
      const worker = yield* makeTopicWorker(topic, topicConfig, {
        initialRows: options.initialRows?.[topic],
        snapshotBackend,
        maxQueueDepth: config.worker.maxQueueDepth,
        mutationLogSize: config.worker.mutationLogSize,
        deltaCoalescing: config.worker.deltaCoalescing,
        maxActivePlans: config.worker.maxActivePlans,
        maxActivePlanEstimatedBytes: config.worker.maxActivePlanEstimatedBytes,
        activePlanAutoBuildMaxRows: config.worker.activePlanAutoBuildMaxRows,
        activePlanBuildConcurrency: config.worker.activePlanBuildConcurrency,
        groupedRefreshDebounceMs: config.worker.groupedRefreshDebounceMs,
      });
      workers.set(topic, worker);
      placements.push({
        topic,
        config: topicConfig,
        worker,
        snapshotBackend,
      });
    }

    return {
      placements,
      workers,
    };
  })();
}

function resolveSnapshotBackend(
  topic: string,
  topicConfig: TopicConfig,
  options: TopicPlacementOptions,
): Effect.Effect<SnapshotBackend, ViewServerError> {
  if (topic === VIEW_SERVER_HEALTH_TOPIC) {
    return Effect.succeed(createMemorySnapshotBackend());
  }
  if (options.__testingUseMemorySnapshotBackend === true) {
    return Effect.succeed(createMemorySnapshotBackend());
  }
  const injected = options.__testingSnapshotBackends?.[topic];
  if (injected !== undefined) {
    return Effect.succeed(injected);
  }
  if (options.__testingSnapshotBackendFactory !== undefined) {
    return Effect.succeed(options.__testingSnapshotBackendFactory(topic, topicConfig));
  }
  return Effect.tryPromise({
    try: async () => {
      const { createChdbSnapshotBackend } = await import("../snapshot/chdb-backend.ts");
      return createChdbSnapshotBackend();
    },
    catch: (error) => snapshotBackendFailed(topic, error),
  });
}

function shouldResolveSnapshotBackend(options: TopicPlacementOptions): boolean {
  return (
    options.topicWorkerFactory === undefined ||
    options.__testingUseMemorySnapshotBackend === true ||
    options.__testingSnapshotBackends !== undefined ||
    options.__testingSnapshotBackendFactory !== undefined
  );
}
