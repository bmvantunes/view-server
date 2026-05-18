export type BenchmarkProfileName =
  | "ci-smoke"
  | "firehose-ci"
  | "websocket-fanout"
  | "dev-fast"
  | "rc-1m"
  | "soak-10m"
  | "grouped-heavy"
  | "active-plan-startup"
  | "chdb-worker-overlap";

export type BenchmarkProfileCwd = "package" | "repo";

export type BenchmarkProfileBenchmark = {
  readonly name: string;
  readonly description: string;
  readonly artifactFile: string;
  readonly metrics?: string | undefined;
  readonly baselineFile?: string | undefined;
  readonly blocking?: boolean | undefined;
  readonly script?: string | undefined;
  readonly command?: readonly string[] | undefined;
  readonly cwd?: BenchmarkProfileCwd | undefined;
  readonly env: Readonly<Record<string, string>>;
};

export type BenchmarkProfile = {
  readonly name: BenchmarkProfileName;
  readonly description: string;
  readonly ciSafe: boolean;
  readonly coverageGaps: readonly string[];
  readonly benchmarks: readonly BenchmarkProfileBenchmark[];
};

export const benchmarkProfileNames: readonly BenchmarkProfileName[] = [
  "ci-smoke",
  "firehose-ci",
  "websocket-fanout",
  "dev-fast",
  "rc-1m",
  "soak-10m",
  "grouped-heavy",
  "active-plan-startup",
  "chdb-worker-overlap",
];

const runtimeWebsocketAttributionMetrics = [
  "mutationP50Ms",
  "mutationP95Ms",
  "mutationP99Ms",
  "mutationMaxMs",
  "retryCount",
  "backpressureCount",
  "cleanupLeakCount",
  "maxQueueDepthObserved",
  "maxSubscriptionLagVersionsObserved",
  "maxChdbPendingRequestsObserved",
  "maxChdbBackendLagVersionsObserved",
  "workerMaxGateWaitMs",
  "workerMaxApplyMemoryMs",
  "workerMaxActiveGroupedViewUpdateMs",
  "workerMaxFanoutLoopMs",
  "workerMaxDeltaConstructionMs",
  "workerMaxStreamOfferMs",
  "workerMaxSubscriptionsTouchedPerMutation",
  "workerTotalDeltasGenerated",
  "workerTotalStatusGenerated",
  "workerTotalSnapshotsGenerated",
  "maxQueueDepthAfterCleanup",
  "maxSubscriptionLagVersionsAfterCleanup",
  "chdbPendingRequestsAfterCleanup",
  "chdbBackendLagVersionsAfterCleanup",
  "chdbNotReadyAfterCleanupCount",
  "websocketActiveClientsAfterCleanup",
  "websocketTotalBytes",
  "websocketTotalEncodeMs",
  "websocketTotalWriteMs",
  "websocketTotalProtocolOfferMs",
  "websocketTotalProtocolQueueWaitMs",
  "websocketMaxClientQueuedMessages",
  "websocketMaxClientQueuedBytes",
  "websocketMaxBatchMessages",
  "websocketMaxBatchBytes",
  "websocketMaxEncodeMs",
  "websocketMaxWriteMs",
  "websocketMaxProtocolOfferMs",
  "websocketMaxProtocolQueueWaitMs",
  "transportActiveClientsAfterCleanup",
  "transportTotalMessages",
  "transportTotalBytes",
  "transportTotalEncodeMs",
  "transportTotalWriteMs",
  "transportTotalProtocolQueueWaitMs",
  "transportMaxQueueDepthMessages",
  "transportMaxQueueDepthBytes",
  "transportMaxProtocolQueueWaitMs",
  "transportEventLoopDelayP95Ms",
  "transportEventLoopDelayP99Ms",
  "transportEventLoopDelayMaxMs",
  "eventLoopDelayP95Ms",
  "eventLoopDelayP99Ms",
  "eventLoopDelayMaxMs",
  "snapshotPayloadBytes",
  "deltaPayloadBytes",
  "statusPayloadBytes",
  "topSlowSampleCount",
].join(",");

export const benchmarkProfiles: Readonly<Record<BenchmarkProfileName, BenchmarkProfile>> = {
  "ci-smoke": {
    name: "ci-smoke",
    description: "Tiny reporting-only benchmark shapes for PR and push visibility.",
    ciSafe: true,
    coverageGaps: [
      "Active-plan responsiveness can complete before a build overlaps at smoke scale; large local benchmarks remain the source of truth.",
      "Grouped refresh smoke uses small row counts; 1M/10M chDB grouped overlap remains manual/nightly evidence.",
      "Runtime websocket soak smoke blocks only cleanup/retry/backpressure invariants; the 100-client profile remains report-only.",
    ],
    benchmarks: [
      {
        name: "active-view",
        description: "Shared active raw plan smoke with one hot-key update shape.",
        script: "bench/active-view.bench.ts",
        artifactFile: "active-view.json",
        baselineFile: "active-view.json",
        blocking: true,
        metrics: "activeBuildMs,activeUpdateMs",
        env: {
          VS_ACTIVE_VIEW_ROWS: "1000",
          VS_ACTIVE_VIEW_SUBSCRIPTIONS: "5",
          VS_ACTIVE_VIEW_MUTATIONS: "5",
          VS_ACTIVE_VIEW_BASELINE: "0",
          VS_ACTIVE_VIEW_SCENARIOS: "hot-key-updates",
          VS_ACTIVE_VIEW_PAGE_SIZES: "50",
          VS_ACTIVE_VIEW_SHARING: "shared",
        },
      },
      {
        name: "active-plan-responsiveness",
        description: "Publish latency smoke while an active plan may be building.",
        script: "bench/active-plan-responsiveness.bench.ts",
        artifactFile: "active-plan-responsiveness.json",
        baselineFile: "active-plan-responsiveness.json",
        blocking: false,
        metrics: "operationP99Ms,metricsP99Ms",
        env: {
          VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "1000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "3",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "publish",
        },
      },
      {
        name: "grouped-responsiveness",
        description: "Grouped subscription stale/debounce mutation responsiveness smoke.",
        script: "bench/grouped-responsiveness.bench.ts",
        artifactFile: "grouped-responsiveness.json",
        baselineFile: "grouped-responsiveness.json",
        blocking: true,
        metrics: "operationP99Ms,metricsP99Ms",
        env: {
          VS_GROUPED_RESPONSIVENESS_ROWS: "1000",
          VS_GROUPED_RESPONSIVENESS_OPERATIONS: "3",
          VS_GROUPED_RESPONSIVENESS_AGGREGATES: "3",
        },
      },
      {
        name: "grouped-refresh-overlap",
        description: "Grouped refresh overlap smoke with tiny memory-backed refresh.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap.json",
        baselineFile: "grouped-refresh-overlap.json",
        blocking: true,
        metrics: "operationP99Ms,startGapMaxMs",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "1000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "3",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "3",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "memory",
        },
      },
      {
        name: "runtime-websocket-soak",
        description: "Real websocket runtime soak invariant smoke with chDB and reconnects.",
        script: "bench/runtime-websocket-soak.bench.ts",
        artifactFile: "runtime-websocket-soak.json",
        baselineFile: "runtime-websocket-soak.json",
        blocking: true,
        metrics:
          "cleanupLeakCount,retryCount,backpressureCount,maxQueueDepthAfterCleanup,maxSubscriptionLagVersionsAfterCleanup,chdbPendingRequestsAfterCleanup,chdbBackendLagVersionsAfterCleanup,chdbNotReadyAfterCleanupCount,websocketActiveClientsAfterCleanup",
        env: {
          VS_RUNTIME_WEBSOCKET_SOAK_ROWS: "1000",
          VS_RUNTIME_WEBSOCKET_SOAK_RAW_CLIENTS: "12",
          VS_RUNTIME_WEBSOCKET_SOAK_GROUPED_CLIENTS: "3",
          VS_RUNTIME_WEBSOCKET_SOAK_MUTATIONS: "120",
          VS_RUNTIME_WEBSOCKET_SOAK_RECONNECT_CLIENTS: "10",
          VS_RUNTIME_WEBSOCKET_SOAK_TIMEOUT_MS: "60000",
          VS_RUNTIME_WEBSOCKET_SOAK_HEALTH_SAMPLE_INTERVAL: "10",
          VS_RUNTIME_WEBSOCKET_TRANSPORT_MODE: "in-process",
        },
      },
    ],
  },
  "firehose-ci": {
    name: "firehose-ci",
    description:
      "CI-visible report-only firehose thresholds for batching, chDB apply, fanout, and the 1M alpha soak.",
    ciSafe: true,
    coverageGaps: [
      "Report-only by policy: regressions should be investigated but should not block PRs.",
      "The 1M alpha soak uses the direct worker harness with the snapshot accelerator disabled; production chDB is covered by the chDB apply benchmark.",
      "The 10M raw soak is intentionally manual/nightly and is not run in CI.",
      "The runtime websocket 100-client profile is transport-real but hardware-sensitive, so it warns only.",
    ],
    benchmarks: [
      {
        name: "worker-mutation-batch",
        description: "Worker mutateBatch latency versus single-row worker publish calls.",
        script: "bench/worker-mutation-batch.bench.ts",
        artifactFile: "worker-mutation-batch.json",
        baselineFile: "worker-mutation-batch.json",
        blocking: false,
        metrics: "batchedMs",
        env: {
          VS_WORKER_MUTATION_BATCH_SIZES: "1000,10000",
          VS_WORKER_MUTATION_BATCH_ITERATIONS: "1",
        },
      },
      {
        name: "chdb-apply-batch",
        description: "chDB SQL mirror batched apply throughput versus legacy one-mutation loop.",
        script: "bench/chdb-sql-mirror.bench.ts",
        artifactFile: "chdb-apply-batch.json",
        baselineFile: "chdb-apply-batch.json",
        blocking: false,
        metrics: "applyMutationsMs",
        env: {
          VS_CHDB_SQL_MIRROR_ROWS: "10000",
          VS_CHDB_SQL_MIRROR_COLUMNS: "25",
          VS_CHDB_SQL_MIRROR_MUTATIONS: "1000",
          VS_CHDB_SQL_MIRROR_COMPARE_LEGACY: "1",
        },
      },
      {
        name: "fanout-slow-client",
        description: "Slow-client delta coalescing without queue drain/refill.",
        script: "bench/fanout-queue.bench.ts",
        artifactFile: "fanout-slow-client.json",
        baselineFile: "fanout-slow-client.json",
        blocking: false,
        metrics: "offerMs",
        env: {
          VS_FANOUT_QUEUE_DELTA_COUNTS: "10000",
          VS_FANOUT_QUEUE_OPS_PER_DELTA: "1",
          VS_FANOUT_QUEUE_MAX_DEPTH: "100000",
          VS_FANOUT_QUEUE_COMPARE_LEGACY: "1",
        },
      },
      {
        name: "worker-soak-alpha-1m",
        description: "1M alpha raw+grouped direct worker soak summary artifact.",
        script: "bench/worker-soak.bench.ts",
        artifactFile: "worker-soak-alpha-1m.json",
        baselineFile: "worker-soak-alpha-1m.json",
        blocking: false,
        metrics: "durationMs,subscriptionSetupMs,mutationLoopMs,mutationP99Ms,cleanupLeakCount",
        env: {
          VS_WORKER_SOAK_SCENARIO: "alpha-1m",
          VS_WORKER_SOAK_ROWS: "1000000",
          VS_WORKER_SOAK_RAW_SUBSCRIPTIONS: "250",
          VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS: "20",
          VS_WORKER_SOAK_MUTATIONS: "10000",
          VS_WORKER_SOAK_MUTATION_BATCH_SIZE: "1000",
          VS_WORKER_SOAK_TIMEOUT_MS: "900000",
        },
      },
      {
        name: "runtime-websocket-soak-100-client",
        description:
          "Real websocket 100-client runtime soak with chDB, reconnects, and tail attribution.",
        script: "bench/runtime-websocket-soak.bench.ts",
        artifactFile: "runtime-websocket-soak-100-client.json",
        baselineFile: "runtime-websocket-soak-100-client.json",
        blocking: false,
        metrics: runtimeWebsocketAttributionMetrics,
        env: {
          VS_RUNTIME_WEBSOCKET_SOAK_ROWS: "10000",
          VS_RUNTIME_WEBSOCKET_SOAK_RAW_CLIENTS: "80",
          VS_RUNTIME_WEBSOCKET_SOAK_GROUPED_CLIENTS: "20",
          VS_RUNTIME_WEBSOCKET_SOAK_MUTATIONS: "1000",
          VS_RUNTIME_WEBSOCKET_SOAK_RECONNECT_CLIENTS: "50",
          VS_RUNTIME_WEBSOCKET_SOAK_TIMEOUT_MS: "120000",
          VS_RUNTIME_WEBSOCKET_SOAK_HEALTH_SAMPLE_INTERVAL: "25",
          VS_RUNTIME_WEBSOCKET_TRANSPORT_MODE: "in-process",
        },
      },
    ],
  },
  "websocket-fanout": {
    name: "websocket-fanout",
    description:
      "Manual websocket fanout and serialization profile for 100-client and 250-client transport loads.",
    ciSafe: false,
    coverageGaps: [
      "Hardware-sensitive; use this to explain websocket/event-loop tail latency on the target machine.",
      "The runtime keeps Effect RPC over NDJSON; compact/binary encodings are benchmark follow-ups, not current behavior.",
      "Payload byte metrics in this profile are application-event JSON approximations; websocketTotalBytes is the encoded batched RPC wire byte count.",
    ],
    benchmarks: [
      {
        name: "runtime-websocket-soak-100-client",
        description:
          "Real websocket 100-client runtime soak with chDB, reconnects, payload bytes, and fanout attribution.",
        script: "bench/runtime-websocket-soak.bench.ts",
        artifactFile: "runtime-websocket-soak-100-client.json",
        metrics: runtimeWebsocketAttributionMetrics,
        env: {
          VS_RUNTIME_WEBSOCKET_SOAK_ROWS: "10000",
          VS_RUNTIME_WEBSOCKET_SOAK_RAW_CLIENTS: "80",
          VS_RUNTIME_WEBSOCKET_SOAK_GROUPED_CLIENTS: "20",
          VS_RUNTIME_WEBSOCKET_SOAK_MUTATIONS: "1000",
          VS_RUNTIME_WEBSOCKET_SOAK_RECONNECT_CLIENTS: "50",
          VS_RUNTIME_WEBSOCKET_SOAK_TIMEOUT_MS: "120000",
          VS_RUNTIME_WEBSOCKET_SOAK_HEALTH_SAMPLE_INTERVAL: "25",
          VS_RUNTIME_WEBSOCKET_TRANSPORT_MODE: "in-process",
        },
      },
      {
        name: "runtime-websocket-soak-250-client",
        description:
          "Real websocket 250-client runtime soak with chDB, reconnects, payload bytes, and fanout attribution.",
        script: "bench/runtime-websocket-soak.bench.ts",
        artifactFile: "runtime-websocket-soak-250-client.json",
        metrics: runtimeWebsocketAttributionMetrics,
        env: {
          VS_RUNTIME_WEBSOCKET_SOAK_ROWS: "10000",
          VS_RUNTIME_WEBSOCKET_SOAK_RAW_CLIENTS: "200",
          VS_RUNTIME_WEBSOCKET_SOAK_GROUPED_CLIENTS: "50",
          VS_RUNTIME_WEBSOCKET_SOAK_MUTATIONS: "1000",
          VS_RUNTIME_WEBSOCKET_SOAK_RECONNECT_CLIENTS: "50",
          VS_RUNTIME_WEBSOCKET_SOAK_TIMEOUT_MS: "180000",
          VS_RUNTIME_WEBSOCKET_SOAK_HEALTH_SAMPLE_INTERVAL: "25",
          VS_RUNTIME_WEBSOCKET_TRANSPORT_MODE: "in-process",
        },
      },
      {
        name: "runtime-websocket-soak-100-client-isolated",
        description:
          "Real websocket 100-client runtime soak with isolated transport worker and fanout attribution.",
        script: "bench/runtime-websocket-soak.bench.ts",
        artifactFile: "runtime-websocket-soak-100-client-isolated.json",
        metrics: runtimeWebsocketAttributionMetrics,
        env: {
          VS_RUNTIME_WEBSOCKET_SOAK_ROWS: "10000",
          VS_RUNTIME_WEBSOCKET_SOAK_RAW_CLIENTS: "80",
          VS_RUNTIME_WEBSOCKET_SOAK_GROUPED_CLIENTS: "20",
          VS_RUNTIME_WEBSOCKET_SOAK_MUTATIONS: "1000",
          VS_RUNTIME_WEBSOCKET_SOAK_RECONNECT_CLIENTS: "50",
          VS_RUNTIME_WEBSOCKET_SOAK_TIMEOUT_MS: "120000",
          VS_RUNTIME_WEBSOCKET_SOAK_HEALTH_SAMPLE_INTERVAL: "25",
          VS_RUNTIME_WEBSOCKET_TRANSPORT_MODE: "isolated",
        },
      },
      {
        name: "runtime-websocket-soak-250-client-isolated",
        description:
          "Real websocket 250-client runtime soak with isolated transport worker and fanout attribution.",
        script: "bench/runtime-websocket-soak.bench.ts",
        artifactFile: "runtime-websocket-soak-250-client-isolated.json",
        metrics: runtimeWebsocketAttributionMetrics,
        env: {
          VS_RUNTIME_WEBSOCKET_SOAK_ROWS: "10000",
          VS_RUNTIME_WEBSOCKET_SOAK_RAW_CLIENTS: "200",
          VS_RUNTIME_WEBSOCKET_SOAK_GROUPED_CLIENTS: "50",
          VS_RUNTIME_WEBSOCKET_SOAK_MUTATIONS: "1000",
          VS_RUNTIME_WEBSOCKET_SOAK_RECONNECT_CLIENTS: "50",
          VS_RUNTIME_WEBSOCKET_SOAK_TIMEOUT_MS: "180000",
          VS_RUNTIME_WEBSOCKET_SOAK_HEALTH_SAMPLE_INTERVAL: "25",
          VS_RUNTIME_WEBSOCKET_TRANSPORT_MODE: "isolated",
        },
      },
      {
        name: "runtime-websocket-soak-500-client-isolated",
        description:
          "Manual 500-client runtime soak for isolated transport tail-latency exploration.",
        script: "bench/runtime-websocket-soak.bench.ts",
        artifactFile: "runtime-websocket-soak-500-client-isolated.json",
        metrics: runtimeWebsocketAttributionMetrics,
        env: {
          VS_RUNTIME_WEBSOCKET_SOAK_ROWS: "10000",
          VS_RUNTIME_WEBSOCKET_SOAK_RAW_CLIENTS: "400",
          VS_RUNTIME_WEBSOCKET_SOAK_GROUPED_CLIENTS: "100",
          VS_RUNTIME_WEBSOCKET_SOAK_MUTATIONS: "1000",
          VS_RUNTIME_WEBSOCKET_SOAK_RECONNECT_CLIENTS: "50",
          VS_RUNTIME_WEBSOCKET_SOAK_TIMEOUT_MS: "240000",
          VS_RUNTIME_WEBSOCKET_SOAK_HEALTH_SAMPLE_INTERVAL: "25",
          VS_RUNTIME_WEBSOCKET_TRANSPORT_MODE: "isolated",
        },
      },
    ],
  },
  "dev-fast": {
    name: "dev-fast",
    description: "Local quick checks large enough to catch obvious active/grouped regressions.",
    ciSafe: false,
    coverageGaps: [
      "Does not replace the 1M release-candidate profile.",
      "Uses reduced subscription and mutation counts to stay interactive on laptops.",
    ],
    benchmarks: [
      {
        name: "active-view-shared-100k",
        description: "100k-row shared active-plan updates with hot-key and sorted movement.",
        script: "bench/active-view.bench.ts",
        artifactFile: "active-view-shared-100k.json",
        metrics: "activeBuildMs,activeUpdateMs",
        env: {
          VS_ACTIVE_VIEW_ROWS: "100000",
          VS_ACTIVE_VIEW_SUBSCRIPTIONS: "50",
          VS_ACTIVE_VIEW_MUTATIONS: "100",
          VS_ACTIVE_VIEW_BASELINE: "0",
          VS_ACTIVE_VIEW_SCENARIOS: "hot-key-updates,sorted-row-movement",
          VS_ACTIVE_VIEW_PAGE_SIZES: "50,100",
          VS_ACTIVE_VIEW_SHARING: "shared",
          VS_ACTIVE_VIEW_QUERY_SHAPE: "mixed",
        },
      },
      {
        name: "active-plan-responsiveness-100k",
        description: "100k-row active-plan build overlap responsiveness.",
        script: "bench/active-plan-responsiveness.bench.ts",
        artifactFile: "active-plan-responsiveness-100k.json",
        metrics: "operationP99Ms,operationMaxMs,metricsP99Ms",
        env: {
          VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "100000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "100",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "publish",
        },
      },
      {
        name: "grouped-refresh-overlap-100k",
        description: "100k-row grouped refresh overlap with chDB backend.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap-100k.json",
        metrics: "operationP99Ms,startGapMaxMs,metricsP99Ms",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "100000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "100",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "25",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "chdb",
        },
      },
    ],
  },
  "rc-1m": {
    name: "rc-1m",
    description: "Manual release-candidate 1M-row responsiveness and memory profile.",
    ciSafe: false,
    coverageGaps: [
      "Hardware-sensitive; compare artifacts across the same machine class.",
      "Raw active plan startup is measured separately from mutation hot-path latency.",
    ],
    benchmarks: [
      {
        name: "active-plan-publish-1m",
        description: "1M-row publish responsiveness during active-plan build.",
        script: "bench/active-plan-responsiveness.bench.ts",
        artifactFile: "active-plan-publish-1m.json",
        metrics: "operationP99Ms,operationMaxMs,metricsP99Ms",
        env: {
          VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "1000000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "1000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "publish",
        },
      },
      {
        name: "active-plan-delta-1m",
        description: "1M-row deltaPublish responsiveness during active-plan build.",
        script: "bench/active-plan-responsiveness.bench.ts",
        artifactFile: "active-plan-delta-1m.json",
        metrics: "operationP99Ms,operationMaxMs,metricsP99Ms",
        env: {
          VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "1000000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "1000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "deltaPublish",
        },
      },
      {
        name: "active-plan-delete-1m",
        description: "1M-row deleteById responsiveness during active-plan build.",
        script: "bench/active-plan-responsiveness.bench.ts",
        artifactFile: "active-plan-delete-1m.json",
        metrics: "operationP99Ms,operationMaxMs,metricsP99Ms",
        env: {
          VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "1000000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "1000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "deleteById",
        },
      },
      {
        name: "grouped-refresh-overlap-1m",
        description: "1M-row grouped chDB refresh overlap with 100 aggregates.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap-1m.json",
        metrics: "operationP99Ms,startGapMaxMs,metricsP99Ms",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "1000000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "200",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "100",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "chdb",
        },
      },
    ],
  },
  "soak-10m": {
    name: "soak-10m",
    description: "Manual/nightly 10M raw worker soak using active-plan admission policy.",
    ciSafe: false,
    coverageGaps: [
      "Not a PR or normal release gate.",
      "Grouped subscriptions default to zero; grouped 10M capacity belongs to chDB grouped refresh benchmarks.",
    ],
    benchmarks: [
      {
        name: "worker-soak-10m",
        description: "10M raw worker soak with progress JSONL and summary artifacts.",
        script: "bench/worker-soak.bench.ts",
        artifactFile: "worker-soak-10m.json",
        baselineFile: "worker-soak-10m.json",
        blocking: false,
        metrics: "durationMs,subscriptionSetupMs,mutationLoopMs,mutationP99Ms,cleanupLeakCount",
        env: {
          VS_WORKER_SOAK_SCENARIO: "raw-10m",
          VS_WORKER_SOAK_ROWS: "10000000",
          VS_WORKER_SOAK_RAW_SUBSCRIPTIONS: "250",
          VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS: "0",
          VS_WORKER_SOAK_MUTATIONS: "10000",
          VS_WORKER_SOAK_MUTATION_BATCH_SIZE: "1000",
          VS_WORKER_SOAK_ACTIVE_PLAN_AUTO_BUILD_MAX_ROWS: "1000000",
          VS_WORKER_SOAK_TIMEOUT_MS: "7200000",
          VS_WORKER_SOAK_PROGRESS_INTERVAL_MS: "60000",
          NODE_OPTIONS: "--expose-gc --max-old-space-size=24576",
        },
      },
    ],
  },
  "grouped-heavy": {
    name: "grouped-heavy",
    description: "Manual grouped-query stress profile for accumulator and chDB refresh work.",
    ciSafe: false,
    coverageGaps: [
      "Grouped accumulator is not the default runtime path.",
      "Use chDB overlap results to judge production grouped refresh responsiveness.",
    ],
    benchmarks: [
      {
        name: "grouped-aggregation-1m",
        description: "1M-row grouped accumulator build/apply comparison.",
        script: "bench/grouped-aggregation.bench.ts",
        artifactFile: "grouped-aggregation-1m.json",
        metrics: "groupedSnapshotMs,accumulatorBuildMs,accumulatorApplyMs,fullRecomputeMs",
        env: {
          VS_GROUPED_AGGREGATION_ROWS: "1000000",
          VS_GROUPED_AGGREGATION_GROUPS: "1000",
          VS_GROUPED_AGGREGATION_AGGREGATES: "100",
          VS_GROUPED_AGGREGATION_MUTATIONS: "10000",
          VS_GROUPED_AGGREGATION_ITERATIONS: "1",
        },
      },
      {
        name: "grouped-refresh-overlap-10m",
        description: "10M-row grouped chDB refresh overlap with 100 aggregates.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap-10m.json",
        metrics: "operationP99Ms,startGapMaxMs,metricsP99Ms",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "10000000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "20",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "100",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "chdb",
        },
      },
    ],
  },
  "active-plan-startup": {
    name: "active-plan-startup",
    description: "Manual active-plan build/startup profile for shared-plan policy tuning.",
    ciSafe: false,
    coverageGaps: [
      "Measures startup/build cost, not mutation hot-path latency.",
      "Use memory flags with --expose-gc for retained heap evidence.",
    ],
    benchmarks: [
      {
        name: "active-view-build-same-plan-1m",
        description: "1M-row build-only shared same-plan active-view profile.",
        script: "bench/active-view.bench.ts",
        artifactFile: "active-view-build-same-plan-1m.json",
        metrics: "activeBuildMs,activePlanCount,activeIndexBytes,activeHeapDelta",
        env: {
          VS_ACTIVE_VIEW_ROWS: "1000000",
          VS_ACTIVE_VIEW_SUBSCRIPTIONS: "250",
          VS_ACTIVE_VIEW_MUTATIONS: "0",
          VS_ACTIVE_VIEW_BASELINE: "0",
          VS_ACTIVE_VIEW_SCENARIOS: "hot-key-updates",
          VS_ACTIVE_VIEW_PAGE_SIZES: "50,100",
          VS_ACTIVE_VIEW_SHARING: "shared",
          VS_ACTIVE_VIEW_QUERY_SHAPE: "same-plan",
          VS_ACTIVE_VIEW_MEMORY: "1",
        },
      },
      {
        name: "active-view-build-ten-plans-1m",
        description: "1M-row build-only shared ten-plan active-view profile.",
        script: "bench/active-view.bench.ts",
        artifactFile: "active-view-build-ten-plans-1m.json",
        metrics: "activeBuildMs,activePlanCount,activeIndexBytes,activeHeapDelta",
        env: {
          VS_ACTIVE_VIEW_ROWS: "1000000",
          VS_ACTIVE_VIEW_SUBSCRIPTIONS: "250",
          VS_ACTIVE_VIEW_MUTATIONS: "0",
          VS_ACTIVE_VIEW_BASELINE: "0",
          VS_ACTIVE_VIEW_SCENARIOS: "hot-key-updates",
          VS_ACTIVE_VIEW_PAGE_SIZES: "50,100",
          VS_ACTIVE_VIEW_SHARING: "shared",
          VS_ACTIVE_VIEW_QUERY_SHAPE: "ten-plans",
          VS_ACTIVE_VIEW_MEMORY: "1",
        },
      },
      {
        name: "active-view-build-guarded-unique-1m",
        description: "1M-row build-only unique-plan profile with maxActivePlans guard.",
        script: "bench/active-view.bench.ts",
        artifactFile: "active-view-build-guarded-unique-1m.json",
        metrics:
          "activeBuildMs,activePlanCount,activeFallbackCount,activeIndexBytes,activeHeapDelta",
        env: {
          VS_ACTIVE_VIEW_ROWS: "1000000",
          VS_ACTIVE_VIEW_SUBSCRIPTIONS: "250",
          VS_ACTIVE_VIEW_MUTATIONS: "0",
          VS_ACTIVE_VIEW_BASELINE: "0",
          VS_ACTIVE_VIEW_SCENARIOS: "hot-key-updates",
          VS_ACTIVE_VIEW_PAGE_SIZES: "50,100",
          VS_ACTIVE_VIEW_SHARING: "shared",
          VS_ACTIVE_VIEW_QUERY_SHAPE: "unique-plans",
          VS_ACTIVE_VIEW_MAX_ACTIVE_PLANS: "10",
          VS_ACTIVE_VIEW_MEMORY: "1",
        },
      },
    ],
  },
  "chdb-worker-overlap": {
    name: "chdb-worker-overlap",
    description: "Manual chDB worker-isolated grouped refresh overlap profile.",
    ciSafe: false,
    coverageGaps: [
      "Does not exercise websocket clients; it targets topic worker scheduling during chDB grouped refresh.",
      "Run beside deployment smoke for full transport confidence.",
    ],
    benchmarks: [
      {
        name: "grouped-refresh-overlap-chdb-1m",
        description: "1M-row chDB grouped refresh overlap profile.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap-chdb-1m.json",
        metrics: "operationP99Ms,startGapMaxMs,metricsP99Ms",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "1000000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "200",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "100",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "chdb",
        },
      },
      {
        name: "grouped-refresh-overlap-chdb-10m",
        description: "10M-row chDB grouped refresh overlap profile.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap-chdb-10m.json",
        metrics: "operationP99Ms,startGapMaxMs,metricsP99Ms",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "10000000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "20",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "100",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "chdb",
        },
      },
    ],
  },
};

export function listBenchmarkProfiles(): readonly BenchmarkProfile[] {
  return benchmarkProfileNames.map((name) => benchmarkProfiles[name]);
}

export function getBenchmarkProfile(name: string): BenchmarkProfile | undefined {
  switch (name) {
    case "ci-smoke":
      return benchmarkProfiles["ci-smoke"];
    case "firehose-ci":
      return benchmarkProfiles["firehose-ci"];
    case "websocket-fanout":
      return benchmarkProfiles["websocket-fanout"];
    case "dev-fast":
      return benchmarkProfiles["dev-fast"];
    case "rc-1m":
      return benchmarkProfiles["rc-1m"];
    case "soak-10m":
      return benchmarkProfiles["soak-10m"];
    case "grouped-heavy":
      return benchmarkProfiles["grouped-heavy"];
    case "active-plan-startup":
      return benchmarkProfiles["active-plan-startup"];
    case "chdb-worker-overlap":
      return benchmarkProfiles["chdb-worker-overlap"];
    default:
      return undefined;
  }
}

export function benchmarkProfileCoverageGapsEnv(profile: BenchmarkProfile): string {
  return profile.coverageGaps.join("\n");
}
