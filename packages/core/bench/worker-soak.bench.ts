import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkPrimitive,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";

type SoakShape = {
  readonly rows: number;
  readonly rawSubscriptions: number;
  readonly groupedSubscriptions: number;
  readonly mutations: number;
  readonly mutationBatchSize: number;
  readonly rawPageCycle: number;
  readonly groupedRefreshDebounceMs: number;
  readonly activePlanAutoBuildMaxRows: number;
};

type SoakLatencyStats = {
  readonly count: number;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly p99Ms: number;
  readonly maxMs: number;
};

type SoakEvents = {
  readonly snapshots: number;
  readonly deltas: number;
  readonly status: number;
};

type SoakSummary = {
  readonly shape: SoakShape;
  readonly durationMs: number;
  readonly rowGenerationMs: number;
  readonly workerSeedMs: number;
  readonly subscriptionSetupMs: number;
  readonly mutationLoopMs: number;
  readonly mutationBatchSize: number;
  readonly mutationBatches: number;
  readonly mutationAndSettleMs: number;
  readonly settleMs: number;
  readonly cleanupMs: number;
  readonly mutationLatencyMs: SoakLatencyStats;
  readonly finalRows: number;
  readonly finalVersion: string;
  readonly subscribersBeforeCleanup: number;
  readonly subscribersAfterCleanup: number;
  readonly activePlanCountBeforeCleanup: number;
  readonly activeViewCountBeforeCleanup: number;
  readonly activePlanFallbackCountBeforeCleanup: number;
  readonly activePlanAutoBuildSkippedCountBeforeCleanup: number;
  readonly activePlanCountAfterCleanup: number;
  readonly activeViewCountAfterCleanup: number;
  readonly activePlanFallbackCountAfterCleanup: number;
  readonly activePlanAutoBuildSkippedCountAfterCleanup: number;
  readonly activePlanBuildQueueDepthAfterCleanup: number;
  readonly activePlanBuildingCountAfterCleanup: number;
  readonly activePlanPendingCountAfterCleanup: number;
  readonly activePlanIndexEstimatedBytesAfterCleanup: number;
  readonly maxSubscriptionLagVersionsAfterSettle: number;
  readonly totalSubscriptionLagVersionsAfterSettle: number;
  readonly maxSubscriptionLagVersionsAfterCleanup: number;
  readonly totalSubscriptionLagVersionsAfterCleanup: number;
  readonly queueDepthAfterSettle: number;
  readonly queueDepthAfterCleanup: number;
  readonly heapBaselineBytes: number;
  readonly heapSubscribedBytes: number;
  readonly heapLoadedBytes: number;
  readonly heapReleasedBytes: number;
  readonly rssBaselineBytes: number;
  readonly rssLoadedBytes: number;
  readonly rssReleasedBytes: number;
  readonly heapGrowthRatio: number;
  readonly gcAvailable: boolean;
  readonly events: SoakEvents;
  readonly retries: number;
  readonly backpressureErrors: number;
  readonly reconnects: number;
};

class WorkerSoakBenchmarkError extends Schema.TaggedErrorClass<WorkerSoakBenchmarkError>()(
  "WorkerSoakBenchmarkError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

void Effect.runPromise(
  Effect.fn("view-server.bench.worker_soak")(function* () {
    const summaryPath = workerSoakSummaryPath();
    yield* Effect.logInfo(`worker-soak benchmark summaryPath=${summaryPath}`);
    yield* runWorkerSoak(summaryPath);
    const summary = yield* readWorkerSoakSummary(summaryPath);
    const artifact = yield* writeBenchmarkArtifact(
      "worker-soak",
      workerSoakConfig(summary),
      [workerSoakResult(summary)],
      {
        notes: [
          "Wraps the direct topic-worker soak test and converts its JSON summary into a benchmark artifact.",
          "This is a worker-level capacity signal; production runtime still requires chDB and real transport smoke coverage.",
          `progress artifact: ${displayPath(workerSoakProgressPath(summaryPath))}`,
        ],
      },
    );
    yield* Effect.logInfo(
      [
        "worker-soak benchmark result",
        `scenario=${workerSoakScenario()}`,
        `rows=${summary.shape.rows}`,
        `rawSubscriptions=${summary.shape.rawSubscriptions}`,
        `groupedSubscriptions=${summary.shape.groupedSubscriptions}`,
        `mutations=${summary.shape.mutations}`,
        `durationMs=${summary.durationMs.toFixed(2)}`,
        `subscriptionSetupMs=${summary.subscriptionSetupMs.toFixed(2)}`,
        `mutationLoopMs=${summary.mutationLoopMs.toFixed(2)}`,
        `mutationP99Ms=${summary.mutationLatencyMs.p99Ms.toFixed(2)}`,
        `cleanupLeakCount=${cleanupLeakCount(summary)}`,
        `artifact=${artifact.artifactPath}`,
        `baselineCompared=${artifact.compared}`,
      ].join(" "),
    );
  })(),
);

function runWorkerSoak(summaryPath: string): Effect.Effect<void, WorkerSoakBenchmarkError> {
  return Effect.fn("view-server.bench.worker_soak.run_vitest")(function* () {
    const vitestEntry = resolve(packageRoot, "node_modules/.bin/vitest");
    const env = {
      ...process.env,
      VS_WORKER_SOAK_SUMMARY_PATH: summaryPath,
      VS_WORKER_SOAK_PROGRESS_PATH:
        process.env.VS_WORKER_SOAK_PROGRESS_PATH ?? workerSoakProgressPath(summaryPath),
    };
    yield* spawnCommand(
      [vitestEntry, "run", "--config", "vitest.config.ts", "tests/worker-soak.test.ts"],
      packageRoot,
      env,
    );
  })();
}

function spawnCommand(
  command: readonly string[],
  cwd: string,
  env: Readonly<Record<string, string | undefined>>,
): Effect.Effect<void, WorkerSoakBenchmarkError> {
  return Effect.callback<void, WorkerSoakBenchmarkError>((resume) => {
    const [executable, ...args] = command;
    if (executable === undefined) {
      resume(
        Effect.fail(
          new WorkerSoakBenchmarkError({
            message: "Worker soak benchmark has no executable",
          }),
        ),
      );
      return Effect.void;
    }
    let completed = false;
    const child = spawn(executable, args, {
      cwd,
      env,
      stdio: "inherit",
    });
    child.on("error", (cause) => {
      if (completed) {
        return;
      }
      completed = true;
      resume(
        Effect.fail(
          new WorkerSoakBenchmarkError({
            message: `Failed to start worker soak benchmark: ${String(cause)}`,
            cause,
          }),
        ),
      );
    });
    child.on("exit", (code, signal) => {
      if (completed) {
        return;
      }
      completed = true;
      if (code === 0) {
        resume(Effect.void);
        return;
      }
      resume(
        Effect.fail(
          new WorkerSoakBenchmarkError({
            message: `Worker soak benchmark failed with code ${String(code)} signal ${String(signal)}`,
          }),
        ),
      );
    });
    return Effect.sync(() => {
      child.kill("SIGTERM");
    });
  });
}

function readWorkerSoakSummary(path: string): Effect.Effect<SoakSummary, WorkerSoakBenchmarkError> {
  return Effect.tryPromise({
    try: () => readFile(path, "utf8"),
    catch: (cause) =>
      new WorkerSoakBenchmarkError({
        message: `Failed to read worker soak summary ${path}: ${String(cause)}`,
        cause,
      }),
  }).pipe(
    Effect.flatMap((json) =>
      Effect.try({
        try: () => decodeWorkerSoakSummary(JSON.parse(json)),
        catch: (cause) =>
          new WorkerSoakBenchmarkError({
            message: `Invalid worker soak summary ${path}: ${String(cause)}`,
            cause,
          }),
      }),
    ),
  );
}

function decodeWorkerSoakSummary(value: unknown): SoakSummary {
  const record = recordField({ summary: value }, "summary");
  const shape = recordField(record, "shape");
  const mutationLatencyMs = recordField(record, "mutationLatencyMs");
  const events = recordField(record, "events");
  return {
    shape: {
      rows: numberField(shape, "rows"),
      rawSubscriptions: numberField(shape, "rawSubscriptions"),
      groupedSubscriptions: numberField(shape, "groupedSubscriptions"),
      mutations: numberField(shape, "mutations"),
      mutationBatchSize: numberField(shape, "mutationBatchSize"),
      rawPageCycle: numberField(shape, "rawPageCycle"),
      groupedRefreshDebounceMs: numberField(shape, "groupedRefreshDebounceMs"),
      activePlanAutoBuildMaxRows: numberField(shape, "activePlanAutoBuildMaxRows"),
    },
    durationMs: numberField(record, "durationMs"),
    rowGenerationMs: numberField(record, "rowGenerationMs"),
    workerSeedMs: numberField(record, "workerSeedMs"),
    subscriptionSetupMs: numberField(record, "subscriptionSetupMs"),
    mutationLoopMs: numberField(record, "mutationLoopMs"),
    mutationBatchSize: numberField(record, "mutationBatchSize"),
    mutationBatches: numberField(record, "mutationBatches"),
    mutationAndSettleMs: numberField(record, "mutationAndSettleMs"),
    settleMs: numberField(record, "settleMs"),
    cleanupMs: numberField(record, "cleanupMs"),
    mutationLatencyMs: {
      count: numberField(mutationLatencyMs, "count"),
      p50Ms: numberField(mutationLatencyMs, "p50Ms"),
      p95Ms: numberField(mutationLatencyMs, "p95Ms"),
      p99Ms: numberField(mutationLatencyMs, "p99Ms"),
      maxMs: numberField(mutationLatencyMs, "maxMs"),
    },
    finalRows: numberField(record, "finalRows"),
    finalVersion: stringField(record, "finalVersion"),
    subscribersBeforeCleanup: numberField(record, "subscribersBeforeCleanup"),
    subscribersAfterCleanup: numberField(record, "subscribersAfterCleanup"),
    activePlanCountBeforeCleanup: numberField(record, "activePlanCountBeforeCleanup"),
    activeViewCountBeforeCleanup: numberField(record, "activeViewCountBeforeCleanup"),
    activePlanFallbackCountBeforeCleanup: numberField(
      record,
      "activePlanFallbackCountBeforeCleanup",
    ),
    activePlanAutoBuildSkippedCountBeforeCleanup: numberField(
      record,
      "activePlanAutoBuildSkippedCountBeforeCleanup",
    ),
    activePlanCountAfterCleanup: numberField(record, "activePlanCountAfterCleanup"),
    activeViewCountAfterCleanup: numberField(record, "activeViewCountAfterCleanup"),
    activePlanFallbackCountAfterCleanup: numberField(record, "activePlanFallbackCountAfterCleanup"),
    activePlanAutoBuildSkippedCountAfterCleanup: numberField(
      record,
      "activePlanAutoBuildSkippedCountAfterCleanup",
    ),
    activePlanBuildQueueDepthAfterCleanup: numberField(
      record,
      "activePlanBuildQueueDepthAfterCleanup",
    ),
    activePlanBuildingCountAfterCleanup: numberField(record, "activePlanBuildingCountAfterCleanup"),
    activePlanPendingCountAfterCleanup: numberField(record, "activePlanPendingCountAfterCleanup"),
    activePlanIndexEstimatedBytesAfterCleanup: numberField(
      record,
      "activePlanIndexEstimatedBytesAfterCleanup",
    ),
    maxSubscriptionLagVersionsAfterSettle: numberField(
      record,
      "maxSubscriptionLagVersionsAfterSettle",
    ),
    totalSubscriptionLagVersionsAfterSettle: numberField(
      record,
      "totalSubscriptionLagVersionsAfterSettle",
    ),
    maxSubscriptionLagVersionsAfterCleanup: numberField(
      record,
      "maxSubscriptionLagVersionsAfterCleanup",
    ),
    totalSubscriptionLagVersionsAfterCleanup: numberField(
      record,
      "totalSubscriptionLagVersionsAfterCleanup",
    ),
    queueDepthAfterSettle: numberField(record, "queueDepthAfterSettle"),
    queueDepthAfterCleanup: numberField(record, "queueDepthAfterCleanup"),
    heapBaselineBytes: numberField(record, "heapBaselineBytes"),
    heapSubscribedBytes: numberField(record, "heapSubscribedBytes"),
    heapLoadedBytes: numberField(record, "heapLoadedBytes"),
    heapReleasedBytes: numberField(record, "heapReleasedBytes"),
    rssBaselineBytes: numberField(record, "rssBaselineBytes"),
    rssLoadedBytes: numberField(record, "rssLoadedBytes"),
    rssReleasedBytes: numberField(record, "rssReleasedBytes"),
    heapGrowthRatio: numberField(record, "heapGrowthRatio"),
    gcAvailable: booleanField(record, "gcAvailable"),
    events: {
      snapshots: numberField(events, "snapshots"),
      deltas: numberField(events, "deltas"),
      status: numberField(events, "status"),
    },
    retries: numberField(record, "retries"),
    backpressureErrors: numberField(record, "backpressureErrors"),
    reconnects: numberField(record, "reconnects"),
  };
}

function workerSoakResult(summary: SoakSummary): BenchmarkResult {
  return {
    case: {
      operation: "workerSoak",
      scenario: workerSoakScenario(),
      rows: summary.shape.rows,
      rawSubscriptions: summary.shape.rawSubscriptions,
      groupedSubscriptions: summary.shape.groupedSubscriptions,
      mutations: summary.shape.mutations,
      mutationBatchSize: summary.shape.mutationBatchSize,
    },
    metrics: workerSoakMetrics(summary),
  };
}

function workerSoakMetrics(summary: SoakSummary): readonly BenchmarkMetric[] {
  return [
    { name: "durationMs", value: summary.durationMs, unit: "ms" },
    { name: "rowGenerationMs", value: summary.rowGenerationMs, unit: "ms" },
    { name: "workerSeedMs", value: summary.workerSeedMs, unit: "ms" },
    { name: "subscriptionSetupMs", value: summary.subscriptionSetupMs, unit: "ms" },
    { name: "mutationLoopMs", value: summary.mutationLoopMs, unit: "ms" },
    { name: "mutationP50Ms", value: summary.mutationLatencyMs.p50Ms, unit: "ms" },
    { name: "mutationP95Ms", value: summary.mutationLatencyMs.p95Ms, unit: "ms" },
    { name: "mutationP99Ms", value: summary.mutationLatencyMs.p99Ms, unit: "ms" },
    { name: "mutationMaxMs", value: summary.mutationLatencyMs.maxMs, unit: "ms" },
    { name: "settleMs", value: summary.settleMs, unit: "ms" },
    { name: "cleanupMs", value: summary.cleanupMs, unit: "ms" },
    { name: "cleanupLeakCount", value: cleanupLeakCount(summary), unit: "count" },
    { name: "subscribersAfterCleanup", value: summary.subscribersAfterCleanup, unit: "count" },
    {
      name: "activePlanCountAfterCleanup",
      value: summary.activePlanCountAfterCleanup,
      unit: "count",
    },
    {
      name: "activeViewCountAfterCleanup",
      value: summary.activeViewCountAfterCleanup,
      unit: "count",
    },
    {
      name: "queueDepthAfterCleanup",
      value: summary.queueDepthAfterCleanup,
      unit: "count",
    },
    {
      name: "maxSubscriptionLagVersionsAfterCleanup",
      value: summary.maxSubscriptionLagVersionsAfterCleanup,
      unit: "count",
    },
    {
      name: "totalSubscriptionLagVersionsAfterCleanup",
      value: summary.totalSubscriptionLagVersionsAfterCleanup,
      unit: "count",
    },
    {
      name: "maxSubscriptionLagVersionsAfterSettle",
      value: summary.maxSubscriptionLagVersionsAfterSettle,
      unit: "count",
      lowerIsBetter: false,
    },
    {
      name: "totalSubscriptionLagVersionsAfterSettle",
      value: summary.totalSubscriptionLagVersionsAfterSettle,
      unit: "count",
      lowerIsBetter: false,
    },
    {
      name: "activePlanAutoBuildSkippedCountBeforeCleanup",
      value: summary.activePlanAutoBuildSkippedCountBeforeCleanup,
      unit: "count",
      lowerIsBetter: false,
    },
    {
      name: "mutationBatches",
      value: summary.mutationBatches,
      unit: "count",
      lowerIsBetter: false,
    },
    { name: "finalRows", value: summary.finalRows, unit: "count", lowerIsBetter: false },
    {
      name: "snapshotEventCount",
      value: summary.events.snapshots,
      unit: "count",
      lowerIsBetter: false,
    },
    {
      name: "deltaEventCount",
      value: summary.events.deltas,
      unit: "count",
      lowerIsBetter: false,
    },
    {
      name: "statusEventCount",
      value: summary.events.status,
      unit: "count",
      lowerIsBetter: false,
    },
    { name: "heapGrowthRatio", value: summary.heapGrowthRatio, unit: "ratio" },
    {
      name: "heapReleasedBytes",
      value: summary.heapReleasedBytes,
      unit: "bytes",
      lowerIsBetter: false,
    },
    {
      name: "rssReleasedBytes",
      value: summary.rssReleasedBytes,
      unit: "bytes",
      lowerIsBetter: false,
    },
  ];
}

function workerSoakConfig(summary: SoakSummary): Readonly<Record<string, BenchmarkPrimitive>> {
  return {
    scenario: workerSoakScenario(),
    rows: summary.shape.rows,
    rawSubscriptions: summary.shape.rawSubscriptions,
    groupedSubscriptions: summary.shape.groupedSubscriptions,
    mutations: summary.shape.mutations,
    mutationBatchSize: summary.shape.mutationBatchSize,
    rawPageCycle: summary.shape.rawPageCycle,
    groupedRefreshDebounceMs: summary.shape.groupedRefreshDebounceMs,
    activePlanAutoBuildMaxRows: summary.shape.activePlanAutoBuildMaxRows,
    gcAvailable: summary.gcAvailable,
  };
}

function cleanupLeakCount(summary: SoakSummary): number {
  return (
    summary.subscribersAfterCleanup +
    summary.activePlanCountAfterCleanup +
    summary.activeViewCountAfterCleanup +
    summary.activePlanFallbackCountAfterCleanup +
    summary.activePlanBuildQueueDepthAfterCleanup +
    summary.activePlanBuildingCountAfterCleanup +
    summary.activePlanPendingCountAfterCleanup +
    summary.queueDepthAfterCleanup +
    summary.maxSubscriptionLagVersionsAfterCleanup +
    summary.totalSubscriptionLagVersionsAfterCleanup
  );
}

function workerSoakScenario(): string {
  return process.env.VS_WORKER_SOAK_SCENARIO ?? "worker-soak";
}

function workerSoakSummaryPath(): string {
  const explicit = process.env.VS_WORKER_SOAK_SUMMARY_PATH;
  if (explicit !== undefined && explicit.length > 0) {
    return explicit;
  }
  const artifactPath = process.env.VS_BENCH_ARTIFACT;
  if (artifactPath !== undefined && artifactPath.length > 0) {
    return resolve(packageRoot, artifactPath.replace(/\.json$/, ".summary.json"));
  }
  return resolve(
    packageRoot,
    `bench/.artifacts/worker-soak-${workerSoakScenario()}-${Date.now()}.summary.json`,
  );
}

function workerSoakProgressPath(summaryPath: string): string {
  return `${summaryPath}.progress.jsonl`;
}

function displayPath(path: string): string {
  const relativePath = relative(packageRoot, path);
  return relativePath.startsWith("..") ? path : relativePath;
}

function recordField(
  parent: Readonly<Record<string, unknown>>,
  field: string,
): Record<string, unknown> {
  const value = parent[field];
  if (!isRecord(value)) {
    throw new Error(`${field} must be an object`);
  }
  return value;
}

function numberField(parent: Readonly<Record<string, unknown>>, field: string): number {
  const value = parent[field];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`${field} must be a finite number`);
  }
  return value;
}

function stringField(parent: Readonly<Record<string, unknown>>, field: string): string {
  const value = parent[field];
  if (typeof value !== "string") {
    throw new Error(`${field} must be a string`);
  }
  return value;
}

function booleanField(parent: Readonly<Record<string, unknown>>, field: string): boolean {
  const value = parent[field];
  if (typeof value !== "boolean") {
    throw new Error(`${field} must be a boolean`);
  }
  return value;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
