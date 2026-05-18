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

type RuntimeWebsocketSoakShape = {
  readonly rows: number;
  readonly rawClients: number;
  readonly groupedClients: number;
  readonly mutations: number;
  readonly reconnectClients: number;
  readonly connectConcurrency: number;
  readonly rawPageCycle: number;
  readonly healthSampleInterval: number;
};

type LatencyStats = {
  readonly count: number;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly p99Ms: number;
  readonly maxMs: number;
};

type SoakEventCounts = {
  readonly snapshots: number;
  readonly deltas: number;
  readonly status: number;
};

type RuntimeWebsocketObservedMetrics = {
  readonly maxQueueDepth: number;
  readonly maxSubscriptionLagVersions: number;
  readonly maxChdbPendingRequests: number;
  readonly maxChdbBackendLagVersions: number;
};

type RuntimeWebsocketSoakSummary = {
  readonly shape: RuntimeWebsocketSoakShape;
  readonly durationMs: number;
  readonly subscriptionSetupMs: number;
  readonly mutationLoopMs: number;
  readonly cleanupMs: number;
  readonly mutationLatencyMs: LatencyStats;
  readonly maxSubscriptionLagVersionsBeforeCleanup: number;
  readonly totalSubscriptionLagVersionsBeforeCleanup: number;
  readonly chdbBackendVersionBeforeCleanup: string;
  readonly workerVersionBeforeCleanup: string;
  readonly finalRows: number;
  readonly finalVersion: string;
  readonly chdbBackendVersionAfterCleanup: string;
  readonly subscribersAfterCleanup: number;
  readonly activePlanCountAfterCleanup: number;
  readonly activeViewCountAfterCleanup: number;
  readonly activePlanBuildQueueDepthAfterCleanup: number;
  readonly activePlanBuildingCountAfterCleanup: number;
  readonly activePlanPendingCountAfterCleanup: number;
  readonly queueDepthAfterCleanup: number;
  readonly maxSubscriptionLagVersionsAfterCleanup: number;
  readonly totalSubscriptionLagVersionsAfterCleanup: number;
  readonly chdbStatusAfterCleanup: string;
  readonly chdbPendingRequestsAfterCleanup: number;
  readonly events: SoakEventCounts;
  readonly retries: number;
  readonly backpressureErrors: number;
  readonly reconnects: number;
  readonly observed: RuntimeWebsocketObservedMetrics;
  readonly topSlowMutations: readonly unknown[];
};

class RuntimeWebsocketSoakBenchmarkError extends Schema.TaggedErrorClass<RuntimeWebsocketSoakBenchmarkError>()(
  "RuntimeWebsocketSoakBenchmarkError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

const packageRoot = fileURLToPath(new URL("..", import.meta.url));

void Effect.runPromise(
  Effect.fn("view-server.bench.runtime_websocket_soak")(function* () {
    const summaryPath = runtimeWebsocketSoakSummaryPath();
    yield* Effect.logInfo(`runtime-websocket-soak benchmark summaryPath=${summaryPath}`);
    yield* runRuntimeWebsocketSoak(summaryPath);
    const summary = yield* readRuntimeWebsocketSoakSummary(summaryPath);
    const artifact = yield* writeBenchmarkArtifact(
      "runtime-websocket-soak",
      runtimeWebsocketSoakConfig(summary),
      [runtimeWebsocketSoakResult(summary)],
      {
        notes: [
          "Wraps the real websocket runtime soak test and converts its JSON summary into a benchmark artifact.",
          "CI smoke should block only on deterministic cleanup/retry/backpressure invariants; latency thresholds are visibility signals.",
          `summary artifact: ${displayPath(summaryPath)}`,
        ],
      },
    );
    yield* Effect.logInfo(
      [
        "runtime-websocket-soak benchmark result",
        `rows=${summary.shape.rows}`,
        `clients=${summary.shape.rawClients + summary.shape.groupedClients}`,
        `mutations=${summary.shape.mutations}`,
        `reconnects=${summary.reconnects}`,
        `mutationP99Ms=${summary.mutationLatencyMs.p99Ms.toFixed(2)}`,
        `mutationMaxMs=${summary.mutationLatencyMs.maxMs.toFixed(2)}`,
        `cleanupLeakCount=${cleanupLeakCount(summary)}`,
        `artifact=${artifact.artifactPath}`,
        `baselineCompared=${artifact.compared}`,
      ].join(" "),
    );
  })(),
);

function runRuntimeWebsocketSoak(
  summaryPath: string,
): Effect.Effect<void, RuntimeWebsocketSoakBenchmarkError> {
  return Effect.fn("view-server.bench.runtime_websocket_soak.run_vitest")(function* () {
    const vitestEntry = resolve(packageRoot, "node_modules/.bin/vitest");
    const env = {
      ...process.env,
      VS_RUNTIME_WEBSOCKET_SOAK_SUMMARY_PATH: summaryPath,
    };
    yield* spawnCommand(
      [vitestEntry, "run", "--config", "vitest.config.ts", "tests/runtime-websocket-soak.test.ts"],
      packageRoot,
      env,
    );
  })();
}

function spawnCommand(
  command: readonly string[],
  cwd: string,
  env: Readonly<Record<string, string | undefined>>,
): Effect.Effect<void, RuntimeWebsocketSoakBenchmarkError> {
  return Effect.callback<void, RuntimeWebsocketSoakBenchmarkError>((resume) => {
    const [executable, ...args] = command;
    if (executable === undefined) {
      resume(
        Effect.fail(
          new RuntimeWebsocketSoakBenchmarkError({
            message: "Runtime websocket soak benchmark has no executable",
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
          new RuntimeWebsocketSoakBenchmarkError({
            message: `Failed to start runtime websocket soak benchmark: ${String(cause)}`,
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
          new RuntimeWebsocketSoakBenchmarkError({
            message: `Runtime websocket soak benchmark failed with code ${String(code)} signal ${String(signal)}`,
          }),
        ),
      );
    });
    return Effect.sync(() => {
      child.kill("SIGTERM");
    });
  });
}

function readRuntimeWebsocketSoakSummary(
  path: string,
): Effect.Effect<RuntimeWebsocketSoakSummary, RuntimeWebsocketSoakBenchmarkError> {
  return Effect.tryPromise({
    try: () => readFile(path, "utf8"),
    catch: (cause) =>
      new RuntimeWebsocketSoakBenchmarkError({
        message: `Failed to read runtime websocket soak summary ${path}: ${String(cause)}`,
        cause,
      }),
  }).pipe(
    Effect.flatMap((json) =>
      Effect.try({
        try: () => decodeRuntimeWebsocketSoakSummary(JSON.parse(json)),
        catch: (cause) =>
          new RuntimeWebsocketSoakBenchmarkError({
            message: `Invalid runtime websocket soak summary ${path}: ${String(cause)}`,
            cause,
          }),
      }),
    ),
  );
}

function decodeRuntimeWebsocketSoakSummary(value: unknown): RuntimeWebsocketSoakSummary {
  const record = recordField({ summary: value }, "summary");
  const shape = recordField(record, "shape");
  const mutationLatencyMs = recordField(record, "mutationLatencyMs");
  const events = recordField(record, "events");
  const observed = recordField(record, "observed");
  return {
    shape: {
      rows: numberField(shape, "rows"),
      rawClients: numberField(shape, "rawClients"),
      groupedClients: numberField(shape, "groupedClients"),
      mutations: numberField(shape, "mutations"),
      reconnectClients: numberField(shape, "reconnectClients"),
      connectConcurrency: numberField(shape, "connectConcurrency"),
      rawPageCycle: numberField(shape, "rawPageCycle"),
      healthSampleInterval: numberField(shape, "healthSampleInterval"),
    },
    durationMs: numberField(record, "durationMs"),
    subscriptionSetupMs: numberField(record, "subscriptionSetupMs"),
    mutationLoopMs: numberField(record, "mutationLoopMs"),
    cleanupMs: numberField(record, "cleanupMs"),
    mutationLatencyMs: {
      count: numberField(mutationLatencyMs, "count"),
      p50Ms: numberField(mutationLatencyMs, "p50Ms"),
      p95Ms: numberField(mutationLatencyMs, "p95Ms"),
      p99Ms: numberField(mutationLatencyMs, "p99Ms"),
      maxMs: numberField(mutationLatencyMs, "maxMs"),
    },
    maxSubscriptionLagVersionsBeforeCleanup: numberField(
      record,
      "maxSubscriptionLagVersionsBeforeCleanup",
    ),
    totalSubscriptionLagVersionsBeforeCleanup: numberField(
      record,
      "totalSubscriptionLagVersionsBeforeCleanup",
    ),
    chdbBackendVersionBeforeCleanup: stringField(record, "chdbBackendVersionBeforeCleanup"),
    workerVersionBeforeCleanup: stringField(record, "workerVersionBeforeCleanup"),
    finalRows: numberField(record, "finalRows"),
    finalVersion: stringField(record, "finalVersion"),
    chdbBackendVersionAfterCleanup: stringField(record, "chdbBackendVersionAfterCleanup"),
    subscribersAfterCleanup: numberField(record, "subscribersAfterCleanup"),
    activePlanCountAfterCleanup: numberField(record, "activePlanCountAfterCleanup"),
    activeViewCountAfterCleanup: numberField(record, "activeViewCountAfterCleanup"),
    activePlanBuildQueueDepthAfterCleanup: numberField(
      record,
      "activePlanBuildQueueDepthAfterCleanup",
    ),
    activePlanBuildingCountAfterCleanup: numberField(record, "activePlanBuildingCountAfterCleanup"),
    activePlanPendingCountAfterCleanup: numberField(record, "activePlanPendingCountAfterCleanup"),
    queueDepthAfterCleanup: numberField(record, "queueDepthAfterCleanup"),
    maxSubscriptionLagVersionsAfterCleanup: numberField(
      record,
      "maxSubscriptionLagVersionsAfterCleanup",
    ),
    totalSubscriptionLagVersionsAfterCleanup: numberField(
      record,
      "totalSubscriptionLagVersionsAfterCleanup",
    ),
    chdbStatusAfterCleanup: stringField(record, "chdbStatusAfterCleanup"),
    chdbPendingRequestsAfterCleanup: numberField(record, "chdbPendingRequestsAfterCleanup"),
    events: {
      snapshots: numberField(events, "snapshots"),
      deltas: numberField(events, "deltas"),
      status: numberField(events, "status"),
    },
    retries: numberField(record, "retries"),
    backpressureErrors: numberField(record, "backpressureErrors"),
    reconnects: numberField(record, "reconnects"),
    observed: {
      maxQueueDepth: numberField(observed, "maxQueueDepth"),
      maxSubscriptionLagVersions: numberField(observed, "maxSubscriptionLagVersions"),
      maxChdbPendingRequests: numberField(observed, "maxChdbPendingRequests"),
      maxChdbBackendLagVersions: numberField(observed, "maxChdbBackendLagVersions"),
    },
    topSlowMutations: arrayField(record, "topSlowMutations"),
  };
}

function runtimeWebsocketSoakResult(summary: RuntimeWebsocketSoakSummary): BenchmarkResult {
  return {
    case: {
      operation: "runtimeWebsocketSoak",
      rows: summary.shape.rows,
      clients: summary.shape.rawClients + summary.shape.groupedClients,
      rawClients: summary.shape.rawClients,
      groupedClients: summary.shape.groupedClients,
      mutations: summary.shape.mutations,
      reconnectClients: summary.shape.reconnectClients,
    },
    metrics: runtimeWebsocketSoakMetrics(summary),
  };
}

function runtimeWebsocketSoakMetrics(
  summary: RuntimeWebsocketSoakSummary,
): readonly BenchmarkMetric[] {
  return [
    { name: "durationMs", value: summary.durationMs, unit: "ms" },
    { name: "subscriptionSetupMs", value: summary.subscriptionSetupMs, unit: "ms" },
    { name: "mutationLoopMs", value: summary.mutationLoopMs, unit: "ms" },
    { name: "mutationP50Ms", value: summary.mutationLatencyMs.p50Ms, unit: "ms" },
    { name: "mutationP95Ms", value: summary.mutationLatencyMs.p95Ms, unit: "ms" },
    { name: "mutationP99Ms", value: summary.mutationLatencyMs.p99Ms, unit: "ms" },
    { name: "mutationMaxMs", value: summary.mutationLatencyMs.maxMs, unit: "ms" },
    { name: "cleanupMs", value: summary.cleanupMs, unit: "ms" },
    { name: "retryCount", value: summary.retries, unit: "count" },
    { name: "backpressureCount", value: summary.backpressureErrors, unit: "count" },
    { name: "cleanupLeakCount", value: cleanupLeakCount(summary), unit: "count" },
    {
      name: "maxQueueDepthObserved",
      value: summary.observed.maxQueueDepth,
      unit: "count",
    },
    {
      name: "maxSubscriptionLagVersionsObserved",
      value: summary.observed.maxSubscriptionLagVersions,
      unit: "count",
    },
    {
      name: "maxChdbPendingRequestsObserved",
      value: summary.observed.maxChdbPendingRequests,
      unit: "count",
    },
    {
      name: "maxChdbBackendLagVersionsObserved",
      value: summary.observed.maxChdbBackendLagVersions,
      unit: "count",
    },
    {
      name: "maxQueueDepthAfterCleanup",
      value: summary.queueDepthAfterCleanup,
      unit: "count",
    },
    {
      name: "maxSubscriptionLagVersionsAfterCleanup",
      value: summary.maxSubscriptionLagVersionsAfterCleanup,
      unit: "count",
    },
    {
      name: "chdbPendingRequestsAfterCleanup",
      value: summary.chdbPendingRequestsAfterCleanup,
      unit: "count",
    },
    {
      name: "chdbBackendLagVersionsAfterCleanup",
      value: chdbBackendLagVersionsAfterCleanup(summary),
      unit: "count",
    },
    {
      name: "chdbNotReadyAfterCleanupCount",
      value: summary.chdbStatusAfterCleanup === "ready" ? 0 : 1,
      unit: "count",
    },
    {
      name: "reconnectCount",
      value: summary.reconnects,
      unit: "count",
      lowerIsBetter: false,
    },
    {
      name: "topSlowSampleCount",
      value: summary.topSlowMutations.length,
      unit: "count",
      lowerIsBetter: false,
    },
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
    { name: "finalRows", value: summary.finalRows, unit: "count", lowerIsBetter: false },
  ];
}

function runtimeWebsocketSoakConfig(
  summary: RuntimeWebsocketSoakSummary,
): Readonly<Record<string, BenchmarkPrimitive>> {
  return {
    rows: summary.shape.rows,
    rawClients: summary.shape.rawClients,
    groupedClients: summary.shape.groupedClients,
    mutations: summary.shape.mutations,
    reconnectClients: summary.shape.reconnectClients,
    connectConcurrency: summary.shape.connectConcurrency,
    rawPageCycle: summary.shape.rawPageCycle,
    healthSampleInterval: summary.shape.healthSampleInterval,
  };
}

function cleanupLeakCount(summary: RuntimeWebsocketSoakSummary): number {
  return (
    summary.subscribersAfterCleanup +
    summary.activePlanCountAfterCleanup +
    summary.activeViewCountAfterCleanup +
    summary.activePlanBuildQueueDepthAfterCleanup +
    summary.activePlanBuildingCountAfterCleanup +
    summary.activePlanPendingCountAfterCleanup +
    summary.queueDepthAfterCleanup +
    summary.maxSubscriptionLagVersionsAfterCleanup +
    summary.totalSubscriptionLagVersionsAfterCleanup +
    summary.chdbPendingRequestsAfterCleanup +
    chdbBackendLagVersionsAfterCleanup(summary) +
    (summary.chdbStatusAfterCleanup === "ready" ? 0 : 1)
  );
}

function chdbBackendLagVersionsAfterCleanup(summary: RuntimeWebsocketSoakSummary): number {
  return versionLag(summary.finalVersion, summary.chdbBackendVersionAfterCleanup);
}

function versionLag(workerVersion: string, backendVersion: string): number {
  const lag = BigInt(workerVersion) - BigInt(backendVersion);
  if (lag <= 0n) {
    return 0;
  }
  return lag > BigInt(Number.MAX_SAFE_INTEGER) ? Number.MAX_SAFE_INTEGER : Number(lag);
}

function runtimeWebsocketSoakSummaryPath(): string {
  const explicit = process.env.VS_RUNTIME_WEBSOCKET_SOAK_SUMMARY_PATH;
  if (explicit !== undefined && explicit.length > 0) {
    return explicit;
  }
  const artifactPath = process.env.VS_BENCH_ARTIFACT;
  if (artifactPath !== undefined && artifactPath.length > 0) {
    return resolve(packageRoot, artifactPath.replace(/\.json$/, ".summary.json"));
  }
  return resolve(packageRoot, `bench/.artifacts/runtime-websocket-soak-${Date.now()}.summary.json`);
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

function arrayField(parent: Readonly<Record<string, unknown>>, field: string): readonly unknown[] {
  const value = parent[field];
  if (!Array.isArray(value)) {
    throw new Error(`${field} must be an array`);
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
