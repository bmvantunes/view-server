import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { spawn } from "node:child_process";
import { copyFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

type SmokeBenchmark = {
  readonly name: string;
  readonly script: string;
  readonly artifactFile: string;
  readonly baselineFile: string;
  readonly metrics: string;
  readonly env: Readonly<Record<string, string>>;
};

class BenchmarkSmokeError extends Schema.TaggedErrorClass<BenchmarkSmokeError>()(
  "BenchmarkSmokeError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const artifactRoot = process.env.VS_BENCH_SMOKE_ARTIFACT_DIR ?? "bench/.artifacts/ci";
const baselineRoot = process.env.VS_BENCH_SMOKE_BASELINE_DIR ?? "bench/baselines/ci-smoke";

const smokeBenchmarks: readonly SmokeBenchmark[] = [
  {
    name: "active-view",
    script: "bench/active-view.bench.ts",
    artifactFile: "active-view.json",
    baselineFile: "active-view.json",
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
    script: "bench/active-plan-responsiveness.bench.ts",
    artifactFile: "active-plan-responsiveness.json",
    baselineFile: "active-plan-responsiveness.json",
    metrics: "operationP99Ms,metricsP99Ms",
    env: {
      VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "1000",
      VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "3",
      VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "publish",
    },
  },
  {
    name: "grouped-responsiveness",
    script: "bench/grouped-responsiveness.bench.ts",
    artifactFile: "grouped-responsiveness.json",
    baselineFile: "grouped-responsiveness.json",
    metrics: "operationP99Ms,metricsP99Ms",
    env: {
      VS_GROUPED_RESPONSIVENESS_ROWS: "1000",
      VS_GROUPED_RESPONSIVENESS_OPERATIONS: "3",
      VS_GROUPED_RESPONSIVENESS_AGGREGATES: "3",
    },
  },
  {
    name: "grouped-refresh-overlap",
    script: "bench/grouped-refresh-overlap.bench.ts",
    artifactFile: "grouped-refresh-overlap.json",
    baselineFile: "grouped-refresh-overlap.json",
    metrics: "operationP99Ms,startGapMaxMs",
    env: {
      VS_GROUPED_REFRESH_OVERLAP_ROWS: "1000",
      VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "3",
      VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "3",
      VS_GROUPED_REFRESH_OVERLAP_BACKEND: "memory",
    },
  },
];

void Effect.runPromise(
  Effect.gen(function* () {
    const refreshBaselines = process.argv.includes("--refresh-baselines");
    for (const benchmark of smokeBenchmarks) {
      yield* Effect.logInfo(
        `${refreshBaselines ? "refreshing" : "running"} benchmark smoke ${benchmark.name}`,
      );
      yield* runBenchmark(benchmark, refreshBaselines);
      if (refreshBaselines) {
        yield* refreshBaseline(benchmark);
      }
    }
  }),
);

function runBenchmark(
  benchmark: SmokeBenchmark,
  refreshBaselines: boolean,
): Effect.Effect<void, BenchmarkSmokeError> {
  return Effect.callback<void, BenchmarkSmokeError>((resume) => {
    const env = {
      ...process.env,
      ...benchmark.env,
      VS_BENCH_ARTIFACT: benchmarkArtifactPath(benchmark),
      ...(refreshBaselines
        ? {}
        : {
            VS_BENCH_BASELINE: benchmarkBaselinePath(benchmark),
            VS_BENCH_REGRESSION_METRICS: benchmark.metrics,
            VS_BENCH_REGRESSION_MIN_DELTA_MS: process.env.VS_BENCH_REGRESSION_MIN_DELTA_MS ?? "5",
            VS_BENCH_REGRESSION_REPORT_ONLY: process.env.VS_BENCH_BLOCKING === "1" ? "0" : "1",
          }),
    };
    const child = spawn(process.execPath, ["--experimental-strip-types", benchmark.script], {
      cwd: packageRoot,
      env,
      stdio: "inherit",
    });
    child.on("error", (cause) => {
      resume(
        Effect.fail(
          new BenchmarkSmokeError({
            message: `Failed to start benchmark smoke ${benchmark.name}: ${String(cause)}`,
            cause,
          }),
        ),
      );
    });
    child.on("exit", (code, signal) => {
      if (code === 0) {
        resume(Effect.void);
        return;
      }
      resume(
        Effect.fail(
          new BenchmarkSmokeError({
            message: `Benchmark smoke ${benchmark.name} failed with code ${String(code)} signal ${String(signal)}`,
          }),
        ),
      );
    });
    return Effect.sync(() => {
      child.kill("SIGTERM");
    });
  });
}

function refreshBaseline(benchmark: SmokeBenchmark): Effect.Effect<void> {
  const artifactPath = resolve(packageRoot, benchmarkArtifactPath(benchmark));
  const baselinePath = resolve(packageRoot, benchmarkBaselinePath(benchmark));
  return Effect.tryPromise({
    try: () =>
      mkdir(dirname(baselinePath), { recursive: true }).then(() =>
        copyFile(artifactPath, baselinePath),
      ),
    catch: (cause) =>
      new BenchmarkSmokeError({
        message: `Failed to refresh benchmark baseline ${benchmark.name}: ${String(cause)}`,
        cause,
      }),
  }).pipe(
    Effect.tap(() =>
      Effect.logInfo(`refreshed benchmark baseline ${benchmarkBaselinePath(benchmark)}`),
    ),
    Effect.orDie,
  );
}

function benchmarkArtifactPath(benchmark: SmokeBenchmark): string {
  return `${artifactRoot}/${benchmark.artifactFile}`;
}

function benchmarkBaselinePath(benchmark: SmokeBenchmark): string {
  return `${baselineRoot}/${benchmark.baselineFile}`;
}
