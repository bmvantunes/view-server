import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { spawn } from "node:child_process";
import { copyFile, mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  benchmarkProfileCoverageGapsEnv,
  benchmarkProfiles,
  type BenchmarkProfileBenchmark,
} from "./benchmark-profiles.ts";

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
const smokeProfile = benchmarkProfiles["ci-smoke"];
const smokeBenchmarks = smokeProfile.benchmarks;

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
  benchmark: BenchmarkProfileBenchmark,
  refreshBaselines: boolean,
): Effect.Effect<void, BenchmarkSmokeError> {
  return Effect.callback<void, BenchmarkSmokeError>((resume) => {
    const script = benchmark.script;
    if (script === undefined) {
      resume(
        Effect.fail(
          new BenchmarkSmokeError({
            message: `Benchmark smoke ${benchmark.name} does not define a node script`,
          }),
        ),
      );
      return Effect.void;
    }
    const env = {
      ...process.env,
      ...benchmark.env,
      VS_BENCH_PROFILE: smokeProfile.name,
      VS_BENCH_PROFILE_BENCHMARK: benchmark.name,
      VS_BENCH_PROFILE_COVERAGE_GAPS: benchmarkProfileCoverageGapsEnv(smokeProfile),
      VS_BENCH_ARTIFACT: benchmarkArtifactPath(benchmark),
      ...(refreshBaselines
        ? {}
        : {
            VS_BENCH_BASELINE: benchmarkBaselinePath(benchmark),
            ...(benchmark.metrics === undefined
              ? {}
              : { VS_BENCH_REGRESSION_METRICS: benchmark.metrics }),
            VS_BENCH_REGRESSION_MIN_DELTA_MS: process.env.VS_BENCH_REGRESSION_MIN_DELTA_MS ?? "5",
            VS_BENCH_REGRESSION_REPORT_ONLY: process.env.VS_BENCH_BLOCKING === "1" ? "0" : "1",
          }),
    };
    const child = spawn(process.execPath, ["--experimental-strip-types", script], {
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

function refreshBaseline(benchmark: BenchmarkProfileBenchmark): Effect.Effect<void> {
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

function benchmarkArtifactPath(benchmark: BenchmarkProfileBenchmark): string {
  return `${artifactRoot}/${benchmark.artifactFile}`;
}

function benchmarkBaselinePath(benchmark: BenchmarkProfileBenchmark): string {
  return `${baselineRoot}/${benchmark.baselineFile ?? benchmark.artifactFile}`;
}
