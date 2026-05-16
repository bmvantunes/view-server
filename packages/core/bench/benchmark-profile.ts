import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { spawn } from "node:child_process";
import { mkdir } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  benchmarkProfileCoverageGapsEnv,
  getBenchmarkProfile,
  listBenchmarkProfiles,
  type BenchmarkProfile,
  type BenchmarkProfileBenchmark,
} from "./benchmark-profiles.ts";

class BenchmarkProfileError extends Schema.TaggedErrorClass<BenchmarkProfileError>()(
  "BenchmarkProfileError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

type BenchmarkProfileCliOptions = {
  readonly list: boolean;
  readonly dryRun: boolean;
  readonly compare: boolean;
  readonly refreshBaselines: boolean;
  readonly profileName: string | undefined;
};

const packageRoot = fileURLToPath(new URL("..", import.meta.url));
const repoRoot = fileURLToPath(new URL("../../..", import.meta.url));
const artifactRoot = process.env.VS_BENCH_PROFILE_ARTIFACT_DIR ?? "bench/.artifacts/profiles";
const baselineRoot = process.env.VS_BENCH_PROFILE_BASELINE_DIR ?? "bench/baselines";

void Effect.runPromise(
  Effect.fn("view-server.bench.profile.cli")(function* () {
    const options = parseArgs(process.argv.slice(2));
    if (options.list) {
      yield* writeStdout(formatProfileList(listBenchmarkProfiles()));
      return;
    }

    const profileName = options.profileName ?? "ci-smoke";
    const profile = getBenchmarkProfile(profileName);
    if (profile === undefined) {
      return yield* Effect.fail(
        new BenchmarkProfileError({
          message: `Unknown benchmark profile ${profileName}`,
        }),
      );
    }

    yield* runProfile(profile, options);
  })(),
);

function runProfile(
  profile: BenchmarkProfile,
  options: BenchmarkProfileCliOptions,
): Effect.Effect<void, BenchmarkProfileError> {
  return Effect.fn("view-server.bench.profile.run")(function* () {
    yield* Effect.logInfo(
      `${options.dryRun ? "dry-running" : "running"} benchmark profile ${profile.name}`,
    );
    for (const benchmark of profile.benchmarks) {
      if (options.dryRun) {
        yield* writeStdout(`${formatBenchmarkDryRun(profile, benchmark, options)}\n`);
      } else {
        yield* runBenchmark(profile, benchmark, options);
      }
    }
  })();
}

function runBenchmark(
  profile: BenchmarkProfile,
  benchmark: BenchmarkProfileBenchmark,
  options: BenchmarkProfileCliOptions,
): Effect.Effect<void, BenchmarkProfileError> {
  return Effect.fn("view-server.bench.profile.run_benchmark")(function* () {
    yield* Effect.annotateCurrentSpan({
      "view_server.benchmark_profile": profile.name,
      "view_server.benchmark": benchmark.name,
    });
    const command = benchmarkCommand(benchmark);
    const cwd = benchmark.cwd === "repo" ? repoRoot : packageRoot;
    const env = benchmarkEnv(profile, benchmark, options);
    const artifactPath = env.VS_BENCH_ARTIFACT;
    if (artifactPath !== undefined) {
      yield* Effect.tryPromise({
        try: () => mkdir(dirname(resolve(cwd, artifactPath)), { recursive: true }),
        catch: (cause) =>
          new BenchmarkProfileError({
            message: `Failed to create benchmark artifact directory for ${benchmark.name}: ${String(cause)}`,
            cause,
          }),
      });
    }
    yield* Effect.logInfo(`running benchmark ${benchmark.name}`);
    yield* spawnCommand(command, cwd, env, benchmark.name);
  })();
}

function spawnCommand(
  command: readonly string[],
  cwd: string,
  env: Readonly<Record<string, string | undefined>>,
  name: string,
): Effect.Effect<void, BenchmarkProfileError> {
  return Effect.callback<void, BenchmarkProfileError>((resume) => {
    const [executable, ...args] = command;
    if (executable === undefined) {
      resume(
        Effect.fail(
          new BenchmarkProfileError({
            message: `Benchmark ${name} has no executable`,
          }),
        ),
      );
      return Effect.void;
    }
    const child = spawn(executable, args, {
      cwd,
      env: {
        ...process.env,
        ...env,
      },
      stdio: "inherit",
    });
    child.on("error", (cause) => {
      resume(
        Effect.fail(
          new BenchmarkProfileError({
            message: `Failed to start benchmark ${name}: ${String(cause)}`,
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
          new BenchmarkProfileError({
            message: `Benchmark ${name} failed with code ${String(code)} signal ${String(signal)}`,
          }),
        ),
      );
    });
    return Effect.sync(() => {
      child.kill("SIGTERM");
    });
  });
}

function parseArgs(args: readonly string[]): BenchmarkProfileCliOptions {
  let list = false;
  let dryRun = false;
  let compare = false;
  let refreshBaselines = false;
  let profileName: string | undefined;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index];
    if (arg === "--list") {
      list = true;
    } else if (arg === "--dry-run") {
      dryRun = true;
    } else if (arg === "--compare") {
      compare = true;
    } else if (arg === "--refresh-baselines") {
      refreshBaselines = true;
    } else if (arg === "--profile") {
      profileName = args[index + 1];
      index += 1;
    } else if (arg !== undefined && !arg.startsWith("--") && profileName === undefined) {
      profileName = arg;
    }
  }
  return {
    list,
    dryRun,
    compare,
    refreshBaselines,
    profileName,
  };
}

function benchmarkCommand(benchmark: BenchmarkProfileBenchmark): readonly string[] {
  if (benchmark.command !== undefined) {
    return benchmark.command;
  }
  if (benchmark.script !== undefined) {
    return [process.execPath, "--experimental-strip-types", benchmark.script];
  }
  return [];
}

function benchmarkEnv(
  profile: BenchmarkProfile,
  benchmark: BenchmarkProfileBenchmark,
  options: BenchmarkProfileCliOptions,
): Readonly<Record<string, string | undefined>> {
  const artifactPath = `${artifactRoot}/${profile.name}/${benchmark.artifactFile}`;
  return {
    ...benchmark.env,
    VS_BENCH_PROFILE: profile.name,
    VS_BENCH_PROFILE_BENCHMARK: benchmark.name,
    VS_BENCH_PROFILE_COVERAGE_GAPS: benchmarkProfileCoverageGapsEnv(profile),
    VS_BENCH_ARTIFACT: artifactPath,
    ...(options.compare && benchmark.baselineFile !== undefined
      ? {
          VS_BENCH_BASELINE: `${baselineRoot}/${profile.name}/${benchmark.baselineFile}`,
          ...(benchmark.metrics === undefined
            ? {}
            : { VS_BENCH_REGRESSION_METRICS: benchmark.metrics }),
          VS_BENCH_REGRESSION_MIN_DELTA_MS: process.env.VS_BENCH_REGRESSION_MIN_DELTA_MS ?? "5",
          VS_BENCH_REGRESSION_REPORT_ONLY: process.env.VS_BENCH_BLOCKING === "1" ? "0" : "1",
        }
      : {}),
    ...(options.refreshBaselines ? { VS_BENCH_REFRESH_BASELINES: "1" } : {}),
  };
}

function formatProfileList(profiles: readonly BenchmarkProfile[]): string {
  const lines = ["Benchmark profiles:", ""];
  for (const profile of profiles) {
    lines.push(
      `${profile.name} (${profile.ciSafe ? "ci-safe" : "manual"}): ${profile.description}`,
    );
    for (const gap of profile.coverageGaps) {
      lines.push(`  gap: ${gap}`);
    }
  }
  return `${lines.join("\n")}\n`;
}

function formatBenchmarkDryRun(
  profile: BenchmarkProfile,
  benchmark: BenchmarkProfileBenchmark,
  options: BenchmarkProfileCliOptions,
): string {
  const command = benchmarkCommand(benchmark).join(" ");
  const env = benchmarkEnv(profile, benchmark, options);
  const entries = Object.entries(env)
    .filter((entry): entry is [string, string] => entry[1] !== undefined)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}=${JSON.stringify(value)}`);
  return [
    `profile=${profile.name}`,
    `benchmark=${benchmark.name}`,
    `cwd=${benchmark.cwd ?? "package"}`,
    `command=${command}`,
    "env:",
    ...entries.map((entry) => `  ${entry}`),
  ].join("\n");
}

function writeStdout(text: string): Effect.Effect<void> {
  return Effect.sync(() => {
    process.stdout.write(text);
  });
}
