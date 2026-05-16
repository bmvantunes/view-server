import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { appendFile, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

export type BenchmarkPrimitive = string | number | boolean | null;

export type BenchmarkMetric = {
  readonly name: string;
  readonly value: number;
  readonly unit: "ms" | "count" | "bytes" | "ratio";
  readonly lowerIsBetter?: boolean | undefined;
};

export type BenchmarkResult = {
  readonly case: Readonly<Record<string, BenchmarkPrimitive>>;
  readonly metrics: readonly BenchmarkMetric[];
};

export type BenchmarkArtifact = {
  readonly schemaVersion: 1;
  readonly benchmark: string;
  readonly generatedAt: string;
  readonly config: Readonly<Record<string, BenchmarkPrimitive>>;
  readonly notes?: readonly string[] | undefined;
  readonly results: readonly BenchmarkResult[];
};

export type BenchmarkArtifactResult = {
  readonly artifactPath: string;
  readonly compared: boolean;
  readonly regressionCount: number;
  readonly warningCount: number;
  readonly summaryPath?: string | undefined;
};

export type BenchmarkArtifactOptions = {
  readonly notes?: readonly string[] | undefined;
};

type BenchmarkComparisonStatus = "pass" | "warn" | "fail";

type BenchmarkComparison = {
  readonly benchmark: string;
  readonly caseKey: string;
  readonly metric: string;
  readonly unit: BenchmarkMetric["unit"];
  readonly baselineValue: number;
  readonly currentValue: number;
  readonly delta: number;
  readonly deltaPercent: number;
  readonly allowedValue: number;
  readonly status: BenchmarkComparisonStatus;
  readonly artifactPath: string;
};

class BenchmarkArtifactError extends Schema.TaggedErrorClass<BenchmarkArtifactError>()(
  "BenchmarkArtifactError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

export function writeBenchmarkArtifact(
  benchmark: string,
  config: Readonly<Record<string, BenchmarkPrimitive>>,
  results: readonly BenchmarkResult[],
  options: BenchmarkArtifactOptions = {},
): Effect.Effect<BenchmarkArtifactResult> {
  return Effect.fn("view-server.bench.artifact.write")(function* () {
    const generatedAt = DateTime.formatIso(yield* DateTime.now);
    const artifactConfig = benchmarkConfigWithProfile(config);
    const artifactNotes = benchmarkNotesWithProfile(options.notes);
    const artifact: BenchmarkArtifact = {
      schemaVersion: 1,
      benchmark,
      generatedAt,
      config: artifactConfig,
      ...(artifactNotes.length === 0 ? {} : { notes: artifactNotes }),
      results,
    };
    const artifactPath = benchmarkArtifactPath(benchmark, artifact.generatedAt);
    yield* Effect.tryPromise({
      try: () =>
        mkdir(dirname(artifactPath), { recursive: true }).then(() =>
          writeFile(artifactPath, `${JSON.stringify(artifact, null, 2)}\n`),
        ),
      catch: (cause) =>
        new BenchmarkArtifactError({
          message: `Failed to write benchmark artifact: ${String(cause)}`,
          cause,
        }),
    }).pipe(Effect.orDie);

    const baselinePath = process.env.VS_BENCH_BASELINE;
    if (baselinePath === undefined || baselinePath.length === 0) {
      return {
        artifactPath,
        compared: false,
        regressionCount: 0,
        warningCount: 0,
      };
    }

    const baseline = yield* readBenchmarkArtifact(baselinePath);
    const tolerance = regressionTolerance();
    const comparisons = benchmarkComparisons(
      baseline,
      artifact,
      tolerance,
      regressionMinimumDeltaMs(),
      regressionMetricFilter(),
      artifactPath,
    );
    const summaryPath = yield* appendBenchmarkSummary(
      comparisons,
      artifactPath,
      baselinePath,
      artifact.notes,
    );
    const regressions = comparisons.filter((comparison) => comparison.status === "fail");
    const warningCount = comparisons.filter((comparison) => comparison.status === "warn").length;
    if (regressions.length > 0) {
      const message = [
        `Benchmark regression exceeded ${(tolerance * 100).toFixed(1)}% tolerance`,
        `artifact=${artifactPath}`,
        `baseline=${baselinePath}`,
        ...regressions.map(regressionMessage),
      ].join("\n");
      if (!regressionReportOnly()) {
        return yield* Effect.die(new Error(message));
      }
      yield* Effect.logWarning(message);
    }

    return {
      artifactPath,
      compared: true,
      regressionCount: regressions.length,
      warningCount,
      ...(summaryPath === undefined ? {} : { summaryPath }),
    };
  })();
}

function benchmarkConfigWithProfile(
  config: Readonly<Record<string, BenchmarkPrimitive>>,
): Readonly<Record<string, BenchmarkPrimitive>> {
  const profile = process.env.VS_BENCH_PROFILE;
  const profileBenchmark = process.env.VS_BENCH_PROFILE_BENCHMARK;
  return {
    ...config,
    ...(profile === undefined || profile.length === 0 ? {} : { profile }),
    ...(profileBenchmark === undefined || profileBenchmark.length === 0
      ? {}
      : { profileBenchmark }),
  };
}

function benchmarkNotesWithProfile(notes: readonly string[] | undefined): readonly string[] {
  const profileCoverageGaps = process.env.VS_BENCH_PROFILE_COVERAGE_GAPS;
  const gaps =
    profileCoverageGaps === undefined || profileCoverageGaps.length === 0
      ? []
      : profileCoverageGaps
          .split("\n")
          .map((entry) => entry.trim())
          .filter((entry) => entry.length > 0)
          .map((entry) => `coverage gap: ${entry}`);
  return [...(notes ?? []), ...gaps];
}

function benchmarkArtifactPath(benchmark: string, generatedAt: string): string {
  const explicitPath = process.env.VS_BENCH_ARTIFACT;
  if (explicitPath !== undefined && explicitPath.length > 0) {
    return explicitPath;
  }
  const safeTimestamp = generatedAt.replaceAll(":", "-");
  return fileURLToPath(new URL(`.artifacts/${benchmark}-${safeTimestamp}.json`, import.meta.url));
}

function readBenchmarkArtifact(path: string): Effect.Effect<BenchmarkArtifact> {
  return Effect.tryPromise({
    try: () => readFile(path, "utf8"),
    catch: (cause) =>
      new BenchmarkArtifactError({
        message: `Failed to read benchmark baseline: ${String(cause)}`,
        cause,
      }),
  }).pipe(
    Effect.flatMap((json) =>
      Effect.try({
        try: () => decodeBenchmarkArtifact(JSON.parse(json)),
        catch: (cause) =>
          new BenchmarkArtifactError({
            message: `Invalid benchmark baseline JSON: ${String(cause)}`,
            cause,
          }),
      }),
    ),
    Effect.orDie,
  );
}

function decodeBenchmarkArtifact(value: unknown): BenchmarkArtifact {
  if (!isRecord(value)) {
    throw new Error("benchmark artifact must be an object");
  }
  const schemaVersion = value.schemaVersion;
  const benchmark = value.benchmark;
  const generatedAt = value.generatedAt;
  const config = decodePrimitiveRecord(value.config, "config");
  const notes = decodeNotes(value.notes);
  const results = decodeBenchmarkResults(value.results);
  if (schemaVersion !== 1 || typeof benchmark !== "string" || typeof generatedAt !== "string") {
    throw new Error("benchmark artifact metadata is invalid");
  }
  return {
    schemaVersion,
    benchmark,
    generatedAt,
    config,
    ...(notes === undefined ? {} : { notes }),
    results,
  };
}

function decodeNotes(value: unknown): readonly string[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value) || value.some((entry) => typeof entry !== "string")) {
    throw new Error("benchmark artifact notes must be an array of strings");
  }
  return value;
}

function decodeBenchmarkResults(value: unknown): readonly BenchmarkResult[] {
  if (!Array.isArray(value)) {
    throw new Error("benchmark artifact results must be an array");
  }
  return value.map((entry) => {
    if (!isRecord(entry)) {
      throw new Error("benchmark result must be an object");
    }
    return {
      case: decodePrimitiveRecord(entry.case, "case"),
      metrics: decodeBenchmarkMetrics(entry.metrics),
    };
  });
}

function decodeBenchmarkMetrics(value: unknown): readonly BenchmarkMetric[] {
  if (!Array.isArray(value)) {
    throw new Error("benchmark metrics must be an array");
  }
  return value.map((entry) => {
    if (!isRecord(entry)) {
      throw new Error("benchmark metric must be an object");
    }
    const name = entry.name;
    const metricValue = entry.value;
    const unit = entry.unit;
    const lowerIsBetter = entry.lowerIsBetter;
    if (
      typeof name !== "string" ||
      typeof metricValue !== "number" ||
      !Number.isFinite(metricValue) ||
      !isMetricUnit(unit) ||
      (lowerIsBetter !== undefined && typeof lowerIsBetter !== "boolean")
    ) {
      throw new Error("benchmark metric is invalid");
    }
    return {
      name,
      value: metricValue,
      unit,
      ...(lowerIsBetter === undefined ? {} : { lowerIsBetter }),
    };
  });
}

function decodePrimitiveRecord(
  value: unknown,
  label: string,
): Readonly<Record<string, BenchmarkPrimitive>> {
  if (!isRecord(value)) {
    throw new Error(`benchmark ${label} must be an object`);
  }
  const decoded: Record<string, BenchmarkPrimitive> = {};
  for (const [key, entry] of Object.entries(value)) {
    if (!isBenchmarkPrimitive(entry)) {
      throw new Error(`benchmark ${label}.${key} must be a primitive`);
    }
    decoded[key] = entry;
  }
  return decoded;
}

function benchmarkComparisons(
  baseline: BenchmarkArtifact,
  current: BenchmarkArtifact,
  tolerance: number,
  minimumDeltaMs: number,
  metricFilter: ReadonlySet<string> | undefined,
  artifactPath: string,
): readonly BenchmarkComparison[] {
  const baselineResults = new Map(baseline.results.map((result) => [caseKey(result.case), result]));
  const comparisons: BenchmarkComparison[] = [];
  for (const result of current.results) {
    const resultCaseKey = caseKey(result.case);
    const baselineResult = baselineResults.get(resultCaseKey);
    if (baselineResult === undefined) {
      continue;
    }
    const baselineMetrics = new Map(baselineResult.metrics.map((metric) => [metric.name, metric]));
    for (const metric of result.metrics) {
      if (metric.lowerIsBetter === false) {
        continue;
      }
      if (metricFilter !== undefined && !metricFilter.has(metric.name)) {
        continue;
      }
      const baselineMetric = baselineMetrics.get(metric.name);
      if (baselineMetric === undefined || baselineMetric.value <= 0) {
        continue;
      }
      const allowed = baselineMetric.value * (1 + tolerance);
      const delta = metric.value - baselineMetric.value;
      const status = benchmarkComparisonStatus(metric, allowed, delta, minimumDeltaMs);
      comparisons.push({
        benchmark: current.benchmark,
        caseKey: resultCaseKey,
        metric: metric.name,
        unit: metric.unit,
        baselineValue: baselineMetric.value,
        currentValue: metric.value,
        delta,
        deltaPercent: (delta / baselineMetric.value) * 100,
        allowedValue: allowed,
        status,
        artifactPath,
      });
    }
  }
  return comparisons;
}

function benchmarkComparisonStatus(
  metric: BenchmarkMetric,
  allowed: number,
  delta: number,
  minimumDeltaMs: number,
): BenchmarkComparisonStatus {
  if (metric.value <= allowed) {
    return "pass";
  }
  if (metric.unit === "ms" && Math.abs(delta) < minimumDeltaMs) {
    return "warn";
  }
  return "fail";
}

function regressionMessage(comparison: BenchmarkComparison): string {
  return [
    `${comparison.benchmark}`,
    `case=${comparison.caseKey}`,
    `metric=${comparison.metric}`,
    `baseline=${formatMetricValue(comparison.baselineValue, comparison.unit)}`,
    `current=${formatMetricValue(comparison.currentValue, comparison.unit)}`,
    `delta=${formatDelta(comparison)}`,
    `allowed=${formatMetricValue(comparison.allowedValue, comparison.unit)}`,
  ].join(" ");
}

function appendBenchmarkSummary(
  comparisons: readonly BenchmarkComparison[],
  artifactPath: string,
  baselinePath: string,
  notes: readonly string[] | undefined,
): Effect.Effect<string | undefined> {
  const summaryPath = process.env.GITHUB_STEP_SUMMARY;
  if (summaryPath === undefined || summaryPath.length === 0) {
    return Effect.succeed(undefined);
  }
  return Effect.tryPromise({
    try: () =>
      appendFile(
        summaryPath,
        benchmarkSummaryMarkdown(comparisons, artifactPath, baselinePath, notes),
      ),
    catch: (cause) =>
      new BenchmarkArtifactError({
        message: `Failed to append benchmark summary: ${String(cause)}`,
        cause,
      }),
  }).pipe(Effect.as(summaryPath), Effect.orDie);
}

function benchmarkSummaryMarkdown(
  comparisons: readonly BenchmarkComparison[],
  artifactPath: string,
  baselinePath: string,
  notes: readonly string[] | undefined,
): string {
  const title = comparisons[0]?.benchmark ?? "benchmark";
  const lines = [
    `### Benchmark: ${escapeMarkdown(title)}`,
    "",
    `Artifact: \`${artifactPath}\``,
    "",
    `Baseline: \`${baselinePath}\``,
    "",
    "| status | metric | case | current | baseline | delta | artifact |",
    "| --- | --- | --- | ---: | ---: | ---: | --- |",
  ];
  for (const comparison of comparisons) {
    lines.push(
      `| ${[
        comparison.status,
        `\`${escapeMarkdown(comparison.metric)}\``,
        `\`${escapeMarkdown(comparison.caseKey)}\``,
        formatMetricValue(comparison.currentValue, comparison.unit),
        formatMetricValue(comparison.baselineValue, comparison.unit),
        formatDelta(comparison),
        `\`${artifactPath}\``,
      ].join(" | ")} |`,
    );
  }
  if (comparisons.length === 0) {
    lines.push("| pass | no matching metrics | n/a | n/a | n/a | n/a | `" + artifactPath + "` |");
  }
  if (notes !== undefined && notes.length > 0) {
    lines.push("", "Notes:");
    for (const note of notes) {
      lines.push(`- ${escapeMarkdown(note)}`);
    }
  }
  return `${lines.join("\n")}\n\n`;
}

function regressionTolerance(): number {
  const value = process.env.VS_BENCH_REGRESSION_TOLERANCE;
  if (value === undefined || value.length === 0) {
    return 0.1;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0.1;
}

function regressionMinimumDeltaMs(): number {
  const value = process.env.VS_BENCH_REGRESSION_MIN_DELTA_MS;
  if (value === undefined || value.length === 0) {
    return 0;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0;
}

function regressionReportOnly(): boolean {
  return process.env.VS_BENCH_REGRESSION_REPORT_ONLY === "1";
}

function regressionMetricFilter(): ReadonlySet<string> | undefined {
  const value = process.env.VS_BENCH_REGRESSION_METRICS;
  if (value === undefined || value.length === 0) {
    return undefined;
  }
  const names = value
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
  return names.length === 0 ? undefined : new Set(names);
}

function caseKey(value: Readonly<Record<string, BenchmarkPrimitive>>): string {
  return Object.entries(value)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, entry]) => `${key}=${String(entry)}`)
    .join(",");
}

function formatMetricValue(value: number, unit: BenchmarkMetric["unit"]): string {
  const formatted = unit === "count" || unit === "bytes" ? value.toFixed(0) : value.toFixed(2);
  return unit === "ratio" ? formatted : `${formatted}${unit}`;
}

function formatDelta(comparison: BenchmarkComparison): string {
  return `${comparison.deltaPercent.toFixed(1)}% (${formatMetricValue(comparison.delta, comparison.unit)})`;
}

function escapeMarkdown(value: string): string {
  return value.replaceAll("|", "\\|").replaceAll("`", "\\`");
}

function isMetricUnit(value: unknown): value is BenchmarkMetric["unit"] {
  return value === "ms" || value === "count" || value === "bytes" || value === "ratio";
}

function isBenchmarkPrimitive(value: unknown): value is BenchmarkPrimitive {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "boolean"
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
