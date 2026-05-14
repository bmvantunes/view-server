import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { mkdir, readFile, writeFile } from "node:fs/promises";
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
  readonly results: readonly BenchmarkResult[];
};

export type BenchmarkArtifactResult = {
  readonly artifactPath: string;
  readonly compared: boolean;
  readonly regressionCount: number;
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
): Effect.Effect<BenchmarkArtifactResult> {
  return Effect.fn("view-server.bench.artifact.write")(function* () {
    const generatedAt = DateTime.formatIso(yield* DateTime.now);
    const artifact: BenchmarkArtifact = {
      schemaVersion: 1,
      benchmark,
      generatedAt,
      config,
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
      };
    }

    const baseline = yield* readBenchmarkArtifact(baselinePath);
    const tolerance = regressionTolerance();
    const regressions = benchmarkRegressions(
      baseline,
      artifact,
      tolerance,
      regressionMetricFilter(),
    );
    if (regressions.length > 0) {
      return yield* Effect.die(
        new Error(
          [
            `Benchmark regression exceeded ${(tolerance * 100).toFixed(1)}% tolerance`,
            `artifact=${artifactPath}`,
            `baseline=${baselinePath}`,
            ...regressions,
          ].join("\n"),
        ),
      );
    }

    return {
      artifactPath,
      compared: true,
      regressionCount: 0,
    };
  })();
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
  const results = decodeBenchmarkResults(value.results);
  if (schemaVersion !== 1 || typeof benchmark !== "string" || typeof generatedAt !== "string") {
    throw new Error("benchmark artifact metadata is invalid");
  }
  return {
    schemaVersion,
    benchmark,
    generatedAt,
    config,
    results,
  };
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

function benchmarkRegressions(
  baseline: BenchmarkArtifact,
  current: BenchmarkArtifact,
  tolerance: number,
  metricFilter: ReadonlySet<string> | undefined,
): readonly string[] {
  const baselineResults = new Map(baseline.results.map((result) => [caseKey(result.case), result]));
  const regressions: string[] = [];
  for (const result of current.results) {
    const baselineResult = baselineResults.get(caseKey(result.case));
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
      if (metric.value > allowed) {
        regressions.push(
          `${current.benchmark} case=${caseKey(result.case)} metric=${metric.name} baseline=${baselineMetric.value.toFixed(2)} current=${metric.value.toFixed(2)} allowed=${allowed.toFixed(2)}`,
        );
      }
    }
  }
  return regressions;
}

function regressionTolerance(): number {
  const value = process.env.VS_BENCH_REGRESSION_TOLERANCE;
  if (value === undefined || value.length === 0) {
    return 0.1;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : 0.1;
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
