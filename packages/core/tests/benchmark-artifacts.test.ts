import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { writeBenchmarkArtifact, type BenchmarkArtifact } from "../bench/benchmark-artifacts.ts";

class BenchmarkArtifactTestError extends Schema.TaggedErrorClass<BenchmarkArtifactTestError>()(
  "BenchmarkArtifactTestError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect),
  },
) {}

describe("benchmark artifacts", () => {
  it.effect("writes benchmark results to the configured artifact path", () =>
    withBenchmarkEnv(
      {
        VS_BENCH_ARTIFACT: undefined,
        VS_BENCH_BASELINE: undefined,
        VS_BENCH_REGRESSION_TOLERANCE: undefined,
        VS_BENCH_REGRESSION_METRICS: undefined,
      },
      Effect.gen(function* () {
        const artifactPath = yield* tempArtifactPath("current.json");
        process.env.VS_BENCH_ARTIFACT = artifactPath;

        const result = yield* writeBenchmarkArtifact(
          "active-view",
          { rows: 1_000, subscriptions: 5 },
          [
            {
              case: { scenario: "hot-key", pageSize: 50 },
              metrics: [{ name: "operationP99Ms", value: 3.5, unit: "ms" }],
            },
          ],
        );

        const artifact = yield* readArtifact(result.artifactPath);
        expect(result).toEqual({
          artifactPath,
          compared: false,
          regressionCount: 0,
        });
        expect(artifact).toMatchObject({
          schemaVersion: 1,
          benchmark: "active-view",
          config: {
            rows: 1_000,
            subscriptions: 5,
          },
          results: [
            {
              case: {
                scenario: "hot-key",
                pageSize: 50,
              },
              metrics: [
                {
                  name: "operationP99Ms",
                  value: 3.5,
                  unit: "ms",
                },
              ],
            },
          ],
        });
        expect(typeof artifact.generatedAt).toBe("string");
      }),
    ),
  );

  it.effect("fails when a selected lower-is-better metric exceeds tolerance", () =>
    withBenchmarkEnv(
      {
        VS_BENCH_ARTIFACT: undefined,
        VS_BENCH_BASELINE: undefined,
        VS_BENCH_REGRESSION_TOLERANCE: undefined,
        VS_BENCH_REGRESSION_METRICS: undefined,
      },
      Effect.gen(function* () {
        const baselinePath = yield* tempArtifactPath("baseline.json");
        const artifactPath = yield* tempArtifactPath("current.json");
        yield* writeJson(baselinePath, {
          schemaVersion: 1,
          benchmark: "active-view",
          generatedAt: "2026-01-01T00:00:00.000Z",
          config: { rows: 1_000 },
          results: [
            {
              case: { scenario: "hot-key", pageSize: 50 },
              metrics: [{ name: "operationP99Ms", value: 100, unit: "ms" }],
            },
          ],
        });
        process.env.VS_BENCH_ARTIFACT = artifactPath;
        process.env.VS_BENCH_BASELINE = baselinePath;
        process.env.VS_BENCH_REGRESSION_TOLERANCE = "0.1";
        process.env.VS_BENCH_REGRESSION_METRICS = "operationP99Ms";

        const exit = yield* writeBenchmarkArtifact("active-view", { rows: 1_000 }, [
          {
            case: { scenario: "hot-key", pageSize: 50 },
            metrics: [{ name: "operationP99Ms", value: 120, unit: "ms" }],
          },
        ]).pipe(Effect.exit);

        expect(Exit.isFailure(exit)).toBe(true);
      }),
    ),
  );

  it.effect("ignores better higher-is-better metrics during regression comparison", () =>
    withBenchmarkEnv(
      {
        VS_BENCH_ARTIFACT: undefined,
        VS_BENCH_BASELINE: undefined,
        VS_BENCH_REGRESSION_TOLERANCE: undefined,
        VS_BENCH_REGRESSION_METRICS: undefined,
      },
      Effect.gen(function* () {
        const baselinePath = yield* tempArtifactPath("baseline.json");
        const artifactPath = yield* tempArtifactPath("current.json");
        yield* writeJson(baselinePath, {
          schemaVersion: 1,
          benchmark: "throughput",
          generatedAt: "2026-01-01T00:00:00.000Z",
          config: { rows: 1_000 },
          results: [
            {
              case: { scenario: "hot-key" },
              metrics: [
                {
                  name: "rowsPerSecond",
                  value: 1_000,
                  unit: "count",
                  lowerIsBetter: false,
                },
              ],
            },
          ],
        });
        process.env.VS_BENCH_ARTIFACT = artifactPath;
        process.env.VS_BENCH_BASELINE = baselinePath;

        const result = yield* writeBenchmarkArtifact("throughput", { rows: 1_000 }, [
          {
            case: { scenario: "hot-key" },
            metrics: [
              {
                name: "rowsPerSecond",
                value: 500,
                unit: "count",
                lowerIsBetter: false,
              },
            ],
          },
        ]);

        expect(result.compared).toBe(true);
      }),
    ),
  );
});

function withBenchmarkEnv<R, E, A>(
  values: Readonly<Record<string, string | undefined>>,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> {
  const previous = new Map(Object.keys(values).map((key) => [key, process.env[key]]));
  for (const [key, value] of Object.entries(values)) {
    setEnv(key, value);
  }
  return effect.pipe(
    Effect.ensuring(
      Effect.sync(() => {
        for (const [key, value] of previous) {
          setEnv(key, value);
        }
      }),
    ),
  );
}

function tempArtifactPath(name: string): Effect.Effect<string> {
  return Effect.tryPromise({
    try: () => mkdtemp(join(tmpdir(), "view-server-bench-")).then((dir) => join(dir, name)),
    catch: (cause) =>
      new BenchmarkArtifactTestError({
        message: `Failed to create benchmark temp path: ${String(cause)}`,
        cause,
      }),
  }).pipe(Effect.orDie);
}

function readArtifact(path: string): Effect.Effect<BenchmarkArtifact> {
  return Effect.tryPromise({
    try: () => readFile(path, "utf8"),
    catch: (cause) =>
      new BenchmarkArtifactTestError({
        message: `Failed to read benchmark artifact: ${String(cause)}`,
        cause,
      }),
  }).pipe(
    Effect.map((json) => parseArtifact(json)),
    Effect.orDie,
  );
}

function writeJson(path: string, value: BenchmarkArtifact): Effect.Effect<void> {
  return Effect.tryPromise({
    try: () => writeFile(path, `${JSON.stringify(value, null, 2)}\n`),
    catch: (cause) =>
      new BenchmarkArtifactTestError({
        message: `Failed to write benchmark JSON: ${String(cause)}`,
        cause,
      }),
  }).pipe(Effect.orDie);
}

function parseArtifact(json: string): BenchmarkArtifact {
  const value = JSON.parse(json);
  if (!isBenchmarkArtifact(value)) {
    throw new Error("Invalid benchmark artifact");
  }
  return value;
}

function isBenchmarkArtifact(value: unknown): value is BenchmarkArtifact {
  return (
    isRecord(value) &&
    value.schemaVersion === 1 &&
    typeof value.benchmark === "string" &&
    typeof value.generatedAt === "string" &&
    isRecord(value.config) &&
    Array.isArray(value.results)
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function setEnv(key: string, value: string | undefined): void {
  if (value === undefined) {
    delete process.env[key];
  } else {
    process.env[key] = value;
  }
}
