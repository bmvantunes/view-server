import { Session } from "chdb";
import * as Effect from "effect/Effect";
import { performance } from "node:perf_hooks";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";
import type { RuntimeRow } from "../src/protocol/index.ts";
import { ChdbSqlMirror } from "../src/snapshot/chdb-sql-mirror.ts";
import type { MutationLogEntry } from "../src/worker/mutation-log.ts";

type BenchConfig = {
  readonly rows: number;
  readonly columns: number;
  readonly mutations: number;
  readonly gc: boolean;
  readonly compareLegacy: boolean;
};

const config: BenchConfig = {
  rows: envNumber("VS_CHDB_SQL_MIRROR_ROWS", 100_000),
  columns: envNumber("VS_CHDB_SQL_MIRROR_COLUMNS", 25),
  mutations: envNumber("VS_CHDB_SQL_MIRROR_MUTATIONS", 10_000),
  gc: process.env.VS_CHDB_SQL_MIRROR_GC === "1",
  compareLegacy: process.env.VS_CHDB_SQL_MIRROR_COMPARE_LEGACY === "1",
};

void Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `chdb sql mirror benchmark rows=${config.rows} columns=${config.columns} mutations=${config.mutations} gc=${config.gc ? "on" : "off"} compareLegacy=${config.compareLegacy ? "on" : "off"}`,
    );
    const rows = makeRows(config.rows, config.columns);
    const mutations = makeMutations(config.mutations, config.rows, config.columns);
    const session = new Session();
    const mirrors: ChdbSqlMirror[] = [];
    try {
      forceGc(config.gc);
      const before = process.memoryUsage();
      const batched = runMirror(session, mirrors, rows, mutations, "batched");
      const legacy = config.compareLegacy
        ? runMirror(session, mirrors, rows, mutations, "legacy-single-mutation")
        : undefined;
      forceGc(config.gc);
      const after = process.memoryUsage();
      const metrics: BenchmarkMetric[] = [
        { name: "initMs", value: batched.initMs, unit: "ms" },
        { name: "applyMutationsMs", value: batched.applyMs, unit: "ms" },
        {
          name: "batchedMutationsPerSecond",
          value: perSecond(config.mutations, batched.applyMs),
          unit: "count",
          lowerIsBetter: false,
        },
        {
          name: "columns",
          value: batched.columns,
          unit: "count",
          lowerIsBetter: false,
        },
        { name: "heapDeltaBytes", value: after.heapUsed - before.heapUsed, unit: "bytes" },
        { name: "rssDeltaBytes", value: after.rss - before.rss, unit: "bytes" },
      ];
      if (legacy !== undefined) {
        metrics.push(
          { name: "legacyInitMs", value: legacy.initMs, unit: "ms" },
          { name: "legacySingleMutationApplyMs", value: legacy.applyMs, unit: "ms" },
          {
            name: "legacyMutationsPerSecond",
            value: perSecond(config.mutations, legacy.applyMs),
            unit: "count",
            lowerIsBetter: false,
          },
          {
            name: "applySpeedupRatio",
            value:
              batched.applyMs === 0 ? Number.MAX_SAFE_INTEGER : legacy.applyMs / batched.applyMs,
            unit: "ratio",
            lowerIsBetter: false,
          },
        );
      }
      const result: BenchmarkResult = {
        case: {
          operation: "chdbSqlMirror",
          scenario: "init-and-apply",
          rows: config.rows,
          columns: config.columns,
          mutations: config.mutations,
        },
        metrics,
      };
      const artifact = yield* writeBenchmarkArtifact(
        "chdb-sql-mirror",
        {
          rows: config.rows,
          columns: config.columns,
          mutations: config.mutations,
          gc: config.gc,
          compareLegacy: config.compareLegacy,
        },
        [result],
        {
          notes: [
            "Measures chDB SQL mirror streaming column inference and one-pass JSONEachRow payload construction.",
            "When VS_CHDB_SQL_MIRROR_COMPARE_LEGACY=1, compares one applyMutations call against the legacy one-mutation apply loop.",
            "Run with --expose-gc and VS_CHDB_SQL_MIRROR_GC=1 for a stronger retained heap signal.",
          ],
        },
      );
      yield* Effect.logInfo(
        [
          `chdb sql mirror result`,
          `initMs=${batched.initMs.toFixed(2)}`,
          `applyMutationsMs=${batched.applyMs.toFixed(2)}`,
          `batchedMutationsPerSecond=${perSecond(config.mutations, batched.applyMs).toFixed(2)}`,
          ...(legacy === undefined
            ? []
            : [
                `legacySingleMutationApplyMs=${legacy.applyMs.toFixed(2)}`,
                `legacyMutationsPerSecond=${perSecond(config.mutations, legacy.applyMs).toFixed(2)}`,
                `applySpeedupRatio=${(legacy.applyMs / batched.applyMs).toFixed(2)}`,
              ]),
          `columns=${batched.columns}`,
          `heapDeltaBytes=${after.heapUsed - before.heapUsed}`,
          `rssDeltaBytes=${after.rss - before.rss}`,
          `artifact=${artifact.artifactPath}`,
          `baselineCompared=${artifact.compared}`,
        ].join(" "),
      );
      for (const mirror of mirrors) {
        mirror.drop();
      }
    } finally {
      session.cleanup();
    }
  }),
);

function runMirror(
  session: Session,
  mirrors: ChdbSqlMirror[],
  rows: readonly RuntimeRow[],
  mutations: readonly MutationLogEntry[],
  mode: "batched" | "legacy-single-mutation",
): { readonly initMs: number; readonly applyMs: number; readonly columns: number } {
  const mirror = new ChdbSqlMirror(session, `bench_mirror_${mode}_${Date.now()}`);
  mirrors.push(mirror);
  const init = timed(() =>
    mirror.init({
      idField: "id",
      rows,
      version: 1n,
    }),
  );
  const apply = timed(() => {
    if (mode === "batched") {
      mirror.applyMutations(mutations);
      return;
    }
    for (const mutation of mutations) {
      mirror.applyMutations([mutation]);
    }
  });
  return {
    initMs: init.ms,
    applyMs: apply.ms,
    columns: mirror.columns.length,
  };
}

function timed(run: () => void): { readonly ms: number } {
  const started = performance.now();
  run();
  return { ms: performance.now() - started };
}

function perSecond(count: number, ms: number): number {
  return ms === 0 ? Number.MAX_SAFE_INTEGER : count / (ms / 1_000);
}

function makeRows(count: number, columns: number): readonly RuntimeRow[] {
  return Array.from({ length: count }, (_, index) => row(index, columns));
}

function makeMutations(
  count: number,
  existingRows: number,
  columns: number,
): readonly MutationLogEntry[] {
  return Array.from({ length: count }, (_, index): MutationLogEntry => {
    const id = `row-${index % existingRows}`;
    return {
      version: BigInt(index + 2),
      kind: "update",
      id,
      after: row(index + existingRows, columns, id),
      changedFields: new Set(["id", "c0", "c1", "flag"]),
    };
  });
}

function row(index: number, columns: number, id = `row-${index}`): RuntimeRow {
  const next: RuntimeRow = {
    id,
    symbol: `sym-${index % 128}`,
    flag: index % 2 === 0,
  };
  for (let column = 0; column < columns; column++) {
    next[`c${column}`] =
      column % 5 === 0
        ? `v-${index % 1_024}`
        : column % 5 === 1
          ? index * (column + 1)
          : column % 5 === 2
            ? BigInt(index + column)
            : column % 5 === 3
              ? index % 17 === 0
                ? null
                : index + column / 100
              : index % 2 === 0;
  }
  return next;
}

function forceGc(enabled: boolean): void {
  if (enabled) {
    globalThis.gc?.();
  }
}

function envNumber(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined || value.length === 0) {
    return fallback;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.trunc(parsed) : fallback;
}
