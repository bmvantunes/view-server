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
};

const config: BenchConfig = {
  rows: envNumber("VS_CHDB_SQL_MIRROR_ROWS", 100_000),
  columns: envNumber("VS_CHDB_SQL_MIRROR_COLUMNS", 25),
  mutations: envNumber("VS_CHDB_SQL_MIRROR_MUTATIONS", 10_000),
  gc: process.env.VS_CHDB_SQL_MIRROR_GC === "1",
};

void Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `chdb sql mirror benchmark rows=${config.rows} columns=${config.columns} mutations=${config.mutations} gc=${config.gc ? "on" : "off"}`,
    );
    const rows = makeRows(config.rows, config.columns);
    const mutations = makeMutations(config.mutations, config.rows, config.columns);
    const session = new Session();
    try {
      forceGc(config.gc);
      const before = process.memoryUsage();
      const mirror = new ChdbSqlMirror(session, `bench_mirror_${Date.now()}`);
      const init = timed(() =>
        mirror.init({
          idField: "id",
          rows,
          version: 1n,
        }),
      );
      const apply = timed(() => mirror.applyMutations(mutations));
      forceGc(config.gc);
      const after = process.memoryUsage();
      const result: BenchmarkResult = {
        case: {
          operation: "chdbSqlMirror",
          scenario: "init-and-apply",
          rows: config.rows,
          columns: config.columns,
          mutations: config.mutations,
        },
        metrics: [
          { name: "initMs", value: init.ms, unit: "ms" },
          { name: "applyMutationsMs", value: apply.ms, unit: "ms" },
          {
            name: "columns",
            value: mirror.columns.length,
            unit: "count",
            lowerIsBetter: false,
          },
          { name: "heapDeltaBytes", value: after.heapUsed - before.heapUsed, unit: "bytes" },
          { name: "rssDeltaBytes", value: after.rss - before.rss, unit: "bytes" },
        ] satisfies BenchmarkMetric[],
      };
      const artifact = yield* writeBenchmarkArtifact(
        "chdb-sql-mirror",
        {
          rows: config.rows,
          columns: config.columns,
          mutations: config.mutations,
          gc: config.gc,
        },
        [result],
        {
          notes: [
            "Measures chDB SQL mirror streaming column inference and one-pass JSONEachRow payload construction.",
            "Run with --expose-gc and VS_CHDB_SQL_MIRROR_GC=1 for a stronger retained heap signal.",
          ],
        },
      );
      yield* Effect.logInfo(
        [
          `chdb sql mirror result`,
          `initMs=${init.ms.toFixed(2)}`,
          `applyMutationsMs=${apply.ms.toFixed(2)}`,
          `columns=${mirror.columns.length}`,
          `heapDeltaBytes=${after.heapUsed - before.heapUsed}`,
          `rssDeltaBytes=${after.rss - before.rss}`,
          `artifact=${artifact.artifactPath}`,
          `baselineCompared=${artifact.compared}`,
        ].join(" "),
      );
      mirror.drop();
    } finally {
      session.cleanup();
    }
  }),
);

function timed(run: () => void): { readonly ms: number } {
  const started = performance.now();
  run();
  return { ms: performance.now() - started };
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
