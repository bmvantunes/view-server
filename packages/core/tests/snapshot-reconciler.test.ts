import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import { snapshotBackendFailed } from "../src/errors.ts";
import type { RuntimeRawQuery, RuntimeRow } from "../src/protocol/index.ts";
import type { SnapshotBackend, SnapshotBackendResult } from "../src/snapshot/index.ts";
import { MutationStore } from "../src/worker/mutation-store.ts";
import { makeSnapshotReconciler } from "../src/worker/snapshot-reconciler.ts";

describe("SnapshotReconciler", () => {
  const query = {
    fields: { id: true, value: true, exact: true },
    orderBy: [{ field: "id", direction: "asc" }],
  } satisfies RuntimeRawQuery;

  it.effect("accepts an exact backend snapshot", () =>
    Effect.gen(function* () {
      const rows = [{ id: "a", value: 1 }];
      const reconciler = makeTestReconciler({
        rows,
        candidate: { rows, totalRows: 1, backendVersion: 2n },
      });

      const result = yield* reconciler.query({ query, targetVersion: 2n });

      expect(result.source).toBe("backend");
      expect(result.backendVersion).toBe(2n);
      expect(result.rows).toEqual(rows);
    }),
  );

  it.effect("replays backend lag when the mutation log covers the gap", () =>
    Effect.gen(function* () {
      const store = new MutationStore({ idField: "id", mutationLogSize: 10 });
      const baseRows = [
        { id: "a", value: 1, exact: BigDecimal.fromStringUnsafe("1.10") },
        { id: "b", value: 2, exact: BigDecimal.fromStringUnsafe("2.20") },
      ];
      store.loadInitialRows(baseRows);
      store.publish({ id: "a", value: 3, exact: BigDecimal.fromStringUnsafe("3.30") }, "a");
      store.deleteById("b");
      store.publish({ id: "c", value: 4, exact: BigDecimal.fromStringUnsafe("4.40") }, "c");
      const reconciler = makeTestReconciler({
        rows: store.rows(),
        candidate: {
          rows: baseRows,
          totalRows: 2,
          backendVersion: 0n,
          replayRows: baseRows,
        },
        canReplay: (fromVersion, toVersion) => store.canReplay(fromVersion, toVersion),
        replayRowsFrom: (rows, fromVersion, toVersion) =>
          store.replayRowsFrom(rows, fromVersion, toVersion),
      });

      const result = yield* reconciler.query({ query, targetVersion: store.version() });

      expect(result.source).toBe("replay");
      expect(result.totalRows).toBe(2);
      expect(result.rows.map((row) => row.id)).toEqual(["a", "c"]);
      expect(
        BigDecimal.equals(
          expectBigDecimal(result.rows[0]?.exact),
          BigDecimal.fromStringUnsafe("3.30"),
        ),
      ).toBe(true);
    }),
  );

  it.effect(
    "falls back to authoritative memory for future backend versions, gaps, and failures",
    () =>
      Effect.gen(function* () {
        const rows = [{ id: "memory", value: 10 }];
        const future = makeTestReconciler({
          rows,
          candidate: { rows: [{ id: "future", value: 999 }], totalRows: 1, backendVersion: 3n },
        });
        const gap = makeTestReconciler({
          rows,
          candidate: {
            rows: [{ id: "stale", value: 1 }],
            totalRows: 1,
            backendVersion: 0n,
            replayRows: [{ id: "stale", value: 1 }],
          },
          canReplay: () => false,
        });
        const failed = makeTestReconciler({
          rows,
          failure: true,
        });

        const futureResult = yield* future.query({ query, targetVersion: 1n });
        const gapResult = yield* gap.query({ query, targetVersion: 1n });
        const failedResult = yield* failed.query({ query, targetVersion: 1n });

        expect(futureResult.source).toBe("memory");
        expect(gapResult.source).toBe("memory");
        expect(failedResult.source).toBe("memory");
        expect(failedResult.backendFailed).toBe(true);
        expect(futureResult.rows).toEqual(rows);
        expect(gapResult.rows).toEqual(rows);
        expect(failedResult.rows).toEqual(rows);
      }),
  );
});

function makeTestReconciler(args: {
  readonly rows: readonly RuntimeRow[];
  readonly candidate?: SnapshotBackendResult | undefined;
  readonly failure?: boolean | undefined;
  readonly canReplay?: ((fromVersion: bigint, toVersion: bigint) => boolean) | undefined;
  readonly replayRowsFrom?:
    | ((
        baseRows: readonly RuntimeRow[],
        fromVersion: bigint,
        toVersion: bigint,
      ) => readonly RuntimeRow[])
    | undefined;
}) {
  const backend: SnapshotBackend = {
    init: () => Effect.void,
    applyBatch: () => Effect.void,
    snapshot: () =>
      args.failure === true
        ? Effect.fail(snapshotBackendFailed("orders", "backend failed"))
        : Effect.succeed(args.candidate ?? { rows: [], totalRows: 0, backendVersion: 0n }),
    close: () => Effect.void,
  };
  return makeSnapshotReconciler({
    topic: "orders",
    idField: "id",
    backend,
    rows: () => args.rows,
    canReplay: args.canReplay ?? (() => false),
    replayRowsFrom: args.replayRowsFrom ?? ((rows) => rows),
  });
}

function expectBigDecimal(value: unknown): BigDecimal.BigDecimal {
  if (!BigDecimal.isBigDecimal(value)) {
    throw new Error("Expected BigDecimal");
  }
  return value;
}
