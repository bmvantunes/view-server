import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { snapshotBackendFailed } from "../src/errors.ts";
import {
  createMemorySnapshotBackend,
  type SnapshotBackend,
  type SnapshotBackendResult,
} from "../src/snapshot/snapshot-backend.ts";
import type { RuntimeQuery } from "../src/protocol/index.ts";
import { makeTopicWorkerCore } from "../src/worker/index.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

const StatusOrder = Schema.Struct({
  id: Schema.String,
  status: Schema.Literals(["OPEN", "CLOSED"]),
  label: Schema.String,
});

const query = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 5,
} satisfies RuntimeQuery;

const firstRowQuery = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 1,
} satisfies RuntimeQuery;

describe("topic worker version fence", () => {
  it.effect("falls back to authoritative memory when the snapshot backend is behind", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          snapshotBackend: laggingBackend(),
        },
      );

      yield* worker.publish({ id: "o-1", symbol: "AAPL", price: 100 });
      yield* worker.publish({ id: "o-2", symbol: "MSFT", price: 200 });

      const [snapshot] = yield* worker
        .subscribe("sub-1", query)
        .pipe(Stream.take(1), Stream.runCollect);

      if (snapshot?.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.rows).toEqual([
        { id: "o-2", price: 200 },
        { id: "o-1", price: 100 },
      ]);
      expect(snapshot.meta.version).toBe("2");
      expect(snapshot.meta.backendVersion).toBeUndefined();
      expect(snapshot.meta.totalRows).toBe(2);
    }).pipe(Effect.scoped),
  );

  it.effect("uses a fenced backend snapshot when backend and worker versions match", () =>
    Effect.gen(function* () {
      const backend = createMemorySnapshotBackend();
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          snapshotBackend: backend,
          initialRows: [{ id: "o-1", symbol: "AAPL", price: 100 }],
        },
      );

      const [snapshot] = yield* worker
        .subscribe("sub-2", query)
        .pipe(Stream.take(1), Stream.runCollect);

      if (snapshot?.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.meta.version).toBe("0");
      expect(snapshot.meta.backendVersion).toBe("0");
    }).pipe(Effect.scoped),
  );

  it.effect("uses a fenced backend snapshot for one-shot queries", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          snapshotBackend: matchingBackend(),
          initialRows: [{ id: "o-1", symbol: "AAPL", price: 100 }],
        },
      );

      const result = yield* worker.query(query);
      expect(result).toEqual({
        rows: [{ id: "backend", price: 999 }],
        totalRows: 1,
        version: "0",
      });
    }).pipe(Effect.scoped),
  );

  it.effect("replays mutation-log entries when the backend is behind but covered", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          initialRows: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          snapshotBackend: replayableBackend([{ id: "o-1", symbol: "AAPL", price: 100 }], 0n),
        },
      );

      yield* worker.publish({ id: "o-2", symbol: "MSFT", price: 200 });

      const [snapshot] = yield* worker
        .subscribe("sub-replay", query)
        .pipe(Stream.take(1), Stream.runCollect);

      if (snapshot?.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.meta.version).toBe("1");
      expect(snapshot.meta.backendVersion).toBe("0");
      expect(snapshot.rows).toEqual([
        { id: "o-2", price: 200 },
        { id: "o-1", price: 100 },
      ]);
      expect(snapshot.meta.totalRows).toBe(2);
    }).pipe(Effect.scoped),
  );

  it.effect("replays all mutation-log entries above the backend contiguous version", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          snapshotBackend: replayableBackend([{ id: "o-1", symbol: "AAPL", price: 100 }], 1n),
        },
      );

      yield* worker.publish({ id: "o-1", symbol: "AAPL", price: 100 });
      yield* worker.publish({ id: "o-2", symbol: "MSFT", price: 200 });
      yield* worker.publish({ id: "o-1", symbol: "AAPL", price: 125 });

      const [snapshot] = yield* worker
        .subscribe("sub-replay-2-through-3", query)
        .pipe(Stream.take(1), Stream.runCollect);

      if (snapshot?.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.meta.version).toBe("3");
      expect(snapshot.meta.backendVersion).toBe("1");
      expect(snapshot.rows).toEqual([
        { id: "o-2", price: 200 },
        { id: "o-1", price: 125 },
      ]);
      expect(snapshot.meta.totalRows).toBe(2);
    }).pipe(Effect.scoped),
  );

  it.effect("falls back to memory when the backend is too far behind for the mutation log", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          initialRows: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          mutationLogSize: 1,
          snapshotBackend: replayableBackend([{ id: "o-1", symbol: "AAPL", price: 100 }], 0n),
        },
      );

      yield* worker.publish({ id: "o-2", symbol: "MSFT", price: 200 });
      yield* worker.publish({ id: "o-3", symbol: "NVDA", price: 300 });

      const [snapshot] = yield* worker
        .subscribe("sub-gap", query)
        .pipe(Stream.take(1), Stream.runCollect);

      if (snapshot?.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.meta.version).toBe("2");
      expect(snapshot.meta.backendVersion).toBeUndefined();
      expect(snapshot.rows).toEqual([
        { id: "o-3", price: 300 },
        { id: "o-2", price: 200 },
        { id: "o-1", price: 100 },
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("falls back to memory when the backend snapshot fails", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          initialRows: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          snapshotBackend: failingSnapshotBackend(),
        },
      );

      const [snapshot] = yield* worker
        .subscribe("sub-failure", query)
        .pipe(Stream.take(1), Stream.runCollect);

      if (snapshot?.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.meta.version).toBe("0");
      expect(snapshot.meta.backendVersion).toBeUndefined();
      expect(snapshot.rows).toEqual([{ id: "o-1", price: 100 }]);
    }).pipe(Effect.scoped),
  );

  it.effect("uses the fenced backend again after a transient snapshot failure recovers", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          initialRows: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          snapshotBackend: transientSnapshotFailureBackend(),
        },
      );

      const first = yield* worker.query(query);
      expect(first).toEqual({
        rows: [{ id: "o-1", price: 100 }],
        totalRows: 1,
        version: "0",
      });

      const second = yield* worker.query(query);
      expect(second).toEqual({
        rows: [{ id: "backend-recovered", price: 999 }],
        totalRows: 1,
        version: "0",
      });
    }).pipe(Effect.scoped),
  );

  it.effect("does not leak stale deleted rows when replaying a backend snapshot", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          initialRows: [
            { id: "o-1", symbol: "AAPL", price: 100 },
            { id: "o-2", symbol: "MSFT", price: 200 },
          ],
          snapshotBackend: replayableBackend(
            [
              { id: "o-1", symbol: "AAPL", price: 100 },
              { id: "o-2", symbol: "MSFT", price: 200 },
            ],
            0n,
          ),
        },
      );

      yield* worker.deleteById("o-1");

      const [snapshot] = yield* worker
        .subscribe("sub-delete-replay", query)
        .pipe(Stream.take(1), Stream.runCollect);

      if (snapshot?.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.meta.version).toBe("1");
      expect(snapshot.meta.backendVersion).toBe("0");
      expect(snapshot.rows).toEqual([{ id: "o-2", price: 200 }]);
      expect(snapshot.meta.totalRows).toBe(1);
    }).pipe(Effect.scoped),
  );

  it.effect("does not block publish or live fanout on a stalled snapshot backend flush", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          initialRows: [{ id: "o-1", symbol: "AAPL", price: 100 }],
          snapshotBackend: stalledApplyBackend([{ id: "o-1", symbol: "AAPL", price: 100 }]),
        },
      );

      const events = yield* worker
        .subscribe("sub-stalled-apply", query)
        .pipe(Stream.toQueue({ capacity: 16 }));
      const snapshot = yield* Queue.take(events);
      expect(snapshot.type).toBe("snapshot");

      yield* worker
        .publish({ id: "o-2", symbol: "MSFT", price: 200 })
        .pipe(Effect.timeout("1 second"));

      const delta = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      if (delta.type !== "delta") {
        throw new Error("Expected delta");
      }
      expect(delta.meta.totalRows).toBe(2);
    }).pipe(Effect.scoped),
  );

  it.effect("falls back to authoritative memory for one-shot queries when backend is behind", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          snapshotBackend: laggingBackend(),
        },
      );

      yield* worker.publish({ id: "o-1", symbol: "AAPL", price: 100 });
      yield* worker.publish({ id: "o-2", symbol: "MSFT", price: 200 });

      const result = yield* worker.query(query);
      expect(result).toEqual({
        rows: [
          { id: "o-2", price: 200 },
          { id: "o-1", price: 100 },
        ],
        totalRows: 2,
        version: "2",
      });
    }).pipe(Effect.scoped),
  );

  it.effect("keeps literal string equality strict while broad strings stay case-insensitive", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: StatusOrder,
        },
        {
          initialRows: [{ id: "o-1", status: "OPEN", label: "Alpha" }],
        },
      );

      const literalResult = yield* worker.query({
        fields: {
          id: true,
          status: true,
        },
        where: {
          field: "status",
          comparator: "equals",
          value: "open",
        },
      });
      expect(literalResult.rows).toEqual([]);

      const broadResult = yield* worker.query({
        fields: {
          id: true,
          label: true,
        },
        where: {
          field: "label",
          comparator: "equals",
          value: "alpha",
        },
      });
      expect(broadResult.rows).toEqual([{ id: "o-1", label: "Alpha" }]);
    }).pipe(Effect.scoped),
  );

  it.effect("emits totalRows-only deltas without visible row churn", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        {
          initialRows: [
            { id: "o-1", symbol: "AAPL", price: 100 },
            { id: "o-2", symbol: "MSFT", price: 200 },
          ],
        },
      );

      const events = yield* worker
        .subscribe("sub-total-rows", firstRowQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));

      const snapshot = yield* Queue.take(events);
      if (snapshot.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.rows).toEqual([{ id: "o-2", price: 200 }]);
      expect(snapshot.meta.totalRows).toBe(2);

      yield* worker.publish({ id: "o-0", symbol: "IBM", price: 50 });

      const delta = yield* Queue.take(events);
      if (delta.type !== "delta") {
        throw new Error("Expected delta");
      }
      expect(delta.ops).toEqual([]);
      expect(delta.meta.totalRows).toBe(3);
      expect(delta.meta.fromVersion).toBe("0");
      expect(delta.meta.toVersion).toBe("1");
    }).pipe(Effect.scoped),
  );
});

function laggingBackend(): SnapshotBackend {
  return {
    init: () => Effect.void,
    applyBatch: () => Effect.void,
    snapshot: (): Effect.Effect<SnapshotBackendResult> =>
      Effect.succeed({
        rows: [],
        totalRows: 0,
        backendVersion: 0n,
      }),
    close: () => Effect.void,
  };
}

function matchingBackend(): SnapshotBackend {
  let backendVersion = 0n;
  return {
    init: (args) =>
      Effect.sync(() => {
        backendVersion = args.version;
      }),
    applyBatch: (args) =>
      Effect.sync(() => {
        backendVersion = args.highestVersion;
      }),
    snapshot: (): Effect.Effect<SnapshotBackendResult> =>
      Effect.succeed({
        rows: [{ id: "backend", price: 999 }],
        totalRows: 1,
        backendVersion,
      }),
    close: () => Effect.void,
  };
}

function replayableBackend(
  replayRows: readonly Record<string, unknown>[],
  backendVersion: bigint,
): SnapshotBackend {
  return {
    init: () => Effect.void,
    applyBatch: () => Effect.void,
    snapshot: (): Effect.Effect<SnapshotBackendResult> =>
      Effect.succeed({
        rows: [{ id: "stale", price: -1 }],
        totalRows: 1,
        backendVersion,
        replayRows,
      }),
    close: () => Effect.void,
  };
}

function failingSnapshotBackend(): SnapshotBackend {
  return {
    init: () => Effect.void,
    applyBatch: () => Effect.void,
    snapshot: () => Effect.fail(snapshotBackendFailed("orders", new Error("boom"))),
    close: () => Effect.void,
  };
}

function transientSnapshotFailureBackend(): SnapshotBackend {
  let failed = false;
  return {
    init: () => Effect.void,
    applyBatch: () => Effect.void,
    snapshot: (): ReturnType<SnapshotBackend["snapshot"]> => {
      if (!failed) {
        failed = true;
        return Effect.fail(snapshotBackendFailed("orders", new Error("transient")));
      }
      return Effect.succeed({
        rows: [{ id: "backend-recovered", price: 999 }],
        totalRows: 1,
        backendVersion: 0n,
      });
    },
    close: () => Effect.void,
  };
}

function stalledApplyBackend(replayRows: readonly Record<string, unknown>[]): SnapshotBackend {
  return {
    init: () => Effect.void,
    applyBatch: () => Effect.never,
    snapshot: (): Effect.Effect<SnapshotBackendResult> =>
      Effect.succeed({
        rows: replayRows,
        totalRows: replayRows.length,
        backendVersion: 0n,
        replayRows,
      }),
    close: () => Effect.void,
  };
}
