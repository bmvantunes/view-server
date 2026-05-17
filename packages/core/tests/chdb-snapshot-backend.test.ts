import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { defineConfig, VIEW_SERVER_HEALTH_TOPIC } from "../src/config/index.ts";
import { snapshotBackendFailed, type ViewServerError } from "../src/errors.ts";
import type { RuntimeQuery, RuntimeRow } from "../src/protocol/index.ts";
import { createChdbSnapshotBackend } from "../src/snapshot/chdb-backend.ts";
import type { SnapshotBackend, SnapshotBackendHealth } from "../src/snapshot/index.ts";
import { makeViewServerRuntime, type HealthResponse } from "../src/server/index.ts";
import { makeTopicWorkerCore } from "../src/worker/index.ts";
import type { MutationLogEntry, WorkerVersion } from "../src/worker/mutation-log.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

const DecimalOrder = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.BigDecimal,
});

const pagedQuery = {
  fields: {
    id: true,
    symbol: true,
    price: true,
  },
  where: {
    field: "symbol",
    comparator: "contains",
    value: "a",
  },
  orderBy: [{ field: "price", direction: "asc" }],
  offset: 1,
  limit: 2,
} satisfies RuntimeQuery;

const escapedStringQuery = {
  fields: {
    id: true,
    symbol: true,
  },
  where: {
    field: "symbol",
    comparator: "equals",
    value: "O'Reilly \\ Books",
  },
  limit: 5,
} satisfies RuntimeQuery;

const strictLiteralStringQuery = {
  fields: {
    id: true,
    status: true,
  },
  where: {
    field: "status",
    comparator: "equals",
    value: "open",
  },
  limit: 5,
} satisfies RuntimeQuery;

const broadStringQuery = {
  fields: {
    id: true,
    symbol: true,
  },
  where: {
    field: "symbol",
    comparator: "equals",
    value: "aapl",
  },
  limit: 5,
} satisfies RuntimeQuery;

const allOrdersQuery = {
  fields: {
    id: true,
    symbol: true,
    price: true,
  },
  orderBy: [{ field: "id", direction: "asc" }],
  limit: 10,
} satisfies RuntimeQuery;

const healthChdbQuery = {
  fields: {
    id: true,
    topic: true,
    chdbStatus: true,
    chdbPid: true,
    chdbRestarts: true,
    chdbPendingRequests: true,
    chdbLastError: true,
    chdbBackendVersion: true,
    status: true,
  },
  orderBy: [{ field: "id", direction: "asc" }],
  limit: 10,
} satisfies RuntimeQuery;

const allCustomIdOrdersQuery = {
  fields: {
    orderId: true,
    symbol: true,
    price: true,
  },
  orderBy: [{ field: "orderId", direction: "asc" }],
  limit: 10,
} satisfies RuntimeQuery;

const decimalQuery = {
  fields: {
    id: true,
    price: true,
  },
  where: {
    field: "price",
    comparator: "greater_than",
    value: BigDecimal.fromStringUnsafe("10.000000000000000001"),
  },
  orderBy: [{ field: "price", direction: "asc" }],
  limit: 5,
} satisfies RuntimeQuery;

const decimalGroupedQuery = {
  groupBy: ["symbol"],
  aggregates: {
    totalPrice: {
      aggFunc: "sum",
      field: "price",
    },
    maxPrice: {
      aggFunc: "max",
      field: "price",
    },
  },
  orderBy: [{ field: "symbol", direction: "asc" }],
  limit: 5,
} satisfies RuntimeQuery;

const groupedQuery = {
  groupBy: ["symbol"],
  aggregates: {
    trades: {
      aggFunc: "count",
      field: "id",
    },
    totalPrice: {
      aggFunc: "sum",
      field: "price",
    },
    labels: {
      aggFunc: "string_concat",
      field: "id",
      joiner: ",",
      sort: "asc",
    },
  },
  orderBy: [{ field: "symbol", direction: "asc" }],
  limit: 10,
} satisfies RuntimeQuery;

describe("chDB snapshot backend", () => {
  it.effect("serves raw filtered sorted paginated snapshots from chDB", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 7n,
        rows: versionedRows([
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "MSFT", price: 200 },
          { id: "o-3", symbol: "AMZN", price: 150 },
          { id: "o-4", symbol: "NVDA", price: 300 },
          { id: "o-5", symbol: "O'Reilly \\ Books", price: 50 },
        ]),
      });

      const result = yield* backend.snapshot({ query: pagedQuery, targetVersion: 7n });

      expect(result.backendVersion).toBe(7n);
      expect(result.totalRows).toBe(3);
      expect(result.rows).toEqual([
        { id: "o-3", symbol: "AMZN", price: 150 },
        { id: "o-4", symbol: "NVDA", price: 300 },
      ]);

      const escaped = yield* backend.snapshot({ query: escapedStringQuery, targetVersion: 7n });
      expect(escaped.totalRows).toBe(1);
      expect(escaped.rows).toEqual([{ id: "o-5", symbol: "O'Reilly \\ Books" }]);
    }).pipe(Effect.scoped),
  );

  it.effect("serves grouped aggregate snapshots from chDB", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 3n,
        rows: versionedRows([
          { id: "a-1", symbol: "AAPL", price: 100 },
          { id: "a-2", symbol: "AAPL", price: 125 },
          { id: "m-1", symbol: "MSFT", price: 200 },
        ]),
      });

      const result = yield* backend.snapshot({ query: groupedQuery, targetVersion: 3n });

      expect(result.backendVersion).toBe(3n);
      expect(result.totalRows).toBe(2);
      expect(result.rows).toEqual([
        { symbol: "AAPL", trades: 2, totalPrice: 225, labels: "a-1,a-2" },
        { symbol: "MSFT", trades: 1, totalPrice: 200, labels: "m-1" },
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("keeps literal string equality strict in chDB snapshots", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        literalStringFields: new Set(["status"]),
        rows: versionedRows([{ id: "o-1", symbol: "AAPL", status: "OPEN" }]),
      });

      const literalResult = yield* backend.snapshot({
        query: strictLiteralStringQuery,
        targetVersion: 1n,
      });
      expect(literalResult.rows).toEqual([]);

      const broadResult = yield* backend.snapshot({
        query: broadStringQuery,
        targetVersion: 1n,
      });
      expect(broadResult.rows).toEqual([{ id: "o-1", symbol: "AAPL" }]);
    }).pipe(Effect.scoped),
  );

  it.effect("applies mutation batches through an append-only latest-row view", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows([
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "MSFT", price: 200 },
        ]),
      });

      yield* backend.applyBatch({
        highestVersion: 4n,
        mutations: [
          {
            version: 2n,
            kind: "update",
            id: "o-1",
            before: { id: "o-1", symbol: "AAPL", price: 100 },
            after: { id: "o-1", symbol: "AAPL", price: 125 },
            changedFields: new Set(["price"]),
          },
          {
            version: 3n,
            kind: "delete",
            id: "o-2",
            before: { id: "o-2", symbol: "MSFT", price: 200 },
            changedFields: new Set(["id"]),
          },
          {
            version: 4n,
            kind: "insert",
            id: "o-3",
            after: { id: "o-3", symbol: "AMZN", price: 150 },
            changedFields: new Set(["id", "symbol", "price"]),
          },
        ],
      });
      yield* flushChdb();

      const result = yield* backend.snapshot({ query: allOrdersQuery, targetVersion: 4n });

      expect(result.backendVersion).toBe(4n);
      expect(result.replayRows).toBeUndefined();
      expect(result.totalRows).toBe(2);
      expect(result.rows).toEqual([
        { id: "o-1", symbol: "AAPL", price: 125 },
        { id: "o-3", symbol: "AMZN", price: 150 },
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("creates the chDB table from the first async mutation batch", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "orderId",
        version: 0n,
        rows: [],
      });

      yield* backend.applyBatch({
        highestVersion: 2n,
        mutations: [
          {
            version: 1n,
            kind: "insert",
            id: "o-1",
            after: { orderId: "o-1", symbol: "AAPL", price: 100 },
            changedFields: new Set(["orderId", "symbol", "price"]),
          },
          {
            version: 2n,
            kind: "delete",
            id: "o-1",
            before: { orderId: "o-1", symbol: "AAPL", price: 100 },
            changedFields: new Set(["orderId"]),
          },
        ],
      });
      yield* flushChdb();

      const result = yield* backend.snapshot({
        query: allCustomIdOrdersQuery,
        targetVersion: 2n,
      });

      expect(result.backendVersion).toBe(2n);
      expect(result.totalRows).toBe(0);
      expect(result.rows).toEqual([]);
    }).pipe(Effect.scoped),
  );

  it.effect("only advances backendVersion through contiguous mutation versions", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 0n,
        rows: [],
      });

      yield* backend.applyBatch({
        highestVersion: 2n,
        mutations: [
          {
            version: 2n,
            kind: "update",
            id: "o-1",
            before: { id: "o-1", symbol: "AAPL", price: 100 },
            after: { id: "o-1", symbol: "AAPL", price: 125 },
            changedFields: new Set(["price"]),
          },
        ],
      });
      yield* flushChdb();

      const gapResult = yield* backend.snapshot({
        query: allOrdersQuery,
        targetVersion: 2n,
      });
      expect(gapResult.backendVersion).toBe(0n);
      expect(gapResult.rows).toEqual([]);

      yield* backend.applyBatch({
        highestVersion: 1n,
        mutations: [
          {
            version: 1n,
            kind: "insert",
            id: "o-1",
            after: { id: "o-1", symbol: "AAPL", price: 100 },
            changedFields: new Set(["id", "symbol", "price"]),
          },
        ],
      });
      yield* flushChdb();

      const repairedResult = yield* backend.snapshot({
        query: allOrdersQuery,
        targetVersion: 2n,
      });
      expect(repairedResult.backendVersion).toBe(2n);
      expect(repairedResult.rows).toEqual([{ id: "o-1", symbol: "AAPL", price: 125 }]);
    }).pipe(Effect.scoped),
  );

  it.effect("buffers later contiguous gaps without claiming the highest seen version", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 0n,
        rows: [],
      });

      yield* backend.applyBatch({
        highestVersion: 3n,
        mutations: [
          {
            version: 1n,
            kind: "insert",
            id: "o-1",
            after: { id: "o-1", symbol: "AAPL", price: 100 },
            changedFields: new Set(["id", "symbol", "price"]),
          },
          {
            version: 3n,
            kind: "insert",
            id: "o-3",
            after: { id: "o-3", symbol: "NVDA", price: 300 },
            changedFields: new Set(["id", "symbol", "price"]),
          },
        ],
      });
      yield* flushChdb();

      const gapResult = yield* backend.snapshot({
        query: allOrdersQuery,
        targetVersion: 3n,
      });
      expect(gapResult.backendVersion).toBe(1n);
      expect(gapResult.rows).toEqual([{ id: "o-1", symbol: "AAPL", price: 100 }]);

      yield* backend.applyBatch({
        highestVersion: 2n,
        mutations: [
          {
            version: 2n,
            kind: "insert",
            id: "o-2",
            after: { id: "o-2", symbol: "MSFT", price: 200 },
            changedFields: new Set(["id", "symbol", "price"]),
          },
        ],
      });
      yield* flushChdb();

      const repairedResult = yield* backend.snapshot({
        query: allOrdersQuery,
        targetVersion: 3n,
      });
      expect(repairedResult.backendVersion).toBe(3n);
      expect(repairedResult.rows).toEqual([
        { id: "o-1", symbol: "AAPL", price: 100 },
        { id: "o-2", symbol: "MSFT", price: 200 },
        { id: "o-3", symbol: "NVDA", price: 300 },
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect("ignores duplicate mutation versions after they are already contiguous", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 0n,
        rows: [],
      });

      yield* backend.applyBatch({
        highestVersion: 2n,
        mutations: [
          {
            version: 1n,
            kind: "insert",
            id: "o-1",
            after: { id: "o-1", symbol: "AAPL", price: 100 },
            changedFields: new Set(["id", "symbol", "price"]),
          },
          {
            version: 2n,
            kind: "update",
            id: "o-1",
            before: { id: "o-1", symbol: "AAPL", price: 100 },
            after: { id: "o-1", symbol: "AAPL", price: 125 },
            changedFields: new Set(["price"]),
          },
        ],
      });
      yield* flushChdb();

      yield* backend.applyBatch({
        highestVersion: 2n,
        mutations: [
          {
            version: 2n,
            kind: "update",
            id: "o-1",
            before: { id: "o-1", symbol: "AAPL", price: 100 },
            after: { id: "o-1", symbol: "AAPL", price: 125 },
            changedFields: new Set(["price"]),
          },
        ],
      });
      yield* flushChdb();

      const result = yield* backend.snapshot({ query: allOrdersQuery, targetVersion: 2n });
      expect(result.backendVersion).toBe(2n);
      expect(result.rows).toEqual([{ id: "o-1", symbol: "AAPL", price: 125 }]);
    }).pipe(Effect.scoped),
  );

  it.effect("preserves BigDecimal values in chDB snapshots", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      const exact = BigDecimal.fromStringUnsafe("1234567890.123456789");
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 2n,
        rows: versionedRows([
          { id: "small", price: BigDecimal.fromStringUnsafe("10.000000000000000001") },
          { id: "exact", price: exact },
        ]),
      });

      const result = yield* backend.snapshot({ query: decimalQuery, targetVersion: 2n });

      expect(result.totalRows).toBe(1);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]?.id).toBe("exact");
      expect(BigDecimal.equals(expectBigDecimal(result.rows[0]?.price), exact)).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("preserves BigDecimal aggregate values in chDB grouped snapshots", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 2n,
        rows: versionedRows([
          { id: "a", symbol: "AAPL", price: BigDecimal.fromStringUnsafe("1.1") },
          { id: "b", symbol: "AAPL", price: BigDecimal.fromStringUnsafe("2.2") },
        ]),
      });

      const result = yield* backend.snapshot({ query: decimalGroupedQuery, targetVersion: 2n });

      expect(result.totalRows).toBe(1);
      expect(result.rows[0]?.symbol).toBe("AAPL");
      expect(
        BigDecimal.equals(
          expectBigDecimal(result.rows[0]?.totalPrice),
          BigDecimal.fromStringUnsafe("3.3"),
        ),
      ).toBe(true);
      expect(
        BigDecimal.equals(
          expectBigDecimal(result.rows[0]?.maxPrice),
          BigDecimal.fromStringUnsafe("2.2"),
        ),
      ).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("refreshes grouped BigDecimal subscriptions through the chDB worker", () =>
    Effect.gen(function* () {
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: DecimalOrder,
        },
        {
          initialRows: [
            { id: "a", symbol: "AAPL", price: BigDecimal.fromStringUnsafe("1.1") },
            { id: "b", symbol: "AAPL", price: BigDecimal.fromStringUnsafe("2.2") },
          ],
          snapshotBackend: createChdbSnapshotBackend(),
          groupedRefreshDebounceMs: 0,
        },
      );
      const events = yield* worker
        .subscribe("chdb-decimal-grouped-refresh", decimalGroupedQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));

      const initial = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(initial.type).toBe("snapshot");
      if (initial.type !== "snapshot") {
        throw new Error("Expected initial grouped snapshot");
      }
      expect(initial.rows[0]?.symbol).toBe("AAPL");
      expect(
        BigDecimal.equals(
          expectBigDecimal(initial.rows[0]?.totalPrice),
          BigDecimal.fromStringUnsafe("3.3"),
        ),
      ).toBe(true);

      yield* worker.publish({
        id: "c",
        symbol: "AAPL",
        price: BigDecimal.fromStringUnsafe("3.3"),
      });

      const stale = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(stale.type).toBe("status");
      if (stale.type !== "status") {
        throw new Error("Expected stale grouped status");
      }
      expect(stale.status).toBe("stale");

      const refreshed = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
      expect(refreshed.type).toBe("snapshot");
      if (refreshed.type !== "snapshot") {
        throw new Error("Expected refreshed grouped snapshot");
      }
      expect(refreshed.meta.version).toBe("1");
      expect(refreshed.meta.totalRows).toBe(1);
      expect(refreshed.rows[0]?.symbol).toBe("AAPL");
      expect(
        BigDecimal.equals(
          expectBigDecimal(refreshed.rows[0]?.totalPrice),
          BigDecimal.fromStringUnsafe("6.6"),
        ),
      ).toBe(true);
      expect(
        BigDecimal.equals(
          expectBigDecimal(refreshed.rows[0]?.maxPrice),
          BigDecimal.fromStringUnsafe("3.3"),
        ),
      ).toBe(true);

      yield* worker.unsubscribe("chdb-decimal-grouped-refresh");
    }).pipe(Effect.scoped),
  );

  it.effect("uses a version-fenced chDB snapshot through the topic worker", () =>
    Effect.gen(function* () {
      const backend = createChdbSnapshotBackend();
      const worker = yield* makeTopicWorkerCore(
        "orders",
        {
          id: "id",
          schema: Order,
        },
        { snapshotBackend: backend },
      );

      yield* worker.publish({ id: "o-1", symbol: "AAPL", price: 100 });
      yield* worker.publish({ id: "o-2", symbol: "MSFT", price: 200 });

      const events = yield* worker
        .subscribe("chdb-worker-sub", pagedQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));

      const snapshot = yield* Queue.take(events);
      if (snapshot.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.meta.version).toBe("2");
      expect(snapshot.meta.backendVersion).toEqual(expect.any(String));
      expect(snapshot.meta.totalRows).toBe(1);
      expect(snapshot.rows).toEqual([]);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "falls back to memory when chDB has seen a future mutation without a contiguous fence",
    () =>
      Effect.gen(function* () {
        const held = yield* Deferred.make<void>();
        const forwarded = yield* Deferred.make<void>();
        const controlled = holdVersionChdbBackend(1n, held, forwarded);
        const worker = yield* makeTopicWorkerCore(
          "orders",
          {
            id: "id",
            schema: Order,
          },
          { snapshotBackend: controlled.backend },
        );

        yield* worker.publish({ id: "o-1", symbol: "AAPL", price: 100 });
        yield* worker.publish({ id: "o-2", symbol: "MSFT", price: 200 });
        yield* Deferred.await(held).pipe(Effect.timeout("1 second"));
        yield* Deferred.await(forwarded).pipe(Effect.timeout("1 second"));
        yield* flushChdb();

        const [fallbackSnapshot] = yield* worker
          .subscribe("chdb-gap-worker-sub", allOrdersQuery)
          .pipe(Stream.take(1), Stream.runCollect);
        if (fallbackSnapshot?.type !== "snapshot") {
          throw new Error("Expected snapshot");
        }
        expect(fallbackSnapshot.meta.version).toBe("2");
        expect(fallbackSnapshot.meta.backendVersion).toBeUndefined();
        expect(fallbackSnapshot.rows).toEqual([
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "MSFT", price: 200 },
        ]);

        yield* controlled.release;
        yield* flushChdb();

        const [chdbSnapshot] = yield* worker
          .subscribe("chdb-repaired-worker-sub", allOrdersQuery)
          .pipe(Stream.take(1), Stream.runCollect);
        if (chdbSnapshot?.type !== "snapshot") {
          throw new Error("Expected snapshot");
        }
        expect(chdbSnapshot.meta.version).toBe("2");
        expect(chdbSnapshot.meta.backendVersion).toBe("2");
        expect(chdbSnapshot.rows).toEqual([
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "MSFT", price: 200 },
        ]);
      }).pipe(Effect.scoped),
  );

  it.effect("uses chDB snapshots by default in the production runtime", () =>
    Effect.gen(function* () {
      const config = defineConfig({
        topics: {
          orders: {
            id: "id",
            schema: Order,
          },
        },
      });
      const runtime = yield* makeViewServerRuntime(config);

      yield* runtime.publish("orders", { id: "o-1", symbol: "AAPL", price: 100 });
      yield* runtime.publish("orders", { id: "o-2", symbol: "MSFT", price: 200 });

      const events = yield* runtime
        .subscribe("runtime-chdb-sub", "orders", pagedQuery)
        .pipe(Stream.toQueue({ capacity: 16 }));

      const snapshot = yield* Queue.take(events);
      if (snapshot.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.meta.version).toBe("2");
      expect(snapshot.meta.backendVersion).toEqual(expect.any(String));
    }).pipe(Effect.scoped),
  );

  it.effect("fails fast when the required snapshot backend cannot initialize", () =>
    Effect.gen(function* () {
      const config = defineConfig({
        topics: {
          orders: {
            id: "id",
            schema: Order,
          },
        },
      });

      const error = yield* makeViewServerRuntime(config, {
        __testingSnapshotBackendFactory: () => failingInitBackend(),
      }).pipe(Effect.flip);

      expect(error._tag).toBe("SnapshotBackendFailed");
    }).pipe(Effect.scoped),
  );

  it.effect("reports degraded runtime health when the chDB child process exits unexpectedly", () =>
    Effect.gen(function* () {
      let childPid: number | undefined;
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          __testingSnapshotBackendFactory: () =>
            createChdbSnapshotBackend({
              onWorkerSpawn: (pid) => {
                childPid = pid;
              },
            }),
        },
      );

      yield* runtime.publish("orders", { id: "o-1", symbol: "AAPL", price: 100 });
      const pid = expectPid(childPid);
      process.kill(pid, "SIGKILL");

      const degraded = yield* waitForRuntimeHealth(runtime, (health) => {
        return health.topics.orders?.status === "degraded";
      });
      expect(degraded.ok).toBe(false);
      expect(degraded.topics.orders).toMatchObject({
        chdbStatus: "degraded",
        chdbPid: pid,
        chdbPendingRequests: 0,
      });
      expect(degraded.topics.orders?.chdbLastError).toContain("SIGKILL");
      const healthTopic = yield* runtime.query(VIEW_SERVER_HEALTH_TOPIC, healthChdbQuery);
      expect(rowById(healthTopic.rows, "topic:orders")).toMatchObject({
        chdbStatus: "degraded",
        chdbPid: pid,
      });
      expect(isProcessAlive(pid)).toBe(false);
      yield* runtime.close.pipe(Effect.timeout("1 second"));
    }).pipe(Effect.scoped),
  );

  it.effect("mirrors ready chDB child health in runtime health and the health topic", () =>
    Effect.gen(function* () {
      let childPid: number | undefined;
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          __testingSnapshotBackendFactory: () =>
            createChdbSnapshotBackend({
              onWorkerSpawn: (pid) => {
                childPid = pid;
              },
            }),
        },
      );

      const health = yield* waitForRuntimeHealth(
        runtime,
        (candidate) =>
          childPid !== undefined &&
          candidate.topics.orders?.chdbPid === childPid &&
          candidate.topics.orders.chdbStatus === "ready",
      );
      const ordersHealth = health.topics.orders;
      expect(ordersHealth).toBeDefined();
      if (ordersHealth === undefined) {
        throw new Error("Missing orders health");
      }
      expect(ordersHealth).toMatchObject({
        chdbStatus: "ready",
        chdbPid: expectPid(childPid),
        chdbRestarts: 0,
        chdbPendingRequests: 0,
        chdbLastError: "",
      });

      const healthTopic = yield* runtime.query(VIEW_SERVER_HEALTH_TOPIC, healthChdbQuery);
      expect(rowById(healthTopic.rows, "topic:orders")).toMatchObject({
        chdbStatus: ordersHealth.chdbStatus,
        chdbPid: ordersHealth.chdbPid,
        chdbRestarts: ordersHealth.chdbRestarts,
        chdbPendingRequests: ordersHealth.chdbPendingRequests,
        chdbLastError: ordersHealth.chdbLastError,
        chdbBackendVersion: ordersHealth.chdbBackendVersion,
      });
      yield* runtime.close.pipe(Effect.timeout("1 second"));
    }).pipe(Effect.scoped),
  );

  it.effect("keeps chDB child failure isolated to its owning topic", () =>
    Effect.gen(function* () {
      const childPids = new Map<string, number>();
      const runtime = yield* makeViewServerRuntime(
        defineConfig({
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
            trades: {
              id: "id",
              schema: Order,
            },
          },
        }),
        {
          __testingSnapshotBackendFactory: (topic) =>
            createChdbSnapshotBackend({
              onWorkerSpawn: (pid) => {
                if (pid !== undefined) {
                  childPids.set(topic, pid);
                }
              },
            }),
        },
      );

      yield* runtime.publish("orders", { id: "o-1", symbol: "AAPL", price: 100 });
      yield* runtime.publish("trades", { id: "t-1", symbol: "MSFT", price: 200 });
      const ordersPid = expectPid(childPids.get("orders"));
      const tradesPid = expectPid(childPids.get("trades"));
      expect(ordersPid).not.toBe(tradesPid);

      process.kill(ordersPid, "SIGKILL");
      const degraded = yield* waitForRuntimeHealth(
        runtime,
        (health) =>
          health.topics.orders?.status === "degraded" && health.topics.trades?.status === "ready",
      );
      expect(degraded.ok).toBe(false);
      expect(degraded.topics.orders?.status).toBe("degraded");
      expect(degraded.topics.orders).toMatchObject({
        chdbStatus: "degraded",
        chdbPid: ordersPid,
      });
      expect(degraded.topics.trades?.status).toBe("ready");
      expect(degraded.topics.trades).toMatchObject({
        chdbStatus: "ready",
        chdbPid: tradesPid,
      });
      expect(isProcessAlive(ordersPid)).toBe(false);
      expect(isProcessAlive(tradesPid)).toBe(true);

      const trades = yield* runtime.query("trades", allOrdersQuery);
      expect(trades.rows).toEqual([{ id: "t-1", symbol: "MSFT", price: 200 }]);
      expect(trades.version).toBe("1");

      const orders = yield* runtime.query("orders", allOrdersQuery);
      expect(orders.rows).toEqual([{ id: "o-1", symbol: "AAPL", price: 100 }]);
      yield* runtime.close.pipe(Effect.timeout("1 second"));
      yield* waitForProcessExit(tradesPid);
    }).pipe(Effect.scoped),
  );

  it.effect("fails pending snapshot and grouped refresh requests when the chDB child exits", () =>
    Effect.gen(function* () {
      let snapshotKilled = false;
      let snapshotPid: number | undefined;
      const snapshotBackend = createChdbSnapshotBackend({
        onWorkerSpawn: (pid) => {
          snapshotPid = pid;
        },
        onWorkerRequest: ({ pid, type }) => {
          if (type === "snapshot" && !snapshotKilled) {
            snapshotKilled = true;
            process.kill(expectPid(pid), "SIGTERM");
          }
        },
      });
      yield* Effect.addFinalizer(() => snapshotBackend.close());
      yield* snapshotBackend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows([{ id: "o-1", symbol: "AAPL", price: 100 }]),
      });

      const snapshotError = yield* snapshotBackend
        .snapshot({ query: allOrdersQuery, targetVersion: 1n })
        .pipe(Effect.flip, Effect.timeout("2 seconds"));
      expect(snapshotError._tag).toBe("SnapshotBackendFailed");
      expect((yield* snapshotBackendHealth(snapshotBackend)).status).toBe("degraded");
      expect(isProcessAlive(expectPid(snapshotPid))).toBe(false);

      let groupedKilled = false;
      let groupedPid: number | undefined;
      const groupedBackend = createChdbSnapshotBackend({
        onWorkerSpawn: (pid) => {
          groupedPid = pid;
        },
        onWorkerRequest: ({ pid, type }) => {
          if (type === "groupedRefreshSnapshot" && !groupedKilled) {
            groupedKilled = true;
            process.kill(expectPid(pid), "SIGKILL");
          }
        },
      });
      yield* Effect.addFinalizer(() => groupedBackend.close());
      yield* groupedBackend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows([{ id: "o-1", symbol: "AAPL", price: 100 }]),
      });
      if (groupedBackend.groupedRefreshSnapshot === undefined) {
        return yield* Effect.die(new Error("Expected grouped refresh snapshot support"));
      }

      const groupedError = yield* groupedBackend
        .groupedRefreshSnapshot({
          query: groupedQuery,
          targetVersion: 1n,
        })
        .pipe(Effect.flip, Effect.timeout("2 seconds"));
      expect(groupedError._tag).toBe("SnapshotBackendFailed");
      expect((yield* snapshotBackendHealth(groupedBackend)).status).toBe("degraded");
      expect(isProcessAlive(expectPid(groupedPid))).toBe(false);
    }).pipe(Effect.scoped),
  );

  it.effect("fails pending applyBatch when the chDB child exits during flush", () =>
    Effect.gen(function* () {
      let killed = false;
      let childPid: number | undefined;
      const backend = createChdbSnapshotBackend({
        onWorkerSpawn: (pid) => {
          childPid = pid;
        },
        onWorkerRequest: ({ pid, type }) => {
          if (type === "applyBatch" && !killed) {
            killed = true;
            process.kill(expectPid(pid), "SIGTERM");
          }
        },
      });
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows([{ id: "o-1", symbol: "AAPL", price: 100 }]),
      });

      const error = yield* backend
        .applyBatch({
          highestVersion: 2n,
          mutations: [
            {
              version: 2n,
              kind: "insert",
              id: "o-2",
              after: { id: "o-2", symbol: "MSFT", price: 200 },
              changedFields: new Set(["id", "symbol", "price"]),
            },
          ],
        })
        .pipe(Effect.flip, Effect.timeout("2 seconds"));
      expect(error._tag).toBe("SnapshotBackendFailed");
      expect((yield* snapshotBackendHealth(backend)).status).toBe("degraded");
      expect(isProcessAlive(expectPid(childPid))).toBe(false);
    }).pipe(Effect.scoped),
  );

  it.effect("can restart a supervised chDB child after an unexpected exit", () =>
    Effect.gen(function* () {
      const childPids: number[] = [];
      const backend = createChdbSnapshotBackend({
        restartWorkerOnUnexpectedExit: true,
        onWorkerSpawn: (pid) => {
          if (pid !== undefined) {
            childPids.push(pid);
          }
        },
      });
      yield* Effect.addFinalizer(() => backend.close());
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows([{ id: "o-1", symbol: "AAPL", price: 100 }]),
      });
      yield* backend.applyBatch({
        highestVersion: 2n,
        mutations: [
          {
            version: 2n,
            kind: "insert",
            id: "o-2",
            after: { id: "o-2", symbol: "MSFT", price: 200 },
            changedFields: new Set(["id", "symbol", "price"]),
          },
        ],
      });

      const firstPid = expectPid(childPids[0]);
      process.kill(firstPid, "SIGKILL");
      const degraded = yield* waitForBackendHealth(
        backend,
        (health) => health.status === "degraded",
      );
      expect(degraded.pid).toBe(firstPid);
      expect(degraded.lastError).toContain("SIGKILL");
      expect(isProcessAlive(firstPid)).toBe(false);

      const result = yield* backend.snapshot({ query: allOrdersQuery, targetVersion: 2n });
      expect(result.backendVersion).toBe(2n);
      expect(result.rows).toEqual([
        { id: "o-1", symbol: "AAPL", price: 100 },
        { id: "o-2", symbol: "MSFT", price: 200 },
      ]);
      expect(childPids.length).toBe(2);
      expect(childPids[1]).not.toBe(firstPid);
      expect(yield* snapshotBackendHealth(backend)).toMatchObject({
        status: "ready",
        pid: childPids[1],
        restarts: 1,
        backendVersion: 2n,
      });
    }).pipe(Effect.scoped),
  );

  it.effect("kills the chDB child process cleanly on shutdown", () =>
    Effect.gen(function* () {
      let childPid: number | undefined;
      const backend = createChdbSnapshotBackend({
        onWorkerSpawn: (pid) => {
          childPid = pid;
        },
      });
      yield* backend.init({
        topic: "orders",
        idField: "id",
        version: 1n,
        rows: versionedRows([{ id: "o-1", symbol: "AAPL", price: 100 }]),
      });
      const pid = expectPid(childPid);
      expect(isProcessAlive(pid)).toBe(true);

      yield* backend.close().pipe(Effect.timeout("2 seconds"));
      expect((yield* snapshotBackendHealth(backend)).status).toBe("stopped");
      yield* waitForProcessExit(pid);
      expect(isProcessAlive(pid)).toBe(false);
    }).pipe(Effect.scoped),
  );
});

function versionedRows(rows: readonly RuntimeRow[]) {
  return rows.map((row, index) => ({ row, version: BigInt(index + 1) }));
}

function rowById(rows: readonly RuntimeRow[], id: string): RuntimeRow {
  const row = rows.find((entry) => entry.id === id);
  if (row === undefined) {
    throw new Error(`Expected row ${id}`);
  }
  return row;
}

function snapshotBackendHealth(backend: SnapshotBackend): Effect.Effect<SnapshotBackendHealth> {
  return backend.health ?? Effect.succeed({ status: "ready" });
}

function waitForBackendHealth(
  backend: SnapshotBackend,
  predicate: (health: SnapshotBackendHealth) => boolean,
) {
  return Effect.gen(function* () {
    while (true) {
      const health = yield* snapshotBackendHealth(backend);
      if (predicate(health)) {
        return health;
      }
      yield* sleepHost(10);
    }
  }).pipe(Effect.timeout("2 seconds"));
}

function waitForRuntimeHealth(
  runtime: { readonly health: Effect.Effect<HealthResponse, ViewServerError> },
  predicate: (health: HealthResponse) => boolean,
) {
  return Effect.gen(function* () {
    while (true) {
      const health = yield* runtime.health;
      if (predicate(health)) {
        return health;
      }
      yield* sleepHost(10);
    }
  }).pipe(Effect.timeout("2 seconds"));
}

function waitForProcessExit(pid: number) {
  return Effect.gen(function* () {
    while (isProcessAlive(pid)) {
      yield* sleepHost(10);
    }
  }).pipe(Effect.timeout("2 seconds"));
}

function sleepHost(milliseconds: number): Effect.Effect<void> {
  return Effect.promise(() => new Promise((resolve) => setTimeout(resolve, milliseconds)));
}

function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function expectPid(pid: number | undefined): number {
  expect(typeof pid).toBe("number");
  if (pid === undefined) {
    throw new Error("Expected chDB child pid");
  }
  return pid;
}

function flushChdb() {
  return Effect.promise<void>(() => new Promise((resolve) => queueMicrotask(resolve)));
}

function failingInitBackend(): SnapshotBackend {
  return {
    init: () => Effect.fail(snapshotBackendFailed("orders", new Error("init failed"))),
    applyBatch: () => Effect.void,
    snapshot: () => Effect.fail(snapshotBackendFailed("orders", new Error("snapshot failed"))),
    close: () => Effect.void,
  };
}

function expectBigDecimal(value: unknown): BigDecimal.BigDecimal {
  if (BigDecimal.isBigDecimal(value)) {
    return value;
  }
  throw new Error(`Expected BigDecimal, got ${String(value)}`);
}

function holdVersionChdbBackend(
  versionToHold: WorkerVersion,
  held: Deferred.Deferred<void>,
  forwarded: Deferred.Deferred<void>,
): {
  readonly backend: SnapshotBackend;
  readonly release: Effect.Effect<void, ViewServerError>;
} {
  const backend = createChdbSnapshotBackend();
  let heldBatch:
    | {
        readonly mutations: readonly MutationLogEntry[];
        readonly highestVersion: WorkerVersion;
      }
    | undefined;
  return {
    backend: {
      init: (args) => backend.init(args),
      snapshot: (args) => backend.snapshot(args),
      close: () => backend.close(),
      applyBatch: (args): Effect.Effect<void, ViewServerError> => {
        if (args.mutations.some((mutation) => mutation.version === versionToHold)) {
          heldBatch = args;
          return Deferred.succeed(held, undefined).pipe(Effect.asVoid);
        }
        return Effect.gen(function* () {
          yield* backend.applyBatch(args);
          yield* Deferred.succeed(forwarded, undefined);
        });
      },
    },
    release: Effect.gen(function* () {
      if (heldBatch === undefined) {
        return;
      }
      const batch = heldBatch;
      heldBatch = undefined;
      yield* backend.applyBatch(batch);
    }),
  };
}
