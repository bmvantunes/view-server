import { describe, expect, it } from "@effect/vitest";
import { BigDecimal, Effect, Queue, Schema, Stream } from "effect";
import { defineConfig } from "../src/config/index.ts";
import type { RuntimeQuery, RuntimeRow } from "../src/protocol/index.ts";
import {
  createChdbSnapshotBackend,
  createChdbSnapshotBackendFactory,
} from "../src/snapshot/chdb-backend.ts";
import { makeViewServerRuntime } from "../src/server/index.ts";
import { makeTopicWorkerCore } from "../src/worker/index.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
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

  it.effect("honors snapshot.backend chdb through the runtime backend factory", () =>
    Effect.gen(function* () {
      const config = defineConfig({
        topics: {
          orders: {
            id: "id",
            schema: Order,
            snapshot: {
              backend: "chdb",
            },
          },
        },
      });
      const runtime = yield* makeViewServerRuntime(config, {
        snapshotBackendFactory: createChdbSnapshotBackendFactory(),
      });

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

  it.effect("fails fast when config requests chDB without Node backend wiring", () =>
    Effect.gen(function* () {
      const config = defineConfig({
        topics: {
          orders: {
            id: "id",
            schema: Order,
            snapshot: {
              backend: "chdb",
            },
          },
        },
      });

      const error = yield* makeViewServerRuntime(config).pipe(Effect.flip);

      expect(error._tag).toBe("SnapshotBackendFailed");
    }).pipe(Effect.scoped),
  );
});

function versionedRows(rows: readonly RuntimeRow[]) {
  return rows.map((row, index) => ({ row, version: BigInt(index + 1) }));
}

function flushChdb() {
  return Effect.promise<void>(() => new Promise((resolve) => queueMicrotask(resolve)));
}

function expectBigDecimal(value: unknown): BigDecimal.BigDecimal {
  if (BigDecimal.isBigDecimal(value)) {
    return value;
  }
  throw new Error(`Expected BigDecimal, got ${String(value)}`);
}
