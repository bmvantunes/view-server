import { describe, expect, it } from "@effect/vitest";
import { BigDecimal, Effect, Schema } from "effect";
import { AsyncResult } from "effect/unstable/reactivity";
import { defineConfig } from "../src/config/index.ts";
import type { RawQuery } from "../src/protocol/index.ts";
import { LiveQueryStore } from "../src/client/live-query-store.ts";
import {
  queryResultToRuntimeRows,
  rowKeyForTypedQuery,
  rpcDeltaPublishPayload,
  rpcPublishPayload,
  rpcQueryPayload,
  rpcQueryRows,
  rpcSubscribePayload,
  rpcSubscriptionEvent,
  runtimeRowsToQueryResult,
} from "../src/client/rpc-boundary.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.BigDecimal,
});

const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

type OrderRow = typeof Order.Type;

const query = {
  fields: {
    id: true,
    price: true,
  },
  where: {
    field: "price",
    comparator: "greater_than",
    value: BigDecimal.fromStringUnsafe("10.5"),
  },
  orderBy: [{ field: "price", direction: "asc" }],
  limit: 5,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

describe("client RPC boundary helpers", () => {
  it("builds typed RPC query payloads without changing query shape", () => {
    expect(rpcQueryPayload<typeof config, "orders", typeof query>("orders", query)).toEqual({
      topic: "orders",
      query,
    });
    expect(
      rpcSubscribePayload<typeof config, "orders", typeof query>("request-1", "orders", query),
    ).toEqual({
      requestId: "request-1",
      topic: "orders",
      query,
    });
  });

  it.effect("converts publish and patch rows through schema decode and wire-row encode", () =>
    Effect.gen(function* () {
      const price = BigDecimal.fromStringUnsafe("123.45");

      const publishPayload = yield* rpcPublishPayload<typeof config, "orders">(config, "orders", {
        id: "o-1",
        symbol: "AAPL",
        price,
      });
      expect(publishPayload).toEqual({
        topic: "orders",
        row: {
          id: "o-1",
          symbol: "AAPL",
          price,
        },
      });

      const deltaPayload = yield* rpcDeltaPublishPayload<typeof config, "orders">(
        config,
        "orders",
        {
          id: "o-1",
          price,
        },
      );
      expect(deltaPayload).toEqual({
        topic: "orders",
        patch: {
          id: "o-1",
          price,
        },
      });
    }),
  );

  it.effect("decodes typed query results back to runtime rows for stores", () =>
    Effect.gen(function* () {
      const rows = yield* rpcQueryRows<typeof config, "orders", typeof query>(
        {
          rows: [{ id: "o-1", price: BigDecimal.fromStringUnsafe("20") }],
          totalRows: 1,
          version: "1",
        },
        query,
        config,
        "orders",
      );

      expect(queryResultToRuntimeRows(rows)).toEqual([
        { id: "o-1", price: BigDecimal.fromStringUnsafe("20") },
      ]);
      expect(
        runtimeRowsToQueryResult<typeof config, "orders", typeof query>(
          [{ id: "o-1", price: BigDecimal.fromStringUnsafe("20") }],
          query,
          config,
          "orders",
        ),
      ).toEqual(rows);
      expect(rowKeyForTypedQuery<typeof config, "orders", typeof query>(query, "id")(rows[0])).toBe(
        "o-1",
      );
    }),
  );

  it.effect("passes typed subscription status events through the RPC boundary", () =>
    Effect.gen(function* () {
      const event = yield* rpcSubscriptionEvent<typeof config, "orders", typeof query>(
        {
          type: "status",
          requestId: "request-1",
          status: "stale",
          meta: {
            version: "2",
            totalRows: 10,
            serverTime: 123,
          },
        },
        query,
        config,
        "orders",
      );

      expect(event).toEqual({
        type: "status",
        requestId: "request-1",
        status: "stale",
        meta: {
          version: "2",
          totalRows: 10,
          serverTime: 123,
        },
      });
    }),
  );

  it("represents subscription status events as stale AsyncResult waiting state", () => {
    const store = new LiveQueryStore({
      rows: [{ id: "o-1", price: BigDecimal.fromStringUnsafe("20") }],
      totalRows: 1,
    });

    store.apply({
      type: "status",
      requestId: "request-1",
      status: "stale",
      meta: {
        version: "2",
        totalRows: 2,
        serverTime: 123,
      },
    });

    const matched = AsyncResult.match(store.snapshot, {
      onInitial: () => "initial",
      onFailure: () => "failure",
      onSuccess: ({ value }) => {
        expect(value.status).toBe("stale");
        expect(value.totalRows).toBe(2);
        expect(value.rows).toEqual([{ id: "o-1", price: BigDecimal.fromStringUnsafe("20") }]);
        return "success";
      },
    });
    expect(matched).toBe("success");
    expect(store.snapshot.waiting).toBe(true);
  });
});
