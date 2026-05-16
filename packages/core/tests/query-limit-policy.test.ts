import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { columnCatalogForTopic, defineConfig, normalizeConfig } from "../src/config/index.ts";
import type { RuntimeFilterNode, RuntimeQuery } from "../src/protocol/index.ts";
import { QueryLimitPolicy } from "../src/server/query-limit-policy.ts";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  region: Schema.String,
  price: Schema.Number,
});

const Trade = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

const baseQuery = {
  fields: {
    id: true,
    symbol: true,
  },
  limit: 2,
} satisfies RuntimeQuery;

describe("QueryLimitPolicy", () => {
  it.effect("applies defaults, validates every configured limit, and counts rejections", () =>
    Effect.gen(function* () {
      const normalized = normalizeConfig(
        defineConfig({
          limits: {
            maxPageSize: 2,
            maxAggregateCount: 1,
            maxGroupByFields: 1,
            maxFilterDepth: 2,
            maxFilterConditions: 2,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
            },
          },
        }),
      );
      const policy = QueryLimitPolicy.fromConfig(normalized);
      const catalog = columnCatalogForTopic("orders", topicConfig(normalized, "orders"));

      const defaulted = yield* policy.validate(
        "orders",
        {
          fields: {
            id: true,
          },
        },
        catalog,
      );
      expect(defaulted.limit).toBe(2);

      const failures = yield* Effect.all([
        policy.validate("orders", { ...baseQuery, limit: 3 }, catalog).pipe(Effect.flip),
        policy
          .validate(
            "orders",
            {
              groupBy: ["symbol"],
              aggregates: {
                orders: { aggFunc: "count", field: "id" },
                regions: { aggFunc: "count_distinct", field: "region" },
              },
              limit: 2,
            },
            catalog,
          )
          .pipe(Effect.flip),
        policy
          .validate(
            "orders",
            {
              groupBy: ["symbol", "region"],
              aggregates: {
                orders: { aggFunc: "count", field: "id" },
              },
              limit: 2,
            },
            catalog,
          )
          .pipe(Effect.flip),
        policy
          .validate("orders", { ...baseQuery, where: nestedFilter(3) }, catalog)
          .pipe(Effect.flip),
        policy
          .validate(
            "orders",
            {
              ...baseQuery,
              where: {
                op: "and",
                conditions: [
                  { field: "price", comparator: "greater_than", value: 1 },
                  { field: "price", comparator: "less_than", value: 100 },
                  { field: "symbol", comparator: "equals", value: "AAPL" },
                ],
              },
            },
            catalog,
          )
          .pipe(Effect.flip),
      ]);

      expect(failures.map((error) => error._tag)).toEqual([
        "QueryLimitExceeded",
        "QueryLimitExceeded",
        "QueryLimitExceeded",
        "QueryLimitExceeded",
        "QueryLimitExceeded",
      ]);
      expect(policy.rejectedCount("orders")).toBe(5);
      expect(policy.metrics()).toEqual({
        rejectedQueries: 5,
        rejectedQueriesByTopic: {
          orders: 5,
        },
      });
    }),
  );

  it.effect("uses topic-specific limit overrides over global limits", () =>
    Effect.gen(function* () {
      const normalized = normalizeConfig(
        defineConfig({
          limits: {
            maxPageSize: 2,
          },
          topics: {
            orders: {
              id: "id",
              schema: Order,
              limits: {
                maxPageSize: 3,
              },
            },
            trades: {
              id: "id",
              schema: Trade,
            },
          },
        }),
      );
      const policy = QueryLimitPolicy.fromConfig(normalized);
      const ordersCatalog = columnCatalogForTopic("orders", topicConfig(normalized, "orders"));
      const tradesCatalog = columnCatalogForTopic("trades", topicConfig(normalized, "trades"));

      const accepted = yield* policy.validate("orders", { ...baseQuery, limit: 3 }, ordersCatalog);
      expect(accepted.limit).toBe(3);

      const rejected = yield* policy
        .validate("trades", { ...baseQuery, limit: 3 }, tradesCatalog)
        .pipe(Effect.flip);
      expect(rejected._tag).toBe("QueryLimitExceeded");
      if (rejected._tag !== "QueryLimitExceeded") {
        throw new Error(`Unexpected error ${rejected._tag}`);
      }
      expect(rejected.field).toBe("maxPageSize");
      expect(policy.rejectedCount("orders")).toBe(0);
      expect(policy.rejectedCount("trades")).toBe(1);
    }),
  );

  it("rejects invalid topic-level limit configuration during normalization", () => {
    expect(() =>
      normalizeConfig(
        defineConfig({
          topics: {
            orders: {
              id: "id",
              schema: Order,
              limits: {
                maxPageSize: 0,
              },
            },
          },
        }),
      ),
    ).toThrow("topics.orders.limits.maxPageSize must be a positive integer");
  });
});

function topicConfig(config: ReturnType<typeof normalizeConfig>, topic: string) {
  const topicConfig = config.topics[topic];
  if (topicConfig === undefined) {
    throw new Error(`Missing test topic ${topic}`);
  }
  return topicConfig;
}

function nestedFilter(depth: number): RuntimeFilterNode {
  if (depth <= 1) {
    return { field: "price", comparator: "greater_than", value: 1 };
  }
  return {
    op: "and",
    conditions: [nestedFilter(depth - 1)],
  };
}
