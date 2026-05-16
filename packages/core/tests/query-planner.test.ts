import { describe, expect, it } from "@effect/vitest";
import type { RuntimeGroupedQuery, RuntimeRawQuery } from "../src/protocol/index.ts";
import { planQuery, type QueryStrategy } from "../src/worker/query-planner.ts";

describe("QueryPlanner", () => {
  it("classifies raw query and subscription strategies", () => {
    expect(strategy({ operation: "query", query: rawQuery({ limit: 50 }) })).toBe(
      "raw_small_window_snapshot",
    );
    expect(strategy({ operation: "query", query: rawQuery({ offset: 10_000, limit: 50 }) })).toBe(
      "memory_fallback",
    );
    expect(strategy({ operation: "subscription", query: rawQuery({ limit: 50 }) })).toBe(
      "raw_active_plan_eligible",
    );
    expect(
      strategy({
        operation: "subscription",
        query: rawQuery({ limit: 50 }),
        rowCount: 1_000_001,
      }),
    ).toBe("raw_active_plan_skipped");
    expect(
      strategy({
        operation: "subscription",
        query: rawQuery({ limit: 50 }),
        maxActivePlans: 0,
      }),
    ).toBe("raw_active_plan_skipped");
  });

  it("classifies grouped refresh, accumulator, fallback, and rejected strategies", () => {
    expect(
      strategy({
        operation: "subscription",
        query: groupedQuery("sum"),
        rowCount: 50,
        groupedAccumulatorMaxRows: 100,
      }),
    ).toBe("grouped_incremental_accumulator_eligible");
    expect(
      strategy({
        operation: "subscription",
        query: groupedQuery("avg"),
        groupedAccumulatorMaxRows: 100,
      }),
    ).toBe("grouped_chdb_refresh");
    expect(
      strategy({
        operation: "subscription",
        query: groupedQuery("avg"),
        groupedAccumulatorMaxRows: 100,
        supportsGroupedRefreshSnapshots: false,
      }),
    ).toBe("memory_fallback");
    expect(
      strategy({
        operation: "query",
        query: groupedQuery("sum"),
        rejectedByLimits: true,
      }),
    ).toBe("query_rejected_by_limits");
  });
});

type StrategyArgs = {
  readonly operation: "query" | "subscription";
  readonly query: RuntimeRawQuery | RuntimeGroupedQuery;
  readonly rowCount?: number | undefined;
  readonly maxActivePlans?: number | undefined;
  readonly groupedAccumulatorMaxRows?: number | undefined;
  readonly supportsGroupedRefreshSnapshots?: boolean | undefined;
  readonly rejectedByLimits?: boolean | undefined;
};

function strategy(args: StrategyArgs): QueryStrategy {
  return planQuery({
    operation: args.operation,
    query: args.query,
    rowCount: args.rowCount ?? 1_000,
    rawWindowOptimizationLimit: 10_000,
    activePlanAutoBuildMaxRows: 1_000_000,
    maxActivePlans: args.maxActivePlans,
    groupedAccumulatorMaxRows: args.groupedAccumulatorMaxRows ?? 0,
    supportsGroupedRefreshSnapshots: args.supportsGroupedRefreshSnapshots ?? true,
    rejectedByLimits: args.rejectedByLimits,
  }).strategy;
}

function rawQuery(args: {
  readonly limit: number;
  readonly offset?: number | undefined;
}): RuntimeRawQuery {
  return {
    fields: { id: true, price: true },
    orderBy: [{ field: "price", direction: "asc" }],
    limit: args.limit,
    offset: args.offset,
  };
}

function groupedQuery(aggFunc: "sum" | "avg"): RuntimeGroupedQuery {
  return {
    groupBy: ["symbol"],
    aggregates: {
      value: { aggFunc, field: "price" },
    },
    limit: 50,
  };
}
