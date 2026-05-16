import type { RuntimeQuery } from "../protocol/index.ts";
import { isGroupedQuery, normalizeLimit, normalizeOffset } from "./query-engine.ts";
import { isIncrementalGroupedAccumulatorSupported } from "./grouped-accumulator.ts";

export type QueryOperation = "query" | "subscription";

export type QueryStrategy =
  | "raw_small_window_snapshot"
  | "raw_active_plan_eligible"
  | "raw_active_plan_skipped"
  | "grouped_chdb_refresh"
  | "grouped_incremental_accumulator_eligible"
  | "memory_fallback"
  | "query_rejected_by_limits";

export type QueryPlan = {
  readonly operation: QueryOperation;
  readonly strategy: QueryStrategy;
  readonly reason: string;
};

export type QueryPlannerOptions = {
  readonly operation: QueryOperation;
  readonly query: RuntimeQuery;
  readonly rowCount: number;
  readonly rawWindowOptimizationLimit: number;
  readonly activePlanAutoBuildMaxRows: number;
  readonly maxActivePlans?: number | undefined;
  readonly groupedAccumulatorMaxRows: number;
  readonly supportsGroupedRefreshSnapshots: boolean;
  readonly rejectedByLimits?: boolean | undefined;
};

export function planQuery(options: QueryPlannerOptions): QueryPlan {
  if (options.rejectedByLimits === true) {
    return {
      operation: options.operation,
      strategy: "query_rejected_by_limits",
      reason: "query failed configured limit policy",
    };
  }
  if (isGroupedQuery(options.query)) {
    if (
      options.groupedAccumulatorMaxRows > 0 &&
      options.rowCount <= options.groupedAccumulatorMaxRows &&
      isIncrementalGroupedAccumulatorSupported(options.query)
    ) {
      return {
        operation: options.operation,
        strategy: "grouped_incremental_accumulator_eligible",
        reason: "grouped query uses only incremental aggregate functions and fits row admission",
      };
    }
    if (options.supportsGroupedRefreshSnapshots) {
      return {
        operation: options.operation,
        strategy: "grouped_chdb_refresh",
        reason: "grouped query uses chDB refresh snapshot with memory fallback",
      };
    }
    return {
      operation: options.operation,
      strategy: "memory_fallback",
      reason: "grouped query has no supported accelerator",
    };
  }
  if (options.operation === "query") {
    const windowEnd = normalizeOffset(options.query.offset) + normalizeLimit(options.query.limit);
    return windowEnd <= options.rawWindowOptimizationLimit
      ? {
          operation: options.operation,
          strategy: "raw_small_window_snapshot",
          reason: "raw one-shot query fits small-window memory optimization",
        }
      : {
          operation: options.operation,
          strategy: "memory_fallback",
          reason: "raw one-shot query exceeds small-window optimization",
        };
  }
  if (options.rowCount > options.activePlanAutoBuildMaxRows) {
    return {
      operation: options.operation,
      strategy: "raw_active_plan_skipped",
      reason: "topic row count exceeds activePlanAutoBuildMaxRows",
    };
  }
  if (options.maxActivePlans === 0) {
    return {
      operation: options.operation,
      strategy: "raw_active_plan_skipped",
      reason: "maxActivePlans is zero",
    };
  }
  return {
    operation: options.operation,
    strategy: "raw_active_plan_eligible",
    reason: "raw subscription is eligible for active plan admission",
  };
}
