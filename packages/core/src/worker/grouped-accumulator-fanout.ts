import {
  rowKeyForQuery,
  type DeltaOperation,
  type RuntimeGroupedQuery,
  type RuntimeRow,
} from "../protocol/index.ts";
import type { GroupedAccumulator } from "./grouped-accumulator.ts";
import type { MutationLogEntry } from "./mutation-log.ts";
import {
  diffVisibleRows,
  groupedQueryOrderBy,
  matchesFilter,
  normalizeLimit,
  normalizeOffset,
  stableSortRows,
  type QueryExecutionOptions,
  type QueryExecutionResult,
} from "./query-engine.ts";

export type MaterializedSubscriptionChange = {
  readonly operations: readonly DeltaOperation<RuntimeRow>[];
  readonly nextRows?: readonly RuntimeRow[] | undefined;
  readonly totalRows: number;
};

export function materializeGroupedAccumulatorChange(args: {
  readonly query: RuntimeGroupedQuery;
  readonly groupedAccumulator: GroupedAccumulator;
  readonly lastRows: readonly RuntimeRow[];
  readonly lastTotalRows: number;
  readonly mutation: MutationLogEntry;
  readonly idField: string;
  readonly options?: QueryExecutionOptions | undefined;
}): MaterializedSubscriptionChange | undefined {
  const beforeMatches =
    args.mutation.before !== undefined &&
    matchesFilter(args.mutation.before, args.query.where, args.options);
  const afterMatches =
    args.mutation.after !== undefined &&
    matchesFilter(args.mutation.after, args.query.where, args.options);
  if (!beforeMatches && !afterMatches) {
    return undefined;
  }
  if (beforeMatches && afterMatches) {
    args.groupedAccumulator.applyMutation(args.mutation);
  } else if (beforeMatches && args.mutation.before !== undefined) {
    args.groupedAccumulator.applyMutation({
      version: args.mutation.version,
      kind: "delete",
      id: args.mutation.id,
      before: args.mutation.before,
      changedFields: args.mutation.changedFields,
    });
  } else if (afterMatches && args.mutation.after !== undefined) {
    args.groupedAccumulator.applyMutation({
      version: args.mutation.version,
      kind: "insert",
      id: args.mutation.id,
      after: args.mutation.after,
      changedFields: args.mutation.changedFields,
    });
  }
  const next = groupedAccumulatorQueryResult({
    query: args.query,
    groupedAccumulator: args.groupedAccumulator,
  });
  const operations = diffVisibleRows(
    args.lastRows,
    next.rows,
    rowKeyForQuery(args.query, args.idField),
  );
  return operations.length === 0 && args.lastTotalRows === next.totalRows
    ? undefined
    : {
        operations,
        nextRows: next.rows,
        totalRows: next.totalRows,
      };
}

export function groupedAccumulatorQueryResult(args: {
  readonly query: RuntimeGroupedQuery;
  readonly groupedAccumulator: GroupedAccumulator;
}): QueryExecutionResult {
  const sorted = stableSortRows(
    args.groupedAccumulator.groupedRows(),
    groupedQueryOrderBy(args.query),
  );
  const offset = normalizeOffset(args.query.offset);
  const limit = normalizeLimit(args.query.limit);
  return {
    rows: sorted.slice(offset, offset + limit),
    totalRows: sorted.length,
  };
}
