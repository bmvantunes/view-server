import * as BigDecimal from "effect/BigDecimal";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import {
  rowKeyByField,
  rowKeyForQuery,
  type DeltaOperation,
  type OrderBy,
  type RuntimeGroupedQuery,
  type RuntimeFilterNode,
  type RuntimeRawQuery,
  type RuntimeAggregateMap,
  type RuntimeQuery,
  type RuntimeRow,
  type RuntimeRowKey,
  type RuntimeRowKeyFn,
} from "../protocol/index.ts";
import { aggregateRows } from "./aggregate-functions.ts";

export type QueryExecutionResult = {
  readonly rows: readonly RuntimeRow[];
  readonly totalRows: number;
};

export type QueryExecutionOptions = {
  readonly literalStringFields?: ReadonlySet<string> | undefined;
};

export type GroupedQueryExecutionEffectOptions = QueryExecutionOptions & {
  readonly chunkSize?: number | undefined;
  readonly aggregateYieldInterval?: number | undefined;
};

export const DEFAULT_QUERY_LIMIT = 50;
export const DEFAULT_QUERY_OFFSET = 0;
export const RAW_QUERY_WINDOW_OPTIMIZATION_LIMIT = 10_000;

type SortEntry = {
  readonly row: RuntimeRow;
  readonly index: number;
};

export function executeMemoryQuery(
  rows: readonly RuntimeRow[],
  query: RuntimeQuery,
  idField: string,
  options: QueryExecutionOptions = {},
): QueryExecutionResult {
  return isGroupedQuery(query)
    ? executeGroupedQuery(rows, query, options)
    : executeRawQuery(rows, query, idField, options);
}

export function executeRawQuery(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  idField: string,
  options: QueryExecutionOptions = {},
): QueryExecutionResult {
  const offset = normalizeOffset(query.offset);
  const limit = normalizeLimit(query.limit);
  const orderBy = rawQueryOrderBy(query, idField);
  const windowEnd = offset + limit;
  if (windowEnd <= RAW_QUERY_WINDOW_OPTIMIZATION_LIMIT) {
    return executeRawQueryWindowed(rows, query, idField, options, orderBy, offset, limit);
  }
  const filtered = rows.filter((row) => matchesFilter(row, query.where, options));
  const sorted = stableSortRows(filtered, orderBy);
  const totalRows = sorted.length;
  return {
    rows: sorted
      .slice(offset, offset + limit)
      .map((row) => projectRawRow(row, query.fields, idField)),
    totalRows,
  };
}

function executeRawQueryWindowed(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  idField: string,
  options: QueryExecutionOptions,
  orderBy: OrderBy<RuntimeRow>,
  offset: number,
  limit: number,
): QueryExecutionResult {
  const windowEnd = offset + limit;
  const topRows: SortEntry[] = [];
  let totalRows = 0;
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    if (row === undefined || !matchesFilter(row, query.where, options)) {
      continue;
    }
    totalRows += 1;
    if (windowEnd > 0) {
      insertTopSortEntry(topRows, { row, index }, orderBy, windowEnd);
    }
  }
  return {
    rows: topRows
      .slice(offset, offset + limit)
      .map((entry) => projectRawRow(entry.row, query.fields, idField)),
    totalRows,
  };
}

export function executeGroupedQuery(
  rows: readonly RuntimeRow[],
  query: RuntimeGroupedQuery,
  options: QueryExecutionOptions = {},
): QueryExecutionResult {
  const filtered = rows.filter((row) => matchesFilter(row, query.where, options));
  const groups = buildGroups(filtered, query.groupBy, query.aggregates);
  const sorted = stableSortRows(groups, groupedQueryOrderBy(query));
  const totalRows = sorted.length;
  const offset = normalizeOffset(query.offset);
  const limit = normalizeLimit(query.limit);
  return {
    rows: sorted.slice(offset, offset + limit),
    totalRows,
  };
}

export function executeGroupedQueryEffect(
  rows: readonly RuntimeRow[],
  query: RuntimeGroupedQuery,
  options: GroupedQueryExecutionEffectOptions = {},
): Effect.Effect<QueryExecutionResult> {
  return Effect.gen(function* () {
    const chunkSize = normalizePositiveInteger(options.chunkSize, 10_000);
    const aggregateYieldInterval = normalizePositiveInteger(options.aggregateYieldInterval, 50);
    const groups = new Map<string, RuntimeRow[]>();
    const aggregateDefinitions = Object.entries(query.aggregates);
    for (let index = 0; index < rows.length; index++) {
      const row = rows[index];
      if (row !== undefined && matchesFilter(row, query.where, options)) {
        addGroupedRow(groups, row, query.groupBy);
      }
      if ((index + 1) % chunkSize === 0) {
        yield* Effect.yieldNow;
      }
    }

    const result: RuntimeRow[] = [];
    let aggregateCount = 0;
    for (const groupRows of groups.values()) {
      result.push(groupedResultRowSync(groupRows, query.groupBy, query.aggregates));
      aggregateCount += aggregateDefinitions.length;
      if (aggregateCount % aggregateYieldInterval === 0) {
        yield* Effect.yieldNow;
      }
    }

    const sorted = stableSortRows(result, groupedQueryOrderBy(query));
    const totalRows = sorted.length;
    const offset = normalizeOffset(query.offset);
    const limit = normalizeLimit(query.limit);
    return {
      rows: sorted.slice(offset, offset + limit),
      totalRows,
    };
  });
}

export function matchesFilter(
  row: RuntimeRow,
  filter: RuntimeFilterNode | undefined,
  options: QueryExecutionOptions = {},
): boolean {
  if (filter === undefined) {
    return true;
  }
  if ("op" in filter) {
    return filter.op === "and"
      ? filter.conditions.every((condition) => matchesFilter(row, condition, options))
      : filter.conditions.some((condition) => matchesFilter(row, condition, options));
  }

  const rowValue = row[filter.field];
  const filterValue = filter.value;
  const strictStringEquality = options.literalStringFields?.has(filter.field) ?? false;

  switch (filter.comparator) {
    case "equals":
      return compareEquality(rowValue, filterValue, strictStringEquality);
    case "not_equals":
      return !compareEquality(rowValue, filterValue, strictStringEquality);
    case "greater_than":
      return compareComparable(rowValue, filterValue) > 0;
    case "greater_than_or_equal":
      return compareComparable(rowValue, filterValue) >= 0;
    case "less_than":
      return compareComparable(rowValue, filterValue) < 0;
    case "less_than_or_equal":
      return compareComparable(rowValue, filterValue) <= 0;
    case "contains":
      return (
        typeof rowValue === "string" &&
        typeof filterValue === "string" &&
        rowValue.toLocaleLowerCase().includes(filterValue.toLocaleLowerCase())
      );
    case "starts_with":
      return (
        typeof rowValue === "string" &&
        typeof filterValue === "string" &&
        rowValue.toLocaleLowerCase().startsWith(filterValue.toLocaleLowerCase())
      );
    case "one_of":
      return (
        Array.isArray(filterValue) &&
        filterValue.some((candidate) => compareEquality(rowValue, candidate, strictStringEquality))
      );
  }
}

export function diffVisibleRows(
  previousRows: readonly RuntimeRow[],
  nextRows: readonly RuntimeRow[],
  rowKey: RuntimeRowKeyFn,
): readonly DeltaOperation<RuntimeRow>[] {
  const previousRowKeys: RuntimeRowKey[] = [];
  const previousKeyToIndex = new Map<RuntimeRowKey, number>();
  for (let index = 0; index < previousRows.length; index++) {
    const row = previousRows[index];
    if (row !== undefined) {
      const key = rowKey(row);
      previousRowKeys[index] = key;
      if (!previousKeyToIndex.has(key)) {
        previousKeyToIndex.set(key, index);
      }
    }
  }
  const nextRowKeys: RuntimeRowKey[] = [];
  const nextKeys = new Set<RuntimeRowKey>();
  for (let index = 0; index < nextRows.length; index++) {
    const row = nextRows[index];
    if (row !== undefined) {
      const key = rowKey(row);
      nextRowKeys[index] = key;
      nextKeys.add(key);
    }
  }
  const operations: DeltaOperation<RuntimeRow>[] = [];

  for (let index = 0; index < previousRows.length; index++) {
    const key = previousRowKeys[index];
    if (key !== undefined && !nextKeys.has(key)) {
      operations.push({ type: "remove", key });
    }
  }

  for (let index = 0; index < nextRows.length; index++) {
    const row = nextRows[index];
    const key = nextRowKeys[index];
    if (row === undefined || key === undefined) {
      continue;
    }
    const previousIndex = previousKeyToIndex.get(key) ?? -1;
    const previous = previousIndex >= 0 ? previousRows[previousIndex] : undefined;
    if (!previousKeyToIndex.has(key) || previousIndex !== index || !rowsEqual(previous, row)) {
      operations.push({ type: "upsert", key, row, index });
    }
  }

  return operations;
}

export function collectDependencyFields(query: RuntimeQuery, idField: string): ReadonlySet<string> {
  const fields = new Set<string>([idField]);
  collectFilterFields(query.where, fields);
  if (isGroupedQuery(query)) {
    for (const field of query.groupBy) {
      fields.add(field);
    }
    for (const aggregate of Object.values(query.aggregates)) {
      fields.add(aggregate.field);
    }
  } else {
    for (const [field, enabled] of Object.entries(query.fields)) {
      if (enabled) {
        fields.add(field);
      }
    }
  }
  for (const order of query.orderBy ?? []) {
    fields.add(order.field);
  }
  return fields;
}

export function rowId(row: RuntimeRow, idField: string): RuntimeRowKey {
  return rowKeyByField(row, idField);
}

export function changedFields(previous: RuntimeRow, next: RuntimeRow): ReadonlySet<string> {
  const fields = new Set([...Object.keys(previous), ...Object.keys(next)]);
  const changed = new Set<string>();
  for (const field of fields) {
    if (!Object.is(previous[field], next[field])) {
      changed.add(field);
    }
  }
  return changed;
}

export function normalizeLimit(limit: number | undefined): number {
  if (limit === undefined || !Number.isFinite(limit)) {
    return DEFAULT_QUERY_LIMIT;
  }
  return Math.max(0, Math.min(DEFAULT_QUERY_LIMIT, Math.trunc(limit)));
}

export function normalizeOffset(offset: number | undefined): number {
  if (offset === undefined || !Number.isFinite(offset)) {
    return DEFAULT_QUERY_OFFSET;
  }
  return Math.max(0, Math.trunc(offset));
}

export function isGroupedQuery(query: RuntimeQuery): query is RuntimeGroupedQuery {
  return "groupBy" in query && Array.isArray(query.groupBy);
}

export const rowKeyForMemoryQuery = rowKeyForQuery;

export function projectRawRow(
  row: RuntimeRow,
  fields: RuntimeRawQuery["fields"],
  idField: string,
): RuntimeRow {
  const projected: RuntimeRow = {};
  for (const [field, enabled] of Object.entries(fields)) {
    if (enabled) {
      projected[field] = row[field];
    }
  }
  projected[idField] = row[idField];
  return projected;
}

export function rawQueryOrderBy(query: RuntimeRawQuery, idField: string): OrderBy<RuntimeRow> {
  return [
    ...(query.orderBy ?? []),
    ...(query.orderBy?.some((order) => order.field === idField)
      ? []
      : [{ field: idField, direction: "asc" as const }]),
  ];
}

export function compareRowsForOrder(
  left: RuntimeRow,
  right: RuntimeRow,
  orderBy: OrderBy<RuntimeRow>,
): number {
  for (const order of orderBy) {
    const compared = compareSortValue(left[order.field], right[order.field]);
    if (compared !== 0) {
      return order.direction === "asc" ? compared : -compared;
    }
  }
  return 0;
}

export function stableSortRows(
  rows: readonly RuntimeRow[],
  orderBy: OrderBy<RuntimeRow>,
): RuntimeRow[] {
  return rows
    .map((row, index) => ({ row, index }))
    .sort((left, right) => {
      const compared = compareRowsForOrder(left.row, right.row, orderBy);
      if (compared !== 0) {
        return compared;
      }
      return left.index - right.index;
    })
    .map((entry) => entry.row);
}

function insertTopSortEntry(
  entries: SortEntry[],
  candidate: SortEntry,
  orderBy: OrderBy<RuntimeRow>,
  limit: number,
): void {
  const worst = entries[entries.length - 1];
  if (
    entries.length >= limit &&
    worst !== undefined &&
    compareSortEntries(candidate, worst, orderBy) >= 0
  ) {
    return;
  }
  const index = sortEntryInsertionIndex(entries, candidate, orderBy);
  entries.splice(index, 0, candidate);
  if (entries.length > limit) {
    entries.pop();
  }
}

function sortEntryInsertionIndex(
  entries: readonly SortEntry[],
  candidate: SortEntry,
  orderBy: OrderBy<RuntimeRow>,
): number {
  let low = 0;
  let high = entries.length;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    const entry = entries[middle];
    if (entry !== undefined && compareSortEntries(candidate, entry, orderBy) < 0) {
      high = middle;
    } else {
      low = middle + 1;
    }
  }
  return low;
}

function compareSortEntries(
  left: SortEntry,
  right: SortEntry,
  orderBy: OrderBy<RuntimeRow>,
): number {
  const compared = compareRowsForOrder(left.row, right.row, orderBy);
  return compared !== 0 ? compared : left.index - right.index;
}

export function compareSortValue(left: unknown, right: unknown): number {
  if (left == null && right == null) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }
  return compareComparable(left, right);
}

function compareComparable(left: unknown, right: unknown): number {
  if (BigDecimal.isBigDecimal(left) || BigDecimal.isBigDecimal(right)) {
    const leftDecimal = toBigDecimal(left);
    const rightDecimal = toBigDecimal(right);
    if (leftDecimal !== undefined && rightDecimal !== undefined) {
      return BigDecimal.Order(leftDecimal, rightDecimal);
    }
  }
  if (typeof left === "string" && typeof right === "string") {
    return left.toLocaleLowerCase().localeCompare(right.toLocaleLowerCase());
  }
  if (typeof left === "bigint" && typeof right === "bigint") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "number" && typeof right === "number") {
    return left === right ? 0 : left < right ? -1 : 1;
  }
  if (typeof left === "boolean" && typeof right === "boolean") {
    return left === right ? 0 : left ? 1 : -1;
  }
  return String(left).toLocaleLowerCase().localeCompare(String(right).toLocaleLowerCase());
}

function compareEquality(left: unknown, right: unknown, strictStringEquality = false): boolean {
  if (BigDecimal.isBigDecimal(left) || BigDecimal.isBigDecimal(right)) {
    const leftDecimal = toBigDecimal(left);
    const rightDecimal = toBigDecimal(right);
    if (leftDecimal !== undefined && rightDecimal !== undefined) {
      return BigDecimal.equals(leftDecimal, rightDecimal);
    }
  }
  if (typeof left === "string" && typeof right === "string") {
    if (strictStringEquality) {
      return left === right;
    }
    return left.toLocaleLowerCase() === right.toLocaleLowerCase();
  }
  return Object.is(left, right);
}

export function rowsEqual(left: RuntimeRow | undefined, right: RuntimeRow): boolean {
  if (left === undefined) {
    return false;
  }
  const keys = new Set([...Object.keys(left), ...Object.keys(right)]);
  for (const key of keys) {
    if (!Object.is(left[key], right[key])) {
      return false;
    }
  }
  return true;
}

function collectFilterFields(filter: RuntimeFilterNode | undefined, fields: Set<string>): void {
  if (filter === undefined) {
    return;
  }
  if ("op" in filter) {
    for (const condition of filter.conditions) {
      collectFilterFields(condition, fields);
    }
    return;
  }
  fields.add(filter.field);
}

export function groupedQueryOrderBy(query: RuntimeGroupedQuery): OrderBy<RuntimeRow> {
  return [
    ...(query.orderBy ?? []),
    ...query.groupBy
      .filter((field) => !query.orderBy?.some((order) => order.field === field))
      .map((field) => ({ field, direction: "asc" as const })),
  ];
}

function buildGroups(
  rows: readonly RuntimeRow[],
  groupBy: readonly string[],
  aggregates: RuntimeAggregateMap,
): RuntimeRow[] {
  const groups = new Map<string, RuntimeRow[]>();
  for (const row of rows) {
    addGroupedRow(groups, row, groupBy);
  }

  const result: RuntimeRow[] = [];
  for (const groupRows of groups.values()) {
    result.push(groupedResultRowSync(groupRows, groupBy, aggregates));
  }
  return result;
}

function addGroupedRow(
  groups: Map<string, RuntimeRow[]>,
  row: RuntimeRow,
  groupBy: readonly string[],
): void {
  const key = JSON.stringify(groupBy.map((field) => encodeGroupKey(row[field])));
  const existing = groups.get(key);
  if (existing === undefined) {
    groups.set(key, [row]);
  } else {
    existing.push(row);
  }
}

function groupedResultRowSync(
  groupRows: readonly RuntimeRow[],
  groupBy: readonly string[],
  aggregates: RuntimeAggregateMap,
): RuntimeRow {
  const [first] = groupRows;
  const row: RuntimeRow = {};
  for (const field of groupBy) {
    row[field] = first?.[field];
  }
  for (const [alias, aggregate] of Object.entries(aggregates)) {
    row[alias] = aggregateRows(groupRows, aggregate);
  }
  return row;
}

function encodeGroupKey(value: unknown): unknown {
  return typeof value === "bigint" ? value.toString() : value;
}

function normalizePositiveInteger(value: number | undefined, fallback: number): number {
  return value === undefined || !Number.isFinite(value) || value <= 0
    ? fallback
    : Math.trunc(value);
}

function toBigDecimal(value: unknown): BigDecimal.BigDecimal | undefined {
  if (BigDecimal.isBigDecimal(value)) {
    return value;
  }
  if (typeof value === "bigint") {
    return BigDecimal.fromBigInt(value);
  }
  if (typeof value === "number") {
    return Option.getOrUndefined(BigDecimal.fromNumber(value));
  }
  if (typeof value === "string") {
    return Option.getOrUndefined(BigDecimal.fromString(value));
  }
  return undefined;
}
