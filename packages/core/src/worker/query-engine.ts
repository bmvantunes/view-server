import * as Effect from "effect/Effect";
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
import {
  compareFilterValues,
  compareRowsForOrder,
  compareValues as compareSortValue,
  groupedQueryOrderBy,
  rawQueryOrderBy,
  stableSortRows,
  valuesEqual,
} from "../protocol/query-semantics.ts";
import { projectRow as projectRawRow, projectedRowsEqual as rowsEqual } from "./projection.ts";

export {
  compareRowsForOrder,
  compareSortValue,
  groupedQueryOrderBy,
  projectRawRow,
  rawQueryOrderBy,
  rowsEqual,
  stableSortRows,
};

export type QueryExecutionResult = {
  readonly rows: readonly RuntimeRow[];
  readonly totalRows: number;
};

export type QueryExecutionOptions = {
  readonly literalStringFields?: ReadonlySet<string> | undefined;
};

export type CompiledFilter = (row: RuntimeRow) => boolean;

export type GroupedQueryExecutionEffectOptions = QueryExecutionOptions & {
  readonly chunkSize?: number | undefined;
  readonly aggregateYieldInterval?: number | undefined;
};

export const DEFAULT_QUERY_LIMIT = 50;
export const DEFAULT_QUERY_OFFSET = 0;
export const RAW_QUERY_WINDOW_SPLICE_LIMIT = 10_000;
export const RAW_QUERY_WINDOW_OPTIMIZATION_LIMIT = 100_000;

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
  const filter = compileFilter(query.where, options);
  if (windowEnd <= RAW_QUERY_WINDOW_SPLICE_LIMIT) {
    return executeRawQueryWindowedSplice(rows, query, idField, filter, orderBy, offset, limit);
  }
  if (windowEnd <= RAW_QUERY_WINDOW_OPTIMIZATION_LIMIT) {
    return executeRawQueryWindowedHeap(rows, query, idField, filter, orderBy, offset, limit);
  }
  const filtered = rows.filter(filter);
  const sorted = stableSortRows(filtered, orderBy);
  const totalRows = sorted.length;
  return {
    rows: sorted
      .slice(offset, offset + limit)
      .map((row) => projectRawRow(row, query.fields, idField)),
    totalRows,
  };
}

function executeRawQueryWindowedSplice(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  idField: string,
  filter: CompiledFilter,
  orderBy: OrderBy<RuntimeRow>,
  offset: number,
  limit: number,
): QueryExecutionResult {
  const windowEnd = offset + limit;
  const topRows: SortEntry[] = [];
  let totalRows = 0;
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    if (row === undefined || !filter(row)) {
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

function executeRawQueryWindowedHeap(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  idField: string,
  filter: CompiledFilter,
  orderBy: OrderBy<RuntimeRow>,
  offset: number,
  limit: number,
): QueryExecutionResult {
  const windowEnd = offset + limit;
  const topRows: SortEntry[] = [];
  let totalRows = 0;
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    if (row === undefined || !filter(row)) {
      continue;
    }
    totalRows += 1;
    if (windowEnd > 0) {
      offerTopSortEntry(topRows, { row, index }, orderBy, windowEnd);
    }
  }
  topRows.sort((left, right) => compareSortEntries(left, right, orderBy));
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
  const filter = compileFilter(query.where, options);
  const filtered = rows.filter(filter);
  const groups = buildGroups(filtered, query.groupBy, query.aggregates);
  const offset = normalizeOffset(query.offset);
  const limit = normalizeLimit(query.limit);
  return {
    rows: selectWindowRows(groups, groupedQueryOrderBy(query), offset, limit),
    totalRows: groups.length,
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
    const filter = compileFilter(query.where, options);
    for (let index = 0; index < rows.length; index++) {
      const row = rows[index];
      if (row !== undefined && filter(row)) {
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

    const offset = normalizeOffset(query.offset);
    const limit = normalizeLimit(query.limit);
    return {
      rows: selectWindowRows(result, groupedQueryOrderBy(query), offset, limit),
      totalRows: result.length,
    };
  });
}

export function selectWindowRows(
  rows: readonly RuntimeRow[],
  orderBy: OrderBy<RuntimeRow>,
  offset: number,
  limit: number,
): readonly RuntimeRow[] {
  const windowEnd = offset + limit;
  if (windowEnd <= 0) {
    return [];
  }
  if (windowEnd <= RAW_QUERY_WINDOW_SPLICE_LIMIT) {
    return selectWindowRowsSplice(rows, orderBy, offset, limit);
  }
  if (windowEnd <= RAW_QUERY_WINDOW_OPTIMIZATION_LIMIT) {
    return selectWindowRowsHeap(rows, orderBy, offset, limit);
  }
  return stableSortRows(rows, orderBy).slice(offset, offset + limit);
}

function selectWindowRowsSplice(
  rows: readonly RuntimeRow[],
  orderBy: OrderBy<RuntimeRow>,
  offset: number,
  limit: number,
): readonly RuntimeRow[] {
  const windowEnd = offset + limit;
  const topRows: SortEntry[] = [];
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    if (row !== undefined) {
      insertTopSortEntry(topRows, { row, index }, orderBy, windowEnd);
    }
  }
  return topRows.slice(offset, offset + limit).map((entry) => entry.row);
}

function selectWindowRowsHeap(
  rows: readonly RuntimeRow[],
  orderBy: OrderBy<RuntimeRow>,
  offset: number,
  limit: number,
): readonly RuntimeRow[] {
  const windowEnd = offset + limit;
  const topRows: SortEntry[] = [];
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    if (row !== undefined) {
      offerTopSortEntry(topRows, { row, index }, orderBy, windowEnd);
    }
  }
  topRows.sort((left, right) => compareSortEntries(left, right, orderBy));
  return topRows.slice(offset, offset + limit).map((entry) => entry.row);
}

export function matchesFilter(
  row: RuntimeRow,
  filter: RuntimeFilterNode | undefined,
  options: QueryExecutionOptions = {},
): boolean {
  return compileFilter(filter, options)(row);
}

export function compileFilter(
  filter: RuntimeFilterNode | undefined,
  options: QueryExecutionOptions = {},
): CompiledFilter {
  if (filter === undefined) {
    return () => true;
  }
  if ("op" in filter) {
    const conditions = filter.conditions.map((condition) => compileFilter(condition, options));
    return filter.op === "and"
      ? (row) => conditions.every((condition) => condition(row))
      : (row) => conditions.some((condition) => condition(row));
  }

  const strictStringEquality = options.literalStringFields?.has(filter.field) ?? false;
  const equality = compileEqualityMatcher(filter.value, strictStringEquality);

  switch (filter.comparator) {
    case "equals":
      return (row) => equality(row[filter.field]);
    case "not_equals":
      return (row) => !equality(row[filter.field]);
    case "greater_than":
      return (row) => compareFilterValues(row[filter.field], filter.value) > 0;
    case "greater_than_or_equal":
      return (row) => compareFilterValues(row[filter.field], filter.value) >= 0;
    case "less_than":
      return (row) => compareFilterValues(row[filter.field], filter.value) < 0;
    case "less_than_or_equal":
      return (row) => compareFilterValues(row[filter.field], filter.value) <= 0;
    case "contains":
      return compileStringMatcher(filter.field, filter.value, "contains");
    case "starts_with":
      return compileStringMatcher(filter.field, filter.value, "starts_with");
    case "one_of":
      return compileOneOfMatcher(filter.field, filter.value, strictStringEquality);
  }
}

function compileEqualityMatcher(
  filterValue: unknown,
  strictStringEquality: boolean,
): (rowValue: unknown) => boolean {
  if (typeof filterValue === "string") {
    const expected = strictStringEquality ? filterValue : filterValue.toLocaleLowerCase();
    return (rowValue) =>
      typeof rowValue === "string"
        ? (strictStringEquality ? rowValue : rowValue.toLocaleLowerCase()) === expected
        : valuesEqual(rowValue, filterValue, strictStringEquality);
  }
  if (
    filterValue === null ||
    filterValue === undefined ||
    typeof filterValue === "number" ||
    typeof filterValue === "bigint" ||
    typeof filterValue === "boolean"
  ) {
    return (rowValue) =>
      Object.is(rowValue, filterValue) || valuesEqual(rowValue, filterValue, strictStringEquality);
  }
  return (rowValue) => {
    return valuesEqual(rowValue, filterValue, strictStringEquality);
  };
}

function compileStringMatcher(
  field: string,
  filterValue: unknown,
  comparator: "contains" | "starts_with",
): CompiledFilter {
  if (typeof filterValue !== "string") {
    return () => false;
  }
  const needle = filterValue.toLocaleLowerCase();
  return (row) => {
    const rowValue = row[field];
    if (typeof rowValue !== "string") {
      return false;
    }
    const haystack = rowValue.toLocaleLowerCase();
    return comparator === "contains" ? haystack.includes(needle) : haystack.startsWith(needle);
  };
}

function compileOneOfMatcher(
  field: string,
  filterValue: unknown,
  strictStringEquality: boolean,
): CompiledFilter {
  if (!Array.isArray(filterValue)) {
    return () => false;
  }
  const strings = new Set<string>();
  const numbers = new Set<number>();
  const bigints = new Set<bigint>();
  const booleans = new Set<boolean>();
  let hasPositiveZero = false;
  let hasNegativeZero = false;
  let hasNull = false;
  let hasUndefined = false;
  const fallbackCandidates: unknown[] = [];
  for (const candidate of filterValue) {
    switch (typeof candidate) {
      case "string": {
        strings.add(strictStringEquality ? candidate : candidate.toLocaleLowerCase());
        break;
      }
      case "number": {
        if (Object.is(candidate, -0)) {
          hasNegativeZero = true;
        } else if (Object.is(candidate, 0)) {
          hasPositiveZero = true;
        } else {
          numbers.add(candidate);
        }
        break;
      }
      case "bigint": {
        bigints.add(candidate);
        break;
      }
      case "boolean": {
        booleans.add(candidate);
        break;
      }
      case "undefined": {
        hasUndefined = true;
        break;
      }
      default: {
        if (candidate === null) {
          hasNull = true;
        } else {
          fallbackCandidates.push(candidate);
        }
      }
    }
  }
  return (row) => {
    const rowValue = row[field];
    switch (typeof rowValue) {
      case "string":
        if (strings.has(strictStringEquality ? rowValue : rowValue.toLocaleLowerCase())) {
          return true;
        }
        break;
      case "number":
        if (
          (Object.is(rowValue, -0) && hasNegativeZero) ||
          (Object.is(rowValue, 0) && hasPositiveZero) ||
          (!Object.is(rowValue, -0) && !Object.is(rowValue, 0) && numbers.has(rowValue))
        ) {
          return true;
        }
        break;
      case "bigint":
        if (bigints.has(rowValue)) {
          return true;
        }
        break;
      case "boolean":
        if (booleans.has(rowValue)) {
          return true;
        }
        break;
      case "undefined":
        if (hasUndefined) {
          return true;
        }
        break;
      default:
        if (rowValue === null && hasNull) {
          return true;
        }
    }
    const candidates =
      rowValue === null || (typeof rowValue !== "object" && typeof rowValue !== "function")
        ? fallbackCandidates
        : filterValue;
    return candidates.some((candidate) => valuesEqual(rowValue, candidate, strictStringEquality));
  };
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

function offerTopSortEntry(
  heap: SortEntry[],
  candidate: SortEntry,
  orderBy: OrderBy<RuntimeRow>,
  limit: number,
): void {
  const worst = heap[0];
  if (
    heap.length >= limit &&
    worst !== undefined &&
    compareSortEntries(candidate, worst, orderBy) >= 0
  ) {
    return;
  }
  if (heap.length < limit) {
    heap.push(candidate);
    siftTopSortEntryUp(heap, heap.length - 1, orderBy);
    return;
  }
  heap[0] = candidate;
  siftTopSortEntryDown(heap, 0, orderBy);
}

function siftTopSortEntryUp(
  heap: SortEntry[],
  startIndex: number,
  orderBy: OrderBy<RuntimeRow>,
): void {
  let index = startIndex;
  while (index > 0) {
    const parentIndex = Math.floor((index - 1) / 2);
    const parent = heap[parentIndex];
    const current = heap[index];
    if (
      parent === undefined ||
      current === undefined ||
      compareSortEntries(parent, current, orderBy) >= 0
    ) {
      return;
    }
    heap[parentIndex] = current;
    heap[index] = parent;
    index = parentIndex;
  }
}

function siftTopSortEntryDown(
  heap: SortEntry[],
  startIndex: number,
  orderBy: OrderBy<RuntimeRow>,
): void {
  let index = startIndex;
  while (true) {
    const leftIndex = index * 2 + 1;
    const rightIndex = leftIndex + 1;
    let worstIndex = index;
    const left = heap[leftIndex];
    const right = heap[rightIndex];
    const worst = heap[worstIndex];
    if (left !== undefined && worst !== undefined && compareSortEntries(left, worst, orderBy) > 0) {
      worstIndex = leftIndex;
    }
    const currentWorst = heap[worstIndex];
    if (
      right !== undefined &&
      currentWorst !== undefined &&
      compareSortEntries(right, currentWorst, orderBy) > 0
    ) {
      worstIndex = rightIndex;
    }
    if (worstIndex === index) {
      return;
    }
    const next = heap[worstIndex];
    const current = heap[index];
    if (next === undefined || current === undefined) {
      return;
    }
    heap[index] = next;
    heap[worstIndex] = current;
    index = worstIndex;
  }
}

function compareSortEntries(
  left: SortEntry,
  right: SortEntry,
  orderBy: OrderBy<RuntimeRow>,
): number {
  const compared = compareRowsForOrder(left.row, right.row, orderBy);
  return compared !== 0 ? compared : left.index - right.index;
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
