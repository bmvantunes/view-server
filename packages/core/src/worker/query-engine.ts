import { BigDecimal, Option } from "effect";
import {
  rowKeyForQuery,
  type DeltaOperation,
  type OrderBy,
  type RuntimeAggregateDefinition,
  type RuntimeAggregateMap,
  type RuntimeGroupedQuery,
  type RuntimeFilterNode,
  type RuntimeRawQuery,
  type RuntimeQuery,
  type RuntimeRow,
  type RuntimeRowKeyFn,
} from "../protocol/index.ts";

export type QueryExecutionResult = {
  readonly rows: readonly RuntimeRow[];
  readonly totalRows: number;
};

export type QueryExecutionOptions = {
  readonly literalStringFields?: ReadonlySet<string> | undefined;
};

export const DEFAULT_QUERY_LIMIT = 50;
export const DEFAULT_QUERY_OFFSET = 0;

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
  const filtered = rows.filter((row) => matchesFilter(row, query.where, options));
  const sorted = stableSortRows(filtered, [
    ...(query.orderBy ?? []),
    ...(query.orderBy?.some((order) => order.field === idField)
      ? []
      : [{ field: idField, direction: "asc" as const }]),
  ]);
  const totalRows = sorted.length;
  const offset = normalizeOffset(query.offset);
  const limit = normalizeLimit(query.limit);
  return {
    rows: sorted.slice(offset, offset + limit).map((row) => projectRow(row, query.fields, idField)),
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
  const sorted = stableSortRows(groups, [
    ...(query.orderBy ?? []),
    ...query.groupBy
      .filter((field) => !query.orderBy?.some((order) => order.field === field))
      .map((field) => ({ field, direction: "asc" as const })),
  ]);
  const totalRows = sorted.length;
  const offset = normalizeOffset(query.offset);
  const limit = normalizeLimit(query.limit);
  return {
    rows: sorted.slice(offset, offset + limit),
    totalRows,
  };
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
  const previousKeys = new Set(previousRows.map(rowKey));
  const nextKeys = new Set(nextRows.map(rowKey));
  const operations: DeltaOperation<RuntimeRow>[] = [];

  for (const row of previousRows) {
    const key = rowKey(row);
    if (!nextKeys.has(key)) {
      operations.push({ type: "remove", key });
    }
  }

  nextRows.forEach((row, index) => {
    const key = rowKey(row);
    const previousIndex = previousRows.findIndex((previous) => rowKey(previous) === key);
    const previous = previousIndex >= 0 ? previousRows[previousIndex] : undefined;
    if (!previousKeys.has(key) || previousIndex !== index || !rowsEqual(previous, row)) {
      operations.push({ type: "upsert", key, row, index });
    }
  });

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

export function rowId(row: RuntimeRow, idField: string): string | number {
  const value = row[idField];
  if (typeof value === "string" || typeof value === "number") {
    return value;
  }
  return String(value);
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

function projectRow(
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

function stableSortRows(rows: readonly RuntimeRow[], orderBy: OrderBy<RuntimeRow>): RuntimeRow[] {
  return rows
    .map((row, index) => ({ row, index }))
    .sort((left, right) => {
      for (const order of orderBy) {
        const compared = compareSortValue(left.row[order.field], right.row[order.field]);
        if (compared !== 0) {
          return order.direction === "asc" ? compared : -compared;
        }
      }
      return left.index - right.index;
    })
    .map((entry) => entry.row);
}

function compareSortValue(left: unknown, right: unknown): number {
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

function rowsEqual(left: RuntimeRow | undefined, right: RuntimeRow): boolean {
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

function buildGroups(
  rows: readonly RuntimeRow[],
  groupBy: readonly string[],
  aggregates: RuntimeAggregateMap,
): RuntimeRow[] {
  const groups = new Map<string, readonly RuntimeRow[]>();
  for (const row of rows) {
    const key = JSON.stringify(groupBy.map((field) => encodeGroupKey(row[field])));
    const existing = groups.get(key) ?? [];
    groups.set(key, [...existing, row]);
  }

  const result: RuntimeRow[] = [];
  for (const groupRows of groups.values()) {
    const [first] = groupRows;
    const row: RuntimeRow = {};
    for (const field of groupBy) {
      row[field] = first?.[field];
    }
    for (const [alias, aggregate] of Object.entries(aggregates)) {
      row[alias] = aggregateRows(groupRows, aggregate);
    }
    result.push(row);
  }
  return result;
}

function encodeGroupKey(value: unknown): unknown {
  return typeof value === "bigint" ? value.toString() : value;
}

function aggregateRows(
  rows: readonly RuntimeRow[],
  aggregate: RuntimeAggregateDefinition,
): unknown {
  switch (aggregate.aggFunc) {
    case "count":
      return rows.length;
    case "count_distinct":
      return new Set(rows.map((row) => row[aggregate.field])).size;
    case "sum":
      return sumNumeric(rows.map((row) => row[aggregate.field]));
    case "avg":
      return avgNumeric(rows.map((row) => row[aggregate.field]));
    case "min":
      return extrema(
        rows.map((row) => row[aggregate.field]),
        "min",
      );
    case "max":
      return extrema(
        rows.map((row) => row[aggregate.field]),
        "max",
      );
    case "string_concat":
      return sortedStrings(
        rows.map((row) => stringAggregateValue(row[aggregate.field])),
        aggregate.sort,
      ).join(aggregate.joiner);
    case "string_concat_distinct":
      return sortedStrings(
        Array.from(new Set(rows.map((row) => stringAggregateValue(row[aggregate.field])))),
        aggregate.sort ?? "asc",
      ).join(aggregate.joiner);
  }
}

function stringAggregateValue(value: unknown): string {
  if (value == null) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint" || typeof value === "boolean") {
    return String(value);
  }
  return JSON.stringify(value);
}

function numericValue(value: unknown): number {
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "bigint") {
    return Number(value);
  }
  return 0;
}

function sumNumeric(values: readonly unknown[]): number | BigDecimal.BigDecimal {
  if (values.some(BigDecimal.isBigDecimal)) {
    return BigDecimal.sumAll(values.map((value) => toBigDecimal(value) ?? BigDecimal.make(0n, 0)));
  }
  return values.reduce<number>((sum, value) => sum + numericValue(value), 0);
}

function avgNumeric(values: readonly unknown[]): number | BigDecimal.BigDecimal {
  if (values.length === 0) {
    return 0;
  }
  const sum = sumNumeric(values);
  if (BigDecimal.isBigDecimal(sum)) {
    return BigDecimal.divideUnsafe(sum, BigDecimal.fromBigInt(BigInt(values.length)));
  }
  return sum / values.length;
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

function extrema(values: readonly unknown[], direction: "min" | "max"): unknown {
  const usable = values.filter((value) => value != null);
  if (usable.length === 0) {
    return undefined;
  }
  return usable.reduce((current, value) => {
    const comparison = compareComparable(value, current);
    return direction === "min"
      ? comparison < 0
        ? value
        : current
      : comparison > 0
        ? value
        : current;
  });
}

function sortedStrings(
  values: readonly string[],
  direction: "asc" | "desc" | undefined,
): readonly string[] {
  if (direction === undefined) {
    return values;
  }
  const sorted = [...values].sort((left, right) =>
    left.toLocaleLowerCase().localeCompare(right.toLocaleLowerCase()),
  );
  return direction === "asc" ? sorted : sorted.reverse();
}
