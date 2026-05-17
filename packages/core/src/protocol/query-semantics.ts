import * as BigDecimal from "effect/BigDecimal";
import * as Option from "effect/Option";
import type { OrderBy, RuntimeGroupedQuery, RuntimeRawQuery, RuntimeRow } from "./index.ts";

export const QUERY_SEMANTICS_CONTRACT = {
  owner: "protocol/query-semantics",
  memoryAdapter: "worker/query-engine",
  activeViewAdapter: "worker/active-view",
  chdbAdapter: "snapshot/chdb-sql-compiler",
  clientAdapter: "client/rpc-boundary",
  parityTest: "tests/query-semantics-parity.test.ts",
  rules: [
    "raw queries append the topic id field as the stable ascending tiebreak unless already ordered",
    "grouped queries append groupBy fields as stable ascending tiebreaks unless already ordered",
    "sort strings case-insensitively",
    "case-insensitive string ordering compares lower-cased strings with binary JavaScript order to match ClickHouse lower(toString(...)) ordering",
    "filter string equality is case-insensitive except schema literal strings",
    "BigDecimal values compare by Effect BigDecimal semantics",
    "null sorts before non-null for ascending order and after non-null for descending order",
    "undefined and missing row values are materialized as SQL NULL at query boundaries",
    "aggregate functions follow ClickHouse NULL behavior: count() counts rows, other aggregates ignore NULL values and return NULL when no value exists",
  ],
} as const;

export function rawQueryOrderBy(query: RuntimeRawQuery, idField: string): OrderBy<RuntimeRow> {
  return [
    ...(query.orderBy ?? []),
    ...(query.orderBy?.some((order) => order.field === idField)
      ? []
      : [{ field: idField, direction: "asc" as const }]),
  ];
}

export function groupedQueryOrderBy(query: RuntimeGroupedQuery): OrderBy<RuntimeRow> {
  return [
    ...(query.orderBy ?? []),
    ...query.groupBy
      .filter((field) => !query.orderBy?.some((order) => order.field === field))
      .map((field) => ({ field, direction: "asc" as const })),
  ];
}

export function compareRowsForOrder(
  left: RuntimeRow,
  right: RuntimeRow,
  orderBy: OrderBy<RuntimeRow>,
): number {
  for (const order of orderBy) {
    const compared = compareValues(left[order.field], right[order.field]);
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

export function compareValues(left: unknown, right: unknown): number {
  if (left == null && right == null) {
    return 0;
  }
  if (left == null) {
    return -1;
  }
  if (right == null) {
    return 1;
  }
  return compareNonNullValues(left, right);
}

export function compareFilterValues(left: unknown, right: unknown): number {
  if (left == null || right == null) {
    return Number.NaN;
  }
  return compareNonNullValues(left, right);
}

export function valuesEqual(left: unknown, right: unknown, strictStringEquality = false): boolean {
  if (left == null && right == null) {
    return true;
  }
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
    return left.toLowerCase() === right.toLowerCase();
  }
  return Object.is(left, right);
}

export function materializeQueryValue(value: unknown): unknown {
  if (Object.is(value, -0)) {
    return 0;
  }
  return value === undefined ? null : value;
}

function compareNonNullValues(left: unknown, right: unknown): number {
  if (BigDecimal.isBigDecimal(left) || BigDecimal.isBigDecimal(right)) {
    const leftDecimal = toBigDecimal(left);
    const rightDecimal = toBigDecimal(right);
    if (leftDecimal !== undefined && rightDecimal !== undefined) {
      return BigDecimal.Order(leftDecimal, rightDecimal);
    }
  }
  if (typeof left === "string" && typeof right === "string") {
    return compareLowercaseStrings(left, right);
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
  return compareLowercaseStrings(String(left), String(right));
}

function compareLowercaseStrings(left: string, right: string): number {
  const loweredLeft = left.toLowerCase();
  const loweredRight = right.toLowerCase();
  return loweredLeft === loweredRight ? 0 : loweredLeft < loweredRight ? -1 : 1;
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
