import * as BigDecimal from "effect/BigDecimal";
import * as Option from "effect/Option";
import type { RuntimeAggregateDefinition, RuntimeRow } from "../protocol/index.ts";
import { stableStringify } from "../protocol/index.ts";

export type AggregateState = {
  readonly add: (row: RuntimeRow) => void;
  readonly remove: (row: RuntimeRow) => void;
  readonly value: () => unknown;
};

export function makeAggregateState(aggregate: RuntimeAggregateDefinition): AggregateState {
  switch (aggregate.aggFunc) {
    case "count":
      return new CountAggregate();
    case "count_distinct":
      return new CountDistinctAggregate(aggregate.field);
    case "sum":
      return new SumAggregate(aggregate.field);
    case "avg":
      return new AvgAggregate(aggregate.field);
    case "min":
      return new ExtremaAggregate(aggregate.field, "min");
    case "max":
      return new ExtremaAggregate(aggregate.field, "max");
    case "string_concat":
      return new StringConcatAggregate(aggregate.field, aggregate.joiner, aggregate.sort);
    case "string_concat_distinct":
      return new StringConcatDistinctAggregate(aggregate.field, aggregate.joiner, aggregate.sort);
  }
}

export function aggregateRows(
  rows: readonly RuntimeRow[],
  aggregate: RuntimeAggregateDefinition,
): unknown {
  switch (aggregate.aggFunc) {
    case "count":
      return rows.length;
    case "count_distinct":
      return countDistinctRows(rows, aggregate.field);
    case "sum":
      return sumRows(rows, aggregate.field);
    case "avg":
      if (rows.length === 0) {
        return 0;
      }
      const sum = sumRows(rows, aggregate.field);
      return BigDecimal.isBigDecimal(sum)
        ? BigDecimal.divideUnsafe(sum, BigDecimal.fromBigInt(BigInt(rows.length)))
        : sum / rows.length;
    case "min":
      return extremaRows(rows, aggregate.field, "min");
    case "max":
      return extremaRows(rows, aggregate.field, "max");
    case "string_concat":
      return sortedStrings(stringValues(rows, aggregate.field), aggregate.sort).join(
        aggregate.joiner,
      );
    case "string_concat_distinct":
      return sortedStrings(
        Array.from(new Set(stringValues(rows, aggregate.field))),
        aggregate.sort ?? "asc",
      ).join(aggregate.joiner);
  }
}

export function isIncrementalAggregateSupported(aggregate: RuntimeAggregateDefinition): boolean {
  switch (aggregate.aggFunc) {
    case "count":
    case "sum":
    case "min":
    case "max":
      return true;
    case "avg":
    case "count_distinct":
    case "string_concat":
    case "string_concat_distinct":
      return false;
  }
}

class CountAggregate implements AggregateState {
  #count = 0;

  add(): void {
    this.#count += 1;
  }

  remove(): void {
    this.#count = Math.max(0, this.#count - 1);
  }

  value(): number {
    return this.#count;
  }
}

class CountDistinctAggregate implements AggregateState {
  readonly #field: string;
  readonly #counts = new Map<unknown, number>();

  constructor(field: string) {
    this.#field = field;
  }

  add(row: RuntimeRow): void {
    const value = row[this.#field];
    this.#counts.set(value, (this.#counts.get(value) ?? 0) + 1);
  }

  remove(row: RuntimeRow): void {
    decrementNumberCount(this.#counts, row[this.#field]);
  }

  value(): number {
    return this.#counts.size;
  }
}

class SumAggregate implements AggregateState {
  readonly #field: string;
  #numberSum = 0;
  #decimalSum: BigDecimal.BigDecimal | undefined;

  constructor(field: string) {
    this.#field = field;
  }

  add(row: RuntimeRow): void {
    this.#addValue(row[this.#field]);
  }

  remove(row: RuntimeRow): void {
    this.#removeValue(row[this.#field]);
  }

  value(): number | BigDecimal.BigDecimal {
    return this.#decimalSum ?? this.#numberSum;
  }

  #addValue(value: unknown): void {
    if (BigDecimal.isBigDecimal(value)) {
      this.#decimalSum = BigDecimal.sum(
        this.#decimalSum ?? decimalFromNumber(this.#numberSum),
        value,
      );
      return;
    }
    if (this.#decimalSum !== undefined) {
      this.#decimalSum = BigDecimal.sum(
        this.#decimalSum,
        toBigDecimal(value) ?? BigDecimal.make(0n, 0),
      );
      return;
    }
    this.#numberSum += numericValue(value);
  }

  #removeValue(value: unknown): void {
    if (this.#decimalSum !== undefined || BigDecimal.isBigDecimal(value)) {
      this.#decimalSum = BigDecimal.subtract(
        this.#decimalSum ?? decimalFromNumber(this.#numberSum),
        toBigDecimal(value) ?? BigDecimal.make(0n, 0),
      );
      return;
    }
    this.#numberSum -= numericValue(value);
  }
}

class AvgAggregate implements AggregateState {
  readonly #sum: SumAggregate;
  #count = 0;

  constructor(field: string) {
    this.#sum = new SumAggregate(field);
  }

  add(row: RuntimeRow): void {
    this.#count += 1;
    this.#sum.add(row);
  }

  remove(row: RuntimeRow): void {
    this.#count = Math.max(0, this.#count - 1);
    this.#sum.remove(row);
  }

  value(): number | BigDecimal.BigDecimal {
    if (this.#count === 0) {
      return 0;
    }
    const sum = this.#sum.value();
    return BigDecimal.isBigDecimal(sum)
      ? BigDecimal.divideUnsafe(sum, BigDecimal.fromBigInt(BigInt(this.#count)))
      : sum / this.#count;
  }
}

class ExtremaAggregate implements AggregateState {
  readonly #field: string;
  readonly #direction: "min" | "max";
  readonly #values = new Map<string, { readonly value: unknown; count: number }>();

  constructor(field: string, direction: "min" | "max") {
    this.#field = field;
    this.#direction = direction;
  }

  add(row: RuntimeRow): void {
    const value = row[this.#field];
    if (value == null) {
      return;
    }
    const key = aggregateValueKey(value);
    const existing = this.#values.get(key);
    if (existing === undefined) {
      this.#values.set(key, { value, count: 1 });
    } else {
      existing.count += 1;
    }
  }

  remove(row: RuntimeRow): void {
    const value = row[this.#field];
    if (value != null) {
      decrementEntryCount(this.#values, aggregateValueKey(value));
    }
  }

  value(): unknown {
    let hasValue = false;
    let current: unknown;
    for (const entry of this.#values.values()) {
      if (!hasValue) {
        hasValue = true;
        current = entry.value;
        continue;
      }
      const comparison = compareAggregateValue(entry.value, current);
      current =
        this.#direction === "min"
          ? comparison < 0
            ? entry.value
            : current
          : comparison > 0
            ? entry.value
            : current;
    }
    return hasValue ? current : undefined;
  }
}

class StringConcatAggregate implements AggregateState {
  readonly #field: string;
  readonly #joiner: string;
  readonly #sort: "asc" | "desc" | undefined;
  readonly #values: string[] = [];

  constructor(field: string, joiner: string, sort: "asc" | "desc" | undefined) {
    this.#field = field;
    this.#joiner = joiner;
    this.#sort = sort;
  }

  add(row: RuntimeRow): void {
    this.#values.push(stringAggregateValue(row[this.#field]));
  }

  remove(row: RuntimeRow): void {
    const value = stringAggregateValue(row[this.#field]);
    const index = this.#values.indexOf(value);
    if (index >= 0) {
      this.#values.splice(index, 1);
    }
  }

  value(): string {
    return sortedStrings(this.#values, this.#sort).join(this.#joiner);
  }
}

class StringConcatDistinctAggregate implements AggregateState {
  readonly #field: string;
  readonly #joiner: string;
  readonly #sort: "asc" | "desc" | undefined;
  readonly #counts = new Map<string, number>();

  constructor(field: string, joiner: string, sort: "asc" | "desc" | undefined) {
    this.#field = field;
    this.#joiner = joiner;
    this.#sort = sort;
  }

  add(row: RuntimeRow): void {
    const value = stringAggregateValue(row[this.#field]);
    this.#counts.set(value, (this.#counts.get(value) ?? 0) + 1);
  }

  remove(row: RuntimeRow): void {
    decrementNumberCount(this.#counts, stringAggregateValue(row[this.#field]));
  }

  value(): string {
    return sortedStrings([...this.#counts.keys()], this.#sort ?? "asc").join(this.#joiner);
  }
}

function decrementNumberCount<TKey>(counts: Map<TKey, number>, key: TKey): void {
  const current = counts.get(key);
  if (current === undefined) {
    return;
  }
  if (current <= 1) {
    counts.delete(key);
  } else {
    counts.set(key, current - 1);
  }
}

function countDistinctRows(rows: readonly RuntimeRow[], field: string): number {
  const values = new Set<unknown>();
  for (const row of rows) {
    values.add(row[field]);
  }
  return values.size;
}

function sumRows(rows: readonly RuntimeRow[], field: string): number | BigDecimal.BigDecimal {
  let numberSum = 0;
  let decimalSum: BigDecimal.BigDecimal | undefined;
  for (const row of rows) {
    const value = row[field];
    if (BigDecimal.isBigDecimal(value)) {
      decimalSum = BigDecimal.sum(decimalSum ?? decimalFromNumber(numberSum), value);
      continue;
    }
    if (decimalSum !== undefined) {
      decimalSum = BigDecimal.sum(decimalSum, toBigDecimal(value) ?? BigDecimal.make(0n, 0));
      continue;
    }
    numberSum += numericValue(value);
  }
  return decimalSum ?? numberSum;
}

function extremaRows(
  rows: readonly RuntimeRow[],
  field: string,
  direction: "min" | "max",
): unknown {
  let hasValue = false;
  let current: unknown;
  for (const row of rows) {
    const value = row[field];
    if (value == null) {
      continue;
    }
    if (!hasValue) {
      hasValue = true;
      current = value;
      continue;
    }
    const comparison = compareAggregateValue(value, current);
    current =
      direction === "min" ? (comparison < 0 ? value : current) : comparison > 0 ? value : current;
  }
  return hasValue ? current : undefined;
}

function stringValues(rows: readonly RuntimeRow[], field: string): readonly string[] {
  const values: string[] = [];
  for (const row of rows) {
    values.push(stringAggregateValue(row[field]));
  }
  return values;
}

function decrementEntryCount<TKey>(counts: Map<TKey, { count: number }>, key: TKey): void {
  const current = counts.get(key);
  if (current === undefined) {
    return;
  }
  if (current.count <= 1) {
    counts.delete(key);
  } else {
    current.count -= 1;
  }
}

function aggregateValueKey(value: unknown): string {
  return BigDecimal.isBigDecimal(value)
    ? `bigdecimal:${BigDecimal.format(value)}`
    : `${typeof value}:${stableStringify(value)}`;
}

function compareAggregateValue(left: unknown, right: unknown): number {
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

function decimalFromNumber(value: number): BigDecimal.BigDecimal {
  return toBigDecimal(value) ?? BigDecimal.make(0n, 0);
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
  return stableStringify(value);
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
