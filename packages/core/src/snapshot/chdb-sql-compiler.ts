import * as BigDecimal from "effect/BigDecimal";
import type {
  RuntimeAggregateDefinition,
  RuntimeGroupedQuery,
  RuntimeQuery,
} from "../protocol/index.ts";
import { rawQueryOrderBy } from "../protocol/query-semantics.ts";

const BIG_DECIMAL_SCALE = 38;
type BigDecimalColumnType = "Decimal(76, 38)";
export const BIG_DECIMAL_COLUMN_TYPE: BigDecimalColumnType = "Decimal(76, 38)";

export type ColumnType = "String" | "Float64" | "Int64" | "UInt8" | BigDecimalColumnType;

export type Column = {
  readonly name: string;
  readonly type: ColumnType;
  readonly nullable: boolean;
};

export type CompiledQuerySql = {
  readonly rowsSql: string;
  readonly countSql: string;
  readonly decimalFields: ReadonlySet<string>;
  readonly integerFields: ReadonlySet<string>;
  readonly numberFields: ReadonlySet<string>;
};

export function columnSqlType(column: Column): string {
  return column.nullable ? `Nullable(${column.type})` : column.type;
}

export function compileQuerySql(
  query: RuntimeQuery,
  idField: string,
  columns: readonly Column[],
  literalStringFields: ReadonlySet<string>,
  tableName: string,
): CompiledQuerySql {
  const source = latestRowsSql(columns, idField, tableName);
  if (isGroupedQuery(query)) {
    const where = query.where ? `WHERE ${compileFilter(query.where, literalStringFields)}` : "";
    const groupBy = query.groupBy.map(quoteIdentifier).join(", ");
    const select = [
      ...query.groupBy.map(quoteIdentifier),
      ...Object.entries(query.aggregates).map(
        ([alias, aggregate]) =>
          `${compileAggregate(aggregate, columns)} AS ${quoteIdentifier(alias)}`,
      ),
    ].join(", ");
    const grouped = `SELECT ${select} FROM (${source}) ${where} GROUP BY ${groupBy}`;
    return {
      rowsSql: `SELECT * FROM (${grouped}) ${compileOrderBy(query.orderBy ?? [], columns)} ${compileLimit(query)}`,
      countSql: `SELECT count() AS totalRows FROM (${grouped})`,
      decimalFields: groupedDecimalFields(query, columns),
      integerFields: groupedIntegerFields(query, columns),
      numberFields: groupedNumberFields(query),
    };
  }

  const selected = new Set(
    Object.entries(query.fields)
      .filter(([, enabled]) => enabled)
      .map(([field]) => field),
  );
  selected.add(idField);
  const where = query.where ? `WHERE ${compileFilter(query.where, literalStringFields)}` : "";
  return {
    rowsSql: `SELECT ${Array.from(selected).map(quoteIdentifier).join(", ")} FROM (${source}) ${where} ${compileOrderBy(rawQueryOrderBy(query, idField), columns)} ${compileLimit(query)}`,
    countSql: `SELECT count() AS totalRows FROM (${source}) ${where}`,
    decimalFields: selectedDecimalFields(Array.from(selected), columns),
    integerFields: selectedIntegerFields(Array.from(selected), columns),
    numberFields: new Set(),
  };
}

export function quoteIdentifier(identifier: string): string {
  return "`" + identifier.replaceAll("`", "``") + "`";
}

function latestRowsSql(columns: readonly Column[], idField: string, tableName: string): string {
  const selected = columns.map((column) => quoteIdentifier(column.name)).join(", ");
  const id = quoteIdentifier(idField);
  return `SELECT ${selected} FROM (SELECT ${selected}, __view_server_deleted, __view_server_version FROM ${quoteIdentifier(tableName)} ORDER BY ${id}, __view_server_version DESC LIMIT 1 BY ${id}) WHERE __view_server_deleted = 0`;
}

function selectedDecimalFields(
  selected: readonly string[],
  columns: readonly Column[],
): ReadonlySet<string> {
  const decimalColumns = new Set(
    columns
      .filter((column) => column.type === BIG_DECIMAL_COLUMN_TYPE)
      .map((column) => column.name),
  );
  return new Set(selected.filter((field) => decimalColumns.has(field)));
}

function selectedIntegerFields(
  selected: readonly string[],
  columns: readonly Column[],
): ReadonlySet<string> {
  const integerColumns = new Set(
    columns.filter((column) => column.type === "Int64").map((column) => column.name),
  );
  return new Set(selected.filter((field) => integerColumns.has(field)));
}

function groupedDecimalFields(
  query: RuntimeGroupedQuery,
  columns: readonly Column[],
): ReadonlySet<string> {
  const decimalColumns = new Set(
    columns
      .filter((column) => column.type === BIG_DECIMAL_COLUMN_TYPE)
      .map((column) => column.name),
  );
  const fields = new Set(query.groupBy.filter((field) => decimalColumns.has(field)));
  for (const [alias, aggregate] of Object.entries(query.aggregates)) {
    if (
      decimalColumns.has(aggregate.field) &&
      (aggregate.aggFunc === "sum" ||
        aggregate.aggFunc === "avg" ||
        aggregate.aggFunc === "min" ||
        aggregate.aggFunc === "max")
    ) {
      fields.add(alias);
    }
  }
  return fields;
}

function groupedIntegerFields(
  query: RuntimeGroupedQuery,
  columns: readonly Column[],
): ReadonlySet<string> {
  const integerColumns = new Set(
    columns.filter((column) => column.type === "Int64").map((column) => column.name),
  );
  const fields = new Set(query.groupBy.filter((field) => integerColumns.has(field)));
  for (const [alias, aggregate] of Object.entries(query.aggregates)) {
    if (
      integerColumns.has(aggregate.field) &&
      (aggregate.aggFunc === "sum" || aggregate.aggFunc === "min" || aggregate.aggFunc === "max")
    ) {
      fields.add(alias);
    }
  }
  return fields;
}

function groupedNumberFields(query: RuntimeGroupedQuery): ReadonlySet<string> {
  const fields = new Set<string>();
  for (const [alias, aggregate] of Object.entries(query.aggregates)) {
    if (aggregate.aggFunc === "count" || aggregate.aggFunc === "count_distinct") {
      fields.add(alias);
    }
  }
  return fields;
}

function compileLimit(query: {
  readonly limit?: number | undefined;
  readonly offset?: number | undefined;
}): string {
  const limit = Math.max(0, Math.min(50, Math.trunc(query.limit ?? 50)));
  const offset = Math.max(0, Math.trunc(query.offset ?? 0));
  return `LIMIT ${limit} OFFSET ${offset}`;
}

function compileOrderBy(
  orderBy: readonly { readonly field: string; readonly direction: "asc" | "desc" }[],
  columns: readonly Column[],
): string {
  if (orderBy.length === 0) {
    return "";
  }
  const columnTypes = new Map(columns.map((column) => [column.name, column.type]));
  const nullableColumns = new Set(
    columns.filter((column) => column.nullable).map((column) => column.name),
  );
  return `ORDER BY ${orderBy
    .map((order) => {
      const field = quoteIdentifier(order.field);
      const expression =
        columnTypes.get(order.field) === "String" ? `lower(toString(${field}))` : field;
      const nullOrdering = nullableColumns.has(order.field)
        ? `isNull(${field}) ${order.direction === "asc" ? "DESC" : "ASC"}, `
        : "";
      return `${nullOrdering}${expression} ${order.direction.toUpperCase()}`;
    })
    .join(", ")}`;
}

function compileFilter(
  filter: RuntimeQuery["where"],
  literalStringFields: ReadonlySet<string>,
): string {
  if (filter === undefined) {
    return "1";
  }
  if ("op" in filter) {
    const joiner = filter.op === "and" ? " AND " : " OR ";
    return `(${filter.conditions.map((condition) => compileFilter(condition, literalStringFields)).join(joiner)})`;
  }
  const field = quoteIdentifier(filter.field);
  const value = filter.value;
  const strictStringEquality = literalStringFields.has(filter.field);
  if (typeof value === "string") {
    const lowered = `lower(toString(${field}))`;
    if (filter.comparator === "equals") {
      if (strictStringEquality) {
        return `${field} = ${literal(value)}`;
      }
      return `${lowered} = lower(${literal(value)})`;
    }
    if (filter.comparator === "not_equals") {
      if (strictStringEquality) {
        return `${field} != ${literal(value)}`;
      }
      return `${lowered} != lower(${literal(value)})`;
    }
    if (filter.comparator === "contains") {
      return `position(${lowered}, lower(${literal(value)})) > 0`;
    }
    if (filter.comparator === "starts_with") {
      return `startsWith(${lowered}, lower(${literal(value)}))`;
    }
  }
  if (filter.comparator === "one_of" && Array.isArray(value)) {
    if (value.length === 0) {
      return "0";
    }
    if (value.every((item) => typeof item === "string")) {
      if (strictStringEquality) {
        return `${field} IN (${value.map((item) => literal(String(item))).join(", ")})`;
      }
      return `lower(toString(${field})) IN (${value.map((item) => `lower(${literal(String(item))})`).join(", ")})`;
    }
    return `${field} IN (${value.map(compileValue).join(", ")})`;
  }
  switch (filter.comparator) {
    case "equals":
      return `${field} = ${compileValue(value)}`;
    case "not_equals":
      return `${field} != ${compileValue(value)}`;
    case "greater_than":
      return `${field} > ${compileValue(value)}`;
    case "greater_than_or_equal":
      return `${field} >= ${compileValue(value)}`;
    case "less_than":
      return `${field} < ${compileValue(value)}`;
    case "less_than_or_equal":
      return `${field} <= ${compileValue(value)}`;
    default:
      return "0";
  }
}

function compileAggregate(
  aggregate: RuntimeAggregateDefinition,
  columns: readonly Column[],
): string {
  const field = quoteIdentifier(aggregate.field);
  const column = columns.find((candidate) => candidate.name === aggregate.field);
  switch (aggregate.aggFunc) {
    case "count":
      return "count()";
    case "count_distinct":
      return `uniqExact(${field})`;
    case "sum":
      return `sum(${field})`;
    case "avg":
      return `avg(${field})`;
    case "min":
      return column?.type === "String"
        ? `argMin(${field}, lower(toString(${field})))`
        : `min(${field})`;
    case "max":
      return column?.type === "String"
        ? `argMax(${field}, lower(toString(${field})))`
        : `max(${field})`;
    case "string_concat": {
      const values =
        aggregate.sort === "desc"
          ? `arrayReverseSort(value -> lower(value), groupArray(toString(${field})))`
          : aggregate.sort === "asc"
            ? `arraySort(value -> lower(value), groupArray(toString(${field})))`
            : `groupArray(toString(${field}))`;
      return `arrayStringConcat(${values}, ${literal(aggregate.joiner)})`;
    }
    case "string_concat_distinct": {
      const values =
        aggregate.sort === "desc"
          ? `arrayReverseSort(value -> lower(value), groupUniqArray(toString(${field})))`
          : `arraySort(value -> lower(value), groupUniqArray(toString(${field})))`;
      return `arrayStringConcat(${values}, ${literal(aggregate.joiner)})`;
    }
  }
}

function isGroupedQuery(query: RuntimeQuery): query is RuntimeGroupedQuery {
  return "groupBy" in query && Array.isArray(query.groupBy);
}

function compileValue(value: unknown): string {
  if (value == null) {
    return "NULL";
  }
  if (BigDecimal.isBigDecimal(value)) {
    return `toDecimal256(${literal(BigDecimal.format(value))}, ${BIG_DECIMAL_SCALE})`;
  }
  if (typeof value === "string") {
    return literal(value);
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "0";
  }
  if (typeof value === "boolean") {
    return value ? "1" : "0";
  }
  return "0";
}

function literal(value: string): string {
  return `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
}
