import { Session } from "chdb";
import { BigDecimal, Effect } from "effect";
import type { TopicConfig } from "../config/index.ts";
import { snapshotBackendFailed, type ViewServerError } from "../errors.ts";
import type {
  RuntimeAggregateDefinition,
  RuntimeGroupedQuery,
  RuntimeQuery,
  RuntimeRow,
} from "../protocol/index.ts";
import type { MutationLogEntry, WorkerVersion } from "../worker/mutation-log.ts";
import {
  createMemorySnapshotBackend,
  type SnapshotBackend,
  type SnapshotBackendResult,
  type VersionedRow,
} from "./snapshot-backend.ts";

const BIG_DECIMAL_SCALE = 38;
type BigDecimalColumnType = "Decimal(76, 38)";
const BIG_DECIMAL_COLUMN_TYPE: BigDecimalColumnType = "Decimal(76, 38)";

type ColumnType = "String" | "Float64" | "Int64" | "UInt8" | BigDecimalColumnType;

type Column = {
  readonly name: string;
  readonly type: ColumnType;
};

const TABLE_NAME = "topic_rows";
const DELETED_COLUMN = "__view_server_deleted";
const VERSION_COLUMN = "__view_server_version";
const JSON_DECIMAL_SETTINGS = "SETTINGS output_format_json_quote_decimals=1";

export function createChdbSnapshotBackend(): SnapshotBackend {
  return new ChdbSnapshotBackend();
}

export function createChdbSnapshotBackendFactory(): (
  topic: string,
  config: TopicConfig,
) => SnapshotBackend {
  return (_topic, config) =>
    config.snapshot?.backend === "chdb"
      ? createChdbSnapshotBackend()
      : createMemorySnapshotBackend();
}

class ChdbSnapshotBackend implements SnapshotBackend {
  readonly #session = new Session();
  #topic = "";
  #idField = "id";
  #columns: readonly Column[] = [];
  #backendVersion = 0n;
  #literalStringFields: ReadonlySet<string> = new Set();
  #pendingByVersion = new Map<WorkerVersion, MutationLogEntry>();
  #flushScheduled = false;
  #closed = false;
  #lastFlushError: unknown;
  #tableReady = false;

  init(args: {
    readonly topic: string;
    readonly idField: string;
    readonly rows: readonly VersionedRow[];
    readonly version: WorkerVersion;
    readonly literalStringFields?: ReadonlySet<string> | undefined;
  }): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.chdb.init")(function* (backend: ChdbSnapshotBackend) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": args.topic,
        "view_server.rows": args.rows.length,
        "view_server.backend_version": args.version.toString(),
      });
      yield* Effect.try({
        try: () => {
          backend.#topic = args.topic;
          backend.#idField = args.idField;
          const initialRows = args.rows.map((entry) => ({ ...entry.row }));
          backend.#backendVersion = args.version;
          backend.#literalStringFields = args.literalStringFields ?? new Set();
          backend.#pendingByVersion = new Map();
          backend.#flushScheduled = false;
          backend.#closed = false;
          backend.#lastFlushError = undefined;
          backend.#tableReady = false;
          backend.#columns = inferColumns(initialRows, backend.#idField);
          backend.#session.query(`DROP TABLE IF EXISTS ${TABLE_NAME}`);
          if (backend.#columns.length > 0) {
            backend.#createTable();
            backend.#insertEvents(
              initialRows.map((row) => ({
                row,
                deleted: false,
                version: args.version,
              })),
            );
          }
        },
        catch: (error) => snapshotBackendFailed(args.topic, error),
      });
    })(this);
  }

  applyBatch(args: {
    readonly mutations: readonly MutationLogEntry[];
    readonly highestVersion: WorkerVersion;
  }): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (backend: ChdbSnapshotBackend) {
      yield* Effect.try({
        try: () => {
          if (backend.#closed) {
            return;
          }
          for (const mutation of args.mutations) {
            if (mutation.version > backend.#backendVersion) {
              backend.#pendingByVersion.set(mutation.version, mutation);
            }
          }
          backend.#scheduleFlush();
        },
        catch: (error) => snapshotBackendFailed(backend.#topic, error),
      });
    })(this);
  }

  snapshot(args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return Effect.fn("view-server.chdb.snapshot")(function* (backend: ChdbSnapshotBackend) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": backend.#topic,
        "view_server.worker_version": args.targetVersion.toString(),
        "view_server.backend_version": backend.#backendVersion.toString(),
      });
      const result = yield* Effect.try({
        try: () => {
          if (backend.#lastFlushError !== undefined) {
            throw backend.#lastFlushError;
          }
          if (!backend.#tableReady) {
            return {
              rows: [],
              totalRows: 0,
              backendVersion: backend.#backendVersion,
            };
          }
          const sql = compileQuerySql(
            args.query,
            backend.#idField,
            backend.#columns,
            backend.#literalStringFields,
          );
          const rows = parseJsonEachRow(
            backend.#session.query(withJsonSettings(sql.rowsSql), "JSONEachRow"),
            sql.decimalFields,
          );
          const totalRows = parseCount(
            backend.#session.query(withJsonSettings(sql.countSql), "JSONEachRow"),
          );
          return {
            rows,
            totalRows,
            backendVersion: backend.#backendVersion,
          };
        },
        catch: (error) => snapshotBackendFailed(backend.#topic, error),
      });
      yield* Effect.annotateCurrentSpan({
        "view_server.rows": result.rows.length,
        "view_server.total_rows": result.totalRows,
        "view_server.backend_version": result.backendVersion.toString(),
      });
      return result;
    })(this);
  }

  close(): Effect.Effect<void> {
    return Effect.fn("view-server.chdb.close")(function* (backend: ChdbSnapshotBackend) {
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": backend.#topic,
        "view_server.backend_version": backend.#backendVersion.toString(),
      });
      backend.#closed = true;
      yield* backend.#flushPendingEffect();
      yield* Effect.sync(() => {
        backend.#session.cleanup();
      });
    })(this);
  }

  #scheduleFlush(): void {
    if (this.#flushScheduled) {
      return;
    }
    this.#flushScheduled = true;
    queueMicrotask(() => {
      Effect.runFork(this.#flushPendingEffect());
    });
  }

  #flushPendingEffect(): Effect.Effect<void> {
    return Effect.fn("view-server.chdb.flush")(function* (backend: ChdbSnapshotBackend) {
      backend.#flushScheduled = false;
      const mutations = backend.#contiguousPendingMutations();
      yield* Effect.annotateCurrentSpan({
        "view_server.topic": backend.#topic,
        "view_server.batch_size": mutations.length,
        "view_server.backend_version": backend.#backendVersion.toString(),
      });
      if (mutations.length === 0) {
        return;
      }
      const highestVersion = mutations[mutations.length - 1]?.version;
      if (highestVersion === undefined) {
        return;
      }
      let flushedVersion: WorkerVersion | undefined;
      try {
        backend.#applyMutations(mutations);
        for (const mutation of mutations) {
          backend.#pendingByVersion.delete(mutation.version);
        }
        backend.#backendVersion = highestVersion;
        backend.#lastFlushError = undefined;
        flushedVersion = highestVersion;
      } catch (error) {
        backend.#lastFlushError = error;
      }
      if (flushedVersion !== undefined) {
        yield* Effect.annotateCurrentSpan({
          "view_server.backend_version": flushedVersion.toString(),
        });
      }
    })(this);
  }

  #contiguousPendingMutations(): readonly MutationLogEntry[] {
    const mutations: MutationLogEntry[] = [];
    let nextVersion = this.#backendVersion + 1n;
    while (true) {
      const mutation = this.#pendingByVersion.get(nextVersion);
      if (mutation === undefined) {
        return mutations;
      }
      mutations.push(mutation);
      nextVersion = nextVersion + 1n;
    }
  }

  #applyMutations(mutations: readonly MutationLogEntry[]): void {
    const events = mutations.flatMap((mutation) => mutationToEvent(mutation, this.#idField));
    if (events.length === 0) {
      return;
    }
    this.#ensureColumns(events.map((event) => event.row));
    this.#insertEvents(events);
  }

  #ensureColumns(rows: readonly RuntimeRow[]): void {
    const nextColumns = mergeColumns(this.#columns, inferColumns(rows, this.#idField));
    const addedColumns = nextColumns.filter(
      (column) => !this.#columns.some((existing) => existing.name === column.name),
    );
    if (!this.#tableReady) {
      this.#columns = nextColumns;
      this.#createTable();
      return;
    }
    for (const column of addedColumns) {
      this.#session.query(
        `ALTER TABLE ${TABLE_NAME} ADD COLUMN ${quoteIdentifier(column.name)} ${column.type}`,
      );
    }
    this.#columns = nextColumns;
  }

  #createTable(): void {
    this.#session.query(
      `CREATE TABLE ${TABLE_NAME} (${[
        ...this.#columns.map((column) => `${quoteIdentifier(column.name)} ${column.type}`),
        `${DELETED_COLUMN} UInt8`,
        `${VERSION_COLUMN} Int64`,
      ].join(", ")}) ENGINE = Memory`,
    );
    this.#tableReady = true;
  }

  #insertEvents(events: readonly ChdbEvent[]): void {
    if (events.length === 0) {
      return;
    }
    const payload = events
      .map((event) =>
        JSON.stringify({
          ...projectSerializableRow(event.row, this.#columns),
          [DELETED_COLUMN]: event.deleted ? 1 : 0,
          [VERSION_COLUMN]: event.version.toString(),
        }),
      )
      .join("\n");
    this.#session.query(`INSERT INTO ${TABLE_NAME} FORMAT JSONEachRow ${payload}`);
  }
}

type ChdbEvent = {
  readonly row: RuntimeRow;
  readonly deleted: boolean;
  readonly version: WorkerVersion;
};

function mutationToEvent(mutation: MutationLogEntry, idField: string): readonly ChdbEvent[] {
  if (mutation.kind === "delete") {
    return [
      {
        row: {
          ...mutation.before,
          [idField]: mutation.id,
        },
        deleted: true,
        version: mutation.version,
      },
    ];
  }
  if (mutation.after === undefined) {
    return [];
  }
  return [
    {
      row: mutation.after,
      deleted: false,
      version: mutation.version,
    },
  ];
}

function mergeColumns(
  existingColumns: readonly Column[],
  incomingColumns: readonly Column[],
): readonly Column[] {
  const columns = [...existingColumns];
  for (const column of incomingColumns) {
    if (!columns.some((existing) => existing.name === column.name)) {
      columns.push(column);
    }
  }
  return columns;
}

function inferColumns(rows: readonly RuntimeRow[], idField: string): readonly Column[] {
  const names = new Set<string>();
  for (const row of rows) {
    if (row[idField] !== undefined) {
      names.add(idField);
    }
    for (const [key, value] of Object.entries(row)) {
      if (isUserColumn(key) && isScalar(value)) {
        names.add(key);
      }
    }
  }
  return Array.from(names).map((name) => ({
    name,
    type: inferColumnType(rows.map((row) => row[name])),
  }));
}

function isUserColumn(name: string): boolean {
  return name !== DELETED_COLUMN && name !== VERSION_COLUMN;
}

function inferColumnType(values: readonly unknown[]): ColumnType {
  if (values.some((value) => typeof value === "string")) {
    return "String";
  }
  if (values.some(BigDecimal.isBigDecimal)) {
    return BIG_DECIMAL_COLUMN_TYPE;
  }
  if (values.some((value) => typeof value === "boolean")) {
    return "UInt8";
  }
  if (values.some((value) => typeof value === "number")) {
    return "Float64";
  }
  return "Int64";
}

function projectSerializableRow(row: RuntimeRow, columns: readonly Column[]): RuntimeRow {
  const projected: RuntimeRow = {};
  for (const column of columns) {
    const value = row[column.name];
    projected[column.name] = BigDecimal.isBigDecimal(value)
      ? BigDecimal.format(value)
      : typeof value === "bigint"
        ? value.toString()
        : typeof value === "boolean"
          ? value
            ? 1
            : 0
          : (value ?? defaultValue(column.type));
  }
  return projected;
}

function defaultValue(type: ColumnType): string | number {
  return type === "String" ? "" : 0;
}

function isScalar(value: unknown): boolean {
  return (
    value == null ||
    typeof value === "string" ||
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    BigDecimal.isBigDecimal(value)
  );
}

function latestRowsSql(columns: readonly Column[], idField: string): string {
  const selected = columns.map((column) => quoteIdentifier(column.name)).join(", ");
  const id = quoteIdentifier(idField);
  return `SELECT ${selected} FROM (SELECT ${selected}, ${DELETED_COLUMN}, ${VERSION_COLUMN} FROM ${TABLE_NAME} ORDER BY ${id}, ${VERSION_COLUMN} DESC LIMIT 1 BY ${id}) WHERE ${DELETED_COLUMN} = 0`;
}

function compileQuerySql(
  query: RuntimeQuery,
  idField: string,
  columns: readonly Column[],
  literalStringFields: ReadonlySet<string>,
): {
  readonly rowsSql: string;
  readonly countSql: string;
  readonly decimalFields: ReadonlySet<string>;
} {
  const source = latestRowsSql(columns, idField);
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
    };
  }

  const selected = new Set(
    Object.entries(query.fields)
      .filter(([, enabled]) => enabled)
      .map(([field]) => field),
  );
  selected.add(idField);
  const where = query.where ? `WHERE ${compileFilter(query.where, literalStringFields)}` : "";
  const orderBy = [
    ...(query.orderBy ?? []),
    ...(query.orderBy?.some((order) => order.field === idField)
      ? []
      : [{ field: idField, direction: "asc" as const }]),
  ];
  return {
    rowsSql: `SELECT ${Array.from(selected).map(quoteIdentifier).join(", ")} FROM (${source}) ${where} ${compileOrderBy(orderBy, columns)} ${compileLimit(query)}`,
    countSql: `SELECT count() AS totalRows FROM (${source}) ${where}`,
    decimalFields: selectedDecimalFields(Array.from(selected), columns),
  };
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
  return `ORDER BY ${orderBy
    .map((order) => {
      const field = quoteIdentifier(order.field);
      const expression =
        columnTypes.get(order.field) === "String" ? `lower(toString(${field}))` : field;
      return `${expression} ${order.direction.toUpperCase()}`;
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

function quoteIdentifier(identifier: string): string {
  return "`" + identifier.replaceAll("`", "``") + "`";
}

function literal(value: string): string {
  return `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
}

function withJsonSettings(sql: string): string {
  return `${sql} ${JSON_DECIMAL_SETTINGS}`;
}

function parseJsonEachRow(
  output: string,
  decimalFields: ReadonlySet<string> = new Set(),
): RuntimeRow[] {
  const trimmed = output.trim();
  if (trimmed.length === 0) {
    return [];
  }
  return trimmed.split("\n").map((line) => parseJsonRow(line, decimalFields));
}

function parseJsonRow(line: string, decimalFields: ReadonlySet<string>): RuntimeRow {
  const row = JSON.parse(line);
  for (const field of decimalFields) {
    const value = row[field];
    if (typeof value === "string") {
      row[field] = BigDecimal.fromStringUnsafe(value);
    }
  }
  return row;
}

function parseCount(output: string): number {
  const [row] = parseJsonEachRow(output);
  return Number(row?.totalRows ?? 0);
}
