import { Session } from "chdb";
import * as BigDecimal from "effect/BigDecimal";
import type { RuntimeRow } from "../protocol/index.ts";
import type { MutationLogEntry, WorkerVersion } from "../worker/mutation-log.ts";
import {
  BIG_DECIMAL_COLUMN_TYPE,
  columnSqlType,
  quoteIdentifier,
  type Column,
  type ColumnType,
} from "./chdb-sql-compiler.ts";

const DELETED_COLUMN = "__view_server_deleted";
const VERSION_COLUMN = "__view_server_version";

export class ChdbSqlMirror {
  readonly #session: Session;
  readonly #tableName: string;
  #idField = "id";
  #columns: readonly Column[] = [];
  #tableReady = false;

  constructor(session: Session, tableName: string) {
    this.#session = session;
    this.#tableName = tableName;
  }

  get tableName(): string {
    return this.#tableName;
  }

  get columns(): readonly Column[] {
    return this.#columns;
  }

  get tableReady(): boolean {
    return this.#tableReady;
  }

  init(args: {
    readonly idField: string;
    readonly rows: readonly RuntimeRow[];
    readonly version: WorkerVersion;
  }): void {
    this.#idField = args.idField;
    this.#tableReady = false;
    this.#columns = inferColumns(args.rows, args.idField);
    this.#session.query(`DROP TABLE IF EXISTS ${quoteIdentifier(this.#tableName)}`);
    if (this.#columns.length === 0) {
      return;
    }
    this.#createTable();
    this.#insertEvents(
      args.rows.map((row) => ({
        row,
        deleted: false,
        version: args.version,
      })),
    );
  }

  applyMutations(mutations: readonly MutationLogEntry[]): void {
    const events = mutations.flatMap((mutation) => mutationToEvent(mutation, this.#idField));
    if (events.length === 0) {
      return;
    }
    this.#ensureColumns(events.map((event) => event.row));
    this.#insertEvents(events);
  }

  drop(): void {
    this.#session.query(`DROP TABLE IF EXISTS ${quoteIdentifier(this.#tableName)}`);
    this.#tableReady = false;
  }

  #ensureColumns(rows: readonly RuntimeRow[]): void {
    const nextColumns = mergeColumns(this.#columns, inferColumns(rows, this.#idField));
    const addedColumns = nextColumns.filter(
      (column) => !this.#columns.some((existing) => existing.name === column.name),
    );
    const nullableColumns = nextColumns.filter((column) =>
      this.#columns.some(
        (existing) => existing.name === column.name && !existing.nullable && column.nullable,
      ),
    );
    if (!this.#tableReady) {
      this.#columns = nextColumns;
      this.#createTable();
      return;
    }
    for (const column of addedColumns) {
      this.#session.query(
        `ALTER TABLE ${quoteIdentifier(this.#tableName)} ADD COLUMN ${quoteIdentifier(column.name)} ${columnSqlType(column)}`,
      );
    }
    for (const column of nullableColumns) {
      this.#session.query(
        `ALTER TABLE ${quoteIdentifier(this.#tableName)} MODIFY COLUMN ${quoteIdentifier(column.name)} ${columnSqlType(column)}`,
      );
    }
    this.#columns = nextColumns;
  }

  #createTable(): void {
    this.#session.query(
      `CREATE TABLE ${quoteIdentifier(this.#tableName)} (${[
        ...this.#columns.map(
          (column) => `${quoteIdentifier(column.name)} ${columnSqlType(column)}`,
        ),
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
    this.#session.query(
      `INSERT INTO ${quoteIdentifier(this.#tableName)} FORMAT JSONEachRow ${payload}`,
    );
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
    const index = columns.findIndex((existing) => existing.name === column.name);
    if (index < 0) {
      columns.push(column);
      continue;
    }
    const existing = columns[index];
    if (existing !== undefined && column.nullable && !existing.nullable) {
      columns[index] = { ...existing, nullable: true };
    }
  }
  return columns;
}

function inferColumns(rows: readonly RuntimeRow[], idField: string): readonly Column[] {
  const valuesByName = new Map<string, unknown[]>();
  const addValue = (name: string, value: unknown) => {
    const values = valuesByName.get(name);
    if (values === undefined) {
      valuesByName.set(name, [value]);
    } else {
      values.push(value);
    }
  };
  for (const row of rows) {
    if (row[idField] !== undefined) {
      addValue(idField, row[idField]);
    }
    for (const [key, value] of Object.entries(row)) {
      if (isUserColumn(key) && isScalar(value)) {
        addValue(key, value);
      }
    }
  }
  return Array.from(valuesByName).map(([name, values]) => ({
    name,
    type: inferColumnType(values),
    nullable: values.some((value) => value == null),
  }));
}

function isUserColumn(name: string): boolean {
  return name !== DELETED_COLUMN && name !== VERSION_COLUMN;
}

function inferColumnType(values: readonly unknown[]): ColumnType {
  const nonNullValues = values.filter((value) => value != null);
  if (nonNullValues.some((value) => typeof value === "string")) {
    return "String";
  }
  if (nonNullValues.some(BigDecimal.isBigDecimal)) {
    return BIG_DECIMAL_COLUMN_TYPE;
  }
  if (nonNullValues.some((value) => typeof value === "boolean")) {
    return "UInt8";
  }
  if (nonNullValues.some((value) => typeof value === "number")) {
    return "Float64";
  }
  return "Int64";
}

function projectSerializableRow(row: RuntimeRow, columns: readonly Column[]): RuntimeRow {
  const projected: RuntimeRow = {};
  for (const column of columns) {
    const value = row[column.name];
    projected[column.name] =
      value == null && column.nullable
        ? null
        : BigDecimal.isBigDecimal(value)
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
