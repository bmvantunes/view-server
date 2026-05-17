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
    this.#insertRows(args.rows, args.version);
  }

  applyMutations(mutations: readonly MutationLogEntry[]): void {
    if (!hasMutationEvents(mutations)) {
      return;
    }
    this.#ensureMutationColumns(mutations);
    this.#insertMutationEvents(mutations);
  }

  drop(): void {
    this.#session.query(`DROP TABLE IF EXISTS ${quoteIdentifier(this.#tableName)}`);
    this.#tableReady = false;
  }

  #ensureMutationColumns(mutations: readonly MutationLogEntry[]): void {
    const nextColumns = mergeColumns(this.#columns, inferMutationColumns(mutations, this.#idField));
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

  #insertRows(rows: readonly RuntimeRow[], version: WorkerVersion): void {
    if (rows.length === 0) {
      return;
    }
    let payload = "";
    for (const row of rows) {
      payload = appendEventPayload(payload, row, false, version, this.#columns);
    }
    this.#insertPayload(payload);
  }

  #insertMutationEvents(mutations: readonly MutationLogEntry[]): void {
    let payload = "";
    for (const mutation of mutations) {
      const event = mutationToEvent(mutation, this.#idField);
      if (event === undefined) {
        continue;
      }
      payload = appendEventPayload(
        payload,
        event.row,
        event.deleted,
        mutation.version,
        this.#columns,
      );
    }
    if (payload.length > 0) {
      this.#insertPayload(payload);
    }
  }

  #insertPayload(payload: string): void {
    this.#session.query(
      `INSERT INTO ${quoteIdentifier(this.#tableName)} FORMAT JSONEachRow ${payload}`,
    );
  }
}

type ChdbEvent = {
  readonly row: RuntimeRow;
  readonly deleted: boolean;
};

function mutationToEvent(mutation: MutationLogEntry, idField: string): ChdbEvent | undefined {
  if (mutation.kind === "delete") {
    return {
      row: {
        ...mutation.before,
        [idField]: mutation.id,
      },
      deleted: true,
    };
  }
  if (mutation.after === undefined) {
    return undefined;
  }
  return {
    row: mutation.after,
    deleted: false,
  };
}

function hasMutationEvents(mutations: readonly MutationLogEntry[]): boolean {
  return mutations.some((mutation) => mutation.kind === "delete" || mutation.after !== undefined);
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
  const statsByName = new Map<string, ColumnStats>();
  for (const row of rows) {
    addRowColumnStats(statsByName, row, idField);
  }
  return columnStatsToColumns(statsByName);
}

function isUserColumn(name: string): boolean {
  return name !== DELETED_COLUMN && name !== VERSION_COLUMN;
}

function inferMutationColumns(
  mutations: readonly MutationLogEntry[],
  idField: string,
): readonly Column[] {
  const statsByName = new Map<string, ColumnStats>();
  for (const mutation of mutations) {
    const event = mutationToEvent(mutation, idField);
    if (event !== undefined) {
      addRowColumnStats(statsByName, event.row, idField);
    }
  }
  return columnStatsToColumns(statsByName);
}

type ColumnStats = {
  readonly type: ColumnType;
  readonly nullable: boolean;
};

function addRowColumnStats(
  statsByName: Map<string, ColumnStats>,
  row: RuntimeRow,
  idField: string,
): void {
  if (row[idField] !== undefined) {
    addColumnValue(statsByName, idField, row[idField]);
  }
  for (const [key, value] of Object.entries(row)) {
    if (isUserColumn(key) && isScalar(value)) {
      addColumnValue(statsByName, key, value);
    }
  }
}

function addColumnValue(statsByName: Map<string, ColumnStats>, name: string, value: unknown): void {
  const existing = statsByName.get(name) ?? { type: "Int64", nullable: false };
  statsByName.set(name, {
    type: widerColumnType(existing.type, value),
    nullable: existing.nullable || value == null,
  });
}

function widerColumnType(existing: ColumnType, value: unknown): ColumnType {
  if (value == null) {
    return existing;
  }
  const incoming = scalarColumnType(value);
  return columnTypePriority(incoming) > columnTypePriority(existing) ? incoming : existing;
}

function scalarColumnType(value: unknown): ColumnType {
  if (typeof value === "string") {
    return "String";
  }
  if (BigDecimal.isBigDecimal(value)) {
    return BIG_DECIMAL_COLUMN_TYPE;
  }
  if (typeof value === "boolean") {
    return "UInt8";
  }
  if (typeof value === "number") {
    return "Float64";
  }
  return "Int64";
}

function columnTypePriority(type: ColumnType): number {
  switch (type) {
    case "Int64":
      return 0;
    case "Float64":
      return 1;
    case "UInt8":
      return 2;
    case BIG_DECIMAL_COLUMN_TYPE:
      return 3;
    case "String":
      return 4;
  }
}

function columnStatsToColumns(statsByName: ReadonlyMap<string, ColumnStats>): readonly Column[] {
  return Array.from(statsByName, ([name, stats]) => ({
    name,
    type: stats.type,
    nullable: stats.nullable,
  }));
}

function appendEventPayload(
  payload: string,
  row: RuntimeRow,
  deleted: boolean,
  version: WorkerVersion,
  columns: readonly Column[],
): string {
  const line = JSON.stringify({
    ...projectSerializableRow(row, columns),
    [DELETED_COLUMN]: deleted ? 1 : 0,
    [VERSION_COLUMN]: version.toString(),
  });
  return payload.length === 0 ? line : `${payload}\n${line}`;
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
