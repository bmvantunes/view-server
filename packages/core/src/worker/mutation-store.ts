import type { RuntimeRow } from "../protocol/index.ts";
import { changedFields } from "./query-engine.ts";
import { MutationLog, type MutationLogEntry, type WorkerVersion } from "./mutation-log.ts";

export type MutationStoreMetrics = {
  readonly rows: number;
  readonly version: WorkerVersion;
  readonly logCapacity: number;
};

export type MutationStoreChange = {
  readonly fromVersion: WorkerVersion;
  readonly toVersion: WorkerVersion;
  readonly entry: MutationLogEntry;
};

export class MutationStore {
  readonly #idField: string;
  readonly #mutationLog: MutationLog;
  #rows: RuntimeRow[] = [];
  #idIndex = new Map<string | number, number>();
  #version: WorkerVersion = 0n;

  constructor(options: { readonly idField: string; readonly mutationLogSize: number }) {
    this.#idField = options.idField;
    this.#mutationLog = new MutationLog(options.mutationLogSize);
  }

  loadInitialRows(rows: readonly RuntimeRow[]): void {
    this.#rows = rows.map((row) => ({ ...row }));
    this.#replaceIndexes();
  }

  rows(): readonly RuntimeRow[] {
    return this.#rows;
  }

  snapshotRows(): readonly RuntimeRow[] {
    return this.#rows.map((row) => ({ ...row }));
  }

  version(): WorkerVersion {
    return this.#version;
  }

  metrics(): MutationStoreMetrics {
    return {
      rows: this.#rows.length,
      version: this.#version,
      logCapacity: this.#mutationLog.capacity,
    };
  }

  rowById(id: string | number): RuntimeRow | undefined {
    const index = this.#idIndex.get(id);
    return index === undefined ? undefined : this.#rows[index];
  }

  publish(row: RuntimeRow, id: string | number): MutationStoreChange {
    const index = this.#idIndex.get(id);
    if (index === undefined) {
      const next = { ...row };
      this.#rows.push(next);
      this.#idIndex.set(id, this.#rows.length - 1);
      return this.#appendMutation({
        kind: "insert",
        id,
        after: { ...next },
        changedFields: new Set(Object.keys(next)),
      });
    }

    const before = this.#rows[index];
    const after = { ...row };
    this.#rows[index] = after;
    return this.#appendMutation({
      kind: "update",
      id,
      before,
      after,
      changedFields: changedFields(before, after),
    });
  }

  updateExisting(id: string | number, row: RuntimeRow): MutationStoreChange | undefined {
    const index = this.#idIndex.get(id);
    if (index === undefined) {
      return undefined;
    }
    const before = this.#rows[index];
    const after = { ...row };
    this.#rows[index] = after;
    return this.#appendMutation({
      kind: "update",
      id,
      before,
      after,
      changedFields: changedFields(before, after),
    });
  }

  deleteById(id: string | number): MutationStoreChange | undefined {
    const index = this.#idIndex.get(id);
    if (index === undefined) {
      return undefined;
    }
    const before = this.#rows[index];
    removeIndexedRow(this.#rows, this.#idIndex, id, this.#idField);
    return this.#appendMutation({
      kind: "delete",
      id,
      before,
      changedFields: new Set(Object.keys(before)),
    });
  }

  canReplay(fromVersion: WorkerVersion, toVersion: WorkerVersion): boolean {
    return this.#mutationLog.coversExclusive(fromVersion, toVersion);
  }

  entriesExclusive(
    fromVersion: WorkerVersion,
    toVersion: WorkerVersion,
  ): readonly MutationLogEntry[] {
    return this.#mutationLog.entriesExclusive(fromVersion, toVersion);
  }

  replayRowsFrom(
    baseRows: readonly RuntimeRow[],
    fromVersion: WorkerVersion,
    toVersion: WorkerVersion,
  ): readonly RuntimeRow[] {
    return replayMutations(baseRows, this.entriesExclusive(fromVersion, toVersion), this.#idField);
  }

  #appendMutation(mutation: Omit<MutationLogEntry, "version">): MutationStoreChange {
    const fromVersion = this.#version;
    this.#version = this.#version + 1n;
    const entry: MutationLogEntry = { ...mutation, version: this.#version };
    this.#mutationLog.append(entry);
    return {
      fromVersion,
      toVersion: this.#version,
      entry,
    };
  }

  #replaceIndexes(): void {
    this.#idIndex = new Map();
    this.#rows.forEach((row, index) => {
      const id = row[this.#idField];
      if (typeof id === "string" || typeof id === "number") {
        this.#idIndex.set(id, index);
      }
    });
  }
}

export function replayMutations(
  baseRows: readonly RuntimeRow[],
  entries: readonly MutationLogEntry[],
  idField: string,
): readonly RuntimeRow[] {
  const replayRows = baseRows.map((row) => ({ ...row }));
  const replayIndex = new Map<string | number, number>();
  replayRows.forEach((row, index) => {
    replayIndex.set(rowId(row, idField), index);
  });
  for (const entry of entries) {
    if (entry.kind === "delete") {
      removeIndexedRow(replayRows, replayIndex, entry.id, idField);
      continue;
    }
    const next = { ...entry.after };
    const index = replayIndex.get(entry.id);
    if (index === undefined) {
      replayRows.push(next);
      replayIndex.set(entry.id, replayRows.length - 1);
    } else {
      replayRows[index] = next;
    }
  }
  return replayRows;
}

function removeIndexedRow(
  rows: RuntimeRow[],
  indexById: Map<string | number, number>,
  id: string | number,
  idField: string,
): void {
  const index = indexById.get(id);
  if (index === undefined) {
    return;
  }
  const lastIndex = rows.length - 1;
  const last = rows[lastIndex];
  rows.pop();
  indexById.delete(id);
  if (index !== lastIndex && last !== undefined) {
    rows[index] = last;
    indexById.set(rowId(last, idField), index);
  }
}

function rowId(row: RuntimeRow, idField: string): string | number {
  const value = row[idField];
  return typeof value === "string" || typeof value === "number" ? value : String(value);
}
