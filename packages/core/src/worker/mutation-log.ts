import type { RuntimeRow } from "../protocol/index.ts";

export type WorkerVersion = bigint;

export type MutationKind = "insert" | "update" | "delete";

export type MutationLogEntry = {
  readonly version: WorkerVersion;
  readonly kind: MutationKind;
  readonly id: string | number;
  readonly before?: RuntimeRow | undefined;
  readonly after?: RuntimeRow | undefined;
  readonly changedFields: ReadonlySet<string>;
};

export class MutationLog {
  readonly #entries: Array<MutationLogEntry | undefined>;
  #start = 0;
  #size = 0;
  readonly capacity: number;

  constructor(capacity: number) {
    this.capacity = capacity;
    this.#entries = Array.from({ length: Math.max(0, capacity) });
  }

  append(entry: MutationLogEntry): void {
    if (this.capacity <= 0) {
      return;
    }
    if (this.#size < this.capacity) {
      this.#entries[this.#physicalIndex(this.#size)] = entry;
      this.#size += 1;
      return;
    }
    this.#entries[this.#start] = entry;
    this.#start = (this.#start + 1) % this.capacity;
  }

  coversExclusive(fromVersion: WorkerVersion, toVersion: WorkerVersion): boolean {
    if (fromVersion >= toVersion) {
      return true;
    }
    const firstNeeded = fromVersion + 1n;
    const first = this.#entryAt(0)?.version;
    const last = this.#entryAt(this.#size - 1)?.version;
    return first !== undefined && last !== undefined && first <= firstNeeded && last >= toVersion;
  }

  entriesExclusive(
    fromVersion: WorkerVersion,
    toVersion: WorkerVersion,
  ): readonly MutationLogEntry[] {
    const entries: MutationLogEntry[] = [];
    for (let index = 0; index < this.#size; index++) {
      const entry = this.#entryAt(index);
      if (entry !== undefined && entry.version > fromVersion && entry.version <= toVersion) {
        entries.push(entry);
      }
    }
    return entries;
  }

  #physicalIndex(logicalIndex: number): number {
    return (this.#start + logicalIndex) % this.capacity;
  }

  #entryAt(logicalIndex: number): MutationLogEntry | undefined {
    if (logicalIndex < 0 || logicalIndex >= this.#size || this.capacity <= 0) {
      return undefined;
    }
    return this.#entries[this.#physicalIndex(logicalIndex)];
  }
}
