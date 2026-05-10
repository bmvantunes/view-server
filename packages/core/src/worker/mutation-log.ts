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
  readonly #entries: MutationLogEntry[] = [];
  readonly capacity: number;

  constructor(capacity: number) {
    this.capacity = capacity;
  }

  append(entry: MutationLogEntry): void {
    this.#entries.push(entry);
    while (this.#entries.length > this.capacity) {
      this.#entries.shift();
    }
  }

  coversExclusive(fromVersion: WorkerVersion, toVersion: WorkerVersion): boolean {
    if (fromVersion >= toVersion) {
      return true;
    }
    const firstNeeded = fromVersion + 1n;
    const first = this.#entries[0]?.version;
    const last = this.#entries[this.#entries.length - 1]?.version;
    return first !== undefined && last !== undefined && first <= firstNeeded && last >= toVersion;
  }

  entriesExclusive(
    fromVersion: WorkerVersion,
    toVersion: WorkerVersion,
  ): readonly MutationLogEntry[] {
    return this.#entries.filter(
      (entry) => entry.version > fromVersion && entry.version <= toVersion,
    );
  }
}
