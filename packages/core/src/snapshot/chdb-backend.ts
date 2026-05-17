import * as Effect from "effect/Effect";
import type { TopicConfig } from "../config/index.ts";
import type { ViewServerError } from "../errors.ts";
import type { RuntimeQuery, RuntimeRow } from "../protocol/index.ts";
import { isStableKey } from "../protocol/stable-key.ts";
import type { MutationLogEntry, WorkerVersion } from "../worker/mutation-log.ts";
import type {
  SnapshotBackend,
  SnapshotBackendHealth,
  SnapshotBackendResult,
} from "./snapshot-backend.ts";
import {
  ChdbWorkerSupervisor,
  type ChdbWorkerSupervisorOptions,
} from "./chdb-worker-supervisor.ts";

export type ChdbSnapshotBackendOptions = ChdbWorkerSupervisorOptions;

export function createChdbSnapshotBackend(
  options: ChdbSnapshotBackendOptions = {},
): SnapshotBackend {
  return new WorkerChdbSnapshotBackend(options);
}

export function createChdbSnapshotBackendFactory(): (
  topic: string,
  config: TopicConfig,
) => SnapshotBackend {
  return () => createChdbSnapshotBackend();
}

class WorkerChdbSnapshotBackend implements SnapshotBackend {
  readonly #supervisor: ChdbWorkerSupervisor;
  #topic = "chdb";
  #idField = "id";
  #literalStringFields: ReadonlySet<string> | undefined;
  #mirrorRows = new Map<string | number, RuntimeRow>();
  #mirrorPendingByVersion = new Map<WorkerVersion, MutationLogEntry>();
  #mirrorVersion: WorkerVersion = 0n;
  #knownBackendVersion: WorkerVersion = 0n;

  constructor(options: ChdbSnapshotBackendOptions = {}) {
    this.#supervisor = new ChdbWorkerSupervisor(
      options,
      () => this.#restartSnapshot(),
      (version) => {
        this.#knownBackendVersion = version;
      },
    );
  }

  get supportsGroupedRefreshSnapshots(): boolean {
    return true;
  }

  get health(): Effect.Effect<SnapshotBackendHealth> {
    return Effect.sync(() => {
      const workerHealth = this.#supervisor.health;
      return {
        status: workerHealth.status,
        message: workerHealth.message,
        pid: workerHealth.pid ?? 0,
        restarts: workerHealth.restarts ?? 0,
        pendingRequests: workerHealth.pendingRequests ?? 0,
        lastError: workerHealth.lastError ?? workerHealth.message ?? "",
        backendVersion: this.#knownBackendVersion,
      };
    });
  }

  init(args: Parameters<SnapshotBackend["init"]>[0]): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.chdb.worker_backend.init")(function* (
      backend: WorkerChdbSnapshotBackend,
    ) {
      backend.#topic = args.topic;
      backend.#idField = args.idField;
      backend.#literalStringFields = args.literalStringFields;
      backend.#mirrorRows = new Map();
      backend.#mirrorPendingByVersion = new Map();
      backend.#mirrorVersion = args.version;
      backend.#knownBackendVersion = args.version;
      for (const entry of args.rows) {
        const id = entry.row[args.idField];
        if (isStableKey(id)) {
          backend.#mirrorRows.set(id, { ...entry.row });
        }
      }
      yield* backend.#supervisor.init(args);
    })(this);
  }

  applyBatch(
    args: Parameters<SnapshotBackend["applyBatch"]>[0],
  ): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (backend: WorkerChdbSnapshotBackend) {
      backend.#applyBatchToMirror(args);
      yield* backend.#supervisor.applyBatch(args);
      backend.#knownBackendVersion = backend.#mirrorVersion;
    })(this);
  }

  snapshot(args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return Effect.fn("view-server.chdb.worker_backend.snapshot")(function* (
      backend: WorkerChdbSnapshotBackend,
    ) {
      const result = yield* backend.#supervisor.snapshot(args);
      backend.#knownBackendVersion = result.backendVersion;
      return result;
    })(this);
  }

  groupedRefreshSnapshot(args: {
    readonly query: RuntimeQuery;
    readonly targetVersion: WorkerVersion;
  }): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return Effect.fn("view-server.chdb.worker_backend.grouped_refresh_snapshot")(function* (
      backend: WorkerChdbSnapshotBackend,
    ) {
      const result = yield* backend.#supervisor.groupedRefreshSnapshot(args);
      backend.#knownBackendVersion = result.backendVersion;
      return result;
    })(this);
  }

  close(): Effect.Effect<void> {
    return Effect.fn("view-server.chdb.worker_backend.close")(function* (
      backend: WorkerChdbSnapshotBackend,
    ) {
      yield* backend.#supervisor.close().pipe(Effect.ignore);
    })(this);
  }

  #restartSnapshot(): Parameters<SnapshotBackend["init"]>[0] {
    return {
      topic: this.#topic,
      idField: this.#idField,
      rows: Array.from(this.#mirrorRows.values(), (row) => ({
        row,
        version: this.#mirrorVersion,
      })),
      version: this.#mirrorVersion,
      ...(this.#literalStringFields === undefined
        ? {}
        : { literalStringFields: this.#literalStringFields }),
    };
  }

  #applyBatchToMirror(args: Parameters<SnapshotBackend["applyBatch"]>[0]): void {
    for (const mutation of args.mutations) {
      if (mutation.version > this.#mirrorVersion) {
        this.#mirrorPendingByVersion.set(mutation.version, mutation);
      }
    }
    let nextVersion = this.#mirrorVersion + 1n;
    while (true) {
      const mutation = this.#mirrorPendingByVersion.get(nextVersion);
      if (mutation === undefined) {
        return;
      }
      this.#mirrorPendingByVersion.delete(nextVersion);
      this.#applyMutationToMirror(mutation);
      this.#mirrorVersion = mutation.version;
      nextVersion = this.#mirrorVersion + 1n;
    }
  }

  #applyMutationToMirror(mutation: MutationLogEntry): void {
    if (mutation.kind === "delete") {
      this.#mirrorRows.delete(mutation.id);
      return;
    }
    if (mutation.after === undefined) {
      return;
    }
    const id = mutation.after[this.#idField];
    if (isStableKey(id)) {
      this.#mirrorRows.set(id, { ...mutation.after });
    }
  }
}
