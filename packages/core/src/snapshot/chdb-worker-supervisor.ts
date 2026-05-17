import * as Effect from "effect/Effect";
import { snapshotBackendFailed, type ViewServerError } from "../errors.ts";
import type { WorkerVersion } from "../worker/mutation-log.ts";
import type {
  SnapshotBackend,
  SnapshotBackendHealth,
  SnapshotBackendResult,
} from "./snapshot-backend.ts";
import { ChdbProcessClient } from "./chdb-process-client.ts";
import type { ChdbQueryWorkerRequest } from "./chdb-worker-protocol.ts";
import type { ChdbQueryWorkerSuccessResponse } from "./chdb-worker-protocol.ts";
import {
  decodeSnapshotBackendResult,
  encodeMutationLogEntry,
  encodeRuntimeQuery,
  encodeVersionedRow,
} from "./row-wire-codec.ts";

const WORKER_INIT_CHUNK_SIZE = 25_000;

export type ChdbWorkerSupervisorOptions = {
  readonly groupedRefreshWorkerEntryUrl?: string | URL | undefined;
  readonly restartWorkerOnUnexpectedExit?: boolean | undefined;
  /** @internal Test/supervision hook. */
  readonly onWorkerSpawn?: ((pid: number | undefined) => void) | undefined;
  /** @internal Test/supervision hook. */
  readonly onWorkerExit?:
    | ((event: { readonly pid: number | undefined; readonly message: string }) => void)
    | undefined;
  /** @internal Test/supervision hook. */
  readonly onWorkerRequest?:
    | ((request: {
        readonly pid: number | undefined;
        readonly type: ChdbQueryWorkerRequest["type"];
      }) => void)
    | undefined;
};

type ChdbWorkerProcessClient = {
  readonly pendingRequests: number;
  readonly exitedUnexpectedly: boolean;
  readonly health: SnapshotBackendHealth;
  setTopic(topic: string): void;
  nextRequestId(): number;
  request(
    request: ChdbQueryWorkerRequest,
  ): Effect.Effect<ChdbQueryWorkerSuccessResponse, ViewServerError>;
  restart(): Effect.Effect<void, ViewServerError>;
  shutdown(): Effect.Effect<void, ViewServerError>;
};

type ChdbWorkerSupervisorDependencies = {
  readonly makeProcessClient?: (() => ChdbWorkerProcessClient) | undefined;
};

export class ChdbWorkerSupervisor {
  readonly #options: ChdbWorkerSupervisorOptions;
  readonly #restartSnapshot: () => Parameters<SnapshotBackend["init"]>[0];
  readonly #onRestarted: (version: WorkerVersion) => void;
  readonly #dependencies: ChdbWorkerSupervisorDependencies;
  #client: ChdbWorkerProcessClient;
  #topic = "chdb";
  #restarting = false;
  #lastError = "";
  #closed = false;

  constructor(
    options: ChdbWorkerSupervisorOptions,
    restartSnapshot: () => Parameters<SnapshotBackend["init"]>[0],
    onRestarted: (version: WorkerVersion) => void = () => undefined,
    dependencies: ChdbWorkerSupervisorDependencies = {},
  ) {
    this.#options = options;
    this.#restartSnapshot = restartSnapshot;
    this.#onRestarted = onRestarted;
    this.#dependencies = dependencies;
    this.#client = this.#makeClient();
  }

  get health(): SnapshotBackendHealth {
    const processHealth = this.#client.health;
    const processLastError = processHealth.lastError ?? processHealth.message ?? "";
    return {
      status: this.#restarting ? "restarting" : processHealth.status,
      message: processHealth.message,
      pid: processHealth.pid ?? 0,
      restarts: processHealth.restarts ?? 0,
      pendingRequests: processHealth.pendingRequests ?? this.#client.pendingRequests,
      lastError: processLastError.length > 0 ? processLastError : this.#lastError,
    };
  }

  init(args: Parameters<SnapshotBackend["init"]>[0]): Effect.Effect<void, ViewServerError> {
    this.#topic = args.topic;
    this.#client.setTopic(args.topic);
    this.#lastError = "";
    if (args.rows.length > WORKER_INIT_CHUNK_SIZE) {
      return this.#chunkedInit(args);
    }
    return this.#client
      .request({
        id: this.#client.nextRequestId(),
        type: "init",
        args: {
          ...args,
          rows: args.rows.map(encodeVersionedRow),
        },
      })
      .pipe(Effect.asVoid, this.#rememberFailure);
  }

  applyBatch(
    args: Parameters<SnapshotBackend["applyBatch"]>[0],
  ): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (supervisor: ChdbWorkerSupervisor) {
      const client = yield* supervisor.#ensureReady();
      yield* client
        .request({
          id: client.nextRequestId(),
          type: "applyBatch",
          args: {
            mutations: args.mutations.map(encodeMutationLogEntry),
            highestVersion: args.highestVersion,
          },
        })
        .pipe(Effect.asVoid, supervisor.#rememberFailure);
    })(this);
  }

  snapshot(
    args: Parameters<SnapshotBackend["snapshot"]>[0],
  ): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return Effect.fn("view-server.chdb.supervisor.snapshot")(function* (
      supervisor: ChdbWorkerSupervisor,
    ) {
      const client = yield* supervisor.#ensureReady();
      return yield* client
        .request({
          id: client.nextRequestId(),
          type: "snapshot",
          args: {
            query: encodeRuntimeQuery(args.query),
            targetVersion: args.targetVersion,
          },
        })
        .pipe(
          Effect.flatMap((response) =>
            response.result === undefined
              ? Effect.fail(
                  snapshotBackendFailed(supervisor.#topic, "chDB worker returned no snapshot"),
                )
              : Effect.succeed(decodeSnapshotBackendResult(response.result)),
          ),
          supervisor.#rememberFailure,
        );
    })(this);
  }

  groupedRefreshSnapshot(
    args: Parameters<SnapshotBackend["snapshot"]>[0],
  ): Effect.Effect<SnapshotBackendResult, ViewServerError> {
    return Effect.fn("view-server.chdb.supervisor.grouped_refresh_snapshot")(function* (
      supervisor: ChdbWorkerSupervisor,
    ) {
      const client = yield* supervisor.#ensureReady();
      return yield* client
        .request({
          id: client.nextRequestId(),
          type: "groupedRefreshSnapshot",
          args: {
            query: encodeRuntimeQuery(args.query),
            targetVersion: args.targetVersion,
          },
        })
        .pipe(
          Effect.flatMap((response) =>
            response.result === undefined
              ? Effect.fail(
                  snapshotBackendFailed(
                    supervisor.#topic,
                    "chDB worker returned no grouped snapshot",
                  ),
                )
              : Effect.succeed(decodeSnapshotBackendResult(response.result)),
          ),
          supervisor.#rememberFailure,
        );
    })(this);
  }

  close(): Effect.Effect<void, ViewServerError> {
    return Effect.fn("view-server.chdb.supervisor.close")(function* (
      supervisor: ChdbWorkerSupervisor,
    ) {
      supervisor.#closed = true;
      yield* supervisor.#client.shutdown();
    })(this);
  }

  #ensureReady(): Effect.Effect<ChdbWorkerProcessClient, ViewServerError> {
    if (
      this.#closed ||
      !this.#client.exitedUnexpectedly ||
      this.#options.restartWorkerOnUnexpectedExit !== true
    ) {
      return this.#client.exitedUnexpectedly && !this.#closed
        ? Effect.sync(() => {
            this.#lastError = this.#client.health.message ?? "chDB worker exited";
          }).pipe(
            Effect.flatMap(() => Effect.fail(snapshotBackendFailed(this.#topic, this.#lastError))),
          )
        : Effect.succeed(this.#client);
    }
    return Effect.fn("view-server.chdb.supervisor.restart")(function* (
      supervisor: ChdbWorkerSupervisor,
    ) {
      supervisor.#lastError = supervisor.#client.health.message ?? "chDB worker exited";
      supervisor.#restarting = true;
      yield* Effect.logWarning(`view-server chDB worker restarting topic=${supervisor.#topic}`);
      yield* supervisor.#client.restart();
      const restartSnapshot = supervisor.#restartSnapshot();
      yield* supervisor.init(restartSnapshot);
      supervisor.#onRestarted(restartSnapshot.version);
      return supervisor.#client;
    })(this).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          this.#restarting = false;
        }),
      ),
    );
  }

  readonly #rememberFailure = <A>(
    effect: Effect.Effect<A, ViewServerError>,
  ): Effect.Effect<A, ViewServerError> =>
    effect.pipe(
      Effect.tapError((error) =>
        Effect.sync(() => {
          this.#lastError = error.message;
        }),
      ),
    );

  #chunkedInit(args: Parameters<SnapshotBackend["init"]>[0]): Effect.Effect<void, ViewServerError> {
    return Effect.fnUntraced(function* (supervisor: ChdbWorkerSupervisor) {
      yield* supervisor.#client.request({
        id: supervisor.#client.nextRequestId(),
        type: "initStart",
        args: {
          topic: args.topic,
          idField: args.idField,
          version: args.version,
          ...(args.literalStringFields === undefined
            ? {}
            : { literalStringFields: args.literalStringFields }),
        },
      });
      for (let offset = 0; offset < args.rows.length; offset += WORKER_INIT_CHUNK_SIZE) {
        yield* supervisor.#client.request({
          id: supervisor.#client.nextRequestId(),
          type: "initRows",
          rows: args.rows.slice(offset, offset + WORKER_INIT_CHUNK_SIZE).map(encodeVersionedRow),
        });
      }
      yield* supervisor.#client.request({
        id: supervisor.#client.nextRequestId(),
        type: "initCommit",
      });
    })(this).pipe(this.#rememberFailure);
  }

  #makeClient(): ChdbWorkerProcessClient {
    const dependencyClient = this.#dependencies.makeProcessClient?.();
    if (dependencyClient !== undefined) {
      return dependencyClient;
    }
    return new ChdbProcessClient({
      workerEntryUrl: this.#options.groupedRefreshWorkerEntryUrl,
      onWorkerExit: this.#options.onWorkerExit,
      onWorkerRequest: this.#options.onWorkerRequest,
      onWorkerSpawn: this.#options.onWorkerSpawn,
    });
  }
}
