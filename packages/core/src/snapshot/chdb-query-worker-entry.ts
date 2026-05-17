import * as Effect from "effect/Effect";
import { parentPort } from "node:worker_threads";
import type { ViewServerError } from "../errors.ts";
import { createChdbSnapshotBackend } from "./chdb-backend.ts";
import {
  decodeMutationLogEntry,
  decodeRuntimeQuery,
  decodeVersionedRow,
  encodeSnapshotBackendResult,
} from "./row-wire-codec.ts";
import type {
  ChdbQueryWorkerInitStartRequest,
  ChdbQueryWorkerRequest,
  ChdbQueryWorkerResponse,
  ChdbWireHealth,
} from "./chdb-worker-protocol.ts";
import {
  chdbWorkerFailure,
  chdbWorkerHealthSuccess,
  chdbWorkerRequestId,
  chdbWorkerSnapshotSuccess,
  chdbWorkerSuccess,
  decodeChdbWorkerRequest,
} from "./chdb-worker-protocol.ts";
import type { VersionedRow } from "./snapshot-backend.ts";

const port = parentPort;
const sendResponse =
  port === null
    ? (response: ChdbQueryWorkerResponse): void => {
        process.send?.(response);
      }
    : (response: ChdbQueryWorkerResponse): void => {
        port.postMessage(response);
      };

if (port === null && process.send === undefined) {
  throw new Error("chDB query worker requires worker_threads parentPort or child_process IPC");
}

const backend = createChdbSnapshotBackend({ groupedRefreshWorker: false });
let pendingInit: PendingInit | undefined;

type PendingInit = ChdbQueryWorkerInitStartRequest["args"] & {
  readonly rows: VersionedRow[];
};

const onMessage = (message: unknown): void => {
  let request: ChdbQueryWorkerRequest;
  try {
    request = decodeChdbWorkerRequest(message);
  } catch (error) {
    sendResponse(
      chdbWorkerFailure(
        chdbWorkerRequestId(message),
        error instanceof Error ? error.message : String(error),
      ),
    );
    return;
  }
  void Effect.runPromise(handleRequest(request)).then(
    (response) => {
      sendResponse(response);
    },
    (error: unknown) => {
      sendResponse(
        chdbWorkerFailure(request.id, error instanceof Error ? error.message : String(error)),
      );
    },
  );
};

if (port === null) {
  process.on("message", onMessage);
} else {
  port.on("message", onMessage);
}

function handleRequest(
  request: ChdbQueryWorkerRequest,
): Effect.Effect<ChdbQueryWorkerResponse, ViewServerError> {
  switch (request.type) {
    case "init":
      return backend
        .init({
          ...request.args,
          rows: request.args.rows.map(decodeVersionedRow),
        })
        .pipe(Effect.as(chdbWorkerSuccess(request.id)));
    case "initStart":
      return Effect.sync(() => {
        pendingInit = {
          ...request.args,
          rows: [],
        };
      }).pipe(Effect.as(chdbWorkerSuccess(request.id)));
    case "initRows":
      return Effect.sync(() => {
        pendingInit?.rows.push(...request.rows.map(decodeVersionedRow));
      }).pipe(Effect.as(chdbWorkerSuccess(request.id)));
    case "initCommit":
      return pendingInit === undefined
        ? Effect.succeed(chdbWorkerFailure(request.id, "chDB worker initCommit before initStart"))
        : backend.init(pendingInit).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                pendingInit = undefined;
              }),
            ),
            Effect.as(chdbWorkerSuccess(request.id)),
          );
    case "applyBatch":
      return backend
        .applyBatch({
          mutations: request.args.mutations.map(decodeMutationLogEntry),
          highestVersion: request.args.highestVersion,
        })
        .pipe(Effect.as(chdbWorkerSuccess(request.id)));
    case "snapshot":
      return backend
        .snapshot({
          query: decodeRuntimeQuery(request.args.query),
          targetVersion: request.args.targetVersion,
        })
        .pipe(
          Effect.map((result) =>
            chdbWorkerSnapshotSuccess(request.id, encodeSnapshotBackendResult(result)),
          ),
        );
    case "groupedRefreshSnapshot":
      return backend
        .snapshot({
          query: decodeRuntimeQuery(request.args.query),
          targetVersion: request.args.targetVersion,
        })
        .pipe(
          Effect.map((result) =>
            chdbWorkerSnapshotSuccess(request.id, encodeSnapshotBackendResult(result)),
          ),
        );
    case "health":
      if (backend.health === undefined) {
        const health = { status: "ready" } satisfies ChdbWireHealth;
        return Effect.succeed(chdbWorkerHealthSuccess(request.id, health));
      }
      return backend.health.pipe(
        Effect.map((health) => chdbWorkerHealthSuccess(request.id, health)),
      );
    case "close":
      return backend.close().pipe(Effect.as(chdbWorkerSuccess(request.id)));
  }
}
