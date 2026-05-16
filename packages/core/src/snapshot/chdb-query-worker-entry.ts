import * as Effect from "effect/Effect";
import { parentPort } from "node:worker_threads";
import type { ViewServerError } from "../errors.ts";
import { createChdbSnapshotBackend } from "./chdb-backend.ts";
import {
  decodeMutationLogEntry,
  decodeRuntimeQuery,
  decodeVersionedRow,
  encodeSnapshotBackendResult,
} from "./chdb-query-worker-codec.ts";
import type {
  ChdbQueryWorkerInitStartRequest,
  ChdbQueryWorkerRequest,
  ChdbQueryWorkerResponse,
} from "./chdb-query-worker-protocol.ts";
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

const onMessage = (request: ChdbQueryWorkerRequest): void => {
  void Effect.runPromise(handleRequest(request)).then(
    (response) => {
      sendResponse(response);
    },
    (error: unknown) => {
      sendResponse({
        id: request.id,
        success: false,
        error: error instanceof Error ? error.message : String(error),
      } satisfies ChdbQueryWorkerResponse);
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
        .pipe(Effect.as(success(request.id)));
    case "initStart":
      return Effect.sync(() => {
        pendingInit = {
          ...request.args,
          rows: [],
        };
      }).pipe(Effect.as(success(request.id)));
    case "initRows":
      return Effect.sync(() => {
        pendingInit?.rows.push(...request.rows.map(decodeVersionedRow));
      }).pipe(Effect.as(success(request.id)));
    case "initCommit":
      return pendingInit === undefined
        ? Effect.succeed(failure(request.id, "chDB worker initCommit before initStart"))
        : backend.init(pendingInit).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                pendingInit = undefined;
              }),
            ),
            Effect.as(success(request.id)),
          );
    case "applyBatch":
      return backend
        .applyBatch({
          mutations: request.args.mutations.map(decodeMutationLogEntry),
          highestVersion: request.args.highestVersion,
        })
        .pipe(Effect.as(success(request.id)));
    case "snapshot":
      return backend
        .snapshot({
          query: decodeRuntimeQuery(request.args.query),
          targetVersion: request.args.targetVersion,
        })
        .pipe(
          Effect.map((result) => successSnapshot(request.id, encodeSnapshotBackendResult(result))),
        );
    case "close":
      return backend.close().pipe(Effect.as(success(request.id)));
  }
}

function failure(id: number, error: string): ChdbQueryWorkerResponse {
  return {
    id,
    success: false,
    error,
  };
}

function success(id: number): ChdbQueryWorkerResponse {
  return {
    id,
    success: true,
  };
}

function successSnapshot(
  id: number,
  result: NonNullable<Extract<ChdbQueryWorkerResponse, { readonly success: true }>["result"]>,
): ChdbQueryWorkerResponse {
  return {
    id,
    success: true,
    result,
  };
}
