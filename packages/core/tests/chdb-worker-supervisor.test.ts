import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import type { ViewServerError } from "../src/errors.ts";
import { snapshotBackendFailed } from "../src/errors.ts";
import type { RuntimeQuery, RuntimeRow } from "../src/protocol/index.ts";
import { ChdbWorkerSupervisor } from "../src/snapshot/chdb-worker-supervisor.ts";
import type {
  ChdbQueryWorkerRequest,
  ChdbQueryWorkerSuccessResponse,
} from "../src/snapshot/chdb-worker-protocol.ts";
import { encodeSnapshotBackendResult } from "../src/snapshot/row-wire-codec.ts";
import type { SnapshotBackend, SnapshotBackendHealth } from "../src/snapshot/index.ts";

const allOrdersQuery = {
  fields: {
    id: true,
    symbol: true,
    price: true,
  },
  orderBy: [{ field: "id", direction: "asc" }],
  limit: 10,
} satisfies RuntimeQuery;

describe("ChdbWorkerSupervisor", () => {
  it.effect("owns restart, re-init, request encoding, and health", () =>
    Effect.gen(function* () {
      const processClient = new FakeChdbProcessClient();
      processClient.markExited("chDB worker exited from signal SIGKILL");
      const restartedVersions: bigint[] = [];
      const restartRows = [{ id: "o-1", symbol: "AAPL", price: 100 }];
      const supervisor = new ChdbWorkerSupervisor(
        {
          restartWorkerOnUnexpectedExit: true,
        },
        () => initArgs(restartRows, 4n),
        (version) => {
          restartedVersions.push(version);
        },
        { makeProcessClient: () => processClient },
      );

      const result = yield* supervisor.snapshot({ query: allOrdersQuery, targetVersion: 4n });

      expect(processClient.restartCount).toBe(1);
      expect(restartedVersions).toEqual([4n]);
      expect(processClient.requests.map((request) => request.type)).toEqual(["init", "snapshot"]);
      expect(processClient.requests[0]).toMatchObject({
        type: "init",
        args: {
          topic: "orders",
          idField: "id",
          version: 4n,
        },
      });
      expect(supervisor.health).toMatchObject({
        status: "ready",
        restarts: 1,
        pendingRequests: 0,
      });
      expect(result).toEqual({
        backendVersion: 4n,
        totalRows: 1,
        rows: restartRows,
      });
    }),
  );

  it.effect("records typed pending request failures in health", () =>
    Effect.gen(function* () {
      const processClient = new FakeChdbProcessClient();
      processClient.failNextRequest = snapshotBackendFailed("orders", "IPC closed");
      const supervisor = new ChdbWorkerSupervisor(
        {},
        () => initArgs([{ id: "o-1", symbol: "AAPL", price: 100 }], 1n),
        undefined,
        {
          makeProcessClient: () => processClient,
        },
      );

      const error = yield* supervisor
        .snapshot({ query: allOrdersQuery, targetVersion: 1n })
        .pipe(Effect.flip);

      expect(error._tag).toBe("SnapshotBackendFailed");
      expect(supervisor.health.lastError).toContain("IPC closed");
      expect(supervisor.health.pendingRequests).toBe(0);
    }),
  );
});

class FakeChdbProcessClient {
  readonly requests: ChdbQueryWorkerRequest[] = [];
  restartCount = 0;
  failNextRequest: ViewServerError | undefined;
  #nextRequestId = 0;
  #health: SnapshotBackendHealth = {
    status: "ready",
    pid: 1234,
    restarts: 0,
    pendingRequests: 0,
    lastError: "",
  };
  #exitedUnexpectedly = false;

  get pendingRequests(): number {
    return this.#health.pendingRequests ?? 0;
  }

  get exitedUnexpectedly(): boolean {
    return this.#exitedUnexpectedly;
  }

  get health(): SnapshotBackendHealth {
    return this.#health;
  }

  setTopic(_topic: string): void {}

  nextRequestId(): number {
    const id = this.#nextRequestId;
    this.#nextRequestId++;
    return id;
  }

  markExited(message: string): void {
    this.#exitedUnexpectedly = true;
    this.#health = {
      status: "degraded",
      message,
      pid: 1234,
      restarts: this.restartCount,
      pendingRequests: 0,
      lastError: message,
    };
  }

  request(
    request: ChdbQueryWorkerRequest,
  ): Effect.Effect<ChdbQueryWorkerSuccessResponse, ViewServerError> {
    return Effect.sync(() => {
      this.requests.push(request);
      return this.failNextRequest;
    }).pipe(
      Effect.flatMap((failure) => {
        if (failure !== undefined) {
          this.failNextRequest = undefined;
          return Effect.fail(failure);
        }
        return Effect.succeed(this.responseFor(request));
      }),
    );
  }

  restart(): Effect.Effect<void, ViewServerError> {
    return Effect.sync(() => {
      this.restartCount++;
      this.#exitedUnexpectedly = false;
      this.#health = {
        status: "ready",
        pid: 1235,
        restarts: this.restartCount,
        pendingRequests: 0,
        lastError: "",
      };
    });
  }

  shutdown(): Effect.Effect<void, ViewServerError> {
    return Effect.sync(() => {
      this.#health = {
        status: "stopped",
        pid: 1235,
        restarts: this.restartCount,
        pendingRequests: 0,
        lastError: "",
      };
    });
  }

  private responseFor(request: ChdbQueryWorkerRequest): ChdbQueryWorkerSuccessResponse {
    if (request.type === "snapshot" || request.type === "groupedRefreshSnapshot") {
      return {
        id: request.id,
        success: true,
        result: encodeSnapshotBackendResult({
          backendVersion: request.args.targetVersion,
          totalRows: 1,
          rows: [{ id: "o-1", symbol: "AAPL", price: 100 }],
        }),
      };
    }
    return {
      id: request.id,
      success: true,
    };
  }
}

function initArgs(
  rows: readonly RuntimeRow[],
  version: bigint,
): Parameters<SnapshotBackend["init"]>[0] {
  return {
    topic: "orders",
    idField: "id",
    rows: rows.map((row) => ({ row, version })),
    version,
  };
}
