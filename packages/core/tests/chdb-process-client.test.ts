import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import type { RuntimeQuery, RuntimeRow } from "../src/protocol/index.ts";
import {
  decodeSnapshotBackendResult,
  encodeRuntimeQuery,
  encodeVersionedRow,
} from "../src/snapshot/row-wire-codec.ts";
import type {
  ChdbQueryWorkerResponse,
  ChdbWireSnapshotBackendResult,
} from "../src/snapshot/chdb-query-worker-protocol.ts";
import { ChdbProcessClient } from "../src/snapshot/chdb-process-client.ts";
import type { VersionedRow } from "../src/snapshot/index.ts";

const allOrdersQuery = {
  fields: {
    id: true,
    symbol: true,
    price: true,
  },
  orderBy: [{ field: "id", direction: "asc" }],
  limit: 10,
} satisfies RuntimeQuery;

describe("ChdbProcessClient", () => {
  it.effect("sends worker requests and reports process health", () =>
    Effect.gen(function* () {
      const client = new ChdbProcessClient();
      client.setTopic("orders");
      yield* Effect.addFinalizer(() => client.shutdown().pipe(Effect.ignore));

      yield* initOrders(
        client,
        [
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "MSFT", price: 200 },
        ],
        1n,
      );

      expect(client.health).toMatchObject({
        status: "ready",
        pendingRequests: 0,
        restarts: 0,
        lastError: "",
      });
      expect(client.health.pid).toBeGreaterThan(0);

      const response = yield* client.request({
        id: client.nextRequestId(),
        type: "snapshot",
        args: {
          query: encodeRuntimeQuery(allOrdersQuery),
          targetVersion: 1n,
        },
      });
      const result = decodeSnapshotBackendResult(requireSnapshotResult(response));

      expect(result).toEqual({
        backendVersion: 1n,
        totalRows: 2,
        rows: [
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "MSFT", price: 200 },
        ],
      });

      yield* client.shutdown();
      expect(client.health.status).toBe("stopped");
    }).pipe(Effect.scoped),
  );

  it.effect("restarts the child process on demand", () =>
    Effect.gen(function* () {
      const spawnedPids: number[] = [];
      const client = new ChdbProcessClient({
        onWorkerSpawn: (pid) => {
          if (pid !== undefined) {
            spawnedPids.push(pid);
          }
        },
      });
      client.setTopic("orders");
      yield* Effect.addFinalizer(() => client.shutdown().pipe(Effect.ignore));
      yield* initOrders(client, [{ id: "o-1", symbol: "AAPL", price: 100 }], 1n);

      yield* client.restart();
      client.setTopic("orders");
      yield* initOrders(client, [{ id: "o-2", symbol: "MSFT", price: 200 }], 2n);

      const response = yield* client.request({
        id: client.nextRequestId(),
        type: "snapshot",
        args: {
          query: encodeRuntimeQuery(allOrdersQuery),
          targetVersion: 2n,
        },
      });
      const result = decodeSnapshotBackendResult(requireSnapshotResult(response));

      expect(spawnedPids.length).toBe(2);
      expect(spawnedPids[1]).not.toBe(spawnedPids[0]);
      expect(client.health).toMatchObject({
        status: "ready",
        pendingRequests: 0,
        restarts: 1,
        lastError: "",
      });
      expect(result).toEqual({
        backendVersion: 2n,
        totalRows: 1,
        rows: [{ id: "o-2", symbol: "MSFT", price: 200 }],
      });
    }).pipe(Effect.scoped),
  );
});

function initOrders(
  client: ChdbProcessClient,
  rows: readonly RuntimeRow[],
  version: bigint,
): Effect.Effect<void, never, never> {
  return client
    .request({
      id: client.nextRequestId(),
      type: "init",
      args: {
        topic: "orders",
        idField: "id",
        rows: versionedRows(rows, version).map(encodeVersionedRow),
        version,
      },
    })
    .pipe(Effect.asVoid, Effect.orDie);
}

function versionedRows(rows: readonly RuntimeRow[], version: bigint): readonly VersionedRow[] {
  return rows.map((row) => ({ row, version }));
}

function requireSnapshotResult(
  response: Extract<ChdbQueryWorkerResponse, { readonly success: true }>,
): ChdbWireSnapshotBackendResult {
  if (response.result === undefined) {
    throw new Error("Expected chDB worker snapshot result");
  }
  return response.result;
}
