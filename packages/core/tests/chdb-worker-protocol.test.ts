import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Schema from "effect/Schema";
import type { RuntimeQuery } from "../src/protocol/index.ts";
import {
  CHDB_WORKER_PROTOCOL_VERSION,
  CHDB_WORKER_REQUEST_TYPES,
  ChdbWorkerApplyBatchRequest,
  ChdbWorkerRequest,
  ChdbWorkerResponse,
  ChdbWorkerSnapshotRequest,
  chdbWorkerFailure,
  chdbWorkerRequestId,
} from "../src/snapshot/chdb-worker-protocol.ts";
import {
  decodeMutationLogEntry,
  decodeRuntimeQuery,
  decodeSnapshotBackendResult,
  encodeMutationLogEntry,
  encodeRuntimeQuery,
  encodeSnapshotBackendResult,
} from "../src/snapshot/row-wire-codec.ts";
import type { MutationLogEntry } from "../src/worker/mutation-log.ts";

describe("ChdbWorkerProtocol", () => {
  it("rejects invalid commands and malformed payloads", () => {
    const decodeRequest = Schema.decodeUnknownSync(ChdbWorkerRequest);
    const decodeApplyBatch = Schema.decodeUnknownSync(ChdbWorkerApplyBatchRequest);

    expect(CHDB_WORKER_PROTOCOL_VERSION).toBe(1);
    expect(() => decodeRequest({ id: 1, type: "vacuumEverything" })).toThrow();
    expect(() =>
      decodeApplyBatch({
        id: 1,
        type: "applyBatch",
        args: {
          highestVersion: 1n,
          mutations: [
            {
              version: 1n,
              kind: "insert",
              id: "o-1",
              after: { id: "o-1" },
              changedFields: ["id"],
            },
          ],
        },
      }),
    ).toThrow();
  });

  it("round-trips BigDecimal query, mutation, and result payloads", () => {
    const amount = BigDecimal.fromStringUnsafe("123.000000000000000001");
    const runtimeQuery = {
      fields: {
        id: true,
        amount: true,
      },
      where: {
        field: "amount",
        comparator: "greater_than",
        value: amount,
      },
      limit: 10,
    } satisfies RuntimeQuery;
    const snapshotRequest = {
      id: 1,
      type: "snapshot",
      args: {
        query: encodeRuntimeQuery(runtimeQuery),
        targetVersion: 2n,
      },
    } satisfies typeof ChdbWorkerSnapshotRequest.Type;
    const decodedSnapshotRequest = decodeAfterEncode(ChdbWorkerSnapshotRequest, snapshotRequest);
    const decodedRuntimeQuery = decodeRuntimeQuery(decodedSnapshotRequest.args.query);

    if (!("where" in decodedRuntimeQuery) || decodedRuntimeQuery.where === undefined) {
      throw new Error("Expected decoded chDB worker query filter");
    }
    if (!("field" in decodedRuntimeQuery.where)) {
      throw new Error("Expected decoded chDB worker field filter");
    }
    expect(isBigDecimalEqual(decodedRuntimeQuery.where.value, amount)).toBe(true);

    const mutation = {
      version: 2n,
      kind: "insert",
      id: "o-1",
      after: {
        id: "o-1",
        amount,
      },
      changedFields: new Set(["id", "amount"]),
    } satisfies MutationLogEntry;
    const applyBatchRequest = {
      id: 2,
      type: "applyBatch",
      args: {
        mutations: [encodeMutationLogEntry(mutation)],
        highestVersion: 2n,
      },
    } satisfies typeof ChdbWorkerApplyBatchRequest.Type;
    const decodedApplyBatchRequest = decodeAfterEncode(
      ChdbWorkerApplyBatchRequest,
      applyBatchRequest,
    );
    const decodedMutation = decodeMutationLogEntry(decodedApplyBatchRequest.args.mutations[0]);

    expect(isBigDecimalEqual(decodedMutation.after?.amount, amount)).toBe(true);

    const response = {
      id: 3,
      success: true,
      result: encodeSnapshotBackendResult({
        rows: [{ id: "o-1", amount }],
        totalRows: 1,
        backendVersion: 2n,
      }),
    } satisfies typeof ChdbWorkerResponse.Type;
    const decodedResponse = decodeAfterEncode(ChdbWorkerResponse, response);
    if (!decodedResponse.success || decodedResponse.result === undefined) {
      throw new Error("Expected decoded chDB worker result response");
    }
    const decodedResult = decodeSnapshotBackendResult(decodedResponse.result);

    expect(decodedResult.totalRows).toBe(1);
    expect(isBigDecimalEqual(decodedResult.rows[0]?.amount, amount)).toBe(true);
  });

  it("models grouped refresh, health, close, and error responses explicitly", () => {
    expect([...CHDB_WORKER_REQUEST_TYPES]).toEqual([
      "init",
      "initStart",
      "initRows",
      "initCommit",
      "applyBatch",
      "snapshot",
      "groupedRefreshSnapshot",
      "health",
      "close",
    ]);
    expect(chdbWorkerRequestId({ id: 12, type: "bad" })).toBe(12);
    expect(chdbWorkerRequestId({ type: "bad" })).toBe(-1);

    const decodedError = decodeAfterEncode(
      ChdbWorkerResponse,
      chdbWorkerFailure(7, "invalid command"),
    );

    expect(decodedError).toEqual({
      id: 7,
      success: false,
      error: "invalid command",
    });
  });
});

function decodeAfterEncode<const A, const I>(schema: Schema.Codec<A, I>, value: A): A {
  return Schema.decodeUnknownSync(schema)(Schema.encodeUnknownSync(schema)(value));
}

function isBigDecimalEqual(value: unknown, expected: BigDecimal.BigDecimal): boolean {
  return BigDecimal.isBigDecimal(value) && BigDecimal.equals(value, expected);
}
