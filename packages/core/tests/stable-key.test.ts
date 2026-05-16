import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import * as Schema from "effect/Schema";
import { rowKeyByField, type DeltaEvent, type RuntimeRow } from "../src/protocol/index.ts";
import {
  compareStableKeys,
  decodeStableKeyFromWire,
  encodeStableKeyForWire,
  isStableKey,
  stableKeyDebug,
  stableKeyEquals,
  stableKeyFromRow,
  stableKeyFromValue,
} from "../src/protocol/stable-key.ts";
import { wireSubscriptionEvent } from "../src/rpc/wire.ts";
import { RpcDeltaEvent } from "../src/rpc/rpcs.ts";
import {
  decodeMutationLogEntry,
  encodeMutationLogEntry,
} from "../src/snapshot/chdb-query-worker-codec.ts";
import type { MutationLogEntry } from "../src/worker/mutation-log.ts";

describe("StableKey", () => {
  it("keeps numeric id 1 and string id '1' explicit and distinct", () => {
    const numeric = stableKeyFromValue(1, "orders.id");
    const string = stableKeyFromValue("1", "orders.id");
    const keyed = new Map([
      [numeric, "numeric"],
      [string, "string"],
    ]);

    expect(numeric).toBe(1);
    expect(string).toBe("1");
    expect(stableKeyEquals(numeric, string)).toBe(false);
    expect(compareStableKeys(numeric, string)).not.toBe(0);
    expect(stableKeyDebug(numeric)).toBe("number:1");
    expect(stableKeyDebug(string)).toBe("string:1");
    expect(keyed.size).toBe(2);
    expect(keyed.get(1)).toBe("numeric");
    expect(keyed.get("1")).toBe("string");
  });

  it("rejects unsupported id values instead of stringifying them into collisions", () => {
    expect(isStableKey(1)).toBe(true);
    expect(isStableKey("1")).toBe(true);
    expect(isStableKey(Number.NaN)).toBe(false);
    expect(() => stableKeyFromValue(Number.NaN, "orders.id")).toThrow(
      "orders.id must be a string or finite number, got number",
    );
    expect(() => stableKeyFromValue(1n, "orders.id")).toThrow(
      "orders.id must be a string or finite number, got bigint",
    );
    expect(() =>
      stableKeyFromValue(BigDecimal.fromStringUnsafe("1.000000000000000001"), "orders.id"),
    ).toThrow("orders.id must be a string or finite number, got BigDecimal");
  });

  it("extracts row ids through the same stable key path used by query helpers", () => {
    const numericRow = { id: 1, value: "numeric" } satisfies RuntimeRow;
    const stringRow = { id: "1", value: "string" } satisfies RuntimeRow;

    expect(stableKeyFromRow(numericRow, "id")).toBe(1);
    expect(stableKeyFromRow(stringRow, "id")).toBe("1");
    expect(rowKeyByField(numericRow, "id")).toBe(1);
    expect(rowKeyByField(stringRow, "id")).toBe("1");
    expect(() => stableKeyFromRow({ id: undefined }, "id")).toThrow(
      "id must be a string or finite number, got undefined",
    );
  });

  it("keeps delta operation keys stable through RPC wire encoding and schema decoding", () => {
    const event = {
      type: "delta",
      requestId: "request-1",
      ops: [
        { type: "remove", key: 1 },
        { type: "upsert", key: "1", row: { id: "1", price: 10 }, index: 0 },
        { type: "patch", key: 1, changes: { price: 11 }, index: 1 },
      ],
      meta: {
        fromVersion: "1",
        toVersion: "2",
        totalRows: 2,
        serverTime: 123,
      },
    } satisfies DeltaEvent<readonly RuntimeRow[]>;

    const decoded = Schema.decodeUnknownSync(RpcDeltaEvent)(wireSubscriptionEvent(event));
    const [remove, upsert, patch] = decoded.ops;
    if (remove === undefined || remove.type !== "remove") {
      throw new Error("Expected decoded remove operation");
    }
    if (upsert === undefined || upsert.type !== "upsert") {
      throw new Error("Expected decoded upsert operation");
    }
    if (patch === undefined || patch.type !== "patch") {
      throw new Error("Expected decoded patch operation");
    }

    expect(remove.key).toBe(1);
    expect(typeof remove.key).toBe("number");
    expect(upsert.key).toBe("1");
    expect(typeof upsert.key).toBe("string");
    expect(patch.key).toBe(1);
  });

  it("keeps chDB worker mutation ids stable across encode/decode", () => {
    const entries: readonly MutationLogEntry[] = [
      {
        version: 1n,
        kind: "delete",
        id: 1,
        before: { id: 1, price: 10 },
        changedFields: new Set(["id", "price"]),
      },
      {
        version: 2n,
        kind: "insert",
        id: "1",
        after: { id: "1", price: 20 },
        changedFields: new Set(["id", "price"]),
      },
    ];
    const decoded = entries.map((entry) => decodeMutationLogEntry(encodeMutationLogEntry(entry)));

    expect(decoded[0]?.id).toBe(1);
    expect(typeof decoded[0]?.id).toBe("number");
    expect(decoded[1]?.id).toBe("1");
    expect(typeof decoded[1]?.id).toBe("string");
  });

  it("validates explicit stable key wire helpers", () => {
    expect(encodeStableKeyForWire(1)).toBe(1);
    expect(encodeStableKeyForWire("1")).toBe("1");
    expect(decodeStableKeyFromWire(1)).toBe(1);
    expect(decodeStableKeyFromWire("1")).toBe("1");
    expect(() => decodeStableKeyFromWire(undefined)).toThrow(
      "wire row key must be a string or finite number, got undefined",
    );
  });
});
