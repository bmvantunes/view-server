import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import type { RuntimeQuery } from "../src/protocol/index.ts";
import {
  decodeMutationLogEntry,
  decodeRuntimeQuery,
  decodeRuntimeRow,
  encodeMutationLogEntry,
  encodeRuntimeQuery,
  encodeRuntimeRow,
} from "../src/snapshot/row-wire-codec.ts";
import type { MutationLogEntry } from "../src/worker/mutation-log.ts";

describe("RowWireCodec", () => {
  it("roundtrips BigDecimal row values exactly", () => {
    const amount = BigDecimal.fromStringUnsafe("123456789.000000000000000001");
    const decoded = decodeRuntimeRow(
      encodeRuntimeRow({
        id: "o-1",
        amount,
        nested: {
          values: [amount],
        },
      }),
    );

    expectDecimal(decoded.amount, "123456789.000000000000000001");
    const nested = decoded.nested;
    if (!isReadonlyRecord(nested) || !Array.isArray(nested.values)) {
      throw new Error("Expected nested row values to survive wire roundtrip");
    }
    expectDecimal(nested.values[0], "123456789.000000000000000001");
  });

  it("roundtrips query filters with decimal values", () => {
    const query = {
      fields: {
        id: true,
        amount: true,
      },
      where: {
        op: "and",
        conditions: [
          {
            field: "amount",
            comparator: "greater_than",
            value: BigDecimal.fromStringUnsafe("10.000000000000000001"),
          },
          {
            field: "amount",
            comparator: "one_of",
            value: [
              BigDecimal.fromStringUnsafe("11.000000000000000001"),
              BigDecimal.fromStringUnsafe("12.000000000000000001"),
            ],
          },
        ],
      },
      limit: 10,
    } satisfies RuntimeQuery;

    const decoded = decodeRuntimeQuery(encodeRuntimeQuery(query));

    if (decoded.where === undefined || !("conditions" in decoded.where)) {
      throw new Error("Expected decoded query to contain nested filter conditions");
    }
    const [greaterThan, oneOf] = decoded.where.conditions;
    if (greaterThan === undefined || "conditions" in greaterThan) {
      throw new Error("Expected first condition to be a field predicate");
    }
    if (oneOf === undefined || "conditions" in oneOf || !Array.isArray(oneOf.value)) {
      throw new Error("Expected second condition to be a one_of field predicate");
    }
    expectDecimal(greaterThan.value, "10.000000000000000001");
    expectDecimal(oneOf.value[0], "11.000000000000000001");
    expectDecimal(oneOf.value[1], "12.000000000000000001");
  });

  it("roundtrips mutation batches with BigDecimal row values", () => {
    const entries: readonly MutationLogEntry[] = [
      {
        version: 1n,
        kind: "insert",
        id: "o-1",
        after: {
          id: "o-1",
          amount: BigDecimal.fromStringUnsafe("1.000000000000000001"),
        },
        changedFields: new Set(["id", "amount"]),
      },
      {
        version: 2n,
        kind: "update",
        id: "o-1",
        before: {
          id: "o-1",
          amount: BigDecimal.fromStringUnsafe("1.000000000000000001"),
        },
        after: {
          id: "o-1",
          amount: BigDecimal.fromStringUnsafe("2.000000000000000001"),
        },
        changedFields: new Set(["amount"]),
      },
    ];

    const decoded = entries.map((entry) => decodeMutationLogEntry(encodeMutationLogEntry(entry)));

    expectDecimal(decoded[0]?.after?.amount, "1.000000000000000001");
    expectDecimal(decoded[1]?.before?.amount, "1.000000000000000001");
    expectDecimal(decoded[1]?.after?.amount, "2.000000000000000001");
    expect(decoded[1]?.changedFields).toEqual(new Set(["amount"]));
  });
});

function expectDecimal(value: unknown, expected: string): void {
  if (!BigDecimal.isBigDecimal(value)) {
    throw new Error("Expected value to be an Effect BigDecimal");
  }
  expect(BigDecimal.equals(value, BigDecimal.fromStringUnsafe(expected))).toBe(true);
}

function isReadonlyRecord(value: unknown): value is Readonly<Record<string, unknown>> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
