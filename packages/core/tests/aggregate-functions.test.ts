import { describe, expect, it } from "@effect/vitest";
import * as BigDecimal from "effect/BigDecimal";
import { makeAggregateState } from "../src/worker/aggregate-functions.ts";

describe("aggregate functions", () => {
  it("counts rows including null and missing fields, and supports deletes", () => {
    const state = makeAggregateState({ aggFunc: "count", field: "value" });
    const first = { id: "a", value: null };
    const second = { id: "b" };

    state.add(first);
    state.add(second);
    expect(state.value()).toBe(2);

    state.remove(first);
    expect(state.value()).toBe(1);
  });

  it("sums BigDecimal values exactly across updates and deletes", () => {
    const state = makeAggregateState({ aggFunc: "sum", field: "value" });
    const huge = {
      id: "a",
      value: BigDecimal.fromStringUnsafe("9007199254740993.000000000000001"),
    };
    const tiny = { id: "b", value: BigDecimal.fromStringUnsafe("0.000000000000009") };
    const replacement = {
      id: "a",
      value: BigDecimal.fromStringUnsafe("9007199254740994.000000000000001"),
    };

    state.add(huge);
    state.add(tiny);
    expectDecimal(state.value(), "9007199254740993.000000000000010");

    state.remove(huge);
    state.add(replacement);
    expectDecimal(state.value(), "9007199254740994.000000000000010");

    state.remove(tiny);
    expectDecimal(state.value(), "9007199254740994.000000000000001");
  });

  it("tracks min and max while ignoring null values", () => {
    const min = makeAggregateState({ aggFunc: "min", field: "value" });
    const max = makeAggregateState({ aggFunc: "max", field: "value" });
    const rows = [
      { id: "a", value: null },
      { id: "b", value: 30 },
      { id: "c", value: 10 },
      { id: "d", value: 20 },
    ];

    for (const row of rows) {
      min.add(row);
      max.add(row);
    }

    expect(min.value()).toBe(10);
    expect(max.value()).toBe(30);

    min.remove(rows[2] ?? {});
    max.remove(rows[1] ?? {});
    expect(min.value()).toBe(20);
    expect(max.value()).toBe(20);
  });
});

function expectDecimal(value: unknown, expected: string): void {
  if (!BigDecimal.isBigDecimal(value)) {
    throw new Error("Expected BigDecimal aggregate value");
  }
  expect(BigDecimal.equals(value, BigDecimal.fromStringUnsafe(expected))).toBe(true);
}
