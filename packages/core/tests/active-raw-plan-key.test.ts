import { describe, expect, it } from "@effect/vitest";
import type { RuntimeFilterNode, RuntimeRawQuery, RuntimeRow } from "../src/protocol/index.ts";
import {
  ACTIVE_RAW_PLAN_KEY_CACHE_SCOPE,
  activeRawPlanKey,
} from "../src/worker/active-raw-plan-key.ts";
import { makeActiveRawPlan } from "../src/worker/active-view.ts";

describe("ActiveRawPlanKey", () => {
  it("shares a key for the same where/order plan across offset and limit windows", () => {
    const baseQuery = {
      fields: { id: true, status: true, price: true },
      where: { field: "status", comparator: "equals", value: "open" },
      orderBy: [{ field: "price", direction: "asc" }],
      offset: 0,
      limit: 50,
    } satisfies RuntimeRawQuery;
    const secondWindow = {
      ...baseQuery,
      offset: 50,
      limit: 100,
    } satisfies RuntimeRawQuery;

    expect(activeRawPlanKey(baseQuery, "id")).toBe(activeRawPlanKey(secondWindow, "id"));
  });

  it("shares a key for different projections while snapshots still project per view query", () => {
    const rows: readonly RuntimeRow[] = [
      { id: "a", status: "open", price: 10 },
      { id: "b", status: "open", price: 20 },
    ];
    const wideQuery = {
      fields: { id: true, status: true, price: true },
      where: { field: "status", comparator: "equals", value: "open" },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 10,
    } satisfies RuntimeRawQuery;
    const narrowQuery = {
      ...wideQuery,
      fields: { id: true },
      offset: 1,
      limit: 1,
    } satisfies RuntimeRawQuery;
    const plan = makeActiveRawPlan(rows, wideQuery, "id");

    expect(activeRawPlanKey(wideQuery, "id")).toBe(activeRawPlanKey(narrowQuery, "id"));
    expect(plan.snapshot(wideQuery).rows).toEqual([
      { id: "a", status: "open", price: 10 },
      { id: "b", status: "open", price: 20 },
    ]);
    expect(plan.snapshot(narrowQuery).rows).toEqual([{ id: "b" }]);
  });

  it("normalizes where object property order and distinguishes different filters", () => {
    const whereLeft = {
      field: "status",
      comparator: "equals",
      value: "open",
    } satisfies RuntimeFilterNode;
    const whereRight = {
      value: "open",
      comparator: "equals",
      field: "status",
    } satisfies RuntimeFilterNode;
    const openQuery = {
      fields: { id: true },
      where: whereLeft,
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 10,
    } satisfies RuntimeRawQuery;
    const reorderedQuery = {
      ...openQuery,
      where: whereRight,
    } satisfies RuntimeRawQuery;
    const closedQuery = {
      ...openQuery,
      where: { ...whereLeft, value: "closed" },
    } satisfies RuntimeRawQuery;

    expect(activeRawPlanKey(openQuery, "id")).toBe(activeRawPlanKey(reorderedQuery, "id"));
    expect(activeRawPlanKey(openQuery, "id")).not.toBe(activeRawPlanKey(closedQuery, "id"));
  });

  it("normalizes implicit id tiebreak ordering and distinguishes explicit order plans", () => {
    const defaultOrderQuery = {
      fields: { id: true, price: true },
      limit: 10,
    } satisfies RuntimeRawQuery;
    const explicitDefaultOrderQuery = {
      ...defaultOrderQuery,
      orderBy: [{ field: "id", direction: "asc" }],
    } satisfies RuntimeRawQuery;
    const priceOrderQuery = {
      ...defaultOrderQuery,
      orderBy: [{ field: "price", direction: "asc" }],
    } satisfies RuntimeRawQuery;

    expect(activeRawPlanKey(defaultOrderQuery, "id")).toBe(
      activeRawPlanKey(explicitDefaultOrderQuery, "id"),
    );
    expect(activeRawPlanKey(defaultOrderQuery, "id")).not.toBe(
      activeRawPlanKey(priceOrderQuery, "id"),
    );
  });

  it("keeps literal-string execution options out of a topic-scoped key", () => {
    const rows: readonly RuntimeRow[] = [
      { id: "strict", status: "open", price: 10 },
      { id: "loose", status: "OPEN", price: 20 },
    ];
    const query = {
      fields: { id: true, status: true },
      where: { field: "status", comparator: "equals", value: "open" },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 10,
    } satisfies RuntimeRawQuery;
    const loosePlan = makeActiveRawPlan(rows, query, "id");
    const strictPlan = makeActiveRawPlan(rows, query, "id", {
      literalStringFields: new Set(["status"]),
    });

    expect(ACTIVE_RAW_PLAN_KEY_CACHE_SCOPE).toBe("topic");
    expect(loosePlan.key).toBe(strictPlan.key);
    expect(loosePlan.snapshot(query).totalRows).toBe(2);
    expect(strictPlan.snapshot(query).totalRows).toBe(1);
  });
});
