import { describe, expect, it } from "@effect/vitest";
import type { RuntimeRawQuery, RuntimeRow } from "../src/protocol/index.ts";
import type { ActiveSortedIndexKind } from "../src/worker/active-sorted-index.ts";
import {
  activeRawPlanKey,
  estimateActiveRawPlanIndexBytes,
  makeActiveRawPlan,
  makeActiveRawView,
  makeActiveRawViewFromPlan,
  type ActiveRawViewChange,
} from "../src/worker/active-view.ts";
import type { MutationLogEntry } from "../src/worker/mutation-log.ts";
import { executeRawQuery, type QueryExecutionOptions } from "../src/worker/query-engine.ts";

const activeSortedIndexKinds: readonly ActiveSortedIndexKind[] = ["array", "blocks"];

describe("ActiveRawView", () => {
  it("matches raw memory query ordering for nulls, strings, and stable id tiebreaks", () => {
    const rows: RuntimeRow[] = [
      { id: "b", name: "bruno", score: 10 },
      { id: "a", name: "Bruno", score: 10 },
      { id: "d", name: "alice", score: null },
      { id: "c", name: null, score: 99 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, name: true, score: true },
      orderBy: [
        { field: "name", direction: "asc" },
        { field: "score", direction: "desc" },
      ],
      limit: 10,
    };

    expectActiveViewMatchesMemory(rows, query, []);
  });

  it("updates rows entering and leaving a filtered window", () => {
    const rows: RuntimeRow[] = [
      { id: "a", status: "open", price: 100 },
      { id: "b", status: "closed", price: 90 },
      { id: "c", status: "open", price: 80 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, price: true, status: true },
      where: { field: "status", comparator: "equals", value: "open" },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 10,
    };
    const mutations = [
      update(1n, "b", rows[1], { id: "b", status: "open", price: 70 }),
      update(2n, "a", rows[0], { id: "a", status: "closed", price: 100 }),
    ];

    expectActiveViewMatchesMemory(rows, query, mutations);
  });

  it("uses strict literal string filters when schema introspection requires it", () => {
    const rows: RuntimeRow[] = [
      { id: "a", status: "OPEN", price: 100 },
      { id: "b", status: "open", price: 90 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, status: true },
      where: { field: "status", comparator: "equals", value: "open" },
      orderBy: [{ field: "id", direction: "asc" }],
      limit: 10,
    };

    expectActiveViewMatchesMemory(rows, query, [], {
      literalStringFields: new Set(["status"]),
    });
  });

  it("backfills the visible window after deletes", () => {
    const rows: RuntimeRow[] = [
      { id: "a", price: 10 },
      { id: "b", price: 20 },
      { id: "c", price: 30 },
      { id: "d", price: 40 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, price: true },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 2,
    };

    expectActiveViewMatchesMemory(rows, query, [remove(1n, "a", rows[0])]);
  });

  it("removes the exact id when comparator-equal string ids differ by case", () => {
    const rows: RuntimeRow[] = [
      { id: "A", price: 10 },
      { id: "a", price: 10 },
      { id: "b", price: 20 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, price: true },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 10,
    };

    expectActiveViewMatchesMemory(rows, query, [remove(1n, "a", rows[1])]);
  });

  it("inserts comparator-equal string ids after existing equal ids", () => {
    const rows: RuntimeRow[] = [
      { id: "A", price: 10 },
      { id: "b", price: 20 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, price: true },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 10,
    };

    expectActiveViewMatchesMemory(rows, query, [insert(1n, "a", { id: "a", price: 10 })]);
  });

  it("moves sorted rows across an offset window", () => {
    const rows: RuntimeRow[] = [
      { id: "a", price: 10 },
      { id: "b", price: 20 },
      { id: "c", price: 30 },
      { id: "d", price: 40 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, price: true },
      orderBy: [{ field: "price", direction: "asc" }],
      offset: 1,
      limit: 2,
    };

    expectActiveViewMatchesMemory(rows, query, [
      update(1n, "d", rows[3], { id: "d", price: 15 }),
      update(2n, "b", rows[1], { id: "b", price: 50 }),
    ]);
  });

  it("emits totalRows-only changes when matching rows enter outside the visible window", () => {
    const rows: RuntimeRow[] = [
      { id: "a", status: "open", price: 10 },
      { id: "b", status: "open", price: 20 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, price: true },
      where: { field: "status", comparator: "equals", value: "open" },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 1,
    };

    for (const sortedIndex of activeSortedIndexKinds) {
      const view = makeActiveRawView(rows, query, "id", { sortedIndex });
      const before = view.snapshot();
      const change = view.applyMutation(insert(1n, "c", { id: "c", status: "open", price: 30 }));

      expect(change).toEqual({
        type: "totalRowsOnly",
        totalRows: before.totalRows + 1,
      });
    }
  });

  it("reports no-op changes without materializing a new visible page", () => {
    const rows: RuntimeRow[] = [
      { id: "a", status: "open", price: 10, irrelevant: 0 },
      { id: "b", status: "open", price: 20, irrelevant: 0 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, price: true },
      where: { field: "status", comparator: "equals", value: "open" },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 1,
    };

    for (const sortedIndex of activeSortedIndexKinds) {
      const view = makeActiveRawView(rows, query, "id", { sortedIndex });

      expect(view.applyMutation(update(1n, "b", rows[1], { ...rows[1], irrelevant: 1 }))).toEqual({
        type: "noop",
      });
    }
  });

  it("reports changed snapshots when a visible projected value changes in place", () => {
    const rows: RuntimeRow[] = [
      { id: "a", price: 10, quantity: 1 },
      { id: "b", price: 20, quantity: 1 },
      { id: "c", price: 30, quantity: 1 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, quantity: true },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 2,
    };

    for (const sortedIndex of activeSortedIndexKinds) {
      const view = makeActiveRawView(rows, query, "id", { sortedIndex });

      expect(view.applyMutation(update(1n, "a", rows[0], { ...rows[0], quantity: 2 }))).toEqual({
        type: "changed",
        result: {
          rows: [
            { id: "a", quantity: 2 },
            { id: "b", quantity: 1 },
          ],
          totalRows: 3,
        },
      });
    }
  });

  it("reports no-op changes when only visible non-projected values change", () => {
    const rows: RuntimeRow[] = [
      { id: "a", price: 10, note: "old" },
      { id: "b", price: 20, note: "old" },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, price: true },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 2,
    };

    for (const sortedIndex of activeSortedIndexKinds) {
      const view = makeActiveRawView(rows, query, "id", { sortedIndex });

      expect(view.applyMutation(update(1n, "a", rows[0], { ...rows[0], note: "new" }))).toEqual({
        type: "noop",
      });
    }
  });

  it("returns changed snapshots only when the visible window changes", () => {
    const rows: RuntimeRow[] = [
      { id: "a", price: 10 },
      { id: "b", price: 20 },
      { id: "c", price: 30 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, price: true },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 2,
    };

    for (const sortedIndex of activeSortedIndexKinds) {
      const view = makeActiveRawView(rows, query, "id", { sortedIndex });

      expect(view.applyMutation(update(1n, "c", rows[2], { id: "c", price: 5 }))).toEqual({
        type: "changed",
        result: {
          rows: [
            { id: "c", price: 5 },
            { id: "a", price: 10 },
          ],
          totalRows: 3,
        },
      });
    }
  });

  it("matches memory query after mixed inserts, updates, deletes, and case-insensitive filters", () => {
    const rows: RuntimeRow[] = [
      { id: "a", symbol: "AAPL", price: 100, venue: "NASDAQ" },
      { id: "b", symbol: "MSFT", price: 200, venue: "nasdaq" },
      { id: "c", symbol: "ORCL", price: 150, venue: "NYSE" },
      { id: "d", symbol: "NVDA", price: 300, venue: null },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, symbol: true, price: true },
      where: { field: "venue", comparator: "equals", value: "NASDAQ" },
      orderBy: [
        { field: "price", direction: "desc" },
        { field: "symbol", direction: "asc" },
      ],
      limit: 3,
    };
    const mutations = [
      insert(1n, "e", { id: "e", symbol: "AMZN", price: 250, venue: "NASDAQ" }),
      update(2n, "b", rows[1], { id: "b", symbol: "MSFT", price: 50, venue: "nasdaq" }),
      update(3n, "c", rows[2], { id: "c", symbol: "ORCL", price: 400, venue: "NASDAQ" }),
      remove(4n, "a", rows[0]),
    ];

    expectActiveViewMatchesMemory(rows, query, mutations);
  });

  it("matches memory over deterministic random insert, update, and delete sequences", () => {
    const queries: readonly RuntimeRawQuery[] = [
      {
        fields: { id: true, name: true, price: true, score: true, venue: true },
        where: { field: "venue", comparator: "equals", value: "nasdaq" },
        orderBy: [
          { field: "name", direction: "asc" },
          { field: "score", direction: "desc" },
        ],
        offset: 3,
        limit: 9,
      },
      {
        fields: { id: true, status: true, price: true, quantity: true },
        where: {
          op: "and",
          conditions: [
            { field: "status", comparator: "equals", value: "open" },
            { field: "quantity", comparator: "greater_than_or_equal", value: 2 },
          ],
        },
        orderBy: [
          { field: "price", direction: "asc" },
          { field: "name", direction: "desc" },
        ],
        offset: 11,
        limit: 7,
      },
      {
        fields: { id: true, name: true, status: true, price: true },
        orderBy: [
          { field: "price", direction: "desc" },
          { field: "name", direction: "asc" },
        ],
        offset: 0,
        limit: 12,
      },
    ];

    for (const seed of [1, 7, 42, 99]) {
      const rows = makeFuzzRows(80);
      const mutations = makeFuzzMutations(rows, seed, 180);
      for (const query of queries) {
        expectActiveViewMatchesMemory(rows, query, mutations);
      }
    }
  });

  it("shares a raw plan across offset windows and updates the plan once per mutation", () => {
    const rows = makeFuzzRows(60);
    const baseQuery: RuntimeRawQuery = {
      fields: { id: true, name: true, price: true, score: true },
      where: { field: "venue", comparator: "equals", value: "nasdaq" },
      orderBy: [
        { field: "price", direction: "asc" },
        { field: "name", direction: "asc" },
      ],
      limit: 5,
    };
    const firstPage: RuntimeRawQuery = {
      ...baseQuery,
      offset: 0,
    };
    const secondPage: RuntimeRawQuery = {
      ...baseQuery,
      offset: 5,
    };
    const plan = makeActiveRawPlan(rows, firstPage, "id", { sortedIndex: "blocks" });
    const firstView = makeActiveRawViewFromPlan(plan, firstPage, "id");
    const secondView = makeActiveRawViewFromPlan(plan, secondPage, "id");
    let memoryRows = [...rows];

    expect(firstView.snapshot()).toEqual(executeRawQuery(memoryRows, firstPage, "id"));
    expect(secondView.snapshot()).toEqual(executeRawQuery(memoryRows, secondPage, "id"));

    for (const mutation of makeFuzzMutations(rows, 123, 80)) {
      memoryRows = applyMutation(memoryRows, mutation);
      plan.applyMutation(mutation);
      expect(resultFromChange(firstView.applyMutation(mutation), firstView)).toEqual(
        executeRawQuery(memoryRows, firstPage, "id"),
      );
      expect(resultFromChange(secondView.applyMutation(mutation), secondView)).toEqual(
        executeRawQuery(memoryRows, secondPage, "id"),
      );
    }
  });

  it("keeps schema execution options out of the plan key because caches are topic-scoped", () => {
    const rows: RuntimeRow[] = [
      { id: "strict", status: "open", price: 10 },
      { id: "loose", status: "OPEN", price: 20 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, status: true },
      where: { field: "status", comparator: "equals", value: "open" },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 10,
    };
    const secondWindow: RuntimeRawQuery = {
      ...query,
      fields: { id: true },
      offset: 1,
      limit: 1,
    };

    expect(activeRawPlanKey(query, "id")).toBe(activeRawPlanKey(secondWindow, "id"));

    const loosePlan = makeActiveRawPlan(rows, query, "id");
    const strictPlan = makeActiveRawPlan(rows, query, "id", {
      literalStringFields: new Set(["status"]),
    });

    expect(loosePlan.key).toBe(strictPlan.key);
    expect(loosePlan.snapshot(query).totalRows).toBe(2);
    expect(strictPlan.snapshot(query).totalRows).toBe(1);
  });

  it("preflights active plan index bytes without requiring full index construction", () => {
    const rows: RuntimeRow[] = [
      { id: "a", status: "open", price: 10 },
      { id: "b", status: "closed", price: 20 },
      { id: "c", status: "open", price: 30 },
    ];
    const query: RuntimeRawQuery = {
      fields: { id: true, status: true },
      where: { field: "status", comparator: "equals", value: "open" },
      orderBy: [{ field: "price", direction: "asc" }],
      limit: 10,
    };
    const options = { sortedIndex: "blocks", blockSize: 2 } satisfies QueryExecutionOptions & {
      readonly sortedIndex: "blocks";
      readonly blockSize: number;
    };
    const plan = makeActiveRawPlan(rows, query, "id", options);
    const fullEstimate = estimateActiveRawPlanIndexBytes(rows, query, options);
    const stoppedEstimate = estimateActiveRawPlanIndexBytes(rows, query, options, 1);

    expect(fullEstimate).toBe(plan.estimatedIndexBytes());
    expect(stoppedEstimate).toBeGreaterThan(1);
    expect(stoppedEstimate).toBeLessThan(fullEstimate);
  });
});

function expectActiveViewMatchesMemory(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  mutations: readonly MutationLogEntry[],
  options: QueryExecutionOptions = {},
): void {
  for (const sortedIndex of activeSortedIndexKinds) {
    let memoryRows = [...rows];
    const view = makeActiveRawView(memoryRows, query, "id", { ...options, sortedIndex });
    expect(view.snapshot()).toEqual(executeRawQuery(memoryRows, query, "id", options));
    for (const mutation of mutations) {
      memoryRows = applyMutation(memoryRows, mutation);
      expect(resultFromChange(view.applyMutation(mutation), view)).toEqual(
        executeRawQuery(memoryRows, query, "id", options),
      );
    }
  }
}

function resultFromChange(change: ActiveRawViewChange, view: ReturnType<typeof makeActiveRawView>) {
  switch (change.type) {
    case "noop":
    case "totalRowsOnly":
      return view.snapshot();
    case "changed":
      return change.result;
  }
}

function applyMutation(rows: readonly RuntimeRow[], mutation: MutationLogEntry): RuntimeRow[] {
  switch (mutation.kind) {
    case "insert":
      return [...rows, mutationAfter(mutation)];
    case "update":
      return rows.map((row) => (row.id === mutation.id ? mutationAfter(mutation) : row));
    case "delete":
      return rows.filter((row) => row.id !== mutation.id);
  }
}

function insert(version: bigint, id: string, after: RuntimeRow): MutationLogEntry {
  return {
    version,
    kind: "insert",
    id,
    after,
    changedFields: new Set(Object.keys(after)),
  };
}

function update(
  version: bigint,
  id: string,
  before: RuntimeRow | undefined,
  after: RuntimeRow,
): MutationLogEntry {
  return {
    version,
    kind: "update",
    id,
    before,
    after,
    changedFields: changedFields(before, after),
  };
}

function remove(version: bigint, id: string, before: RuntimeRow | undefined): MutationLogEntry {
  return {
    version,
    kind: "delete",
    id,
    before,
    changedFields: new Set(Object.keys(before ?? {})),
  };
}

function changedFields(before: RuntimeRow | undefined, after: RuntimeRow): ReadonlySet<string> {
  return new Set([...Object.keys(before ?? {}), ...Object.keys(after)]);
}

function mutationAfter(mutation: MutationLogEntry): RuntimeRow {
  if (mutation.after === undefined) {
    throw new Error(`Expected ${mutation.kind} mutation after row`);
  }
  return mutation.after;
}

function makeFuzzRows(count: number): RuntimeRow[] {
  return Array.from({ length: count }, (_, index) => fuzzRow(fuzzId(index), index));
}

function makeFuzzMutations(
  initialRows: readonly RuntimeRow[],
  seed: number,
  count: number,
): readonly MutationLogEntry[] {
  const random = new DeterministicRandom(seed);
  const rowsById = new Map(initialRows.map((row) => [String(row.id), row]));
  const mutations: MutationLogEntry[] = [];
  let nextInsertIndex = initialRows.length;
  for (let index = 0; index < count; index++) {
    const version = BigInt(index + 1);
    const operation = random.integer(10);
    if (operation < 2 || rowsById.size < 20) {
      const row = fuzzRow(fuzzId(nextInsertIndex), nextInsertIndex);
      nextInsertIndex++;
      rowsById.set(String(row.id), row);
      mutations.push(insert(version, String(row.id), row));
      continue;
    }
    const id = randomExistingId(rowsById, random);
    const before = rowsById.get(id);
    if (before === undefined) {
      throw new Error(`Missing fuzz row ${id}`);
    }
    if (operation < 4 && rowsById.size > 20) {
      rowsById.delete(id);
      mutations.push(remove(version, id, before));
      continue;
    }
    const after = mutateFuzzRow(before, random, index);
    rowsById.set(id, after);
    mutations.push(update(version, id, before, after));
  }
  return mutations;
}

function fuzzRow(id: string, index: number): RuntimeRow {
  return {
    id,
    name: index % 9 === 0 ? null : index % 4 === 0 ? `Name-${index % 7}` : `name-${index % 7}`,
    status: index % 5 === 0 ? "OPEN" : index % 2 === 0 ? "open" : "closed",
    venue: index % 11 === 0 ? null : index % 3 === 0 ? "NASDAQ" : "nasdaq",
    price: index % 13 === 0 ? null : (index * 37) % 211,
    score: index % 6 === 0 ? null : index % 17,
    quantity: (index % 5) + 1,
  };
}

function fuzzId(index: number): string {
  const ids = ["A", "a", "B", "b", "C", "c"];
  return ids[index] ?? `row-${index}`;
}

function mutateFuzzRow(row: RuntimeRow, random: DeterministicRandom, index: number): RuntimeRow {
  switch (random.integer(6)) {
    case 0:
      return { ...row, name: random.integer(5) === 0 ? null : `Name-${random.integer(9)}` };
    case 1:
      return { ...row, status: random.integer(2) === 0 ? "open" : "closed" };
    case 2:
      return {
        ...row,
        venue: random.integer(4) === 0 ? null : random.integer(2) === 0 ? "NASDAQ" : "nasdaq",
      };
    case 3:
      return { ...row, price: random.integer(7) === 0 ? null : random.integer(251) };
    case 4:
      return { ...row, score: random.integer(6) === 0 ? null : random.integer(23) };
    default:
      return { ...row, quantity: (index % 7) + 1 };
  }
}

function randomExistingId(
  rowsById: ReadonlyMap<string, RuntimeRow>,
  random: DeterministicRandom,
): string {
  const target = random.integer(rowsById.size);
  let index = 0;
  for (const id of rowsById.keys()) {
    if (index === target) {
      return id;
    }
    index++;
  }
  throw new Error("Expected at least one fuzz row");
}

class DeterministicRandom {
  #state: number;

  constructor(seed: number) {
    this.#state = seed >>> 0;
  }

  integer(maxExclusive: number): number {
    this.#state = (Math.imul(this.#state, 1_664_525) + 1_013_904_223) >>> 0;
    return this.#state % maxExclusive;
  }
}
