import { describe, expect, it } from "@effect/vitest";
import { MutationStore, replayMutations } from "../src/worker/mutation-store.ts";

describe("MutationStore", () => {
  it("owns publish, update, delete, version, and swap-delete index maintenance", () => {
    const store = new MutationStore({ idField: "id", mutationLogSize: 10 });
    store.loadInitialRows([
      { id: "a", value: 1 },
      { id: "b", value: 2 },
      { id: "c", value: 3 },
    ]);

    const update = store.publish({ id: "b", value: 20 }, "b");
    const insert = store.publish({ id: "d", value: 4 }, "d");
    const deleted = store.deleteById("a");

    expect(update.toVersion).toBe(1n);
    expect(update.entry.kind).toBe("update");
    expect(insert.toVersion).toBe(2n);
    expect(insert.entry.kind).toBe("insert");
    expect(deleted?.toVersion).toBe(3n);
    expect(store.version()).toBe(3n);
    expect(store.rowById("a")).toBeUndefined();
    expect(store.rowById("c")).toEqual({ id: "c", value: 3 });
    expect(store.rowById("d")).toEqual({ id: "d", value: 4 });
  });

  it("replays covered mutation ranges onto backend rows without rebuilding indexes per mutation", () => {
    const store = new MutationStore({ idField: "id", mutationLogSize: 10 });
    const baseRows = [
      { id: "a", value: 1 },
      { id: "b", value: 2 },
    ];
    store.loadInitialRows(baseRows);
    store.publish({ id: "a", value: 10 }, "a");
    store.publish({ id: "c", value: 3 }, "c");
    store.deleteById("b");

    expect(store.canReplay(0n, 3n)).toBe(true);
    expect(store.replayRowsFrom(baseRows, 0n, 3n)).toEqual(
      expect.arrayContaining([
        { id: "a", value: 10 },
        { id: "c", value: 3 },
      ]),
    );
  });

  it("detects mutation log gaps", () => {
    const store = new MutationStore({ idField: "id", mutationLogSize: 2 });
    store.loadInitialRows([{ id: "a", value: 1 }]);
    store.publish({ id: "a", value: 2 }, "a");
    store.publish({ id: "a", value: 3 }, "a");
    store.publish({ id: "a", value: 4 }, "a");

    expect(store.canReplay(0n, 3n)).toBe(false);
    expect(store.canReplay(1n, 3n)).toBe(true);
  });

  it("matches full final state through deterministic random publish/update/delete replay", () => {
    const store = new MutationStore({ idField: "id", mutationLogSize: 200 });
    store.loadInitialRows([]);
    const expected = new Map<string, { readonly id: string; readonly value: number }>();

    for (let step = 0; step < 80; step++) {
      const id = `row-${(step * 17) % 19}`;
      if (step % 7 === 0) {
        store.deleteById(id);
        expected.delete(id);
      } else {
        const row = { id, value: step * 11 };
        store.publish(row, id);
        expected.set(id, row);
      }
    }

    const replayed = replayMutations([], store.entriesExclusive(0n, store.version()), "id");
    expect(sortById(replayed)).toEqual(sortById([...expected.values()]));
  });
});

function sortById(rows: readonly Record<string, unknown>[]): readonly Record<string, unknown>[] {
  return rows.toSorted((left, right) => String(left.id).localeCompare(String(right.id)));
}
