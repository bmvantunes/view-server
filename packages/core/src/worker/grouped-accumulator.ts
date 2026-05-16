import type {
  RuntimeAggregateMap,
  RuntimeGroupedQuery,
  RuntimeRow,
  RuntimeRowKey,
} from "../protocol/index.ts";
import { stableStringify } from "../protocol/index.ts";
import {
  isIncrementalAggregateSupported,
  makeAggregateState,
  type AggregateState,
} from "./aggregate-functions.ts";
import type { MutationLogEntry } from "./mutation-log.ts";

export type GroupedAccumulator = {
  readonly add: (row: RuntimeRow, id: RuntimeRowKey) => void;
  readonly remove: (row: RuntimeRow, id: RuntimeRowKey) => void;
  readonly applyMutation: (mutation: MutationLogEntry) => void;
  readonly groupedRows: () => readonly RuntimeRow[];
};

type GroupState = {
  readonly groupValues: RuntimeRow;
  readonly rowIds: Set<RuntimeRowKey>;
  readonly states: readonly AggregateState[];
};

type AggregatePlan = {
  readonly aliases: readonly string[];
  readonly aggregates: RuntimeAggregateMap;
};

export function buildGroupedRows(
  rows: readonly RuntimeRow[],
  groupBy: readonly string[],
  aggregates: RuntimeAggregateMap,
): readonly RuntimeRow[] {
  const accumulator = makeStreamingGroupedAccumulator(groupBy, aggregates);
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    if (row !== undefined) {
      accumulator.add(row, index);
    }
  }
  return accumulator.groupedRows();
}

export function makeIncrementalGroupedAccumulator(args: {
  readonly rows: readonly RuntimeRow[];
  readonly query: RuntimeGroupedQuery;
  readonly idOf: (row: RuntimeRow) => RuntimeRowKey;
}): GroupedAccumulator | undefined {
  if (!isIncrementalGroupedAccumulatorSupported(args.query)) {
    return undefined;
  }
  const accumulator = makeStreamingGroupedAccumulator(args.query.groupBy, args.query.aggregates);
  for (const row of args.rows) {
    accumulator.add(row, args.idOf(row));
  }
  return accumulator;
}

export function isIncrementalGroupedAccumulatorSupported(query: RuntimeGroupedQuery): boolean {
  return Object.values(query.aggregates).every(isIncrementalAggregateSupported);
}

export function makeStreamingGroupedAccumulator(
  groupBy: readonly string[],
  aggregates: RuntimeAggregateMap,
): GroupedAccumulator {
  const groups = new Map<string, GroupState>();
  const rowGroupKeys = new Map<RuntimeRowKey, string>();
  const plan = makeAggregatePlan(aggregates);

  function add(row: RuntimeRow, id: RuntimeRowKey): void {
    const key = groupedAccumulatorKey(row, groupBy);
    let group = groups.get(key);
    if (group === undefined) {
      group = {
        groupValues: groupedValues(row, groupBy),
        rowIds: new Set(),
        states: makeAggregateStates(plan),
      };
      groups.set(key, group);
    }
    group.rowIds.add(id);
    rowGroupKeys.set(id, key);
    for (let index = 0; index < group.states.length; index++) {
      group.states[index]?.add(row);
    }
  }

  function remove(row: RuntimeRow, id: RuntimeRowKey): void {
    const key = rowGroupKeys.get(id) ?? groupedAccumulatorKey(row, groupBy);
    const group = groups.get(key);
    if (group === undefined) {
      return;
    }
    group.rowIds.delete(id);
    rowGroupKeys.delete(id);
    for (let index = 0; index < group.states.length; index++) {
      group.states[index]?.remove(row);
    }
    if (group.rowIds.size === 0) {
      groups.delete(key);
    }
  }

  return {
    add,
    remove,
    applyMutation: (mutation) => {
      switch (mutation.kind) {
        case "insert":
          if (mutation.after !== undefined) {
            add(mutation.after, mutation.id);
          }
          break;
        case "update":
          if (mutation.before !== undefined) {
            remove(mutation.before, mutation.id);
          }
          if (mutation.after !== undefined) {
            add(mutation.after, mutation.id);
          }
          break;
        case "delete":
          if (mutation.before !== undefined) {
            remove(mutation.before, mutation.id);
          }
          break;
      }
    },
    groupedRows: () => {
      const result: RuntimeRow[] = [];
      for (const group of groups.values()) {
        const row: RuntimeRow = { ...group.groupValues };
        for (let index = 0; index < plan.aliases.length; index++) {
          const alias = plan.aliases[index];
          const state = group.states[index];
          if (alias !== undefined && state !== undefined) {
            row[alias] = state.value();
          }
        }
        result.push(row);
      }
      return result;
    },
  };
}

function makeAggregatePlan(aggregates: RuntimeAggregateMap): AggregatePlan {
  return {
    aliases: Object.keys(aggregates),
    aggregates,
  };
}

function makeAggregateStates(plan: AggregatePlan): readonly AggregateState[] {
  const states: AggregateState[] = [];
  for (const alias of plan.aliases) {
    const aggregate = plan.aggregates[alias];
    if (aggregate !== undefined) {
      states.push(makeAggregateState(aggregate));
    }
  }
  return states;
}

function groupedValues(row: RuntimeRow, groupBy: readonly string[]): RuntimeRow {
  const values: RuntimeRow = {};
  for (const field of groupBy) {
    values[field] = row[field];
  }
  return values;
}

function groupedAccumulatorKey(row: RuntimeRow, groupBy: readonly string[]): string {
  return stableStringify(groupBy.map((field) => row[field]));
}
