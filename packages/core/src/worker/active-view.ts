import * as Effect from "effect/Effect";
import type { RuntimeRawQuery, RuntimeRow, RuntimeRowKey } from "../protocol/index.ts";
import { activeRawPlanKey } from "./active-raw-plan-key.ts";
import {
  estimateActiveSortedIndexBytes,
  makeActiveSortedIndex,
  makeActiveSortedIndexFromSortedIds,
  type ActiveSortedIndex,
  type ActiveSortedIndexKind,
} from "./active-sorted-index.ts";
import {
  compareRowsForOrder,
  matchesFilter,
  normalizeLimit,
  normalizeOffset,
  projectRawRow,
  rawQueryOrderBy,
  rowId,
  rowsEqual,
  type QueryExecutionOptions,
  type QueryExecutionResult,
} from "./query-engine.ts";
import type { MutationLogEntry } from "./mutation-log.ts";

export { activeRawPlanKey, stableStringify } from "./active-raw-plan-key.ts";

export type ActiveRawViewOptions = QueryExecutionOptions & {
  readonly sortedIndex?: ActiveSortedIndexKind | undefined;
  readonly blockSize?: number | undefined;
  readonly buildChunkSize?: number | undefined;
};

export type ActiveRawPlan = {
  readonly key: string;
  readonly applyMutation: (mutation: MutationLogEntry) => void;
  readonly estimatedIndexBytes: () => number;
  readonly snapshot: (query: RuntimeRawQuery) => QueryExecutionResult;
  readonly totalRows: () => number;
  readonly visibleIds: (query: RuntimeRawQuery) => readonly RuntimeRowKey[];
};

export type ActiveRawView = {
  readonly snapshot: () => QueryExecutionResult;
  readonly applyMutation: (mutation: MutationLogEntry) => ActiveRawViewChange;
};

export type ActiveRawViewChange =
  | {
      readonly type: "noop";
    }
  | {
      readonly type: "totalRowsOnly";
      readonly totalRows: number;
    }
  | {
      readonly type: "changed";
      readonly result: QueryExecutionResult;
    };

export function makeActiveRawView(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  idField: string,
  options: ActiveRawViewOptions = {},
): ActiveRawView {
  return new PlanBackedActiveRawView(
    makeActiveRawPlan(rows, query, idField, options),
    query,
    idField,
    true,
  );
}

export function makeActiveRawPlan(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  idField: string,
  options: ActiveRawViewOptions = {},
): ActiveRawPlan {
  return new IncrementalRawPlan(rows, query, idField, options);
}

export function makeActiveRawPlanEffect(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  idField: string,
  options: ActiveRawViewOptions = {},
): Effect.Effect<ActiveRawPlan> {
  return Effect.fn("view-server.active_plan.build.cooperative")(function* () {
    const builder = yield* CooperativeRawPlanBuilder.make(query, idField, options);
    yield* builder.collect(rows);
    yield* builder.sort();
    return builder.plan();
  })();
}

export function estimateActiveRawPlanIndexBytes(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  options: ActiveRawViewOptions = {},
  stopAfterBytes?: number,
): number {
  let matchingRows = 0;
  for (const row of rows) {
    if (matchesFilter(row, query.where, options)) {
      matchingRows++;
      if (
        stopAfterBytes !== undefined &&
        estimateActiveSortedIndexBytes(matchingRows, {
          kind: options.sortedIndex ?? "blocks",
          blockSize: options.blockSize,
        }) > stopAfterBytes
      ) {
        const estimated = estimateActiveSortedIndexBytes(matchingRows, {
          kind: options.sortedIndex ?? "blocks",
          blockSize: options.blockSize,
        });
        return estimated;
      }
    }
  }
  return estimateActiveSortedIndexBytes(matchingRows, {
    kind: options.sortedIndex ?? "blocks",
    blockSize: options.blockSize,
  });
}

export function estimateActiveRawPlanIndexBytesEffect(
  rows: readonly RuntimeRow[],
  query: RuntimeRawQuery,
  options: ActiveRawViewOptions = {},
  stopAfterBytes?: number,
): Effect.Effect<number> {
  return Effect.gen(function* () {
    const chunkSize = normalizedBuildChunkSize(options.buildChunkSize);
    let matchingRows = 0;
    for (let index = 0; index < rows.length; index++) {
      const row = rows[index];
      if (row !== undefined && matchesFilter(row, query.where, options)) {
        matchingRows++;
        if (
          stopAfterBytes !== undefined &&
          estimateActiveSortedIndexBytes(matchingRows, {
            kind: options.sortedIndex ?? "blocks",
            blockSize: options.blockSize,
          }) > stopAfterBytes
        ) {
          return estimateActiveSortedIndexBytes(matchingRows, {
            kind: options.sortedIndex ?? "blocks",
            blockSize: options.blockSize,
          });
        }
      }
      if ((index + 1) % chunkSize === 0 && index + 1 < rows.length) {
        yield* Effect.yieldNow;
      }
    }
    return estimateActiveSortedIndexBytes(matchingRows, {
      kind: options.sortedIndex ?? "blocks",
      blockSize: options.blockSize,
    });
  });
}

class CooperativeRawPlanBuilder {
  private readonly query: RuntimeRawQuery;
  private readonly idField: string;
  private readonly options: ActiveRawViewOptions;
  private readonly orderBy;
  private readonly chunkSize: number;
  private readonly rowsById = new Map<RuntimeRowKey, RuntimeRow>();
  private readonly ids: RuntimeRowKey[] = [];
  private sortedIds: readonly RuntimeRowKey[] = [];

  private constructor(query: RuntimeRawQuery, idField: string, options: ActiveRawViewOptions) {
    this.query = query;
    this.idField = idField;
    this.options = options;
    this.orderBy = rawQueryOrderBy(query, idField);
    this.chunkSize = normalizedBuildChunkSize(options.buildChunkSize);
  }

  static make(
    query: RuntimeRawQuery,
    idField: string,
    options: ActiveRawViewOptions,
  ): Effect.Effect<CooperativeRawPlanBuilder> {
    return Effect.succeed(new CooperativeRawPlanBuilder(query, idField, options));
  }

  collect(rows: readonly RuntimeRow[]): Effect.Effect<void> {
    const query = this.query;
    const options = this.options;
    const idField = this.idField;
    const rowsById = this.rowsById;
    const ids = this.ids;
    const chunkSize = this.chunkSize;
    return Effect.gen(function* () {
      for (let index = 0; index < rows.length; index++) {
        const row = rows[index];
        if (row !== undefined && matchesFilter(row, query.where, options)) {
          const id = rowId(row, idField);
          rowsById.set(id, row);
          ids.push(id);
        }
        if ((index + 1) % chunkSize === 0 && index + 1 < rows.length) {
          yield* Effect.yieldNow;
        }
      }
    });
  }

  sort(): Effect.Effect<void> {
    return cooperativeSortIds(
      this.ids,
      (left, right) => this.compareIds(left, right),
      this.chunkSize,
    ).pipe(
      Effect.tap((sortedIds) =>
        Effect.sync(() => {
          this.sortedIds = sortedIds;
        }),
      ),
    );
  }

  plan(): ActiveRawPlan {
    const rowsById = this.rowsById;
    const orderBy = this.orderBy;
    const sortedIndex = makeActiveSortedIndexFromSortedIds(this.sortedIds, {
      kind: this.options.sortedIndex ?? "blocks",
      compareIds: (left, right) => compareIdsFromRows(rowsById, orderBy, left, right),
      blockSize: this.options.blockSize,
    });
    return new IncrementalRawPlan([], this.query, this.idField, this.options, {
      rowsById,
      sortedIndex,
    });
  }

  private compareIds(left: RuntimeRowKey, right: RuntimeRowKey): number {
    return compareIdsFromRows(this.rowsById, this.orderBy, left, right);
  }
}

export function makeActiveRawViewFromPlan(
  plan: ActiveRawPlan,
  query: RuntimeRawQuery,
  idField: string,
): ActiveRawView {
  return new PlanBackedActiveRawView(plan, query, idField, false);
}

class IncrementalRawPlan implements ActiveRawPlan {
  readonly key: string;
  private readonly orderBy;
  private readonly rowsById: Map<RuntimeRowKey, RuntimeRow>;
  private readonly query: RuntimeRawQuery;
  private readonly idField: string;
  private readonly options: ActiveRawViewOptions;
  private readonly sortedIndex: ActiveSortedIndex;

  constructor(
    rows: readonly RuntimeRow[],
    query: RuntimeRawQuery,
    idField: string,
    options: ActiveRawViewOptions,
    seed?: {
      readonly rowsById: Map<RuntimeRowKey, RuntimeRow>;
      readonly sortedIndex: ActiveSortedIndex;
    },
  ) {
    this.query = query;
    this.idField = idField;
    this.options = options;
    this.orderBy = rawQueryOrderBy(query, idField);
    this.key = activeRawPlanKey(query, idField);
    this.rowsById = seed?.rowsById ?? new Map();
    if (seed === undefined) {
      for (const row of rows) {
        if (matchesFilter(row, query.where, options)) {
          this.rowsById.set(rowId(row, idField), row);
        }
      }
      this.sortedIndex = makeActiveSortedIndex(this.rowsById.keys(), {
        kind: options.sortedIndex ?? "blocks",
        compareIds: (left, right) => this.compareIds(left, right),
        blockSize: options.blockSize,
      });
    } else {
      this.sortedIndex = seed.sortedIndex;
    }
  }

  snapshot(query: RuntimeRawQuery): QueryExecutionResult {
    const visibleIds = this.visibleIds(query);
    return {
      rows: visibleIds.map((id) => this.projectById(id, query)),
      totalRows: this.totalRows(),
    };
  }

  totalRows(): number {
    return this.sortedIndex.size();
  }

  estimatedIndexBytes(): number {
    return this.sortedIndex.estimatedSizeBytes();
  }

  visibleIds(query: RuntimeRawQuery): readonly RuntimeRowKey[] {
    const offset = normalizeOffset(query.offset);
    const limit = normalizeLimit(query.limit);
    return this.sortedIndex.slice(offset, limit);
  }

  applyMutation(mutation: MutationLogEntry): void {
    switch (mutation.kind) {
      case "insert": {
        this.addMutationAfterIfMatching(mutation);
        break;
      }
      case "update": {
        this.updateMutation(mutation);
        break;
      }
      case "delete": {
        this.removeById(mutation.id);
        break;
      }
    }
  }

  private addMutationAfterIfMatching(mutation: MutationLogEntry): void {
    if (mutation.after === undefined) {
      throw new Error(`Active view ${mutation.kind} mutation is missing after row`);
    }
    this.addIfMatching(mutation.after);
  }

  private addIfMatching(row: RuntimeRow): void {
    if (!matchesFilter(row, this.query.where, this.options)) {
      return;
    }
    const id = rowId(row, this.idField);
    this.rowsById.set(id, row);
    this.sortedIndex.insert(id);
  }

  private updateMutation(mutation: MutationLogEntry): void {
    if (mutation.after === undefined) {
      throw new Error("Active view update mutation is missing after row");
    }
    const before = this.rowsById.get(mutation.id);
    const afterMatches = matchesFilter(mutation.after, this.query.where, this.options);
    if (before === undefined) {
      if (afterMatches) {
        this.addIfMatching(mutation.after);
      }
      return;
    }
    if (!afterMatches) {
      this.removeById(mutation.id);
      return;
    }
    if (compareRowsForOrder(before, mutation.after, this.orderBy) === 0) {
      this.rowsById.set(mutation.id, mutation.after);
      return;
    }
    this.removeById(mutation.id);
    this.addIfMatching(mutation.after);
  }

  private removeById(id: RuntimeRowKey): void {
    const row = this.rowsById.get(id);
    if (row === undefined) {
      return;
    }
    this.sortedIndex.remove(id);
    this.rowsById.delete(id);
  }

  private compareIds(left: RuntimeRowKey, right: RuntimeRowKey): number {
    return compareRowsForOrder(this.rowById(left), this.rowById(right), this.orderBy);
  }

  private projectById(id: RuntimeRowKey, query: RuntimeRawQuery): RuntimeRow {
    return projectRawRow(this.rowById(id), query.fields, this.idField);
  }

  private rowById(id: RuntimeRowKey | undefined): RuntimeRow {
    if (id === undefined) {
      throw new Error("Active view row id is missing from sorted index");
    }
    const row = this.rowsById.get(id);
    if (row === undefined) {
      throw new Error(`Active view row ${String(id)} is missing from row index`);
    }
    return row;
  }
}

class PlanBackedActiveRawView implements ActiveRawView {
  private readonly plan: ActiveRawPlan;
  private readonly query: RuntimeRawQuery;
  private readonly idField: string;
  private readonly applyPlanMutations: boolean;
  private visibleIds: readonly RuntimeRowKey[];
  private totalRows: number;

  constructor(
    plan: ActiveRawPlan,
    query: RuntimeRawQuery,
    idField: string,
    applyPlanMutations: boolean,
  ) {
    this.plan = plan;
    this.query = query;
    this.idField = idField;
    this.applyPlanMutations = applyPlanMutations;
    this.visibleIds = plan.visibleIds(query);
    this.totalRows = plan.totalRows();
  }

  snapshot(): QueryExecutionResult {
    return this.plan.snapshot(this.query);
  }

  applyMutation(mutation: MutationLogEntry): ActiveRawViewChange {
    const previousVisibleIds = this.visibleIds;
    const previousTotalRows = this.totalRows;
    if (this.applyPlanMutations) {
      this.plan.applyMutation(mutation);
    }
    const nextVisibleIds = this.plan.visibleIds(this.query);
    const nextTotalRows = this.plan.totalRows();
    const visibleChanged =
      !sameIds(previousVisibleIds, nextVisibleIds) ||
      this.visibleProjectionChanged(mutation, nextVisibleIds);
    this.visibleIds = nextVisibleIds;
    this.totalRows = nextTotalRows;
    if (visibleChanged) {
      return {
        type: "changed",
        result: this.snapshot(),
      };
    }
    if (previousTotalRows !== nextTotalRows) {
      return {
        type: "totalRowsOnly",
        totalRows: nextTotalRows,
      };
    }
    return {
      type: "noop",
    };
  }

  private visibleProjectionChanged(
    mutation: MutationLogEntry,
    visibleIds: readonly RuntimeRowKey[],
  ): boolean {
    if (
      mutation.kind !== "update" ||
      mutation.before === undefined ||
      mutation.after === undefined ||
      !this.projectedFieldsMayHaveChanged(mutation)
    ) {
      return false;
    }
    const beforeId = rowId(mutation.before, this.idField);
    const afterId = rowId(mutation.after, this.idField);
    const rowIsVisible = visibleIds.some(
      (id) => Object.is(id, beforeId) || Object.is(id, afterId) || Object.is(id, mutation.id),
    );
    if (!rowIsVisible) {
      return false;
    }
    return !rowsEqual(
      projectRawRow(mutation.before, this.query.fields, this.idField),
      projectRawRow(mutation.after, this.query.fields, this.idField),
    );
  }

  private projectedFieldsMayHaveChanged(mutation: MutationLogEntry): boolean {
    if (mutation.changedFields.has(this.idField)) {
      return true;
    }
    for (const [field, enabled] of Object.entries(this.query.fields)) {
      if (enabled && mutation.changedFields.has(field)) {
        return true;
      }
    }
    return false;
  }
}

function cooperativeSortIds(
  ids: readonly RuntimeRowKey[],
  compareIds: (left: RuntimeRowKey, right: RuntimeRowKey) => number,
  chunkSize: number,
): Effect.Effect<readonly RuntimeRowKey[]> {
  return Effect.gen(function* () {
    if (ids.length <= 1) {
      return ids;
    }
    let chunks: RuntimeRowKey[][] = [];
    for (let start = 0; start < ids.length; start += chunkSize) {
      chunks.push(ids.slice(start, start + chunkSize).sort(compareIds));
      if (start + chunkSize < ids.length) {
        yield* Effect.yieldNow;
      }
    }
    while (chunks.length > 1) {
      const nextChunks: RuntimeRowKey[][] = [];
      for (let index = 0; index < chunks.length; index += 2) {
        const left = chunks[index];
        const right = chunks[index + 1];
        if (left === undefined) {
          continue;
        }
        if (right === undefined) {
          nextChunks.push(left);
        } else {
          nextChunks.push(yield* mergeSortedIds(left, right, compareIds, chunkSize));
        }
      }
      chunks = nextChunks;
      if (chunks.length > 1) {
        yield* Effect.yieldNow;
      }
    }
    return chunks[0] ?? [];
  });
}

function mergeSortedIds(
  left: readonly RuntimeRowKey[],
  right: readonly RuntimeRowKey[],
  compareIds: (left: RuntimeRowKey, right: RuntimeRowKey) => number,
  chunkSize: number,
): Effect.Effect<RuntimeRowKey[]> {
  return Effect.gen(function* () {
    const merged: RuntimeRowKey[] = [];
    let leftIndex = 0;
    let rightIndex = 0;
    while (leftIndex < left.length || rightIndex < right.length) {
      const leftId = left[leftIndex];
      const rightId = right[rightIndex];
      if (leftId === undefined) {
        yield* appendRemainingIds(merged, right, rightIndex, chunkSize);
        break;
      }
      if (rightId === undefined) {
        yield* appendRemainingIds(merged, left, leftIndex, chunkSize);
        break;
      }
      if (compareIds(leftId, rightId) <= 0) {
        merged.push(leftId);
        leftIndex++;
      } else {
        merged.push(rightId);
        rightIndex++;
      }
      if (
        merged.length % chunkSize === 0 &&
        (leftIndex < left.length || rightIndex < right.length)
      ) {
        yield* Effect.yieldNow;
      }
    }
    return merged;
  });
}

function appendRemainingIds(
  target: RuntimeRowKey[],
  source: readonly RuntimeRowKey[],
  start: number,
  chunkSize: number,
): Effect.Effect<void> {
  return Effect.gen(function* () {
    for (let index = start; index < source.length; index += chunkSize) {
      const end = Math.min(index + chunkSize, source.length);
      for (let cursor = index; cursor < end; cursor++) {
        const id = source[cursor];
        if (id !== undefined) {
          target.push(id);
        }
      }
      if (end < source.length) {
        yield* Effect.yieldNow;
      }
    }
  });
}

function compareIdsFromRows(
  rowsById: ReadonlyMap<RuntimeRowKey, RuntimeRow>,
  orderBy: ReturnType<typeof rawQueryOrderBy>,
  left: RuntimeRowKey,
  right: RuntimeRowKey,
): number {
  return compareRowsForOrder(rowFromMap(rowsById, left), rowFromMap(rowsById, right), orderBy);
}

function rowFromMap(
  rowsById: ReadonlyMap<RuntimeRowKey, RuntimeRow>,
  id: RuntimeRowKey | undefined,
): RuntimeRow {
  if (id === undefined) {
    throw new Error("Active view row id is missing from sorted index");
  }
  const row = rowsById.get(id);
  if (row === undefined) {
    throw new Error(`Active view row ${String(id)} is missing from row index`);
  }
  return row;
}

function normalizedBuildChunkSize(chunkSize: number | undefined): number {
  return Math.max(1, Math.trunc(chunkSize ?? 8_192));
}

function sameIds(left: readonly RuntimeRowKey[], right: readonly RuntimeRowKey[]): boolean {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((id, index) => Object.is(id, right[index]));
}
