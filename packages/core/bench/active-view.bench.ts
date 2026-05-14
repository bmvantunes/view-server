import * as Effect from "effect/Effect";
import { performance } from "node:perf_hooks";
import {
  writeBenchmarkArtifact,
  type BenchmarkMetric,
  type BenchmarkResult,
} from "./benchmark-artifacts.ts";
import type { RuntimeRawQuery, RuntimeRow } from "../src/protocol/index.ts";
import type { ActiveSortedIndexKind } from "../src/worker/active-sorted-index.ts";
import type { ActiveRawViewChange } from "../src/worker/active-view.ts";
import {
  activeRawPlanKey,
  estimateActiveRawPlanIndexBytes,
  makeActiveRawPlan,
  makeActiveRawView,
  makeActiveRawViewFromPlan,
} from "../src/worker/active-view.ts";
import type { MutationLogEntry } from "../src/worker/mutation-log.ts";
import { collectDependencyFields, executeRawQuery } from "../src/worker/query-engine.ts";

// Bench note: ActiveRawView defaults to the block list index and keeps the array
// index available as a baseline for storage comparisons.
// Set VS_ACTIVE_VIEW_BASELINE=0 for large active-path timing when full recompute
// is too slow to finish interactively. Set VS_ACTIVE_VIEW_VALIDATE=0 only for
// pure timing after correctness has already been checked. Set
// VS_ACTIVE_VIEW_SCENARIOS=hot-key-updates,sorted-row-movement to target a subset.
// Set VS_ACTIVE_VIEW_INDEXES=array,blocks and VS_ACTIVE_VIEW_BLOCK_SIZE=1024
// to compare ordered storage implementations. Set VS_ACTIVE_VIEW_MUTATIONS=0
// to measure active-view build time separately from mutation updates.
// Set VS_ACTIVE_VIEW_SHARING=per-subscription,shared and
// VS_ACTIVE_VIEW_QUERY_SHAPE=same-plan|ten-plans|unique-plans|mixed to compare shared plans.
// Set VS_ACTIVE_VIEW_MAX_ACTIVE_PLANS or VS_ACTIVE_VIEW_MAX_PLAN_ESTIMATED_BYTES
// to simulate active-plan guardrail fallback in shared mode.
// Set VS_ACTIVE_VIEW_MEMORY=1 and run node with --expose-gc to measure retained
// heap after active-view build while plans/views are still strongly referenced.

type ActiveViewSharing = "per-subscription" | "shared";

type QueryShape = "mixed" | "same-plan" | "ten-plans" | "unique-plans";

type BenchConfig = {
  readonly rows: number;
  readonly subscriptions: number;
  readonly mutations: number;
  readonly pageSizes: readonly number[];
  readonly scenarios: readonly string[];
  readonly indexes: readonly ActiveSortedIndexKind[];
  readonly sharing: readonly ActiveViewSharing[];
  readonly queryShape: QueryShape;
  readonly blockSize: number;
  readonly maxActivePlans?: number | undefined;
  readonly maxActivePlanEstimatedBytes?: number | undefined;
  readonly memory: boolean;
  readonly baseline: boolean;
  readonly validate: boolean;
};

type BenchScenario = {
  readonly name: string;
  readonly mutations: readonly MutationLogEntry[];
};

type TimedResult = {
  readonly ms: number;
  readonly checksum: number | undefined;
};

type ActiveTimedResult = {
  readonly buildMs: number;
  readonly update: TimedResult;
  readonly validationMs: number;
  readonly planCount: number;
  readonly fallbackCount: number;
  readonly estimatedIndexBytes?: number | undefined;
  readonly fallbackBuildMs: number;
  readonly fallbackEstimateMs: number;
  readonly memory: MemoryMeasurement | undefined;
};

type MemorySnapshot = {
  readonly heapUsed: number;
  readonly heapTotal: number;
  readonly rss: number;
  readonly external: number;
  readonly arrayBuffers: number;
};

type MemoryMeasurement = {
  readonly before: MemorySnapshot;
  readonly after: MemorySnapshot;
  readonly heapUsedDelta: number;
  readonly rssDelta: number;
  readonly gcAvailable: boolean;
};

type SharedActiveSubscription =
  | {
      readonly type: "active";
      readonly view: ReturnType<typeof makeActiveRawViewFromPlan>;
    }
  | {
      readonly type: "fallback";
      readonly query: RuntimeRawQuery;
    };

const config: BenchConfig = {
  rows: positiveInteger("VS_ACTIVE_VIEW_ROWS", 250_000),
  subscriptions: positiveInteger("VS_ACTIVE_VIEW_SUBSCRIPTIONS", 250),
  mutations: nonNegativeInteger("VS_ACTIVE_VIEW_MUTATIONS", 500),
  pageSizes: pageSizes(),
  scenarios: scenarioNames(),
  indexes: sortedIndexKinds(),
  sharing: sharingKinds(),
  queryShape: queryShape(),
  blockSize: positiveInteger("VS_ACTIVE_VIEW_BLOCK_SIZE", 1024),
  maxActivePlans: optionalPositiveInteger("VS_ACTIVE_VIEW_MAX_ACTIVE_PLANS"),
  maxActivePlanEstimatedBytes: optionalPositiveInteger("VS_ACTIVE_VIEW_MAX_PLAN_ESTIMATED_BYTES"),
  memory: envFlag("VS_ACTIVE_VIEW_MEMORY", false),
  baseline: envFlag("VS_ACTIVE_VIEW_BASELINE", true),
  validate: envFlag("VS_ACTIVE_VIEW_VALIDATE", true),
};

void Effect.runPromise(
  Effect.gen(function* () {
    yield* Effect.logInfo(
      `active-view benchmark rows=${config.rows} subscriptions=${config.subscriptions} mutations=${config.mutations} pageSizes=${config.pageSizes.join(",")} scenarios=${config.scenarios.join(",")} indexes=${config.indexes.join(",")} sharing=${config.sharing.join(",")} queryShape=${config.queryShape} blockSize=${config.blockSize} maxActivePlans=${formatNumber(config.maxActivePlans)} maxActivePlanEstimatedBytes=${formatNumber(config.maxActivePlanEstimatedBytes)} memory=${config.memory} gc=${globalThis.gc === undefined ? "off" : "on"} baseline=${config.baseline} validate=${config.validate}`,
    );
    const baseRows = makeRows(config.rows);
    const scenarios = makeScenarios(baseRows, config.mutations).filter((scenario) =>
      config.scenarios.includes(scenario.name),
    );
    const benchmarkResults: BenchmarkResult[] = [];

    for (const pageSize of config.pageSizes) {
      const queries = makeQueries(config.subscriptions, pageSize, config.queryShape);
      for (const sortedIndex of config.indexes) {
        for (const sharing of config.sharing) {
          for (const scenario of scenarios) {
            const recompute = config.baseline
              ? runFullRecompute(baseRows, queries, scenario.mutations)
              : undefined;
            const active = runActiveViews(
              baseRows,
              queries,
              scenario.mutations,
              sortedIndex,
              sharing,
              config.blockSize,
              config.validate,
            );
            const speedup =
              recompute === undefined
                ? "n/a"
                : active.update.ms === 0
                  ? "inf"
                  : (recompute.ms / active.update.ms).toFixed(2);
            benchmarkResults.push(
              activeViewBenchmarkResult({
                scenario,
                pageSize,
                sortedIndex,
                sharing,
                recompute,
                active,
              }),
            );
            yield* Effect.logInfo(
              [
                `scenario=${scenario.name}`,
                `index=${sortedIndex}`,
                `sharing=${sharing}`,
                `queryShape=${config.queryShape}`,
                `pageSize=${pageSize}`,
                `recomputeMs=${formatMs(recompute?.ms)}`,
                `activeBuildMs=${active.buildMs.toFixed(2)}`,
                `activeUpdateMs=${active.update.ms.toFixed(2)}`,
                `activeValidationMs=${active.validationMs.toFixed(2)}`,
                `activePlanCount=${active.planCount}`,
                `activeFallbackCount=${active.fallbackCount}`,
                `activeIndexBytes=${formatBytes(active.estimatedIndexBytes)}`,
                `activeHeapDelta=${formatBytes(active.memory?.heapUsedDelta)}`,
                `activeRssDelta=${formatBytes(active.memory?.rssDelta)}`,
                `activeHeapAfter=${formatBytes(active.memory?.after.heapUsed)}`,
                `activeRssAfter=${formatBytes(active.memory?.after.rss)}`,
                `activeFallbackBuildMs=${active.fallbackBuildMs.toFixed(2)}`,
                `activeFallbackEstimateMs=${active.fallbackEstimateMs.toFixed(2)}`,
                `speedup=${speedup}`,
                `checksum=${formatChecksum(recompute?.checksum)}:${formatChecksum(active.update.checksum)}`,
              ].join(" "),
            );
          }
        }
      }
    }
    const artifact = yield* writeBenchmarkArtifact(
      "active-view",
      {
        rows: config.rows,
        subscriptions: config.subscriptions,
        mutations: config.mutations,
        pageSizes: config.pageSizes.join(","),
        scenarios: config.scenarios.join(","),
        indexes: config.indexes.join(","),
        sharing: config.sharing.join(","),
        queryShape: config.queryShape,
        blockSize: config.blockSize,
        maxActivePlans: config.maxActivePlans ?? null,
        maxActivePlanEstimatedBytes: config.maxActivePlanEstimatedBytes ?? null,
        memory: config.memory,
        baseline: config.baseline,
        validate: config.validate,
      },
      benchmarkResults,
    );
    yield* Effect.logInfo(
      `active-view benchmark artifact=${artifact.artifactPath} baselineCompared=${artifact.compared} results=${benchmarkResults.length}`,
    );
  }),
);

function activeViewBenchmarkResult(args: {
  readonly scenario: BenchScenario;
  readonly pageSize: number;
  readonly sortedIndex: ActiveSortedIndexKind;
  readonly sharing: ActiveViewSharing;
  readonly recompute: TimedResult | undefined;
  readonly active: ActiveTimedResult;
}): BenchmarkResult {
  const metrics: BenchmarkMetric[] = [
    { name: "activeBuildMs", value: args.active.buildMs, unit: "ms" },
    { name: "activeUpdateMs", value: args.active.update.ms, unit: "ms" },
    { name: "activeValidationMs", value: args.active.validationMs, unit: "ms" },
    { name: "activePlanCount", value: args.active.planCount, unit: "count" },
    { name: "activeFallbackCount", value: args.active.fallbackCount, unit: "count" },
    { name: "activeFallbackBuildMs", value: args.active.fallbackBuildMs, unit: "ms" },
    { name: "activeFallbackEstimateMs", value: args.active.fallbackEstimateMs, unit: "ms" },
  ];
  if (args.recompute !== undefined) {
    metrics.push({ name: "recomputeMs", value: args.recompute.ms, unit: "ms" });
  }
  if (args.active.estimatedIndexBytes !== undefined) {
    metrics.push({
      name: "activeIndexBytes",
      value: args.active.estimatedIndexBytes,
      unit: "bytes",
    });
  }
  if (args.active.memory !== undefined) {
    metrics.push(
      { name: "activeHeapDelta", value: args.active.memory.heapUsedDelta, unit: "bytes" },
      { name: "activeRssDelta", value: args.active.memory.rssDelta, unit: "bytes" },
      { name: "activeHeapAfter", value: args.active.memory.after.heapUsed, unit: "bytes" },
      { name: "activeRssAfter", value: args.active.memory.after.rss, unit: "bytes" },
    );
  }
  return {
    case: {
      scenario: args.scenario.name,
      index: args.sortedIndex,
      sharing: args.sharing,
      queryShape: config.queryShape,
      pageSize: args.pageSize,
    },
    metrics,
  };
}

function runFullRecompute(
  baseRows: readonly RuntimeRow[],
  queries: readonly RuntimeRawQuery[],
  mutations: readonly MutationLogEntry[],
): TimedResult {
  const rows = baseRows.map((row) => ({ ...row }));
  const indexes = indexRows(rows);
  const dependencyFields = queries.map((query) => collectDependencyFields(query, "id"));
  let checksum = 0;
  const started = performance.now();
  for (const mutation of mutations) {
    applyMutationToRows(rows, indexes, mutation);
    for (let index = 0; index < queries.length; index++) {
      const query = queries[index];
      const dependencies = dependencyFields[index];
      if (
        query === undefined ||
        dependencies === undefined ||
        canSkipUpdate(mutation, dependencies)
      ) {
        continue;
      }
      const result = executeRawQuery(rows, query, "id");
      checksum = mixChecksum(checksum, resultChecksum(result.rows, result.totalRows));
    }
  }
  return {
    ms: performance.now() - started,
    checksum,
  };
}

function runActiveViews(
  baseRows: readonly RuntimeRow[],
  queries: readonly RuntimeRawQuery[],
  mutations: readonly MutationLogEntry[],
  sortedIndex: ActiveSortedIndexKind,
  sharing: ActiveViewSharing,
  blockSize: number,
  validate: boolean,
): ActiveTimedResult {
  return sharing === "shared"
    ? runSharedActiveViews(
        baseRows,
        queries,
        mutations,
        sortedIndex,
        blockSize,
        validate,
        config.memory,
        config.maxActivePlans,
        config.maxActivePlanEstimatedBytes,
      )
    : runPerSubscriptionActiveViews(
        baseRows,
        queries,
        mutations,
        sortedIndex,
        blockSize,
        validate,
        config.memory,
      );
}

function runPerSubscriptionActiveViews(
  baseRows: readonly RuntimeRow[],
  queries: readonly RuntimeRawQuery[],
  mutations: readonly MutationLogEntry[],
  sortedIndex: ActiveSortedIndexKind,
  blockSize: number,
  validate: boolean,
  measureMemory: boolean,
): ActiveTimedResult {
  const memoryBefore = memoryBeforeBuild(measureMemory);
  const buildStarted = performance.now();
  const views = queries.map((query) =>
    makeActiveRawView(baseRows, query, "id", { sortedIndex, blockSize }),
  );
  const dependencyFields = queries.map((query) => collectDependencyFields(query, "id"));
  const buildMs = performance.now() - buildStarted;
  const memory = memoryAfterBuild(memoryBefore);
  const updateStarted = performance.now();
  for (const mutation of mutations) {
    for (let index = 0; index < views.length; index++) {
      const view = views[index];
      const dependencies = dependencyFields[index];
      if (
        view === undefined ||
        dependencies === undefined ||
        canSkipUpdate(mutation, dependencies)
      ) {
        continue;
      }
      view.applyMutation(mutation);
    }
  }
  const updateMs = performance.now() - updateStarted;
  const validation = validate
    ? timeResult(() =>
        validateActiveViews(
          baseRows,
          queries,
          mutations,
          sortedIndex,
          "per-subscription",
          blockSize,
        ),
      )
    : {
        ms: 0,
        checksum: undefined,
      };
  return {
    buildMs,
    update: {
      ms: updateMs,
      checksum: validation.checksum,
    },
    validationMs: validation.ms,
    planCount: views.length,
    fallbackCount: 0,
    estimatedIndexBytes: undefined,
    fallbackBuildMs: 0,
    fallbackEstimateMs: 0,
    memory,
  };
}

function runSharedActiveViews(
  baseRows: readonly RuntimeRow[],
  queries: readonly RuntimeRawQuery[],
  mutations: readonly MutationLogEntry[],
  sortedIndex: ActiveSortedIndexKind,
  blockSize: number,
  validate: boolean,
  measureMemory: boolean,
  maxActivePlans: number | undefined,
  maxActivePlanEstimatedBytes: number | undefined,
): ActiveTimedResult {
  const memoryBefore = memoryBeforeBuild(measureMemory);
  const buildStarted = performance.now();
  const plans = new Map<string, ReturnType<typeof makeActiveRawPlan>>();
  let estimatedIndexBytes = 0;
  let fallbackCount = 0;
  let fallbackBuildMs = 0;
  let fallbackEstimateMs = 0;
  const subscriptions: SharedActiveSubscription[] = queries.map((query) => {
    const key = activeRawPlanKey(query, "id");
    const existing = plans.get(key);
    if (existing !== undefined) {
      return {
        type: "active",
        view: makeActiveRawViewFromPlan(existing, query, "id"),
      };
    }
    if (maxActivePlans !== undefined && plans.size >= maxActivePlans) {
      fallbackCount++;
      return {
        type: "fallback",
        query,
      };
    }
    const remainingBytes =
      maxActivePlanEstimatedBytes === undefined
        ? undefined
        : maxActivePlanEstimatedBytes - estimatedIndexBytes;
    if (remainingBytes !== undefined) {
      const estimateStarted = performance.now();
      const estimatedBytes = estimateActiveRawPlanIndexBytes(
        baseRows,
        query,
        { sortedIndex, blockSize },
        remainingBytes,
      );
      if (estimatedBytes > remainingBytes) {
        fallbackCount++;
        fallbackEstimateMs += performance.now() - estimateStarted;
        return {
          type: "fallback",
          query,
        };
      }
    }
    const planStarted = performance.now();
    const plan = makeActiveRawPlan(baseRows, query, "id", { sortedIndex, blockSize });
    const planBuildMs = performance.now() - planStarted;
    const planBytes = plan.estimatedIndexBytes();
    if (
      maxActivePlanEstimatedBytes !== undefined &&
      estimatedIndexBytes + planBytes > maxActivePlanEstimatedBytes
    ) {
      fallbackCount++;
      fallbackBuildMs += planBuildMs;
      return {
        type: "fallback",
        query,
      };
    }
    plans.set(key, plan);
    estimatedIndexBytes += planBytes;
    return {
      type: "active",
      view: makeActiveRawViewFromPlan(plan, query, "id"),
    };
  });
  const dependencyFields = queries.map((query) => collectDependencyFields(query, "id"));
  const buildMs = performance.now() - buildStarted;
  const memory = memoryAfterBuild(memoryBefore);
  const fallbackRows = fallbackCount > 0 ? baseRows.map((row) => ({ ...row })) : undefined;
  const fallbackIndexes = fallbackRows === undefined ? undefined : indexRows(fallbackRows);
  const updateStarted = performance.now();
  for (const mutation of mutations) {
    for (const plan of plans.values()) {
      plan.applyMutation(mutation);
    }
    if (fallbackRows !== undefined && fallbackIndexes !== undefined) {
      applyMutationToRows(fallbackRows, fallbackIndexes, mutation);
    }
    for (let index = 0; index < subscriptions.length; index++) {
      const subscription = subscriptions[index];
      const dependencies = dependencyFields[index];
      if (
        subscription === undefined ||
        dependencies === undefined ||
        canSkipUpdate(mutation, dependencies)
      ) {
        continue;
      }
      if (subscription.type === "active") {
        subscription.view.applyMutation(mutation);
      } else {
        if (fallbackRows === undefined) {
          throw new Error("Fallback rows are missing for active-view benchmark");
        }
        executeRawQuery(fallbackRows, subscription.query, "id");
      }
    }
  }
  const updateMs = performance.now() - updateStarted;
  const validation = validate
    ? timeResult(() =>
        validateActiveViews(baseRows, queries, mutations, sortedIndex, "shared", blockSize),
      )
    : {
        ms: 0,
        checksum: undefined,
      };
  return {
    buildMs,
    update: {
      ms: updateMs,
      checksum: validation.checksum,
    },
    validationMs: validation.ms,
    planCount: plans.size,
    fallbackCount,
    estimatedIndexBytes,
    fallbackBuildMs,
    fallbackEstimateMs,
    memory,
  };
}

function validateActiveViews(
  baseRows: readonly RuntimeRow[],
  queries: readonly RuntimeRawQuery[],
  mutations: readonly MutationLogEntry[],
  sortedIndex: ActiveSortedIndexKind,
  sharing: ActiveViewSharing,
  blockSize: number,
): number {
  const plans = new Map<string, ReturnType<typeof makeActiveRawPlan>>();
  const views = queries.map((query) => {
    if (sharing === "per-subscription") {
      return makeActiveRawView(baseRows, query, "id", { sortedIndex, blockSize });
    }
    const key = activeRawPlanKey(query, "id");
    const existing = plans.get(key);
    if (existing !== undefined) {
      return makeActiveRawViewFromPlan(existing, query, "id");
    }
    const plan = makeActiveRawPlan(baseRows, query, "id", { sortedIndex, blockSize });
    plans.set(key, plan);
    return makeActiveRawViewFromPlan(plan, query, "id");
  });
  const dependencyFields = queries.map((query) => collectDependencyFields(query, "id"));
  let checksum = 0;
  for (const mutation of mutations) {
    if (sharing === "shared") {
      for (const plan of plans.values()) {
        plan.applyMutation(mutation);
      }
    }
    for (let index = 0; index < views.length; index++) {
      const view = views[index];
      const dependencies = dependencyFields[index];
      if (
        view === undefined ||
        dependencies === undefined ||
        canSkipUpdate(mutation, dependencies)
      ) {
        continue;
      }
      const change = view.applyMutation(mutation);
      checksum = mixChecksum(
        checksum,
        changeChecksum(change, () => view.snapshot()),
      );
    }
  }
  return checksum;
}

function changeChecksum(
  change: ActiveRawViewChange,
  snapshot: () => { readonly rows: readonly RuntimeRow[]; readonly totalRows: number },
): number {
  switch (change.type) {
    case "noop": {
      const result = snapshot();
      return resultChecksum(result.rows, result.totalRows);
    }
    case "totalRowsOnly": {
      const result = snapshot();
      return resultChecksum(result.rows, change.totalRows);
    }
    case "changed":
      return resultChecksum(change.result.rows, change.result.totalRows);
  }
}

function resultChecksum(rows: readonly RuntimeRow[], totalRows: number): number {
  let checksum = totalRows | 0;
  for (let index = 0; index < rows.length; index++) {
    const row = rows[index];
    checksum = mixChecksum(checksum, stringChecksum(String(row.id)));
    checksum = mixChecksum(checksum, numberChecksum(row.price));
    checksum = mixChecksum(checksum, stableValueChecksum(row.symbol));
    checksum = mixChecksum(checksum, index + 1);
  }
  return checksum;
}

function mixChecksum(left: number, right: number): number {
  return Math.imul(left ^ right, 16_777_619) >>> 0;
}

function stringChecksum(value: string): number {
  let checksum = 2_166_136_261;
  for (let index = 0; index < value.length; index++) {
    checksum = mixChecksum(checksum, value.charCodeAt(index));
  }
  return checksum;
}

function numberChecksum(value: unknown): number {
  return typeof value === "number" ? value | 0 : stringChecksum(String(value));
}

function stableValueChecksum(value: unknown): number {
  if (value === undefined || value === null) {
    return 0;
  }
  if (typeof value === "string") {
    return stringChecksum(value);
  }
  if (typeof value === "number") {
    return numberChecksum(value);
  }
  return stringChecksum(JSON.stringify(value));
}

function makeRows(count: number): RuntimeRow[] {
  return Array.from({ length: count }, (_, index) => ({
    id: `row-${index.toString().padStart(9, "0")}`,
    symbol: `SYM-${index % 10_000}`,
    status: index % 3 === 0 ? "open" : "closed",
    price: (index * 37) % 1_000_000,
    score: index % 97,
    venue: index % 2 === 0 ? "NASDAQ" : "NYSE",
    irrelevant: 0,
  }));
}

function makeQueries(count: number, pageSize: number, shape: QueryShape): RuntimeRawQuery[] {
  if (shape === "same-plan") {
    return Array.from({ length: count }, (_, index) => ({
      ...queryPlans()[0],
      offset: (index % 250) * pageSize,
      limit: pageSize,
    }));
  }
  if (shape === "ten-plans") {
    const plans = queryPlans();
    return Array.from({ length: count }, (_, index) => ({
      ...plans[index % plans.length],
      offset: Math.floor(index / plans.length) * pageSize,
      limit: pageSize,
    }));
  }
  if (shape === "unique-plans") {
    return Array.from({ length: count }, (_, index) => ({
      fields: rawFields(),
      where: {
        field: "price",
        comparator: "greater_than_or_equal",
        value: index,
      },
      orderBy:
        index % 2 === 0
          ? [{ field: "price", direction: "asc" }]
          : [
              { field: "score", direction: "desc" },
              { field: "price", direction: "asc" },
            ],
      offset: (index % 20) * pageSize,
      limit: pageSize,
    }));
  }
  return Array.from({ length: count }, (_, index) => ({
    fields: {
      id: true,
      symbol: true,
      price: true,
      status: true,
    },
    where:
      index % 2 === 0
        ? { field: "status", comparator: "equals", value: "open" }
        : { field: "venue", comparator: "equals", value: "NASDAQ" },
    orderBy:
      index % 3 === 0
        ? [{ field: "price", direction: "asc" }]
        : [
            { field: "score", direction: "desc" },
            { field: "price", direction: "asc" },
          ],
    offset: (index % 20) * pageSize,
    limit: pageSize,
  }));
}

function queryPlans(): readonly Omit<RuntimeRawQuery, "offset" | "limit">[] {
  return [
    {
      fields: rawFields(),
      where: { field: "status", comparator: "equals", value: "open" },
      orderBy: [{ field: "price", direction: "asc" }],
    },
    {
      fields: rawFields(),
      where: { field: "status", comparator: "equals", value: "closed" },
      orderBy: [{ field: "price", direction: "asc" }],
    },
    {
      fields: rawFields(),
      where: { field: "venue", comparator: "equals", value: "NASDAQ" },
      orderBy: [
        { field: "score", direction: "desc" },
        { field: "price", direction: "asc" },
      ],
    },
    {
      fields: rawFields(),
      where: { field: "venue", comparator: "equals", value: "NYSE" },
      orderBy: [
        { field: "score", direction: "desc" },
        { field: "price", direction: "asc" },
      ],
    },
    {
      fields: rawFields(),
      where: { field: "score", comparator: "greater_than", value: 10 },
      orderBy: [{ field: "symbol", direction: "asc" }],
    },
    {
      fields: rawFields(),
      where: { field: "score", comparator: "less_than_or_equal", value: 40 },
      orderBy: [{ field: "symbol", direction: "desc" }],
    },
    {
      fields: rawFields(),
      where: { field: "price", comparator: "greater_than", value: 100_000 },
      orderBy: [
        { field: "status", direction: "asc" },
        { field: "price", direction: "desc" },
      ],
    },
    {
      fields: rawFields(),
      where: { field: "price", comparator: "less_than", value: 500_000 },
      orderBy: [
        { field: "venue", direction: "asc" },
        { field: "score", direction: "asc" },
      ],
    },
    {
      fields: rawFields(),
      where: { field: "symbol", comparator: "starts_with", value: "SYM-1" },
      orderBy: [{ field: "price", direction: "asc" }],
    },
    {
      fields: rawFields(),
      where: { field: "symbol", comparator: "contains", value: "7" },
      orderBy: [{ field: "score", direction: "desc" }],
    },
  ];
}

function rawFields(): RuntimeRawQuery["fields"] {
  return {
    id: true,
    price: true,
    status: true,
    symbol: true,
  };
}

function makeScenarios(rows: readonly RuntimeRow[], mutations: number): readonly BenchScenario[] {
  return [
    {
      name: "random-updates",
      mutations: Array.from({ length: mutations }, (_, index) => {
        const row = rows[(index * 7919) % rows.length];
        return update(BigInt(index + 1), row, { price: Number(row.price) + 17 });
      }),
    },
    {
      name: "hot-key-updates",
      mutations: Array.from({ length: mutations }, (_, index) => {
        const row = rows[index % Math.min(100, rows.length)];
        return update(BigInt(index + 1), row, { price: Number(row.price) + index + 1 });
      }),
    },
    {
      name: "irrelevant-field-updates",
      mutations: Array.from({ length: mutations }, (_, index) => {
        const row = rows[(index * 3571) % rows.length];
        return update(BigInt(index + 1), row, { irrelevant: index + 1 });
      }),
    },
    {
      name: "threshold-crossing-updates",
      mutations: Array.from({ length: mutations }, (_, index) => {
        const row = rows[(index * 1543) % rows.length];
        return update(BigInt(index + 1), row, {
          status: row.status === "open" ? "closed" : "open",
        });
      }),
    },
    {
      name: "sorted-row-movement",
      mutations: Array.from({ length: mutations }, (_, index) => {
        const row = rows[(index * 9176) % rows.length];
        return update(BigInt(index + 1), row, { price: index % 2 === 0 ? 1 : 999_999 });
      }),
    },
    {
      name: "totalRows-only-inserts",
      mutations: Array.from({ length: mutations }, (_, index) => {
        const row: RuntimeRow = {
          id: `insert-${index.toString().padStart(9, "0")}`,
          symbol: `NEW-${index}`,
          status: "open",
          price: 2_000_000 + index,
          score: 0,
          venue: "NASDAQ",
          irrelevant: 0,
        };
        return insert(BigInt(index + 1), row);
      }),
    },
  ];
}

function update(
  version: bigint,
  before: RuntimeRow,
  changes: Readonly<Record<string, unknown>>,
): MutationLogEntry {
  const after = {
    ...before,
    ...changes,
  };
  return {
    version,
    kind: "update",
    id: String(before.id),
    before,
    after,
    changedFields: new Set(Object.keys(changes)),
  };
}

function insert(version: bigint, after: RuntimeRow): MutationLogEntry {
  return {
    version,
    kind: "insert",
    id: String(after.id),
    after,
    changedFields: new Set(Object.keys(after)),
  };
}

function applyMutationToRows(
  rows: RuntimeRow[],
  indexes: Map<string, number>,
  mutation: MutationLogEntry,
): void {
  const id = String(mutation.id);
  switch (mutation.kind) {
    case "insert": {
      const after = mutationAfter(mutation);
      indexes.set(id, rows.length);
      rows.push(after);
      break;
    }
    case "update": {
      const index = indexes.get(id);
      if (index !== undefined) {
        rows[index] = mutationAfter(mutation);
      }
      break;
    }
    case "delete": {
      const index = indexes.get(id);
      if (index === undefined) {
        break;
      }
      const last = rows[rows.length - 1];
      rows.pop();
      indexes.delete(id);
      if (last !== undefined && index < rows.length) {
        rows[index] = last;
        indexes.set(String(last.id), index);
      }
      break;
    }
  }
}

function indexRows(rows: readonly RuntimeRow[]): Map<string, number> {
  const indexes = new Map<string, number>();
  rows.forEach((row, index) => {
    indexes.set(String(row.id), index);
  });
  return indexes;
}

function canSkipUpdate(mutation: MutationLogEntry, dependencies: ReadonlySet<string>): boolean {
  if (mutation.kind !== "update") {
    return false;
  }
  for (const field of mutation.changedFields) {
    if (dependencies.has(field)) {
      return false;
    }
  }
  return true;
}

function mutationAfter(mutation: MutationLogEntry): RuntimeRow {
  if (mutation.after === undefined) {
    throw new Error(`Expected ${mutation.kind} mutation after row`);
  }
  return mutation.after;
}

function timeResult(run: () => number): TimedResult {
  const started = performance.now();
  const checksum = run();
  return {
    ms: performance.now() - started,
    checksum,
  };
}

function memoryBeforeBuild(enabled: boolean): MemorySnapshot | undefined {
  if (!enabled) {
    return undefined;
  }
  forceGc();
  return memorySnapshot();
}

function memoryAfterBuild(before: MemorySnapshot | undefined): MemoryMeasurement | undefined {
  if (before === undefined) {
    return undefined;
  }
  forceGc();
  const after = memorySnapshot();
  return {
    before,
    after,
    heapUsedDelta: after.heapUsed - before.heapUsed,
    rssDelta: after.rss - before.rss,
    gcAvailable: globalThis.gc !== undefined,
  };
}

function forceGc(): void {
  globalThis.gc?.();
}

function memorySnapshot(): MemorySnapshot {
  const usage = process.memoryUsage();
  return {
    heapUsed: usage.heapUsed,
    heapTotal: usage.heapTotal,
    rss: usage.rss,
    external: usage.external,
    arrayBuffers: usage.arrayBuffers,
  };
}

function positiveInteger(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function optionalPositiveInteger(name: string): number | undefined {
  const value = process.env[name];
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function nonNegativeInteger(name: string, fallback: number): number {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function envFlag(name: string, fallback: boolean): boolean {
  const value = process.env[name];
  if (value === undefined) {
    return fallback;
  }
  return value !== "0" && value.toLocaleLowerCase() !== "false";
}

function formatMs(ms: number | undefined): string {
  return ms === undefined ? "skipped" : ms.toFixed(2);
}

function formatNumber(value: number | undefined): string {
  return value === undefined ? "none" : String(value);
}

function formatBytes(value: number | undefined): string {
  return value === undefined ? "unknown" : String(value);
}

function formatChecksum(checksum: number | undefined): string {
  return checksum === undefined ? "skipped" : String(checksum);
}

function pageSizes(): readonly number[] {
  const value = process.env.VS_ACTIVE_VIEW_PAGE_SIZES;
  if (value === undefined || value.length === 0) {
    return [50, 100];
  }
  const sizes = value
    .split(",")
    .map((entry) => Number.parseInt(entry.trim(), 10))
    .filter((entry) => Number.isFinite(entry) && entry > 0);
  return sizes.length === 0 ? [50, 100] : sizes;
}

function scenarioNames(): readonly string[] {
  const value = process.env.VS_ACTIVE_VIEW_SCENARIOS;
  if (value === undefined || value.length === 0) {
    return [
      "random-updates",
      "hot-key-updates",
      "irrelevant-field-updates",
      "threshold-crossing-updates",
      "sorted-row-movement",
      "totalRows-only-inserts",
    ];
  }
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function sortedIndexKinds(): readonly ActiveSortedIndexKind[] {
  const value = process.env.VS_ACTIVE_VIEW_INDEXES;
  if (value === undefined || value.length === 0) {
    return ["blocks"];
  }
  const indexes: ActiveSortedIndexKind[] = [];
  for (const entry of value.split(",")) {
    const name = entry.trim();
    if (name === "array" || name === "blocks") {
      indexes.push(name);
    }
  }
  return indexes.length === 0 ? ["blocks"] : indexes;
}

function sharingKinds(): readonly ActiveViewSharing[] {
  const value = process.env.VS_ACTIVE_VIEW_SHARING;
  if (value === undefined || value.length === 0) {
    return ["shared"];
  }
  const sharing: ActiveViewSharing[] = [];
  for (const entry of value.split(",")) {
    const name = entry.trim();
    if (name === "per-subscription" || name === "shared") {
      sharing.push(name);
    }
  }
  return sharing.length === 0 ? ["shared"] : sharing;
}

function queryShape(): QueryShape {
  const value = process.env.VS_ACTIVE_VIEW_QUERY_SHAPE;
  if (
    value === "same-plan" ||
    value === "ten-plans" ||
    value === "unique-plans" ||
    value === "mixed"
  ) {
    return value;
  }
  return "mixed";
}
