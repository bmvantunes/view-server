export type BenchmarkProfileName =
  | "ci-smoke"
  | "dev-fast"
  | "rc-1m"
  | "soak-10m"
  | "grouped-heavy"
  | "active-plan-startup"
  | "chdb-worker-overlap";

export type BenchmarkProfileCwd = "package" | "repo";

export type BenchmarkProfileBenchmark = {
  readonly name: string;
  readonly description: string;
  readonly artifactFile: string;
  readonly metrics?: string | undefined;
  readonly baselineFile?: string | undefined;
  readonly script?: string | undefined;
  readonly command?: readonly string[] | undefined;
  readonly cwd?: BenchmarkProfileCwd | undefined;
  readonly env: Readonly<Record<string, string>>;
};

export type BenchmarkProfile = {
  readonly name: BenchmarkProfileName;
  readonly description: string;
  readonly ciSafe: boolean;
  readonly coverageGaps: readonly string[];
  readonly benchmarks: readonly BenchmarkProfileBenchmark[];
};

export const benchmarkProfileNames: readonly BenchmarkProfileName[] = [
  "ci-smoke",
  "dev-fast",
  "rc-1m",
  "soak-10m",
  "grouped-heavy",
  "active-plan-startup",
  "chdb-worker-overlap",
];

export const benchmarkProfiles: Readonly<Record<BenchmarkProfileName, BenchmarkProfile>> = {
  "ci-smoke": {
    name: "ci-smoke",
    description: "Tiny reporting-only benchmark shapes for PR and push visibility.",
    ciSafe: true,
    coverageGaps: [
      "Active-plan responsiveness can complete before a build overlaps at smoke scale; large local benchmarks remain the source of truth.",
      "Grouped refresh smoke uses small row counts; 1M/10M chDB grouped overlap remains manual/nightly evidence.",
    ],
    benchmarks: [
      {
        name: "active-view",
        description: "Shared active raw plan smoke with one hot-key update shape.",
        script: "bench/active-view.bench.ts",
        artifactFile: "active-view.json",
        baselineFile: "active-view.json",
        metrics: "activeBuildMs,activeUpdateMs",
        env: {
          VS_ACTIVE_VIEW_ROWS: "1000",
          VS_ACTIVE_VIEW_SUBSCRIPTIONS: "5",
          VS_ACTIVE_VIEW_MUTATIONS: "5",
          VS_ACTIVE_VIEW_BASELINE: "0",
          VS_ACTIVE_VIEW_SCENARIOS: "hot-key-updates",
          VS_ACTIVE_VIEW_PAGE_SIZES: "50",
          VS_ACTIVE_VIEW_SHARING: "shared",
        },
      },
      {
        name: "active-plan-responsiveness",
        description: "Publish latency smoke while an active plan may be building.",
        script: "bench/active-plan-responsiveness.bench.ts",
        artifactFile: "active-plan-responsiveness.json",
        baselineFile: "active-plan-responsiveness.json",
        metrics: "operationP99Ms,metricsP99Ms",
        env: {
          VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "1000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "3",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "publish",
        },
      },
      {
        name: "grouped-responsiveness",
        description: "Grouped subscription stale/debounce mutation responsiveness smoke.",
        script: "bench/grouped-responsiveness.bench.ts",
        artifactFile: "grouped-responsiveness.json",
        baselineFile: "grouped-responsiveness.json",
        metrics: "operationP99Ms,metricsP99Ms",
        env: {
          VS_GROUPED_RESPONSIVENESS_ROWS: "1000",
          VS_GROUPED_RESPONSIVENESS_OPERATIONS: "3",
          VS_GROUPED_RESPONSIVENESS_AGGREGATES: "3",
        },
      },
      {
        name: "grouped-refresh-overlap",
        description: "Grouped refresh overlap smoke with tiny memory-backed refresh.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap.json",
        baselineFile: "grouped-refresh-overlap.json",
        metrics: "operationP99Ms,startGapMaxMs",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "1000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "3",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "3",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "memory",
        },
      },
    ],
  },
  "dev-fast": {
    name: "dev-fast",
    description: "Local quick checks large enough to catch obvious active/grouped regressions.",
    ciSafe: false,
    coverageGaps: [
      "Does not replace the 1M release-candidate profile.",
      "Uses reduced subscription and mutation counts to stay interactive on laptops.",
    ],
    benchmarks: [
      {
        name: "active-view-shared-100k",
        description: "100k-row shared active-plan updates with hot-key and sorted movement.",
        script: "bench/active-view.bench.ts",
        artifactFile: "active-view-shared-100k.json",
        metrics: "activeBuildMs,activeUpdateMs",
        env: {
          VS_ACTIVE_VIEW_ROWS: "100000",
          VS_ACTIVE_VIEW_SUBSCRIPTIONS: "50",
          VS_ACTIVE_VIEW_MUTATIONS: "100",
          VS_ACTIVE_VIEW_BASELINE: "0",
          VS_ACTIVE_VIEW_SCENARIOS: "hot-key-updates,sorted-row-movement",
          VS_ACTIVE_VIEW_PAGE_SIZES: "50,100",
          VS_ACTIVE_VIEW_SHARING: "shared",
          VS_ACTIVE_VIEW_QUERY_SHAPE: "mixed",
        },
      },
      {
        name: "active-plan-responsiveness-100k",
        description: "100k-row active-plan build overlap responsiveness.",
        script: "bench/active-plan-responsiveness.bench.ts",
        artifactFile: "active-plan-responsiveness-100k.json",
        metrics: "operationP99Ms,operationMaxMs,metricsP99Ms",
        env: {
          VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "100000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "100",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "publish",
        },
      },
      {
        name: "grouped-refresh-overlap-100k",
        description: "100k-row grouped refresh overlap with chDB backend.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap-100k.json",
        metrics: "operationP99Ms,startGapMaxMs,metricsP99Ms",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "100000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "100",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "25",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "chdb",
        },
      },
    ],
  },
  "rc-1m": {
    name: "rc-1m",
    description: "Manual release-candidate 1M-row responsiveness and memory profile.",
    ciSafe: false,
    coverageGaps: [
      "Hardware-sensitive; compare artifacts across the same machine class.",
      "Raw active plan startup is measured separately from mutation hot-path latency.",
    ],
    benchmarks: [
      {
        name: "active-plan-publish-1m",
        description: "1M-row publish responsiveness during active-plan build.",
        script: "bench/active-plan-responsiveness.bench.ts",
        artifactFile: "active-plan-publish-1m.json",
        metrics: "operationP99Ms,operationMaxMs,metricsP99Ms",
        env: {
          VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "1000000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "1000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "publish",
        },
      },
      {
        name: "active-plan-delta-1m",
        description: "1M-row deltaPublish responsiveness during active-plan build.",
        script: "bench/active-plan-responsiveness.bench.ts",
        artifactFile: "active-plan-delta-1m.json",
        metrics: "operationP99Ms,operationMaxMs,metricsP99Ms",
        env: {
          VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "1000000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "1000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "deltaPublish",
        },
      },
      {
        name: "active-plan-delete-1m",
        description: "1M-row deleteById responsiveness during active-plan build.",
        script: "bench/active-plan-responsiveness.bench.ts",
        artifactFile: "active-plan-delete-1m.json",
        metrics: "operationP99Ms,operationMaxMs,metricsP99Ms",
        env: {
          VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS: "1000000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS: "1000",
          VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION: "deleteById",
        },
      },
      {
        name: "grouped-refresh-overlap-1m",
        description: "1M-row grouped chDB refresh overlap with 100 aggregates.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap-1m.json",
        metrics: "operationP99Ms,startGapMaxMs,metricsP99Ms",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "1000000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "200",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "100",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "chdb",
        },
      },
    ],
  },
  "soak-10m": {
    name: "soak-10m",
    description: "Manual/nightly 10M raw worker soak using active-plan admission policy.",
    ciSafe: false,
    coverageGaps: [
      "Not a PR or normal release gate.",
      "Grouped subscriptions default to zero; grouped 10M capacity belongs to chDB grouped refresh benchmarks.",
    ],
    benchmarks: [
      {
        name: "worker-soak-10m",
        description: "10M raw worker soak with progress JSONL and summary artifacts.",
        command: ["pnpm", "run", "soak:10m"],
        cwd: "repo",
        artifactFile: "worker-soak-10m.json",
        env: {
          VS_WORKER_SOAK_ROWS: "10000000",
          VS_WORKER_SOAK_RAW_SUBSCRIPTIONS: "250",
          VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS: "0",
          VS_WORKER_SOAK_MUTATIONS: "10000",
          VS_WORKER_SOAK_ACTIVE_PLAN_AUTO_BUILD_MAX_ROWS: "1000000",
        },
      },
    ],
  },
  "grouped-heavy": {
    name: "grouped-heavy",
    description: "Manual grouped-query stress profile for accumulator and chDB refresh work.",
    ciSafe: false,
    coverageGaps: [
      "Grouped accumulator is not the default runtime path.",
      "Use chDB overlap results to judge production grouped refresh responsiveness.",
    ],
    benchmarks: [
      {
        name: "grouped-aggregation-1m",
        description: "1M-row grouped accumulator build/apply comparison.",
        script: "bench/grouped-aggregation.bench.ts",
        artifactFile: "grouped-aggregation-1m.json",
        metrics: "groupedSnapshotMs,accumulatorBuildMs,accumulatorApplyMs,fullRecomputeMs",
        env: {
          VS_GROUPED_AGGREGATION_ROWS: "1000000",
          VS_GROUPED_AGGREGATION_GROUPS: "1000",
          VS_GROUPED_AGGREGATION_AGGREGATES: "100",
          VS_GROUPED_AGGREGATION_MUTATIONS: "10000",
          VS_GROUPED_AGGREGATION_ITERATIONS: "1",
        },
      },
      {
        name: "grouped-refresh-overlap-10m",
        description: "10M-row grouped chDB refresh overlap with 100 aggregates.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap-10m.json",
        metrics: "operationP99Ms,startGapMaxMs,metricsP99Ms",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "10000000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "20",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "100",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "chdb",
        },
      },
    ],
  },
  "active-plan-startup": {
    name: "active-plan-startup",
    description: "Manual active-plan build/startup profile for shared-plan policy tuning.",
    ciSafe: false,
    coverageGaps: [
      "Measures startup/build cost, not mutation hot-path latency.",
      "Use memory flags with --expose-gc for retained heap evidence.",
    ],
    benchmarks: [
      {
        name: "active-view-build-same-plan-1m",
        description: "1M-row build-only shared same-plan active-view profile.",
        script: "bench/active-view.bench.ts",
        artifactFile: "active-view-build-same-plan-1m.json",
        metrics: "activeBuildMs,activePlanCount,activeIndexBytes,activeHeapDelta",
        env: {
          VS_ACTIVE_VIEW_ROWS: "1000000",
          VS_ACTIVE_VIEW_SUBSCRIPTIONS: "250",
          VS_ACTIVE_VIEW_MUTATIONS: "0",
          VS_ACTIVE_VIEW_BASELINE: "0",
          VS_ACTIVE_VIEW_SCENARIOS: "hot-key-updates",
          VS_ACTIVE_VIEW_PAGE_SIZES: "50,100",
          VS_ACTIVE_VIEW_SHARING: "shared",
          VS_ACTIVE_VIEW_QUERY_SHAPE: "same-plan",
          VS_ACTIVE_VIEW_MEMORY: "1",
        },
      },
      {
        name: "active-view-build-ten-plans-1m",
        description: "1M-row build-only shared ten-plan active-view profile.",
        script: "bench/active-view.bench.ts",
        artifactFile: "active-view-build-ten-plans-1m.json",
        metrics: "activeBuildMs,activePlanCount,activeIndexBytes,activeHeapDelta",
        env: {
          VS_ACTIVE_VIEW_ROWS: "1000000",
          VS_ACTIVE_VIEW_SUBSCRIPTIONS: "250",
          VS_ACTIVE_VIEW_MUTATIONS: "0",
          VS_ACTIVE_VIEW_BASELINE: "0",
          VS_ACTIVE_VIEW_SCENARIOS: "hot-key-updates",
          VS_ACTIVE_VIEW_PAGE_SIZES: "50,100",
          VS_ACTIVE_VIEW_SHARING: "shared",
          VS_ACTIVE_VIEW_QUERY_SHAPE: "ten-plans",
          VS_ACTIVE_VIEW_MEMORY: "1",
        },
      },
      {
        name: "active-view-build-guarded-unique-1m",
        description: "1M-row build-only unique-plan profile with maxActivePlans guard.",
        script: "bench/active-view.bench.ts",
        artifactFile: "active-view-build-guarded-unique-1m.json",
        metrics:
          "activeBuildMs,activePlanCount,activeFallbackCount,activeIndexBytes,activeHeapDelta",
        env: {
          VS_ACTIVE_VIEW_ROWS: "1000000",
          VS_ACTIVE_VIEW_SUBSCRIPTIONS: "250",
          VS_ACTIVE_VIEW_MUTATIONS: "0",
          VS_ACTIVE_VIEW_BASELINE: "0",
          VS_ACTIVE_VIEW_SCENARIOS: "hot-key-updates",
          VS_ACTIVE_VIEW_PAGE_SIZES: "50,100",
          VS_ACTIVE_VIEW_SHARING: "shared",
          VS_ACTIVE_VIEW_QUERY_SHAPE: "unique-plans",
          VS_ACTIVE_VIEW_MAX_ACTIVE_PLANS: "10",
          VS_ACTIVE_VIEW_MEMORY: "1",
        },
      },
    ],
  },
  "chdb-worker-overlap": {
    name: "chdb-worker-overlap",
    description: "Manual chDB worker-isolated grouped refresh overlap profile.",
    ciSafe: false,
    coverageGaps: [
      "Does not exercise websocket clients; it targets topic worker scheduling during chDB grouped refresh.",
      "Run beside deployment smoke for full transport confidence.",
    ],
    benchmarks: [
      {
        name: "grouped-refresh-overlap-chdb-1m",
        description: "1M-row chDB grouped refresh overlap profile.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap-chdb-1m.json",
        metrics: "operationP99Ms,startGapMaxMs,metricsP99Ms",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "1000000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "200",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "100",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "chdb",
        },
      },
      {
        name: "grouped-refresh-overlap-chdb-10m",
        description: "10M-row chDB grouped refresh overlap profile.",
        script: "bench/grouped-refresh-overlap.bench.ts",
        artifactFile: "grouped-refresh-overlap-chdb-10m.json",
        metrics: "operationP99Ms,startGapMaxMs,metricsP99Ms",
        env: {
          VS_GROUPED_REFRESH_OVERLAP_ROWS: "10000000",
          VS_GROUPED_REFRESH_OVERLAP_OPERATIONS: "20",
          VS_GROUPED_REFRESH_OVERLAP_AGGREGATES: "100",
          VS_GROUPED_REFRESH_OVERLAP_BACKEND: "chdb",
        },
      },
    ],
  },
};

export function listBenchmarkProfiles(): readonly BenchmarkProfile[] {
  return benchmarkProfileNames.map((name) => benchmarkProfiles[name]);
}

export function getBenchmarkProfile(name: string): BenchmarkProfile | undefined {
  switch (name) {
    case "ci-smoke":
      return benchmarkProfiles["ci-smoke"];
    case "dev-fast":
      return benchmarkProfiles["dev-fast"];
    case "rc-1m":
      return benchmarkProfiles["rc-1m"];
    case "soak-10m":
      return benchmarkProfiles["soak-10m"];
    case "grouped-heavy":
      return benchmarkProfiles["grouped-heavy"];
    case "active-plan-startup":
      return benchmarkProfiles["active-plan-startup"];
    case "chdb-worker-overlap":
      return benchmarkProfiles["chdb-worker-overlap"];
    default:
      return undefined;
  }
}

export function benchmarkProfileCoverageGapsEnv(profile: BenchmarkProfile): string {
  return profile.coverageGaps.join("\n");
}
