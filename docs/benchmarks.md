# Benchmark CI

The GitHub Actions benchmark workflow runs tiny smoke shapes only. It is meant to prove that benchmark entrypoints still run, produce JSON artifacts, and can compare against a checked-in baseline. It is not the production benchmark platform.

The workflow uploads generated artifacts from `packages/core/bench/.artifacts/ci/*.json`.

By default benchmark regressions are reporting-only:

```yaml
VS_BENCH_BLOCKING: "0"
```

Set `VS_BENCH_BLOCKING=1` in `.github/workflows/benchmarks.yml` when the smoke baselines are stable enough to block PRs.

## Refreshing Baselines

Run the same smoke shapes locally from the repo root, inspect the generated JSON, then replace the corresponding file under `packages/core/bench/baselines/ci-smoke/`.

```bash
VS_ACTIVE_VIEW_ROWS=1000 \
VS_ACTIVE_VIEW_SUBSCRIPTIONS=5 \
VS_ACTIVE_VIEW_MUTATIONS=5 \
VS_ACTIVE_VIEW_BASELINE=0 \
VS_ACTIVE_VIEW_SCENARIOS=hot-key-updates \
VS_ACTIVE_VIEW_PAGE_SIZES=50 \
VS_ACTIVE_VIEW_SHARING=shared \
VS_BENCH_ARTIFACT=packages/core/bench/.artifacts/ci/active-view.json \
node --experimental-strip-types packages/core/bench/active-view.bench.ts
```

```bash
VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS=1000 \
VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS=3 \
VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION=publish \
VS_BENCH_ARTIFACT=packages/core/bench/.artifacts/ci/active-plan-responsiveness.json \
node --experimental-strip-types packages/core/bench/active-plan-responsiveness.bench.ts
```

```bash
VS_GROUPED_RESPONSIVENESS_ROWS=1000 \
VS_GROUPED_RESPONSIVENESS_OPERATIONS=3 \
VS_GROUPED_RESPONSIVENESS_AGGREGATES=3 \
VS_BENCH_ARTIFACT=packages/core/bench/.artifacts/ci/grouped-responsiveness.json \
node --experimental-strip-types packages/core/bench/grouped-responsiveness.bench.ts
```

```bash
VS_GROUPED_REFRESH_OVERLAP_ROWS=1000 \
VS_GROUPED_REFRESH_OVERLAP_OPERATIONS=3 \
VS_GROUPED_REFRESH_OVERLAP_AGGREGATES=3 \
VS_GROUPED_REFRESH_OVERLAP_BACKEND=memory \
VS_BENCH_ARTIFACT=packages/core/bench/.artifacts/ci/grouped-refresh-overlap.json \
node --experimental-strip-types packages/core/bench/grouped-refresh-overlap.bench.ts
```

To test comparison locally, add `VS_BENCH_BASELINE` and `VS_BENCH_REGRESSION_METRICS` using the same values from the workflow.

The checked-in baselines are intentionally lightweight smoke budgets. For serious performance decisions, use the larger benchmark shapes documented in the project plan.
