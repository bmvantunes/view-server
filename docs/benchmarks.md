# Benchmark CI

The GitHub Actions benchmark workflow runs tiny smoke shapes only. It is meant to prove that benchmark entrypoints still run, produce JSON artifacts, append a GitHub Actions summary, and compare against a checked-in baseline. It is not the production benchmark platform.

The workflow uploads generated artifacts from `packages/core/bench/.artifacts/ci/*.json`.

By default benchmark regressions are reporting-only:

```yaml
VS_BENCH_BLOCKING: "0"
```

Set `VS_BENCH_BLOCKING=1` in `.github/workflows/benchmarks.yml` when the smoke baselines are stable enough to block PRs.

`VS_BENCH_REGRESSION_MIN_DELTA_MS=5` prevents tiny timing movements from being counted as hard regressions. A metric that exceeds the percentage threshold but moves by less than that absolute millisecond delta is reported as `warn`, not `fail`.

## Refreshing Baselines

Run the same smoke shapes locally from the repo root:

```bash
vp run core#bench:refresh-baselines
```

Inspect the generated JSON changes under `packages/core/bench/baselines/ci-smoke/` before committing.

To run the same comparison locally without refreshing baselines:

```bash
vp run core#bench:compare
```

The checked-in baselines are intentionally lightweight smoke budgets. For serious performance decisions, use the larger benchmark shapes documented in the project plan.
