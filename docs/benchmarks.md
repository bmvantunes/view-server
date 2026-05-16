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

List the curated benchmark profiles:

```bash
vp run core#bench:profiles
```

Dry-run a named profile to see the exact scripts and environment parameters without running the
work:

```bash
vp run core#bench:profile -- --profile ci-smoke --dry-run
```

Run a named profile:

```bash
vp run core#bench:profile -- --profile dev-fast
```

The current profiles are:

- `ci-smoke`: tiny reporting-only CI visibility.
- `dev-fast`: local 100k-ish active/grouped checks.
- `rc-1m`: manual 1M release-candidate responsiveness checks.
- `soak-10m`: manual/nightly raw 10M worker soak wrapper.
- `grouped-heavy`: grouped accumulator and 10M chDB refresh stress.
- `active-plan-startup`: active raw plan startup/build and memory shapes.
- `chdb-worker-overlap`: chDB worker-isolated grouped refresh overlap.

Profile-run artifacts include `config.profile`, `config.profileBenchmark`, and the benchmark's exact
shape parameters. CI summaries also include declared profile coverage gaps so smoke results are not
mistaken for full capacity proof.

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

## Local Grouped Accumulator Check

The grouped accumulator is opt-in and currently supports count, sum, min, and max grouped
subscriptions. It should be compared against full grouped recompute before changing default grouped
runtime policy:

```bash
VS_GROUPED_AGGREGATION_ROWS=1000000 \
VS_GROUPED_AGGREGATION_GROUPS=1000 \
VS_GROUPED_AGGREGATION_AGGREGATES=100 \
VS_GROUPED_AGGREGATION_MUTATIONS=10000 \
VS_GROUPED_AGGREGATION_ITERATIONS=1 \
node --experimental-strip-types packages/core/bench/grouped-aggregation.bench.ts
```

Latest local result on May 16, 2026:

- grouped snapshot: `940.30ms`
- incremental accumulator build: `4304.61ms`
- incremental apply for 10k mutations: `97.29ms`
- full recompute after mutations: `935.08ms`
- incremental apply speedup: `9.61x`

That says incremental apply is useful once built, but initial accumulator build time is still too high
to make this the default grouped strategy for large topics.
