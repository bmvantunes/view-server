# Benchmark CI

The GitHub Actions benchmark workflow has two layers:

- `ci-smoke`: tiny deterministic smoke shapes that prove benchmark entrypoints still run, produce JSON artifacts, append a GitHub Actions summary, and compare against checked-in baselines. Stable smoke regressions can block PRs.
- `firehose-ci`: report-only firehose thresholds for the new hot-path work: worker mutation batching, chDB apply batching, fanout slow-client coalescing, and the 1M alpha worker soak. These warn loudly in the Actions summary but do not block PRs.

The workflow uploads generated artifacts from `packages/core/bench/.artifacts/**/*.json`.

The workflow sets global blocking on so deterministic `ci-smoke` regressions can fail the job:

```yaml
VS_BENCH_BLOCKING: "1"
```

Individual benchmark profiles can still mark noisy or long-running checks as report-only with
`blocking: false`. The current report-only checks are the active-plan overlap smoke and every
`firehose-ci` benchmark.

To make a local run fully reporting-only:

```yaml
VS_BENCH_BLOCKING: "0"
```

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

- `ci-smoke`: tiny CI visibility; deterministic entries can block PRs.
- `firehose-ci`: report-only firehose thresholds for batching/coalescing/1M soak regressions.
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

Refresh the report-only firehose baselines after an intentional hot-path change:

```bash
vp run core#bench:profile -- --profile firehose-ci --refresh-baselines
```

Compare local firehose results against the checked-in thresholds:

```bash
vp run core#bench:profile -- --profile firehose-ci --compare
```

The 10M raw soak is manual/nightly only. Refresh or compare it explicitly; do not add it to normal
CI:

```bash
vp run core#bench:profile -- --profile soak-10m --refresh-baselines
vp run core#bench:profile -- --profile soak-10m --compare
```

The checked-in baselines are performance budgets for visibility and regression triage, not hardware-independent SLAs. For serious performance decisions, use the larger benchmark shapes documented in the project plan and compare on the same machine class.

## Latest Firehose Hot-Path Pass

Local run on May 17, 2026, after the mutation batching, health-sync scheduling, chDB
apply batching, ring-buffer mutation log, fanout mailbox, and batched soak harness changes.

These numbers are local-machine capacity signals, not SLAs. The 10M direct worker soak uses the
authoritative in-worker memory path with the snapshot accelerator disabled in the harness; production
runtime still requires chDB, and chDB-specific ingestion is covered by the chDB mirror benchmark
below.

### Kafka Decode And Runtime Dispatch

Command:

```bash
VS_KAFKA_BATCH_BENCH_SIZES=1000,10000,100000 \
VS_KAFKA_BATCH_BENCH_ITERATIONS=1 \
node --experimental-strip-types packages/core/bench/kafka-batch.bench.ts
```

Artifact: `packages/core/bench/.artifacts/kafka-batch-2026-05-17T17-11-08.269Z.json`

| records | legacy ms | batched ms | legacy records/sec | batched records/sec | runtime calls |
| ------: | --------: | ---------: | -----------------: | ------------------: | ------------: |
|   1,000 |     13.30 |       4.27 |             75,200 |             234,365 |    1,000 -> 1 |
|  10,000 |     25.81 |      26.55 |            387,485 |             376,665 |   10,000 -> 1 |
| 100,000 |    224.61 |     398.07 |            445,215 |             251,215 |  100,000 -> 1 |

This benchmark isolates decode plus runtime dispatch with a counting runtime. At large synthetic
sizes the batched path pays to accumulate a mutation array, so it is not the whole firehose picture.
The structural win is the runtime call collapse; the worker/chDB benchmarks below show why that
matters once the calls cross real worker/backend paths.

### Worker Mutation Batch

Command:

```bash
VS_WORKER_MUTATION_BATCH_SIZES=1000,10000 \
VS_WORKER_MUTATION_BATCH_ITERATIONS=1 \
node --experimental-strip-types packages/core/bench/worker-mutation-batch.bench.ts
```

Artifact: `packages/core/bench/.artifacts/worker-mutation-batch-2026-05-17T17-11-19.541Z.json`

| mutations | single-row ms | batched ms | single mutations/sec | batched mutations/sec | backend apply calls |
| --------: | ------------: | ---------: | -------------------: | --------------------: | ------------------: |
|     1,000 |         47.25 |      18.94 |               21,162 |                52,789 |          1,000 -> 1 |
|    10,000 |        308.46 |      75.21 |               32,419 |               132,965 |         10,000 -> 1 |

This uses a counting backend to isolate worker mutation/gate/fanout overhead from chDB execution.

### chDB Mirror Apply Batch

Commands:

```bash
VS_CHDB_SQL_MIRROR_ROWS=10000 \
VS_CHDB_SQL_MIRROR_COLUMNS=25 \
VS_CHDB_SQL_MIRROR_MUTATIONS=1000 \
VS_CHDB_SQL_MIRROR_COMPARE_LEGACY=1 \
node --experimental-strip-types packages/core/bench/chdb-sql-mirror.bench.ts

VS_CHDB_SQL_MIRROR_ROWS=10000 \
VS_CHDB_SQL_MIRROR_COLUMNS=25 \
VS_CHDB_SQL_MIRROR_MUTATIONS=10000 \
VS_CHDB_SQL_MIRROR_COMPARE_LEGACY=1 \
node --experimental-strip-types packages/core/bench/chdb-sql-mirror.bench.ts
```

Artifacts:

- `packages/core/bench/.artifacts/chdb-sql-mirror-2026-05-17T17-12-42.905Z.json`
- `packages/core/bench/.artifacts/chdb-sql-mirror-2026-05-17T17-15-43.453Z.json`

| mutations | batched apply ms | legacy one-by-one ms | batched mutations/sec | legacy mutations/sec |   speedup |
| --------: | ---------------: | -------------------: | --------------------: | -------------------: | --------: |
|     1,000 |            21.01 |             3,049.42 |                47,591 |                  328 |   145.12x |
|    10,000 |            85.66 |           176,812.75 |               116,736 |                   57 | 2,064.04x |

This is the strongest chDB ingestion signal: one mutation/request is not viable.

### Health Topic Sync

Command:

```bash
VS_HEALTH_SYNC_MUTATIONS=10000 \
VS_HEALTH_SYNC_TOPICS=25 \
node --experimental-strip-types packages/core/bench/health-sync.bench.ts
```

Artifact: `packages/core/bench/.artifacts/health-sync-2026-05-17T17-11-26.187Z.json`

| mutations | legacy ms | scheduled request+flush ms |  sync calls |
| --------: | --------: | -------------------------: | ----------: |
|    10,000 |     12.53 |                       8.59 | 10,000 -> 1 |

The synthetic wall-clock speedup is modest because the simulated projection is small. The important
runtime property is that Kafka/internal mutation batches no longer rebuild and republish the health
topic once per row.

### Mutation Log Ring Buffer

Command:

```bash
VS_MUTATION_LOG_APPEND_COUNTS=100000,1000000 \
VS_MUTATION_LOG_CAPACITY=10000 \
node --experimental-strip-types packages/core/bench/mutation-log.bench.ts
```

Artifact: `packages/core/bench/.artifacts/mutation-log-2026-05-17T17-11-32.279Z.json`

|   appends | ring ms | legacy shift ms | speedup |
| --------: | ------: | --------------: | ------: |
|   100,000 |   20.14 |           31.02 |   1.54x |
| 1,000,000 |  190.81 |          277.90 |   1.46x |

### Fanout Slow-Client Coalescing

Command:

```bash
VS_FANOUT_QUEUE_DELTA_COUNTS=10000 \
VS_FANOUT_QUEUE_OPS_PER_DELTA=1 \
VS_FANOUT_QUEUE_MAX_DEPTH=100000 \
VS_FANOUT_QUEUE_COMPARE_LEGACY=1 \
node --experimental-strip-types packages/core/bench/fanout-queue.bench.ts
```

Artifact: `packages/core/bench/.artifacts/fanout-queue-2026-05-17T17-12-34.136Z.json`

| deltas | mailbox offer ms | legacy drain/refill ms | queue depth | coalesced ops | speedup |
| -----: | ---------------: | ---------------------: | ----------: | ------------: | ------: |
| 10,000 |            18.71 |                 626.09 |           1 |        10,000 |  33.46x |

### 10M Raw Window Query

Command:

```bash
VS_QUERY_ENGINE_PAGE_SIZES=50,500,10000 \
VS_QUERY_ENGINE_RAW_ROWS=10000000 \
VS_QUERY_ENGINE_RAW_LEGACY_MAX_SIZE=10000 \
VS_QUERY_ENGINE_LEGACY_MAX_SIZE=0 \
VS_QUERY_ENGINE_ITERATIONS=1 \
node --experimental-strip-types packages/core/bench/query-engine.bench.ts
```

Artifact: `packages/core/bench/.artifacts/query-engine-2026-05-17T17-15-57.667Z.json`

| offset+limit window | current ms | legacy splice ms | speedup |
| ------------------: | ---------: | ---------------: | ------: |
|                  50 |     304.67 |           293.48 |   0.96x |
|                 500 |     273.15 |           250.90 |   0.92x |
|              10,000 |   1,135.42 |         1,187.61 |   1.05x |

This confirms the current threshold choice: small windows stay on the splice-maintained path, so
50/500 are effectively unchanged. The heap path is for larger windows; do not sell this as a 50-row
query speedup.

The same run also rechecked the earlier `diffVisibleRows` fix:

| page size | optimized ms | legacy O(n^2) ms | speedup |
| --------: | -----------: | ---------------: | ------: |
|        50 |         0.12 |             0.16 |   1.33x |
|       500 |         0.30 |             1.85 |   6.15x |
|    10,000 |         3.28 |           451.36 | 137.65x |

### Soak Profiles After Batching

10M raw direct worker soak:

```bash
pnpm run soak:10m
```

Artifact: `/private/tmp/view-server-worker-soak-10m-20260517T174202Z.json`

- rows: 10M
- raw subscriptions: 250
- grouped subscriptions: 0
- mutations: 10,000
- mutation batch size: 1,000
- total duration: 113.07s
- row generation: 590.23ms
- worker seed: 10.83s
- subscription setup: 99.84s
- mutation loop: 257.93ms across 10 batches
- mutation batch latency: p50 21.62ms, p95 55.69ms, p99 55.69ms, max 55.69ms
- cleanup: subscribers 0, active plans 0, active views 0, queue depth 0, lag 0
- events: 250 snapshots, 0 deltas, 2,500 status events
- retries/backpressure/reconnects: 0/0/0

Because active plans are skipped above 1M rows, the 10M subscriptions correctly remain stale after
mutation until cleanup: `maxSubscriptionLagVersionsAfterSettle=10000` and
`totalSubscriptionLagVersionsAfterSettle=2500000`. Cleanup releases that lag to zero. This is an
honest stale/catch-up capacity profile, not a live active-plan profile.

1M alpha raw+grouped direct worker soak:

```bash
VS_WORKER_SOAK_ROWS=1000000 \
VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=250 \
VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=20 \
VS_WORKER_SOAK_MUTATIONS=10000 \
VS_WORKER_SOAK_MUTATION_BATCH_SIZE=1000 \
VS_WORKER_SOAK_TIMEOUT_MS=900000 \
VS_WORKER_SOAK_SUMMARY_PATH=/private/tmp/view-server-worker-soak-1m-batched-20260517T1744.json \
pnpm --dir packages/core exec vitest run --config vitest.config.ts tests/worker-soak.test.ts
```

Artifact: `/private/tmp/view-server-worker-soak-1m-batched-20260517T1744.json`

- total duration: 24.79s
- row generation: 59.24ms
- worker seed: 825.25ms
- subscription setup: 12.29s
- mutation loop: 11.46s across 10 batches
- mutation batch latency: p50 952.84ms, p95 2,624.31ms, p99 2,624.31ms, max 2,624.31ms
- cleanup: subscribers 0, active plans 0, active views 0, queue depth 0, lag 0
- active plans before cleanup: 1 plan, 250 active views
- events: 540 snapshots, 1,610,250 deltas, 417,084 status events
- retries/backpressure/reconnects: 0/0/0

The 1M profile has no leak signal, but batch fanout across 250 active raw views plus 20 grouped
subscriptions still makes 1,000-row batch latency visible. Keep this profile in release notes until
active-view batch materialization is optimized further.

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
