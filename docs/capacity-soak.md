# Capacity Soak

The default test suite keeps soak shapes small enough for local and CI feedback. Production capacity validation should use larger manual or nightly profiles, not PR CI.

See `docs/capacity-matrix.md` for the explicit 100k, 1M, and 10M profile commands and latest recorded artifacts.

## Runtime WebSocket Soak

Use this when validating the operational surface that users actually touch: real websocket RPC
clients, production runtime wiring, chDB mirrors, raw subscriptions, grouped subscriptions,
mid-load disconnect/reconnect, mixed mutations, health, and cleanup.

```bash
pnpm --dir packages/core exec vitest run --config vitest.config.ts tests/runtime-websocket-soak.test.ts
```

The default profile is intentionally small enough for regular local runs. Scale it manually before a
release candidate:

```bash
VS_RUNTIME_WEBSOCKET_SOAK_ROWS=10000 \
VS_RUNTIME_WEBSOCKET_SOAK_RAW_CLIENTS=80 \
VS_RUNTIME_WEBSOCKET_SOAK_GROUPED_CLIENTS=20 \
VS_RUNTIME_WEBSOCKET_SOAK_RECONNECT_CLIENTS=50 \
VS_RUNTIME_WEBSOCKET_SOAK_MUTATIONS=1000 \
VS_RUNTIME_WEBSOCKET_SOAK_SUMMARY_PATH=/private/tmp/view-server-runtime-websocket-soak.json \
pnpm --dir packages/core exec vitest run --config vitest.config.ts tests/runtime-websocket-soak.test.ts
```

The summary artifact records subscription setup time, mutation latency, reconnect count, event
counts, approximate snapshot/delta/status application payload bytes, final health, chDB pending
request count, queue depth, subscription lag, active-plan cleanup state, websocket fanout metrics,
event-loop delay, worker mutation timing buckets, and the top 10 slowest mutation samples with
health/fanout/timing snapshots. Keep this out of normal PR CI at large sizes; the default test
already exercises the transport lifecycle without turning every push into a capacity run.

For regression-visible artifacts, run the benchmark wrapper instead of invoking the test directly:

```bash
VS_BENCH_BLOCKING=0 \
node --experimental-strip-types packages/core/bench/benchmark-profile.ts --profile firehose-ci --compare
```

The `runtime-websocket-soak-100-client` entry is report-only. It compares mutation p50/p95/p99/max,
retry/backpressure counts, cleanup leaks, observed queue and subscription lag, observed chDB pending
requests/version lag, reconnect count, worker fanout/delta-construction/enqueue timing, websocket
fanout encode/write/protocol queue metrics, event-loop delay, payload byte counts, and top slow
sample count. The smaller `ci-smoke` entry blocks only deterministic cleanup/retry/backpressure
invariants.

The websocket server uses Effect RPC over NDJSON and coalesces server responses per client into at
most one write per scheduler tick. This preserves request ordering and request-id stale-event guards
while reducing write scheduling overhead. The current 100-client shape has one subscription per
websocket client, so batching is modest (`maxBatchMessages=2`); multi-subscription clients should see
larger batching wins. `websocketTotalBytes` is the encoded batched RPC wire byte count. The
snapshot/delta/status payload byte fields are approximate application-event JSON sizes and are useful
for payload mix, not exact wire accounting.

The runtime websocket soak can run with `VS_RUNTIME_WEBSOCKET_TRANSPORT_MODE=in-process` or
`isolated`. `in-process` uses the normal Effect HTTP websocket route. `isolated` moves websocket
accept/NDJSON encode/write/coalescing into a dedicated Node worker while the main runtime still owns
Effect RPC handlers, query semantics, topic workers, and chDB. Soak summaries include
`transportMode` plus transport-worker event-loop delay metrics so runtime event-loop pressure and
socket/write pressure can be compared separately.

Latest local runtime websocket soak results, May 18 2026:

| transport  |   rows | websocket clients | raw/grouped | mutations | reconnects | duration | mutation p50/p95/p99/max         | cleanup                                                          |
| ---------- | -----: | ----------------: | ----------: | --------: | ---------: | -------: | -------------------------------- | ---------------------------------------------------------------- |
| in-process |  1,000 |                15 |        12/3 |       120 |         10 |    0.80s | 1.95 / 5.02 / 6.40 / 31.09ms     | subscribers=0, activePlans=0, queueDepth=0, lag=0, chdbPending=0 |
| in-process | 10,000 |               100 |       80/20 |     1,000 |         50 |   13.66s | 7.07 / 23.77 / 24.50 / 137.6ms   | subscribers=0, activePlans=0, queueDepth=0, lag=0, chdbPending=0 |
| in-process | 10,000 |               250 |      200/50 |     1,000 |         50 |   33.15s | 17.68 / 60.06 / 62.28 / 256.4ms  | subscribers=0, activePlans=0, queueDepth=0, lag=0, chdbPending=0 |
| isolated   | 10,000 |               100 |       80/20 |     1,000 |         50 |   13.17s | 6.84 / 24.53 / 33.06 / 167.4ms   | subscribers=0, activePlans=0, queueDepth=0, lag=0, chdbPending=0 |
| isolated   | 10,000 |               250 |      200/50 |     1,000 |         50 |   29.03s | 12.97 / 56.01 / 70.17 / 331.6ms  | subscribers=0, activePlans=0, queueDepth=0, lag=0, chdbPending=0 |
| isolated   | 10,000 |               500 |     400/100 |     1,000 |         50 |   56.93s | 25.24 / 111.01 / 142.4 / 436.5ms | subscribers=0, activePlans=0, queueDepth=0, lag=0, chdbPending=0 |

All runs reported `retries=0`, `backpressureErrors=0`, and `chdbBackendVersionBeforeCleanup`
matching the worker version. The manual profile intentionally leaves grouped subscriptions stale
before cleanup while refreshes are advisory; after unsubscribe, lag and queues must return to zero.

Fanout and payload summary from the latest manual profiles:

| clients | RPC messages | batched writes | wire bytes | app payload bytes | max queued msgs/bytes | max batch | encode total/max |  write total/max |
| ------: | -----------: | -------------: | ---------: | ----------------: | --------------------: | --------: | ---------------: | ---------------: |
|     100 |      101,330 |        101,178 |     77.0MB |            72.0MB |             2 / 3.1KB |         2 | 140.6ms / 0.03ms | 557.6ms / 0.23ms |
|     250 |      251,799 |        251,497 |    192.2MB |           180.0MB |             2 / 5.4KB |         2 | 327.9ms / 0.09ms |   1.48s / 0.32ms |

Payload mix in the 100-client profile was about 0.2MB snapshots, 68.7MB deltas, and 3.0MB status
events. In the 250-client profile it was about 0.4MB snapshots, 172.0MB deltas, and 7.6MB status
events.

Worker/protocol/event-loop attribution from the latest manual profiles:

| transport  | clients | worker fanout max | stream offer max | protocol queue-wait max | runtime event-loop p99/max | transport event-loop p99/max |
| ---------- | ------: | ----------------: | ---------------: | ----------------------: | -------------------------: | ---------------------------: |
| in-process |     100 |            1.91ms |           0.46ms |                   122ms |           20.20 / 112.66ms |                            - |
| in-process |     250 |            4.39ms |           2.36ms |                   249ms |           34.90 / 224.53ms |                            - |
| isolated   |     100 |            1.90ms |           0.45ms |                    35ms |           22.74 / 132.38ms |              12.26 / 12.89ms |
| isolated   |     250 |           19.50ms |          17.25ms |                   280ms |           39.42 / 290.98ms |              12.84 / 14.24ms |
| isolated   |     500 |          341.81ms |         143.24ms |                   391ms |           36.67 / 319.03ms |              12.80 / 14.34ms |

Top slow samples still point away from query math and websocket encode/write cost. In-process
250-client runs are dominated by runtime event-loop delay and protocol queue wait. The isolated
transport worker cleanly separates socket pressure (`transportEventLoopDelayMaxMs` stayed around
14ms), but it is not a default latency win on this machine: 100 clients reduced protocol queue-wait
max, 250 clients stayed roughly comparable, and 500 clients exposed runtime-side subscription fanout
and stream-offer pressure before the transport worker. Treat isolated transport as an operational
profiling/scaling mode for now, not as proof that socket isolation alone fixes high-client tail
latency.

The summary currently records `groupedRefreshCountsAvailable=false`; grouped stale pressure is still
visible through subscription lag/status counts, but grouped refresh pending/in-flight counters are
not exposed as first-class health fields yet.

## 10M Raw Worker Soak

Run this before a serious production rollout, after the normal RC checklist is green:

```bash
pnpm run soak:10m
```

The script runs `packages/core/tests/worker-soak.test.ts` with:

- `VS_WORKER_SOAK_ROWS=10000000`
- `VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=250`
- `VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=0`
- `VS_WORKER_SOAK_MUTATIONS=10000`
- `VS_WORKER_SOAK_MUTATION_BATCH_SIZE=1000`
- `VS_WORKER_SOAK_ACTIVE_PLAN_AUTO_BUILD_MAX_ROWS=1000000`
- `node --expose-gc`
- a JSON summary under `/private/tmp/view-server-worker-soak-10m-<timestamp>.json`
- a heartbeat JSONL progress artifact next to the summary

It is intentionally not wired into GitHub Actions. The result is hardware-sensitive and can take a long time.

The soak logs and writes phase progress during long runs. The progress artifact defaults to:

```text
${VS_WORKER_SOAK_SUMMARY_PATH}.progress.jsonl
```

Each line includes the current phase, elapsed time, shape, and phase-specific counters. A healthy long run should emit progress for row generation, worker seed, subscriptions, mutation progress, settle, and cleanup at least every `VS_WORKER_SOAK_PROGRESS_INTERVAL_MS` milliseconds.

The 10M profile intentionally uses the runtime active-plan admission policy. When a topic has more rows than `VS_WORKER_SOAK_ACTIVE_PLAN_AUTO_BUILD_MAX_ROWS`, raw subscriptions still receive their initial snapshot, but automatic active-plan construction is skipped and later mutations mark the view stale instead of building a 10M-row plan on subscription setup. The summary exposes `activePlanAutoBuildSkippedCountBeforeCleanup` so this is visible rather than hidden as a test-only switch.

The direct worker soak disables the test-only memory snapshot accelerator so the hot mutation phase is not dominated by a duplicate in-process backend mirror scanning 10M rows. Production runtime still requires chDB; validate chDB ingestion and grouped refresh with the dedicated chDB benchmarks below.

When `VS_WORKER_SOAK_MUTATION_BATCH_SIZE > 1`, mutation latency fields are batch latency fields. The default 10M profile uses 10 batches of 1,000 mutations to exercise the firehose batch path.

Use these timeout expectations as rough operator guidance, not SLAs:

- Row generation should emit chunk heartbeats every `VS_WORKER_SOAK_ROW_GENERATION_CHUNK_SIZE` rows.
- Worker seed can take tens of seconds at 10M rows depending on heap and CPU.
- Subscription setup should never be silent longer than the heartbeat interval; it reports started/completed counts and active-plan build metrics.
- Mutation run should report latency percentiles every 100 mutations.
- Settle reports queue, lag, active-plan, and skipped-plan metrics until stable.
- Cleanup should return subscribers, active views, build queues, and skipped-plan counts to zero.

Grouped subscriptions default to `0` here on purpose. This worker soak uses the direct in-process worker and memory fallback path. That is useful for raw active-view lag/cleanup checks, but it is not the production grouped-query architecture. Production grouped refresh is chDB-backed and worker-isolated.

Do not use this script with `VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=20` as the default 10M production signal. That shape stress-tests 10M grouped memory fallback, can run for over an hour, and can fail with grouped subscriptions still stale even when raw queues are empty.

## 10M Grouped Capacity

Use the chDB grouped refresh benchmark for grouped capacity:

```bash
VS_GROUPED_REFRESH_OVERLAP_ROWS=10000000 \
VS_GROUPED_REFRESH_OVERLAP_OPERATIONS=20 \
VS_GROUPED_REFRESH_OVERLAP_AGGREGATES=100 \
VS_GROUPED_REFRESH_OVERLAP_BACKEND=chdb \
node --experimental-strip-types packages/core/bench/grouped-refresh-overlap.bench.ts
```

That benchmark is the right path for grouped publish responsiveness because it exercises the chDB-first grouped refresh policy instead of memory fallback.

## Overrides

Use env vars when testing a larger mutation window or a different machine budget:

```bash
VS_WORKER_SOAK_MUTATIONS=100000 \
VS_WORKER_SOAK_MAX_OLD_SPACE_MB=32768 \
VS_WORKER_SOAK_SUMMARY_PATH=/private/tmp/view-server-worker-soak-10m-prod-box.json \
pnpm run soak:10m
```

Relevant env vars:

- `VS_WORKER_SOAK_ROWS`
- `VS_WORKER_SOAK_RAW_SUBSCRIPTIONS`
- `VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS`
- `VS_WORKER_SOAK_MUTATIONS`
- `VS_WORKER_SOAK_RAW_PAGE_CYCLE`
- `VS_WORKER_SOAK_GROUPED_DEBOUNCE_MS`
- `VS_WORKER_SOAK_ACTIVE_PLAN_AUTO_BUILD_MAX_ROWS`
- `VS_WORKER_SOAK_TIMEOUT_MS`
- `VS_WORKER_SOAK_MAX_OLD_SPACE_MB`
- `VS_WORKER_SOAK_SUMMARY_PATH`
- `VS_WORKER_SOAK_PROGRESS_PATH`
- `VS_WORKER_SOAK_PROGRESS_INTERVAL_MS`
- `VS_WORKER_SOAK_ROW_GENERATION_CHUNK_SIZE`
- `VS_WORKER_SOAK_HEAP_GROWTH_THRESHOLD`

## What To Record

Keep the JSON artifact with the release notes or rollout ticket. Track:

- startup/load time
- row generation, worker seed, subscription setup, mutation loop, settle, and cleanup durations
- mutation latency p50, p95, p99, and max
- mutation and settle duration
- final rows and worker version
- subscribers before/after cleanup
- active plan count/view count/fallback count
- active-plan auto-build skipped count
- active plan build queue/building/pending counts
- queue depth
- max and total subscription lag
- heap and RSS before, loaded, and after cleanup
- forced-GC retained heap ratio when `--expose-gc` is available
- snapshot, delta, and stale-status event counts
- retries, backpressure errors, and reconnects

## Scope

This profile validates the worker state machine, active raw views, lag accounting, and cleanup at 10M rows. It complements, but does not replace:

- chDB child supervision and fault tests
- chDB grouped refresh overlap benchmarks
- real websocket disconnect/backpressure tests
- deployment smoke with production chDB startup
- external consumer smoke from packed tarballs

For full production capacity signoff, run this 10M profile plus the deployment and chDB fault validations from `docs/release-checklist.md`.
