# Capacity Soak

The default test suite keeps soak shapes small enough for local and CI feedback. Production capacity validation should use larger manual or nightly profiles, not PR CI.

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
- `node --expose-gc`
- a JSON summary under `/private/tmp/view-server-worker-soak-10m-<timestamp>.json`
- a heartbeat JSONL progress artifact next to the summary

It is intentionally not wired into GitHub Actions. The result is hardware-sensitive and can take a long time.

The soak logs and writes phase progress during long runs. The progress artifact defaults to:

```text
${VS_WORKER_SOAK_SUMMARY_PATH}.progress.jsonl
```

Each line includes the current phase, elapsed time, shape, and phase-specific counters. A healthy long run should emit progress for row generation, worker seed, subscriptions, mutation progress, settle, and cleanup at least every `VS_WORKER_SOAK_PROGRESS_INTERVAL_MS` milliseconds.

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
- mutation and settle duration
- final rows and worker version
- subscribers before/after cleanup
- active plan count/view count/fallback count
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
