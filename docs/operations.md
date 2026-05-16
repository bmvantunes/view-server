# Operational Semantics

View Server is a realtime UI projection engine. Worker memory is authoritative. chDB is a snapshot accelerator. The websocket transport is Effect RPC over NDJSON.

## Subscription Contract

A subscription always emits:

```text
snapshot -> delta/status events until unsubscribe
```

There is no subscription `mode`. Use `client.query(topic, query)` for one-shot reads.

Every snapshot, delta, and status event carries a `requestId`. The generated client ignores events whose `requestId` does not match the current subscription attempt, which protects reconnects and query changes from stale in-flight websocket frames.

Every subscription success value includes:

- `rows`: the current visible window.
- `totalRows`: total matching rows or grouped result rows.
- `status`: `connecting`, `live`, `reconnecting`, or `stale`.
- `connection`: connected flag, attempt count, and connection timestamps.

React hooks return `AsyncResult` directly:

- `AsyncResult.initial(true)`: no snapshot yet.
- `AsyncResult.success(value)`: live view.
- `AsyncResult.success(value, { waiting: true })`: stale data is still visible while the server catches up.
- `AsyncResult.failure(...)`: typed failure, optionally with previous success data available through `AsyncResult.value`.

## Version Fencing

Snapshots are never trusted blindly. The worker captures a target worker version before snapshot/query execution.

- If the snapshot backend is exactly at the target version, the result is accepted.
- If a replayable backend result is behind, the worker replays mutation-log entries up to the target version.
- If the backend is too far behind, missing replay rows, or failing, the worker falls back to authoritative memory.
- chDB grouped refresh results are accepted only when `backendVersion === requestedVersion`.

This is the fence that prevents snapshot/delta gaps.

## chDB Role

chDB is mandatory for production runtime startup and accelerates initial snapshots, one-shot queries, and grouped refresh snapshots. It is not the source of truth.

Each topic owns its own chDB child process through its topic worker backend. There is no shared global chDB process for all topics. This keeps IPC queues, memory accounting, and failure boundaries per topic.

The worker hot path never waits for chDB flushes. chDB writes are serialized behind a contiguous backend-version fence, and grouped chDB refresh runs off the topic worker in the topic-owned chDB child process. If chDB cannot initialize at startup, production runtime startup fails fast. If one topic's chDB process exits or falls behind after startup, that topic reports degraded while worker memory remains authoritative; other topics stay ready if their own workers/backends are healthy.

## Active Raw Views

Raw non-grouped queries can use shared active plans keyed by equivalent `where + orderBy` plans. Offset/limit windows are cheap views over the shared plan.

Important metrics:

- `activePlanCount`: retained shared active plans.
- `activeViewCount`: subscriptions using active plans.
- `activePlanRows`: indexed rows across all retained active plans.
- `activePlanIndexEstimatedBytes`: lower-bound estimate for sorted index references.
- `activePlanBuildQueueDepth`, `activePlanBuildingCount`, `activePlanPendingCount`: startup pressure.
- `activePlanFallbackCount`: subscriptions using memory recompute fallback because guardrails rejected a new active plan.

Recommended guardrail priority:

1. Use `maxActivePlans` as the primary production guardrail.
2. Treat `maxActivePlanEstimatedBytes` as retained-index protection, not a full heap budget.
3. Watch build queue/pending metrics during user-driven query changes.

## Grouped Queries

Grouped subscriptions intentionally do not recompute on every mutation. They emit stale status events and refresh on a debounce/catch-up path. The result remains correct because stale refreshes are discarded when newer relevant mutations arrive.

This is alpha-grade for realtime grouped analytics, not a custom incremental aggregate engine yet. chDB grouped refresh is the preferred accelerator, with cooperative memory fallback.

## Health And Readiness

The websocket server also serves:

- `/health`
- `/ready`

The health topic `__view_server_health` is served through the same public live-query path as user topics. The metrics UI should use `useLiveQuery` instead of a special polling API.

Operational red flags:

- `queueDepth > 0` for sustained periods.
- `maxSubscriptionLagVersions > 0` after settle.
- `activePlanBuildQueueDepth` or `activePlanPendingCount` climbing under query churn.
- `activePlanFallbackCount > 0` without an intentional guardrail hit.
- `chdbStatus != ready`, `chdbPendingRequests` climbing, or `chdbRestarts` increasing unexpectedly.
- `chdbBackendVersion` falling materially behind the topic worker version.
- Kafka `kafkaLagTotal` or `kafkaLagMax` increasing.
- Topic `status = degraded`.

## Benchmark Summaries

Benchmark smoke artifacts are JSON and can be summarized in CI. See `docs/benchmarks.md`.

Useful local commands:

```bash
vp run core#bench:ci-smoke
vp run core#bench:compare
vp run core#bench:refresh-baselines
```

For large leak checks, use the soak test:

```bash
VS_WORKER_SOAK_ROWS=1000000 \
VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=250 \
VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=20 \
VS_WORKER_SOAK_MUTATIONS=10000 \
VS_WORKER_SOAK_TIMEOUT_MS=900000 \
VS_WORKER_SOAK_SUMMARY_PATH=/private/tmp/view-server-worker-soak-1m-summary.json \
pnpm exec vitest run --config packages/core/vitest.config.ts packages/core/tests/worker-soak.test.ts
```

Use a later forced-GC variant with `node --expose-gc` when retained heap is the question.

## Troubleshooting

If a subscription appears stuck:

1. Check `/ready` and `/health`.
2. Inspect `queueDepth`, `maxSubscriptionLagVersions`, and `totalSubscriptionLagVersions`.
3. Check whether `activePlanBuildQueueDepth` or grouped refresh work is pending.
4. Confirm the client is not rendering `AsyncResult.success(..., { waiting: true })` as a fatal error.
5. Check websocket reconnect attempts and request-id changes.

If chDB seems stale:

1. Verify the process passed chDB startup initialization.
2. Check `chdbStatus`, `chdbPid`, `chdbPendingRequests`, `chdbLastError`, and `chdbBackendVersion` in `/health` or `__view_server_health`.
3. Check backend version annotations/spans.
4. Confirm grouped chDB refresh is exact-version accepted, not silently trusted.

If many users create unique sort/filter plans:

1. Lower `maxActivePlans`.
2. Watch `activePlanFallbackCount`.
3. Use benchmark artifacts before changing the active index data structure.
