# View Server Context

This document is the compact domain map for future agents. `plan.md` is the historical source of truth for the initial build plan; this file describes the current implementation language and invariants.

## Domain Vocabulary

- **Topic**: A vertical data slice declared by `defineConfig`. A topic has one schema, one id field, one authoritative in-memory row store, and one chDB mirror. Topics are isolated from each other; there are no cross-topic queries, joins, or subscriptions in the product model.
- **Topic worker**: The owner of a topic's hot state. It owns memory, mutation log, active raw plans, grouped refresh state, fanout queues, and the per-topic chDB backend.
- **Worker memory**: The authoritative source of truth for live rows. chDB never becomes authoritative.
- **chDB mirror**: A per-topic snapshot accelerator. It is mandatory for production runtime, but if it is behind or failing the worker serves from memory.
- **Version fence**: A snapshot can use chDB only if backend version semantics prove correctness. Never emit a chDB-backed snapshot without checking the fence.
- **Mutation log**: The bounded replay log used to catch active plans or snapshots up to the worker version when the gap is covered.
- **Active raw plan**: A shared indexed raw-query plan keyed by where + orderBy inside one topic worker. Offset, limit, and fields are per-subscription windows over the shared plan.
- **Grouped refresh**: Grouped queries are stale/debounced refreshes, chDB-first and memory fallback. They are not yet custom incremental aggregate views.
- **Fanout queue**: Per-subscription event queue with physical depth, logical version lag, delta/status coalescing, and typed backpressure.
- **Live query**: Public React/client subscription model. Hooks return `AsyncResult<LiveQueryValue<Row>, ViewServerError>`.
- **Stale/catch-up**: The honest UX state while active plans or grouped refreshes are warming up. Previous rows stay visible with `AsyncResult` waiting state.

## Module Map

- `packages/core/src/config`: `defineConfig`, schema-derived topic typing, literal-string introspection, startup validation.
- `packages/core/src/protocol`: query protocol, runtime rows, subscription events, row keys, stable stringify.
- `packages/core/src/errors`: Effect `TaggedError` taxonomy.
- `packages/core/src/client`: typed RPC boundary, live query store, visible-row delta application, generated client shape.
- `packages/core/src/client/live-query-lifecycle.ts`: AsyncResult transition semantics for connecting, live, stale, reconnecting, failure with previous, and closed states.
- `packages/core/src/rpc`: Effect RPC group, websocket NDJSON transport, wire codecs.
- `packages/core/src/server`: runtime orchestration, HTTP health/readiness, runtime health projection, shutdown controller.
- `packages/core/src/worker/mutation-store.ts`: row array, id index, version counter, mutation log, replay.
- `packages/core/src/worker/snapshot-reconciler.ts`: version-fenced backend query, replay, memory fallback.
- `packages/core/src/worker/active-plan-coordinator.ts`: active raw plan cache, admission, build lifecycle, ref counts.
- `packages/core/src/worker/grouped-refresh-coordinator.ts`: grouped refresh sharing, in-flight state, dirty rescheduling.
- `packages/core/src/worker/query-planner.ts`: explicit query strategy classification for raw snapshots, active plans, grouped refresh, grouped accumulator, fallback, and limit rejection.
- `packages/core/src/worker/fanout-queue.ts`: queue pressure, coalescing, logical lag, backpressure.
- `packages/core/src/worker/subscription-registry.ts`: subscription ownership, duplicate ids, cleanup hooks.
- `packages/core/src/worker/worker-health-projection.ts`: worker metrics, pressure degradation, chDB health projection.
- `packages/core/src/worker/topic-worker-core.ts`: orchestration glue for the modules above.
- `packages/core/src/snapshot/chdb-backend.ts`: per-topic chDB backend mirror/restart policy and storage.
- `packages/core/src/snapshot/chdb-process-client.ts`: private child-process IPC, pending request failure, health, restart, and shutdown contract for the chDB worker.
- `packages/core/src/snapshot/chdb-sql-mirror.ts`: private chDB table mirror for column inference, table creation, append-only version/tombstone inserts, and teardown.
- `packages/core/src/snapshot/chdb-sql-compiler.ts`: internal SQL compiler contract for chDB snapshots.
- `packages/react`: factory-created provider/hooks; browser package must stay server-dependency-free.
- `packages/testing`: test helpers only. Production correctness should be proven through real server tests where possible.

## Load-Bearing Invariants

- `defineConfig` is the only topic source of truth. Worker threads import the user config module by URL/path and select their topic.
- Topic boundaries are hard runtime boundaries. No query or subscription may span topics, which is what lets each topic worker own its own memory, active plans, grouped refresh state, and chDB mirror independently.
- Per-topic worker + per-topic chDB is therefore an architectural invariant, not an incidental optimization. If a future design adds cross-topic reads, it must introduce an explicit new architecture instead of quietly sharing the current chDB mirrors.
- No user topic may start with `__`; system topics are private.
- Production runtime uses chDB. Memory backend is internal test infrastructure only.
- Worker memory is authoritative; chDB is a snapshot accelerator.
- `backendVersion` means all mutations through version N are durably represented in the backend, not the highest version seen.
- If chDB is behind and replay is unavailable, fall back to memory.
- If active plan build is too expensive or skipped by admission, subscription still receives an initial snapshot and then stale/catch-up signals.
- Grouped queries must not full-recompute on every mutation in the publish path.
- Every subscription event includes `requestId`; clients ignore stale request ids.
- `totalRows` is part of every subscription payload.
- Backpressure is a typed Effect error and must clean up old subscription state.
- React uses `useLiveQuery`; there is no legacy `useSubscription` compatibility API.

## Production Architecture

```text
Kafka/source -> topic worker -> authoritative memory + mutation log
                         |-> per-topic chDB child/mirror
                         |-> active raw plans
                         |-> grouped refresh coordinator
                         |-> Effect RPC websocket NDJSON clients
```

One chDB child belongs to one topic worker. This is valid because topics never query across each other. The topic worker and its chDB child are a deployable/scalable slice, and no global chDB process is needed to coordinate joins or multi-topic subscriptions. If a reader is looking for a shared chDB coordinator, the answer is deliberately "no" under the current topic-isolated product model.

## Testing Philosophy

- Prefer E2E/browser tests for user-visible behavior.
- Use real Effect RPC websocket tests for transport behavior.
- Use `@effect/vitest`; do not import from `vitest`, `node:test`, or `node:assert`.
- Keep browser tests in Vitest browser mode with Playwright.
- App/UI tests should use a real view-server with `isolationId`; in-memory helpers are for library tests.
- Do not fake coverage or add ignore comments.

## Performance Targets

- Hot publish/delta/delete path should stay sub-millisecond p99 in active raw view benchmarks at 1M-row scale.
- 10M capacity runs are manual/nightly, not normal CI gates.
- Long soaks must emit heartbeat progress and summary artifacts.
- Active plan build is admission-controlled and cooperative; it must not freeze worker responsiveness.
- Grouped refresh is chDB-first, worker-isolated, exact-version fenced, and allowed to be stale while catching up.

## Forbidden Shortcuts

- No public memory-backend production choice.
- No legacy `/ws` protocol.
- No subscription `mode` field.
- No casts except `as const`.
- No `console.*`; use Effect logging and spans.
- No server-only imports in browser-facing packages.
- No chDB snapshot emission without version fencing.
- No grouped-query custom incremental engine until raw active views and chDB refresh semantics are stable.
