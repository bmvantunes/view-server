# Architecture

View Server is a realtime UI projection engine, not a general database. It serves independent
topics as live materialized views over Effect RPC websocket streams.

## Load-Bearing Invariants

- `defineConfig` is the source of truth for topics, schemas, id fields, sources, limits, and client
  types.
- Topics are isolated vertical slices. There are no cross-topic joins, queries, or subscriptions.
- Worker memory is authoritative for live state.
- chDB is mandatory in production, but only as a per-topic snapshot accelerator.
- Every production topic owns one topic worker and one topic-owned chDB child process.
- Snapshot results are accepted only through version fencing.
- Subscriptions are always `snapshot -> delta/status`; there is no subscription mode.
- React hooks return Effect `AsyncResult` directly through `useLiveQuery`.

## Runtime Shape

```text
Kafka or Effect source
  -> RuntimeSourceGraph
  -> topic worker
  -> authoritative MutationStore
  -> active raw view or grouped refresh coordinator
  -> Effect RPC websocket + NDJSON
  -> React useLiveQuery AsyncResult
```

`RuntimeSourceGraph` owns startup mapping:

- configured topics to Kafka/Effect/no source mappings
- topics to topic workers
- topic workers to topic-owned snapshot backends
- Kafka topic verification before worker placement

The health topic `__view_server_health` is a private system topic. Operators read it through the
same query/subscription path as user topics, but user config and writes cannot define or mutate
`__` topics.

## Topic Workers

Each topic worker owns:

- `MutationStore`: authoritative rows, id index, mutation log, and version.
- `SnapshotReconciler`: version-fenced snapshot selection and memory fallback.
- `ActivePlanCoordinator`: shared raw active plans and build/admission lifecycle.
- `FanoutQueue`: bounded/coalesced subscriber queues and logical lag metrics.
- `SubscriptionRegistry`: live subscription state and cleanup.
- `GroupedRefreshCoordinator`: stale/debounced grouped refresh lifecycle.
- `WorkerHealthProjection`: topic-level health metrics.

The hot publish path updates worker memory first, emits fanout from memory, and never waits for chDB
flushes. chDB writes are serialized behind a contiguous backend version fence.

## Raw Queries

Raw non-grouped queries can use active plans. Equivalent `where + orderBy` plans share one sorted
index; offset, limit, and projected fields are per-subscription views over that plan.

If an active plan is too expensive or not ready, the subscription keeps the latest safe snapshot,
marks the live value stale/waiting, and catches up when the plan is ready. Massive topics can skip
auto-build through `activePlanAutoBuildMaxRows`.

## Grouped Queries

Grouped queries are not recomputed on every mutation. They use stale/debounced refresh semantics.
chDB grouped refresh runs in the topic-owned chDB child process and is accepted only when the child
returns the exact requested backend version. Cooperative memory refresh remains the safe fallback.

## Client Contract

The public React surface is:

```ts
const { ViewServerProvider, useLiveQuery } = createViewServerReact(config);
const result = useLiveQuery("orders", query);
```

`result` is `AsyncResult<LiveQueryValue<Row>, ViewServerError>`. Success values include `rows`,
`totalRows`, `status`, and `connection` metadata. Stale refresh/reconnect states keep previous rows
visible through `AsyncResult.success(value, { waiting: true })`.

## Operational Boundaries

Production runtime requires Node 26, pnpm 11.0.9, Effect v4 beta, Effect RPC websocket + NDJSON,
and chDB. Memory snapshot backends are private testing infrastructure only.

Use the ADRs for the decisions that should not drift:

- `docs/adr/0001-chdb-mandatory-production-runtime.md`
- `docs/adr/0002-per-topic-chdb-child-process.md`
- `docs/adr/0003-real-server-tests-with-isolation-id.md`
- `docs/adr/0004-react-asyncresult-live-query-api.md`
- `docs/adr/0005-active-raw-views-and-stale-catch-up.md`
- `docs/adr/0006-grouped-query-refresh-strategy.md`
