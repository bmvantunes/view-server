# ADR 0006: Grouped Queries Use chDB Refresh Before Custom Incremental State

## Status

Accepted

## Context

Grouped queries and aggregates can be expensive at 1M+ rows and 50/100 aggregate shapes. A custom incremental grouped engine is a major correctness project.

## Decision

Grouped subscriptions use stale/debounced refresh. Refresh is chDB-first, worker-isolated, exact-version fenced, and falls back to cooperative memory if chDB is behind or failing. Custom incremental grouped state is deferred.

## Consequences

- Publish/delta/delete hot path marks grouped views stale instead of recomputing every mutation.
- Backend grouped refresh results are accepted only when `backendVersion === requestedVersion`.
- Stale grouped views are honest through `AsyncResult` waiting state.
- Grouped refresh can be slower wall-clock than raw live updates; it must not freeze topic-worker responsiveness.

## Alternatives Rejected

- Full grouped recompute on every mutation: rejected because it breaks firehose latency.
- Immediate custom grouped active view engine: rejected until raw active views and chDB refresh semantics are stable and benchmarked.
