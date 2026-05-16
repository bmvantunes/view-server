# ADR 0004: React Hooks Return AsyncResult Live Query State

## Status

Accepted

## Context

The old `{ data, error, loading }` shape could not represent stale data while reconnecting or catching up without side channels.

## Decision

React exposes `useLiveQuery(topic, query, initialData?)` returning `AsyncResult.AsyncResult<LiveQueryValue<Row>, ViewServerError>`. The success value contains rows, totalRows, and connection/status metadata.

## Consequences

- There is no `useSubscription` compatibility alias.
- `AsyncResult.waiting` represents reconnecting/stale-refresh states while preserving previous rows.
- `initialData` uses `{ rows, totalRows }`, not rows-only.
- Metrics UI uses the same public hook path as external users.

## Alternatives Rejected

- Parallel legacy hook shape: rejected because it duplicates state semantics.
- Rows-only `initialData`: rejected because SSR/TanStack hydration needs exact `totalRows` before websocket hydration.
