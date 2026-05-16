# Fault Tolerance

This document describes expected production behavior for common failure modes.

## Kafka Or Source Down

Kafka and `EffectSource` failures mark the affected topic degraded. Readiness returns `ok: false`, while already-ingested worker memory remains authoritative for queries and subscriptions.

Kafka commits must happen only after memory ingest and mirror enqueue policy succeed. If ingest fails mid-batch, the batch is not committed and the topic stays on the last safe worker version.

Shutdown interrupts source fibers and should complete even when a source is stuck, paused, or failed.

## chDB Behind Or Down

chDB is mandatory for production runtime startup, but it is not the source of truth. Worker memory and the mutation log remain authoritative.

chDB supervision is per topic. Each topic worker owns its own chDB child process, so one topic's chDB crash or restart does not poison unrelated topics.

If chDB cannot initialize during startup, production runtime startup fails fast. If snapshot, grouped refresh, or mirror writes fail after startup, the topic reports degraded and falls back to worker memory. A later successful backend operation can return the topic to ready.

Snapshots are accepted from chDB only when the version fence proves correctness. Grouped chDB refreshes require an exact backend version match; stale or failed refreshes are discarded and recomputed from memory.

## WebSocket Reconnect Storm

Each subscription attempt has a request id. Clients ignore events whose request id does not match the current attempt, which protects reconnects from late frames from old streams.

On disconnect, server-side subscription state must drain:

- subscriber count returns to zero
- queue depth returns to zero
- active plan view counts are released
- active builds or grouped refreshes tied only to disconnected clients are discarded

## Slow Clients And Backpressure

Slow clients are isolated with bounded subscription queues. When a queue exceeds `maxQueueDepth`, the stream fails with typed `BackpressureExceeded`.

Generated clients retry by opening a fresh subscription attempt with a new request id. Old subscription state is cleaned up before the new attempt becomes live.

Delta coalescing can reduce physical queue depth, so operators should alert on logical lag metrics too:

- `maxSubscriptionLagVersions`
- `totalSubscriptionLagVersions`

## Shutdown

Shutdown behavior is explicit:

- readiness flips to `stopping`
- new external publish/query/subscribe operations are rejected
- websocket streams receive typed shutdown errors
- source fibers are interrupted
- topic workers drain and close
- each topic's chDB child process is terminated
- backend workers close without orphaning child processes

Health after shutdown should show no subscribers, queue depth, active plan builds, or grouped refresh work remaining.

## Memory Pressure

Primary guardrail: `maxActivePlans`. It limits retained shared raw active plans and avoids unbounded plan explosion.

`maxActivePlanEstimatedBytes` protects sorted-index reference memory. It is a lower-bound estimate, not a full heap budget.

For soak testing retained memory, run with `--expose-gc` and set:

```sh
VS_WORKER_SOAK_HEAP_GROWTH_THRESHOLD=0.25 pnpm --filter @view-server/core test -- tests/worker-soak.test.ts
```

Without the threshold, the soak reports heap/RSS and warns only. Keep it non-blocking in normal CI until the signal is stable on the target runners.
