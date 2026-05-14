# Topic Worker State Machine Review

This checklist is for reviewing `packages/core/src/worker/topic-worker-core.ts` after the active-view, grouped-refresh, and chDB snapshot work.

## Core Invariants

- Worker memory is authoritative. Snapshot backends are accelerators only.
- `version` advances only after the in-memory mutation is applied.
- Snapshot backend results are accepted only when version-fenced:
  - exact backend version, or
  - replayable backend version covered by the mutation log, or
  - authoritative memory fallback.
- Active raw plans may lag while building, but activation must catch up through the mutation log before becoming visible.
- If active-plan catch-up is not covered, the subscription must refresh from memory and fall back safely.
- Grouped refresh results are installed only if no newer relevant mutation dirtied the subscription while the refresh was computing.
- Subscription queues must either deliver ordered version-contiguous events, coalesce safely, or fail with typed `BackpressureExceeded`.
- `requestId` is the subscription attempt id. Client/store code must ignore stale events from older request ids.
- Unsubscribe/finalizers must release subscribers, active views, pending active-plan build references, queue depth, and lag.

## Hardening Coverage

- chDB/backend behind/fails/recovers:
  - `packages/core/tests/worker-version-fence.test.ts`
  - `packages/core/tests/chdb-snapshot-backend.test.ts`
- websocket disconnect/reconnect during stale state:
  - `packages/core/tests/rpc-websocket.test.ts`
- unsubscribe while active/grouped refresh is in flight:
  - `packages/core/tests/rpc-inmemory.test.ts`
- mutation-log gap during active build and stale grouped refresh invalidation:
  - `packages/core/tests/rpc-inmemory.test.ts`
- backpressure during stale status and catch-up snapshot:
  - `packages/core/tests/rpc-inmemory.test.ts`
- mixed-load leak soak:
  - `packages/core/tests/worker-soak.test.ts`

## Soak Shapes

The CI-safe default is intentionally small. To run the production-shaped soak manually:

```bash
VS_WORKER_SOAK_ROWS=1000000 \
VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=250 \
VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=20 \
VS_WORKER_SOAK_MUTATIONS=10000 \
VS_WORKER_SOAK_TIMEOUT_MS=900000 \
pnpm exec vitest run --config vitest.config.ts tests/worker-soak.test.ts
```

The 1M/250/20/10k shape is intentionally heavy. On the current local machine it completed in roughly 10 minutes after grouped refresh work was shared by grouped-query key.

The soak asserts:

- no queued lag leak,
- no subscriber leak after unsubscribe,
- no active plan/view/index leak after unsubscribe,
- no active-plan build reference leak,
- retained active index bytes return to zero,
- heap drops back under the loaded-state high-water mark when `globalThis.gc` is available.
