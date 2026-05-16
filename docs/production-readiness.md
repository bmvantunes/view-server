# Production Readiness

This is the deployment checklist for the current alpha runtime. Worker memory is authoritative. chDB is mandatory for production startup, and chDB plus active plans are accelerators behind version fences.

## Runtime Versions

- Node.js: 26
- pnpm: 11.0.9
- Effect: v4 beta from the workspace catalog

Use Corepack deterministically:

```bash
corepack enable
corepack prepare pnpm@11.0.9 --activate
pnpm install --frozen-lockfile
```

## Startup Validation

Production startup should load the config module from env and fail before opening traffic:

```bash
KAFKA_BROKERS=broker-1:9092,broker-2:9092
VIEW_SERVER_PORT=3000
VIEW_SERVER_RPC_PATH=/rpc
VIEW_SERVER_CONFIG_MODULE=./view-server.config.ts
```

The production loader validates:

- chDB can initialize
- required env vars
- config module export shape
- Kafka source topics through the runtime topic verifier
- no user topic starts with `__`
- schema-derived id fields when the schema shape exposes fields
- worker limits are sane
- query limit config is sane

Private system topics are reserved. User topics must not start with `__`.

There is no public runtime config for choosing a memory snapshot backend. Memory is reserved for internal tests and browser package tests.

## Memory Sizing

Start with conservative worker limits:

```ts
worker: {
  maxQueueDepth: 512,
  mutationLogSize: 100_000,
  deltaCoalescing: true,
  maxActivePlans: 64,
  maxActivePlanEstimatedBytes: 512 * 1024 * 1024,
  activePlanBuildConcurrency: 1,
  groupedRefreshDebounceMs: 100,
}
```

`maxActivePlans` is the primary guardrail. `maxActivePlanEstimatedBytes` is a lower-bound sorted-index estimate, not a full heap budget. Keep process heap headroom for row memory, active plan maps, RPC buffers, one chDB child process per active topic, and Kafka decode bursts.

## Nginx Websocket

Minimal reverse proxy settings:

```nginx
location /rpc {
  proxy_pass http://view_server_upstream;
  proxy_http_version 1.1;
  proxy_set_header Upgrade $http_upgrade;
  proxy_set_header Connection "upgrade";
  proxy_set_header Host $host;
  proxy_read_timeout 3600s;
  proxy_send_timeout 3600s;
  proxy_buffering off;
}

location /health {
  proxy_pass http://view_server_upstream;
}

location /ready {
  proxy_pass http://view_server_upstream;
}
```

Do not buffer the NDJSON websocket stream.

## Health And Readiness

- `/health` reports liveness and current runtime metrics.
- `/ready` returns non-200 when the runtime is degraded or shutting down.
- During shutdown, readiness flips false immediately.
- New public query, subscribe, publish, deltaPublish, and deleteById calls fail with typed `ServerShutdown`.
- Existing subscription streams are closed with typed `ServerShutdown`.

Shutdown order:

1. mark runtime closing
2. sync health/readiness as stopping
3. stop source fibers and Kafka consumers
4. fail open subscription queues with `ServerShutdown`
5. interrupt background active/grouped work
6. close topic workers and snapshot backends

## Query Guardrails

Default public query limits:

- `maxPageSize`: 50
- `maxAggregateCount`: 32
- `maxGroupByFields`: 8
- `maxFilterDepth`: 8
- `maxFilterConditions`: 64

Set tighter limits for public user-defined grids. Limit failures are typed `InvalidQuery` errors.

## Metrics To Alert On

Alert or page on sustained:

- `/ready` non-200
- topic `status = degraded`
- `queueDepth > 0`
- `maxSubscriptionLagVersions > 0`
- `totalSubscriptionLagVersions` rising
- `activePlanBuildQueueDepth` or `activePlanPendingCount` rising
- unexpected `activePlanFallbackCount`
- `activePlanCount` near `maxActivePlans`
- Kafka `kafkaLagTotal` or `kafkaLagMax` rising
- chDB snapshot failures or exact-version misses in spans

Stale status events are advisory. They are coalesced and are not a per-mutation event stream.

## Benchmark Commands

CI smoke:

```bash
vp run core#bench:ci-smoke
vp run core#bench:compare
```

Large raw active-plan responsiveness:

```bash
VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS=1000000 \
VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS=1000 \
VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION=publish \
node --experimental-strip-types packages/core/bench/active-plan-responsiveness.bench.ts
```

Grouped refresh overlap:

```bash
VS_GROUPED_REFRESH_OVERLAP_ROWS=1000000 \
VS_GROUPED_REFRESH_OVERLAP_OPERATIONS=20 \
VS_GROUPED_REFRESH_OVERLAP_AGGREGATES=100 \
VS_GROUPED_REFRESH_OVERLAP_BACKEND=chdb \
node --experimental-strip-types packages/core/bench/grouped-refresh-overlap.bench.ts
```

Soak:

```bash
VS_WORKER_SOAK_ROWS=1000000 \
VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=250 \
VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=20 \
VS_WORKER_SOAK_MUTATIONS=10000 \
VS_WORKER_SOAK_TIMEOUT_MS=900000 \
pnpm exec vitest run --config packages/core/vitest.config.ts packages/core/tests/worker-soak.test.ts
```

Manual/nightly 10M raw capacity soak:

```bash
pnpm run soak:10m
```

Keep this out of normal CI. The script defaults grouped subscriptions to `0`; use the chDB grouped refresh overlap benchmark for 10M grouped capacity. Save and compare the JSON summary artifact before serious production rollout. See `docs/capacity-soak.md`.

## Safe Rollout Checklist

1. Run `vp check`.
2. Run focused startup/security/shutdown tests.
3. Run `vp run core#test`.
4. Run `vp run -r build`.
5. Verify Kafka topic verifier succeeds in the target environment.
6. Verify `/ready` is 200 before routing traffic.
7. Start with low `maxActivePlans` and watch fallback/build metrics.
8. Confirm websocket proxy does not buffer NDJSON.
9. Roll one instance first and watch queue depth, subscription lag, Kafka lag, per-topic chDB process health, and chDB snapshot failures.
10. Exercise shutdown and confirm `/ready` flips non-200 before the instance leaves service.
