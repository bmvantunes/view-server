# Release Checklist

This checklist is for a release-candidate package cut. It is intentionally broader than normal PR validation because it checks package shape, browser imports, type-level API contracts, benchmark visibility, and runtime hardening.

## Required Local Environment

- Node 26
- pnpm 11.0.9 through Corepack
- Effect language service diagnostics enabled from the repo `tsconfig.json`

Recommended setup:

```bash
corepack enable
corepack prepare pnpm@11.0.9 --activate
pnpm install --frozen-lockfile
```

## Validation Commands

Run from the repo root:

```bash
vp check
pnpm exec effect-language-service diagnostics --project packages/core/tsconfig.json --format text --severity error
vp run core#test
vp run react#test
vp run testing#test
vp run -r test
vp run -r build
pnpm run pack:dry-run
```

The public API and package-shape checks live in:

- `packages/core/tests/public-api-smoke.test.ts`
- `packages/core/tests/public-api-types.test.ts`
- `packages/core/tests/package-audit.test.ts`
- `packages/react/tests/public-api.browser.tsx`
- `packages/react/tests/public-api-types.browser.tsx`
- `packages/testing/tests/public-api.browser.tsx`

## Benchmark Commands

Smoke comparison used by CI:

```bash
vp run core#bench:compare
```

Refresh checked-in smoke baselines manually:

```bash
vp run core#bench:refresh-baselines
```

Large raw active-view responsiveness:

```bash
VS_ACTIVE_PLAN_RESPONSIVENESS_ROWS=1000000 \
VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATIONS=1000 \
VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION=publish \
node --experimental-strip-types packages/core/bench/active-plan-responsiveness.bench.ts
```

Repeat with:

```bash
VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION=deltaPublish
VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION=deleteById
```

Grouped refresh overlap:

```bash
VS_GROUPED_REFRESH_OVERLAP_BACKEND=chdb \
VS_GROUPED_REFRESH_OVERLAP_ROWS=1000000 \
VS_GROUPED_REFRESH_OVERLAP_OPERATIONS=20 \
VS_GROUPED_REFRESH_OVERLAP_AGGREGATES=100 \
node --experimental-strip-types packages/core/bench/grouped-refresh-overlap.bench.ts
```

Memory benchmark:

```bash
node --expose-gc --experimental-strip-types packages/core/bench/active-view.bench.ts
```

Set the benchmark env vars documented in `docs/benchmarks.md` and `plan.md` for 1M-row same-plan, ten-plan, and unique-plan shapes.

## Soak Command

CI-safe soak:

```bash
vp run core#test -- tests/worker-soak.test.ts
```

Large soak:

```bash
VS_WORKER_SOAK_ROWS=1000000 \
VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=250 \
VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=20 \
VS_WORKER_SOAK_MUTATIONS=10000 \
VS_WORKER_SOAK_TIMEOUT_MS=900000 \
VS_WORKER_SOAK_SUMMARY_PATH=/private/tmp/view-server-worker-soak-1m-summary.json \
vp run core#test -- tests/worker-soak.test.ts
```

Manual/nightly 10M raw capacity soak:

```bash
pnpm run soak:10m
```

This is not a CI gate. The script defaults grouped subscriptions to `0` because the direct worker soak uses memory fallback, not production chDB grouped refresh. Save the JSON summary artifact and review subscribers, queue depth, subscription lag, active plans, heap/RSS, event counts, retries, backpressure, and reconnects. See `docs/capacity-soak.md`.

For 10M grouped capacity, run the chDB grouped refresh overlap benchmark from `docs/capacity-soak.md`.

Optional retained-memory sentinel:

```bash
node --expose-gc ./node_modules/vitest/vitest.mjs run --config packages/core/vitest.config.ts tests/worker-soak.test.ts
```

## Package Dry Run

The dry-run script builds the three publishable packages, then runs `pnpm pack --dry-run` for each:

```bash
pnpm run pack:dry-run
```

Expected publishable packages:

- `@view-server/core`
- `@view-server/react`
- `@view-server/testing`

Intentionally private:

- workspace root
- `@view-server/utils`
- `metrics`
- `orders-demo`

The tarballs should include `dist`, `src`, and `package.json`. They should not include tests, benchmark artifacts, screenshots, coverage, or local `.vitest-attachments`.

Production runtime requires chDB. `chdb` is a required peer for server/runtime consumers, while React/browser bundles must still avoid importing it. `@effect/platform-node` and `@platformatic/kafka` remain optional peers for the websocket/node-worker and Kafka subpaths.

## External Consumer Smoke

Before a release candidate, install actual tarballs into a fresh temp project outside the monorepo and run the consumer smoke in `docs/consumer-smoke.md`.

Required checks:

```bash
pnpm exec tsc --noEmit
pnpm run node:smoke
pnpm run build
pnpm run bundle:grep
pnpm run test
```

The smoke proves:

- public package subpaths are sufficient for a Node consumer
- the React package builds in a Vite production bundle
- browser assets do not include chDB, Kafka, worker threads, `fs`, or `net`
- `@view-server/testing` works from the packed tarball without Kafka dependencies; memory remains internal test infrastructure
- app UI tests can use `TestingViewServerProvider` with a required `isolationId` against a real View Server

## Deployment Smoke

Build and run the containerized demo server:

```bash
pnpm run smoke:deployment
```

This uses `Dockerfile` and `docker-compose.production-smoke.yml`, waits for `/ready`, exercises the real Effect RPC websocket from the host, verifies raw/grouped query plus publish/delta/delete, and shuts Compose down cleanly. Details and env vars are in `docs/deployment-smoke.md`.

## Manual Demo Smoke

Run the websocket demo:

```bash
vp run orders-demo#server
vp run orders-demo#dev
```

Verify:

- the browser connects over Effect RPC websocket + NDJSON
- the raw orders grid receives a snapshot and deltas
- the grouped metrics panel refreshes after mutations
- React uses `useLiveQuery` and `AsyncResult`
- `totalRows` is visible and correct

## GitHub Actions

Expected checks:

- normal package checks/tests/builds
- browser tests through Vitest browser mode and Playwright
- benchmark smoke workflow uploads artifacts
- benchmark smoke stays reporting-only while `VS_BENCH_BLOCKING=0`

The benchmark summary table should appear in `$GITHUB_STEP_SUMMARY`, and artifacts should upload even when a benchmark reports a warning.

## Policy Scan

Run before release:

```bash
rg -n "console\\." packages apps docs
rg -n "node:assert|node:test|from ['\\\"]vitest['\\\"]" packages apps
rg -n "as never|as any|as unknown" packages apps
rg -n "\\bas\\s+(?!const\\b)" packages apps --pcre2
```

Expected result:

- no `console.*` in source or tests
- no `node:assert`
- no `node:test`
- no direct `vitest` imports in project tests
- no `as never`, `as any`, or `as unknown`
- any remaining `as` should be reviewed manually and justified, with `as const` allowed
