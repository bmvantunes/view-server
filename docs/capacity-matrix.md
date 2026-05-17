# Capacity Profile Matrix

This matrix records the explicit capacity profiles we use for alpha readiness. These are not product SLAs. They are repeatable shapes for finding regressions and known bottlenecks.

## Profiles

| profile        | rows | raw subscriptions |   grouped subscriptions | mutations | active plan policy               | command                                                                                                                                                                                                                                                                                                                                                                                                        | latest result                                                              |
| -------------- | ---: | ----------------: | ----------------------: | --------: | -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| dev            | 100k |                50 |                       5 |        1k | auto-build allowed               | `VS_WORKER_SOAK_ROWS=100000 VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=50 VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=5 VS_WORKER_SOAK_MUTATIONS=1000 VS_WORKER_SOAK_TIMEOUT_MS=300000 VS_WORKER_SOAK_SUMMARY_PATH=/private/tmp/view-server-worker-soak-100k-20260516T2100.json pnpm exec vitest run --config vitest.config.ts tests/worker-soak.test.ts` from `packages/core`                                                  | pass, `/private/tmp/view-server-worker-soak-100k-20260516T2100.json`       |
| alpha          |   1M |               250 |                      20 |       10k | auto-build allowed               | `VS_WORKER_SOAK_ROWS=1000000 VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=250 VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=20 VS_WORKER_SOAK_MUTATIONS=10000 VS_WORKER_SOAK_MUTATION_BATCH_SIZE=1000 VS_WORKER_SOAK_TIMEOUT_MS=900000 VS_WORKER_SOAK_SUMMARY_PATH=/private/tmp/view-server-worker-soak-1m-batched-20260517T1744.json pnpm --dir packages/core exec vitest run --config vitest.config.ts tests/worker-soak.test.ts` | pass, `/private/tmp/view-server-worker-soak-1m-batched-20260517T1744.json` |
| target raw     |  10M |               250 |                       0 |       10k | auto-build skipped above 1M rows | `pnpm run soak:10m`                                                                                                                                                                                                                                                                                                                                                                                            | pass, `/private/tmp/view-server-worker-soak-10m-20260517T174202Z.json`     |
| target grouped |  10M |               n/a | grouped refresh overlap |        20 | chDB grouped refresh             | `VS_GROUPED_REFRESH_OVERLAP_ROWS=10000000 VS_GROUPED_REFRESH_OVERLAP_OPERATIONS=20 VS_GROUPED_REFRESH_OVERLAP_AGGREGATES=100 VS_GROUPED_REFRESH_OVERLAP_BACKEND=chdb node --experimental-strip-types packages/core/bench/grouped-refresh-overlap.bench.ts`                                                                                                                                                     | pending latest 10M artifact                                                |

## Latest Dev Profile

Artifact: `/private/tmp/view-server-worker-soak-100k-20260516T2100.json`

- Duration: 1.63s
- Row generation: 10.38ms
- Worker seed: 84.01ms
- Subscription setup: 358.41ms
- Mutation loop: 1.16s
- Mutation latency: p50 0.94ms, p95 2.57ms, p99 3.30ms, max 7.33ms
- Cleanup: subscribers 0, active plans 0, active views 0, queue depth 0, lag 0
- Events: 110 snapshots, 17,825 deltas, 4,929 status events
- Retries/backpressure/reconnects: 0/0/0

## Latest Alpha Profile

Artifact: `/private/tmp/view-server-worker-soak-1m-batched-20260517T1744.json`

- Duration: 24.79s
- Row generation: 59.24ms
- Worker seed: 825.25ms
- Subscription setup: 12.29s
- Mutation loop: 11.46s across 10 batches of 1,000 mutations
- Mutation batch latency: p50 952.84ms, p95 2,624.31ms, p99 2,624.31ms, max 2,624.31ms
- Cleanup: subscribers 0, active plans 0, active views 0, queue depth 0, lag 0
- Active plans before cleanup: 1 plan, 250 active views
- Events: 540 snapshots, 1,610,250 deltas, 417,084 status events
- Retries/backpressure/reconnects: 0/0/0

The 1M profile remains the primary alpha confidence profile because it includes raw and grouped subscriptions plus 10k mixed mutations. Mutation latency is batch latency when `VS_WORKER_SOAK_MUTATION_BATCH_SIZE > 1`.

## Latest 10M Raw Target Profile

Artifact: `/private/tmp/view-server-worker-soak-10m-20260517T174202Z.json`

- Duration: 113.07s
- Row generation: 590.23ms
- Worker seed: 10.83s
- Subscription setup: 99.84s
- Mutation loop: 257.93ms across 10 batches of 1,000 mutations
- Mutation batch latency: p50 21.62ms, p95 55.69ms, p99 55.69ms, max 55.69ms
- Cleanup: subscribers 0, active plans 0, active views 0, queue depth 0, lag 0
- Active plan admission skipped: 250 subscriptions before cleanup, 0 after cleanup
- Events: 250 snapshots, 0 deltas, 2,500 status events
- Retries/backpressure/reconnects: 0/0/0

The first 10M blocker is resolved: subscription setup reaches mutation phase because `activePlanAutoBuildMaxRows` prevents implicit 10M active-plan builds. Batched mutation fanout is now fast in this stale/skipped-active-plan profile. The remaining known pain is subscription setup latency over 10M rows.

## Known Limits

- 10M grouped memory fallback is not a production signal. Use the chDB grouped refresh overlap benchmark for grouped capacity.
- 10M raw profile currently proves mutation-phase reachability, batched stale-status fanout, and cleanup under skipped active plans; it is not yet a high-throughput 100k mutation proof.
- Subscription setup over 10M rows remains expensive even with active-plan admission. Keep it visible in release notes.
- `activePlanIndexEstimatedBytes` is a lower-bound index-reference estimate, not a total retained heap budget.

## Release Guidance

- Run the dev profile before local release-gate debugging.
- Run the alpha profile before an alpha tag.
- Run the 10M raw target profile before serious production rollout.
- Run the 10M grouped chDB benchmark before promising grouped analytics capacity.
- Keep JSON summary and progress artifacts with release notes or rollout tickets.
