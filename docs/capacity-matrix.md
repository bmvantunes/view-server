# Capacity Profile Matrix

This matrix records the explicit capacity profiles we use for alpha readiness. These are not product SLAs. They are repeatable shapes for finding regressions and known bottlenecks.

## Profiles

| profile        | rows | raw subscriptions |   grouped subscriptions | mutations | active plan policy               | command                                                                                                                                                                                                                                                                                                                                                       | latest result                                                          |
| -------------- | ---: | ----------------: | ----------------------: | --------: | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| dev            | 100k |                50 |                       5 |        1k | auto-build allowed               | `VS_WORKER_SOAK_ROWS=100000 VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=50 VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=5 VS_WORKER_SOAK_MUTATIONS=1000 VS_WORKER_SOAK_TIMEOUT_MS=300000 VS_WORKER_SOAK_SUMMARY_PATH=/private/tmp/view-server-worker-soak-100k-20260516T2100.json pnpm exec vitest run --config vitest.config.ts tests/worker-soak.test.ts` from `packages/core` | pass, `/private/tmp/view-server-worker-soak-100k-20260516T2100.json`   |
| alpha          |   1M |               250 |                      20 |       10k | auto-build allowed               | `VS_WORKER_SOAK_ROWS=1000000 VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=250 VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=20 VS_WORKER_SOAK_MUTATIONS=10000 VS_WORKER_SOAK_TIMEOUT_MS=900000 VS_WORKER_SOAK_SUMMARY_PATH=/private/tmp/view-server-worker-soak-1m-summary.json vp run core#test -- tests/worker-soak.test.ts`                                                     | pass, `/private/tmp/view-server-worker-soak-1m-summary-35840ef.json`   |
| target raw     |  10M |               250 |                       0 |        10 | auto-build skipped above 1M rows | `pnpm run soak:10m`                                                                                                                                                                                                                                                                                                                                           | pass, `/private/tmp/view-server-worker-soak-10m-20260516T202538Z.json` |
| target grouped |  10M |               n/a | grouped refresh overlap |        20 | chDB grouped refresh             | `VS_GROUPED_REFRESH_OVERLAP_ROWS=10000000 VS_GROUPED_REFRESH_OVERLAP_OPERATIONS=20 VS_GROUPED_REFRESH_OVERLAP_AGGREGATES=100 VS_GROUPED_REFRESH_OVERLAP_BACKEND=chdb node --experimental-strip-types packages/core/bench/grouped-refresh-overlap.bench.ts`                                                                                                    | pending latest 10M artifact                                            |

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

Artifact: `/private/tmp/view-server-worker-soak-1m-summary-35840ef.json`

- Duration: 378.30s
- Cleanup: subscribers 0, active plans 0, active views 0, queue depth 0, lag 0
- Events: 540 snapshots, 1,900,939 deltas, 279,522 status events
- Retries/backpressure/reconnects: 0/0/0

The 1M profile remains the primary alpha confidence profile because it includes raw and grouped subscriptions plus 10k mixed mutations. The older summary artifact predates the latest phase timing fields, so rerun it before release notes if those phase timings are needed.

## Latest 10M Raw Target Profile

Artifact: `/private/tmp/view-server-worker-soak-10m-20260516T202538Z.json`

- Duration: 87.23s
- Row generation: 619.74ms
- Worker seed: 11.27s
- Subscription setup: 71.70s
- Mutation loop: 1.48s
- Mutation latency: p50 64.07ms, p95 619.25ms, p99 619.25ms, max 619.25ms
- Cleanup: subscribers 0, active plans 0, active views 0, queue depth 0, lag 0
- Active plan admission skipped: 250 subscriptions before cleanup, 0 after cleanup
- Events: 250 snapshots, 0 deltas, 2,500 status events
- Retries/backpressure/reconnects: 0/0/0

The first 10M blocker is resolved: subscription setup reaches mutation phase because `activePlanAutoBuildMaxRows` prevents implicit 10M active-plan builds. The remaining known pain is subscription setup latency over 10M rows.

## Known Limits

- 10M grouped memory fallback is not a production signal. Use the chDB grouped refresh overlap benchmark for grouped capacity.
- 10M raw profile currently proves mutation-phase reachability and cleanup under skipped active plans; it is not yet a high-throughput 100k mutation proof.
- Subscription setup over 10M rows remains expensive even with active-plan admission. Keep it visible in release notes.
- `activePlanIndexEstimatedBytes` is a lower-bound index-reference estimate, not a total retained heap budget.

## Release Guidance

- Run the dev profile before local release-gate debugging.
- Run the alpha profile before an alpha tag.
- Run the 10M raw target profile before serious production rollout.
- Run the 10M grouped chDB benchmark before promising grouped analytics capacity.
- Keep JSON summary and progress artifacts with release notes or rollout tickets.
