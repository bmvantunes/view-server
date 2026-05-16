# ADR 0005: Active Raw Views And Stale Catch-Up

## Status

Accepted

## Context

Full recompute per mutation per subscription does not scale to 250k/1M rows and hundreds of subscriptions. Active plans are fast after construction, but building them for very large topics can hurt responsiveness.

## Decision

Raw non-grouped subscriptions use shared active raw plans keyed by where + orderBy when admission allows. Active plan builds are cooperative and bounded. If a plan is pending, skipped, or discarded, subscriptions keep the previous snapshot and emit stale/catch-up status rather than recomputing on every publish.

## Consequences

- Offset, limit, and fields are cheap per-subscription windows over a shared plan.
- `activePlanAutoBuildMaxRows` can skip plan builds for very large topics.
- Health exposes active plan count, view count, indexed rows, estimated index bytes, build queue/building/pending counts, fallback count, and auto-build skipped count.
- Some subscriptions can be temporarily stale while plan construction or catch-up finishes.

## Alternatives Rejected

- Always build an active plan on first subscription: rejected because 10M-row topics can stall subscription setup.
- Fully live memory recompute while active plans build: rejected because it damages publish latency.
