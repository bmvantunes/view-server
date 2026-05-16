# ADR 0002: Per-Topic chDB Child Process

## Status

Accepted

## Context

Topics are vertical slices. There are no cross-topic queries, joins, or subscriptions. A single global chDB process would concentrate IPC, serialization, failure, and scheduling pressure across all topic workers without buying query capability the product supports.

## Decision

Each topic worker owns its own chDB child/process/session. Topic A writes to chDB A; Topic B writes to chDB B. The topic worker plus its chDB mirror is the operational slice.

## Consequences

- chDB failure is isolated to the owning topic.
- Per-topic scaling stays local because no query needs a multi-topic chDB coordinator.
- Health exposes per-topic chDB status, pid, restarts, pending requests, last error, and backend version.
- Shutdown must terminate each topic's chDB child cleanly and avoid orphan processes.
- Process count and baseline memory grow with topic count.

## Alternatives Rejected

- One shared chDB process for all topics: rejected because it becomes a global contention and failure point.
- Cross-topic chDB analytics: rejected for this product shape until cross-topic query features exist.
