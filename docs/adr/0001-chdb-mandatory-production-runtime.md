# ADR 0001: chDB Is Mandatory For Production Runtime

## Status

Accepted

## Context

View Server keeps worker memory authoritative, but production startup and large initial snapshots need a native snapshot accelerator. Supporting a user-facing backend choice would make operational behavior harder to explain and test.

## Decision

Production/default runtime always uses chDB. Startup fails fast if chDB cannot initialize. The memory backend remains private/internal test infrastructure only.

## Consequences

- Production docs and deployment smoke require chDB.
- Browser packages must not import chDB; chDB stays server-side.
- Node integration tests should prefer chDB unless they are specifically testing fallback/error paths.
- Memory backend can still power package/browser tests where native chDB is not available.

## Alternatives Rejected

- Public `memory | chdb` backend config: rejected because it would create two production modes and let users accidentally benchmark or deploy the wrong one.
- chDB optional runtime dependency for production: rejected because snapshot semantics and health behavior would diverge.
