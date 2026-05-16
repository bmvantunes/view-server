# ADR 0003: Real Server Tests With isolationId

## Status

Accepted

## Context

The library has in-memory helpers for fast package tests, but app correctness depends on real websocket RPC, server lifecycle, chDB snapshot semantics, and browser behavior.

## Decision

Application/UI tests should use a real View Server, real websocket RPC, chDB runtime, and an `isolationId` per test/story/session. Testing helpers inject `isolationId` into rows/patches and scope test queries.

## Consequences

- Production `ViewServerProvider` does not accept `isolationId`.
- Testing provider requires `isolationId`.
- Test topics that use this pattern must include an `isolationId` field in their schema.
- In-memory server behavior must not be treated as app correctness proof.

## Alternatives Rejected

- Relying on in-memory helpers for app tests: rejected because it bypasses transport and production backend behavior.
- Global test topics without isolation: rejected because parallel browser tests can leak state across cases.
