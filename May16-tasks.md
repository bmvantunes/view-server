# May 16 Architecture / Performance Hardening Tasks

This file is the working backlog for the next agent. It is intentionally large and strict. The goal is not to add random product features. The goal is to deepen the architecture, remove hidden complexity, make the 10M-row path credible, and keep quality high enough that the alpha can become production-grade.

## Non-Negotiable Rules

- Do not add new query features unless a task explicitly asks for one.
- Production runtime uses chDB. Do not reintroduce public backend choice as a product feature.
- Memory backend is internal/private test infrastructure only.
- Prefer real View Server tests using real WebSocket RPC and isolationId for UI/application flows.
- Browser-facing packages must not import chDB, Kafka, worker_threads, fs, net, child_process, or broad server-heavy core entrypoints.
- No TypeScript assertion casts except as const. If a cast seems unavoidable, stop and explain why before adding it.
- No console.\* in source/tests. Use Effect logging/tracing/metrics.
- No node:assert, node:test, or direct imports from vitest. Use the project test helpers and @effect/vitest patterns.
- Do not fake coverage. Do not lower coverage thresholds. Do not add ignore coverage comments.
- Add Effect spans around meaningful boundaries. Do not create per-row spans or per-message firehose spans.
- Keep commits focused. It is okay if history is not perfect, but each commit must leave the repo green.

## Required Validation Matrix

Every task must run the smallest focused validation first, then the broader checks before commit.

Required unless the task is docs-only:

- vp check --fix
- pnpm exec effect-language-service diagnostics --project packages/core/tsconfig.json --format text --severity error
- focused tests for the changed module
- vp run -r test
- vp run -r build
- policy scans for console.\*, node:assert, node:test, direct vitest imports, and assertion casts

Performance-sensitive tasks must also add or update a benchmark and record before/after numbers in docs or plan.md.

Production/runtime-sensitive tasks must include a fault or soak test when feasible.

## Implementation Status

Use this section as the auditable progress ledger. A task is marked complete only when it has
focused tests and the relevant integration tests still pass.

- [x] Task 5: Active Plan Admission Policy.
- [x] Task 28: Capacity Soak Runner visibility for 10M progress and subscription setup heartbeats.
- [x] Task 2: MutationStore extraction.
- [x] Task 3: SnapshotReconciler extraction.
- [x] Task 4: ActivePlanCoordinator extraction.
- [x] Task 6: FanoutQueue extraction.
- [x] Task 7: SubscriptionRegistry extraction.
- [x] Task 8: GroupedRefreshCoordinator extraction.
- [x] Task 19: RuntimeHealthProjection extraction.
- [x] Task 32: RuntimeShutdownController extraction.
- [x] Task 21: KafkaSourceSupervisor extraction for topic verification, source fibers,
      lag metrics, degraded source state, and source shutdown.
- [x] Task 13: Client VisibleRows extraction and large-window delta benchmark.
- [x] Task 12: Query Semantics Parity Suite across memory, ActiveRawView, and chDB.
- [x] Task 39: chDB SQL compiler contract tests and internal compiler module.
- [x] Task 10: Aggregate Function Modules for count, sum, min, max, avg, count distinct,
      and string aggregates with focused BigDecimal/null/update tests.
- [x] Task 9: GroupedAccumulator prototype for opt-in count/sum/min/max grouped subscriptions,
      with worker delta test and 1M rows / 100 aggregates / 10k mutations benchmark evidence.
- [x] Task 11: QueryPlanner module classifies raw, active-plan, grouped chDB refresh,
      grouped accumulator, memory fallback, and rejected strategies with table tests,
      span annotations, and soak progress plannedStrategy fields.
- [x] Task 14: LiveQueryLifecycle module extracted for AsyncResult connecting, live,
      stale, reconnecting, failure-with-previous, and closed transition semantics.
- [x] Task 1: Topic Worker State Machine Split. MutationStore, SnapshotReconciler,
      ActivePlanCoordinator, FanoutQueue, SubscriptionRegistry, GroupedRefreshCoordinator,
      WorkerHealthProjection, RuntimeHealthProjection, and RuntimeShutdownController are extracted
      with focused tests plus the existing duplicate-request, unsubscribe-during-build,
      shutdown-during-refresh, backend-gap, source-failure, and backpressure integration coverage.
- [x] Task 45: CONTEXT.md added with domain vocabulary, invariants, module map,
      production architecture, testing philosophy, performance targets, and forbidden shortcuts.
- [x] Task 46: ADRs added for mandatory chDB, per-topic chDB children, real-server
      isolationId testing, AsyncResult hooks, active raw views, and grouped refresh strategy.
- [x] Task 49: Release gate script added with ci/local/full scopes, summary output,
      benchmark artifacts, policy scans, package dry-run, and optional soak.
- [x] Task 50: Capacity matrix added with 100k, 1M, 10M raw, and 10M grouped
      commands plus latest available artifact status.

## Updated 10M Soak Status

The latest 10M soak evidence says raw query scanning is not the blocker anymore, and the first
active-plan admission blocker is fixed.

Previous blocker evidence:

- 10M rows generated in about 1s to 1.5s.
- Worker seed took about 14s to 17s.
- First raw subscription starts around 15s.
- Subscription setup does not complete.
- Direct executeRawQuery first-page raw snapshot over 10M rows is around 386ms.

Resolved blocker:

- `worker.activePlanAutoBuildMaxRows` now gates active-plan admission.
- If topic row count exceeds the threshold, subscriptions still receive initial snapshots.
- Relevant mutations mark those subscriptions stale/dirty instead of queuing active-plan builds.
- Health exposes `activePlanAutoBuildSkippedCount`.
- The 10M soak profile can configure the threshold explicitly.

Latest 10M smoke result:

- A 10M-row soak reached mutation phase and cleanup with active plans skipped by policy.
- Subscription setup emitted heartbeat progress.
- Cleanup returned subscribers, active plans, build queues, queue depth, and lag to zero.
- The remaining risk is subscription setup time and broader state-machine complexity, not a silent active-plan build hang.

## 1. Topic Worker State Machine Split

Priority: P0

Files to inspect:

- packages/core/src/worker/topic-worker-core.ts
- packages/core/tests/rpc-inmemory.test.ts
- packages/core/tests/fault-injection.test.ts
- packages/core/tests/worker-soak.test.ts

Problem:

topic-worker-core.ts is carrying too many responsibilities: mutation storage, snapshot reconciliation, active plan lifecycle, grouped refresh, queueing, health projection, backpressure, shutdown, chDB fallback, source failure tracking, and subscription lifecycle. It is now the highest-risk Module in the system.

Solution:

Split it into deep modules with small Interfaces:

- MutationStore
- SubscriptionRegistry
- FanoutQueue
- ActivePlanCoordinator
- GroupedRefreshCoordinator
- SnapshotReconciler
- WorkerHealthProjection
- RuntimeShutdownController

The worker core should become orchestration glue, not the place where every invariant lives.

Tests / acceptance:

- Existing worker tests stay green.
- Add state-machine tests for duplicate request ids, unsubscribe during build, shutdown during refresh, backend gaps, source failure, and backpressure.
- Add Deletion test: deleting one extracted module should fail only its local tests plus integration tests, not random unrelated files.

## 2. MutationStore Deep Module

Priority: P0

Problem:

Mutation log, versioning, row storage, id index, swap-remove delete, and gap detection are load-bearing. They should not be scattered inside worker code.

Solution:

Create MutationStore with Interface roughly:

- publish(row)
- deltaPublish(row)
- deleteById(id)
- snapshotRows()
- version()
- replayFrom(fromVersion, toVersion)
- canReplay(fromVersion, toVersion)
- metrics()

It owns row array, id index, version counter, mutation log, and structural mutation invariants.

Tests / acceptance:

- Unit/fuzz tests for publish/update/delete/replay/gaps.
- 1M-row delete benchmark verifies no full id-index rebuild.
- Property test compares replay result to full row state.
- Soak still passes.

## 3. SnapshotReconciler Module

Priority: P0

Problem:

Version-fenced snapshot logic appears in multiple paths: query, subscribe, grouped refresh, chDB fallback, mutation replay, memory fallback.

Solution:

Create SnapshotReconciler as the only module that knows this algorithm:

- Ask backend for snapshot at version N.
- If backend version equals target, accept.
- If backend behind and mutation log can replay, replay to target.
- If backend gap/failure, fallback to authoritative memory.
- Emit metrics and spans for exact, replayed, fallback, failed.

Tests / acceptance:

- Exact backend snapshot.
- Replayable lag.
- Gap fallback.
- Future backend version rejection.
- Backend failure fallback.
- BigDecimal row replay.
- Grouped query refresh uses same reconciler semantics.

## 4. ActivePlanCoordinator Module

Priority: P0

Problem:

Active plan build, sharing, ref counting, stale catch-up, admission limits, build queues, cooperative build, discard, and release are all coupled to worker core.

Solution:

Extract ActivePlanCoordinator. It owns:

- normalized active plan key
- plan cache
- ref counts
- activePlanBuildConcurrency
- active plan admission policy
- build queue
- dirty subscriptions while building
- catch-up from mutation log
- metrics

Tests / acceptance:

- Shared plan reused across 250 subscriptions.
- Unsubscribe releases refs and clears plan.
- Pending build discarded on unsubscribe.
- Build gap falls back safely.
- Build does not block publish/metrics.
- 10M admission policy can skip build without hanging subscription setup.

## 5. Active Plan Admission Policy

Priority: P0

Problem:

10M soak is likely blocked because the first subscription tries to do too much active-plan work. Active plans are not always the right answer.

Solution:

Add explicit ActivePlanAdmissionPolicy:

- maxRowsForAutoBuild
- maxEstimatedBuildMs optional future signal
- maxDistinctPlans
- maxEstimatedBytes
- allowManualOverride if needed later

Default behavior should avoid building active plans for very large topics unless the plan is shared/hot enough or limits allow it.

Tests / acceptance:

- 10M topic + first subscription does not hang.
- Health reports activePlanSkippedCount and activePlanSkipReason.
- Subscription remains correct via snapshot + stale/refresh fallback.
- Soak progress artifact includes admission decision.

## 6. FanoutQueue Module

Priority: P0

Problem:

Queue depth, coalescing, logical lag, stale status pressure, backpressure failure, retry, and request-id stale event guards are a deep concept and should be isolated.

Solution:

Create FanoutQueue with Interface:

- offerSnapshot
- offerDelta
- offerStatus
- coalesce
- failBackpressure
- closeShutdown
- metrics

Tests / acceptance:

- Coalesced queue reports physical queueDepth and logical lag separately.
- Non-coalesced partial drain does not overreport lag.
- BackpressureExceeded is typed and retryable.
- Request-id guard ignores stale events.
- WebSocket E2E covers retry/resubscribe and no leaked subscription.

## 7. SubscriptionRegistry Module

Priority: P0

Problem:

Subscription ownership, finalizers, request ids, active plan refs, grouped refresh refs, queue ownership, and cleanup are too easy to leak.

Solution:

Create SubscriptionRegistry that owns subscription lifecycle:

- register(requestId, topic, query)
- attachQueue
- attachActivePlan
- attachGroupedRefresh
- markDirty
- close(reason)
- closeAll(reason)
- metrics

Tests / acceptance:

- Duplicate request id rejected or replaces safely, whichever current contract says.
- Unsubscribe during pending active plan build cleans everything.
- Shutdown closes all streams with ServerShutdown.
- After cleanup, subscriber count, active views, queues, builds, grouped refresh counts are zero.

## 8. GroupedRefreshCoordinator Module

Priority: P0

Problem:

Grouped queries are currently stale/debounced/chDB-first/cooperative fallback. That is the right policy, but it is hidden in worker core.

Solution:

Extract GroupedRefreshCoordinator:

- normalize grouped query key
- share refresh work across subscriptions
- mark dirty
- debounce
- query chDB worker first
- reject stale backend versions
- cooperative memory fallback
- reschedule if newer mutation arrives
- metrics

Tests / acceptance:

- 20 grouped subscriptions with same query result in one refresh, not 20.
- Mutations during refresh reschedule exactly once as needed.
- Backend behind/failure falls back.
- Shutdown interrupts refresh.
- Metrics expose pending/in-flight/shared refresh counts.

## 9. GroupedAccumulator Module

Priority: P1

Problem:

Grouped refresh still scans real wall time for 1M rows. chDB worker isolation protects event loop, but grouped firehose semantics are stale/refresh, not incremental.

Solution:

Prototype grouped accumulator for small numbers of common grouped queries:

- Maintain per-group aggregate state incrementally for active grouped subscriptions.
- Support count, sum, min, max initially.
- Use BigDecimal-safe math.
- Fall back to chDB refresh for unsupported aggregate/query shapes.

Tests / acceptance:

- Fuzz against executeGroupedQuery/chDB for insert/update/delete.
- BigDecimal sum/max exactness.
- Null group keys.
- Filter enter/leave changes.
- Benchmark 1M rows / 100 aggregates / 10k mutations vs chDB refresh.

## 10. Aggregate Function Modules

Priority: P1

Problem:

Aggregation semantics are subtle: number vs BigDecimal, null handling, min/max ordering, empty groups, deletes, updates.

Solution:

Create per-aggregate modules with strict contracts:

- CountAggregate
- SumAggregate
- MinAggregate
- MaxAggregate
- possibly AvgAggregate later

Tests / acceptance:

- Unit tests for nulls, missing fields, BigDecimal overflow, deletes, updates.
- SQL parity tests against chDB for generated data.
- No JS number overflow for decimal production fields.

## 11. Query Planner Module

Priority: P1

Problem:

Query execution strategy selection is implicit. The worker needs an explicit planner.

Solution:

Create QueryPlanner that classifies a query:

- raw small-window snapshot
- raw active plan eligible
- raw active plan skipped
- grouped chDB refresh
- grouped incremental accumulator eligible
- memory fallback
- query rejected by limits

Tests / acceptance:

- Table-driven tests for every query shape.
- Health/trace includes selected strategy.
- 10M first subscription shows planned strategy in progress artifact.

## 12. Query Semantics Parity Suite

Priority: P0

Problem:

We compare custom engine behavior to chDB in many places, but the parity suite should be explicit and broad.

Solution:

Create a dedicated query semantics parity test suite:

- raw filter/sort/pagination
- grouped aggregates
- projections
- nulls
- strings strict vs broad case-insensitive behavior
- BigDecimal
- deletes
- updates causing row movement
- duplicate comparator-equal values with id tiebreak

Tests / acceptance:

- Random deterministic generator.
- Runs against executeRawQuery, ActiveRawView, chDB snapshot backend.
- All outputs normalized and compared.
- Include failure artifact with seed/query/mutations.

## 13. Client VisibleRows Module

Priority: P0

Problem:

The client store still had per-op findIndex issues according to latest investigation. Server diffVisibleRows was fixed; client visible row application must be audited too.

Solution:

Extract VisibleRows client module:

- applySnapshot
- applyDeltaOps
- applyStatus
- preserve stale previous rows
- request-id guard
- O(n) key maps for larger transitions

Tests / acceptance:

- Snapshot to delta transition with 10k rows does not go quadratic.
- Duplicate/stale request id ignored.
- Remove/upsert/move/update order parity.
- Browser test verifies stale waiting keeps previous rows.

## 14. LiveQueryLifecycle Module

Priority: P1

Problem:

AsyncResult status mapping, reconnecting, stale, waiting, failure with previous, and shutdown are public UX semantics. They should be a small deep module.

Solution:

Create LiveQueryLifecycle:

- maps protocol events to AsyncResult
- owns LiveQueryStatus and connection metadata
- handles BackpressureExceeded retryable failure
- handles ServerShutdown terminal failure

Tests / acceptance:

- Table-driven lifecycle tests.
- Browser test for reconnect/stale/shutdown states.
- No legacy status names return.

## 15. chDB Process Client Module

Priority: P0

Status: Done in `packages/core/src/snapshot/chdb-process-client.ts`.

Problem:

chDB child process supervision is load-bearing: pending requests, restart, shutdown, SIGTERM/SIGKILL, health, pending request failure.

Solution:

Create ChdbProcessClient as a standalone module with a tiny Interface:

- request(command)
- health()
- restart()
- shutdown()

Tests / acceptance:

- Child exits during snapshot/grouped/applyBatch.
- Pending requests fail typed.
- Restart recovers topic health.
- Topic A child death does not degrade Topic B.
- Shutdown kills child and leaves no process.

## 16. chDB SQL Mirror Module

Priority: P1

Status: Done in `packages/core/src/snapshot/chdb-sql-mirror.ts`.

Problem:

The SQL compiler and table mirror are part of the database seam. They should be independent from worker orchestration.

Solution:

Create ChdbSqlMirror:

- create table SQL
- insert batch SQL or native chDB input representation
- tombstone/version SQL
- raw snapshot SQL
- grouped snapshot SQL
- count SQL

Tests / acceptance:

- Compile SQL golden tests.
- WHERE clauses included in rows and count SQL for raw and grouped branches.
- Reserved identifiers escaped correctly.
- BigDecimal Decimal(76,38) mapping verified.

## 17. ColumnCatalog Module

Priority: P1

Status: Done in `packages/core/src/config/column-catalog.ts`.

Problem:

Schema-derived columns, id fields, literal string fields, BigDecimal columns, and query validation should share one source of truth.

Solution:

Create ColumnCatalog from defineConfig topic schema:

- id field
- column names/types
- decimal fields
- literal string fields
- sortable/filterable fields
- SQL types

Tests / acceptance:

- Schema-derived id field exists.
- Invalid filter/sort/group field rejected.
- Literal string strictness verified.
- SQL column generation uses catalog.

## 18. chDB Row Encoder Module

Priority: P1

Status: Done in `packages/core/src/snapshot/row-wire-codec.ts`.

Problem:

Rows cross process boundaries and BigDecimal must survive structured clone / wire encoding.

Solution:

Create RowWireCodec:

- RuntimeRow to worker wire row
- worker wire row to RuntimeRow
- BigDecimal encode/decode
- query filter encode/decode
- mutation encode/decode

Tests / acceptance:

- BigDecimal exact roundtrip.
- Query filters with decimals roundtrip.
- Mutation batches roundtrip.
- No as any / as unknown / as never.

## 19. RuntimeHealthProjection Module

Priority: P1

Status: Done in `packages/core/src/server/runtime-health-projection.ts`.

Problem:

Health data is assembled across runtime, workers, backend, Kafka, active plans, lag, chDB, readiness, source failure. That projection should be explicit.

Solution:

Create RuntimeHealthProjection:

- input: per-topic worker metrics, source status, runtime readiness
- output: public health response and health topic rows
- status rules: ready/degraded/shutting_down

Tests / acceptance:

- Topic degraded when chDB down.
- Topic degraded when source failed.
- Topic degraded near active plan limits.
- Runtime not ready during shutdown.
- Health topic row matches /health response fields.

## 20. Metrics UI Data Model Module

Priority: P2

Status: Done in `packages/react/src/metrics-view-model.ts`.

Problem:

Metrics UI should consume a stable view model, not raw health rows directly.

Solution:

Create metrics view model module in React/app layer:

- formats chDB status
- highlights degraded topics
- shows lag, queue, active plan, grouped refresh, Kafka lag
- no casts

Tests / acceptance:

- Browser metrics test with degraded chDB.
- Snapshot of important labels.
- Browser forbidden import scan remains clean.

## 21. Kafka Source Supervisor Module

Priority: P1

Problem:

Source failure monitoring and topic health degradation are new and important. The source supervisor should be explicit.

Solution:

Create KafkaSourceSupervisor:

- validate topics before start
- run source
- commit batches
- report lag
- fail/degrade/recover
- stop on shutdown

Tests / acceptance:

- Missing topic startup failure.
- Source fail marks degraded.
- Resume clears degraded.
- Commit after batch only.
- Duplicate/out-of-order messages safe.

## 22. Topic Placement Module

Priority: P2

Problem:

Production architecture is one topic worker and one chDB child per topic. This should be encoded as a placement policy.

Solution:

Create TopicPlacement:

- one worker per topic
- one chDB process per topic
- future hook for grouping low-volume topics if ever needed

Tests / acceptance:

- Runtime creates separate backend per topic.
- Killing Topic A backend leaves Topic B ready.
- Docs state per-topic process model.

## 23. Testing Isolation Module

Priority: P1

Problem:

Testing isolationId behavior needs to be a first-class Module, not scattered provider magic.

Solution:

Create TestingIsolation:

- inject isolationId into published rows
- auto-scope queries by isolationId
- force isolationId in testing provider
- reject missing isolationId in app test helpers

Tests / acceptance:

- Two parallel tests do not see each other’s rows.
- Storybook provider requires isolationId.
- Vitest browser provider gets isolationId from test context or explicit provider.
- Production provider cannot accept isolationId.

## 24. Real Server Test Harness Module

Priority: P1

Problem:

If app tests prefer real View Server, starting/stopping and seeding should be easy and repeatable.

Solution:

Create test harness package/module:

- start real server on available port or known Docker port
- wait for /ready
- create client
- publish scoped rows
- cleanup by isolationId if supported
- shutdown only if harness owns server

Tests / acceptance:

- Vitest browser mode example.
- Storybook test example.
- Parallel isolation test.
- No memory backend public API leak.

## 25. Package Export Contract Module

Priority: P1

Problem:

Package exports are public API. They need a precise contract.

Solution:

Keep explicit public subpaths:

- core/client
- core/config
- core/query
- core/errors
- core/runtime
- core/rpc
- core/kafka
- core/snapshot if still intended
- node-only subpaths clearly marked

Tests / acceptance:

- External consumer smoke imports only public subpaths.
- Browser build does not include server deps.
- Type declarations point to dist .d.mts.
- pack dry-run passes.

## 26. Public API Type Test Matrix

Priority: P1

Problem:

Types are part of the product: defineConfig, publish typing, useLiveQuery rows, BigDecimal, literal strings.

Solution:

Add type-level tests for:

- config inference
- topic names
- publish rows
- query filters
- order fields
- group fields
- useLiveQuery value
- BigDecimal fields

Tests / acceptance:

- Valid examples compile.
- Invalid examples fail with expected diagnostics where possible.
- No assertion casts.

## 27. Benchmark Profile Registry

Priority: P1

Problem:

Benchmarks have many environment variables. The knowledge is spread out and easy to misuse.

Solution:

Create BenchmarkProfile registry:

- ci-smoke
- dev-fast
- rc-1m
- soak-10m
- grouped-heavy
- active-plan-startup
- chDB-worker-overlap

Tests / acceptance:

- One command lists profiles.
- One command runs a named profile.
- Artifacts include profile name and exact parameters.
- CI summary links artifacts and notes coverage gaps.

## 28. Capacity Soak Runner Module

Priority: P0

Problem:

10M soaks need better phase visibility and failure classification.

Solution:

Create CapacitySoakRunner:

- progress JSONL every phase
- row generation timing
- seed timing
- subscription admission timing per subscription
- active plan admission/build timing
- mutation phase timing
- cleanup timing
- heartbeat while long loops run

Tests / acceptance:

- Tiny soak writes all expected phases.
- Killed/failed soak leaves useful progress artifact.
- 10M soak reaches mutation phase after active plan admission fix.

## 29. Error Taxonomy Module

Priority: P1

Problem:

Typed errors exist, but error taxonomy should be explicit and complete.

Solution:

Create errors module contract:

- InvalidConfig
- ServerShutdown
- BackpressureExceeded
- QueryLimitExceeded
- UnauthorizedSystemTopic
- SnapshotBackendUnavailable
- SnapshotReplayGap
- SourceFailed
- ChdbChildExited

Tests / acceptance:

- RPC serializes/deserializes each error.
- Client retry policy retries only retryable errors.
- Browser displays useful AsyncResult failure.

## 30. Query Limit Policy Module

Priority: P1

Problem:

Max page size, aggregate count, groupBy fields, filter depth, and filter conditions are security/perf boundaries.

Solution:

Create QueryLimitPolicy:

- validate query before execution
- include topic/config-specific overrides
- return typed errors

Tests / acceptance:

- Every limit has pass/fail tests.
- Error includes field/limit/current value.
- Metrics count rejected queries.

## 31. Auth Policy Module

Priority: P2

Problem:

System topics and health topic access need a clean seam for future auth.

Solution:

Create AuthPolicy Interface:

- canReadTopic
- canPublishTopic
- canReadHealth
- canSubscribe

Default policy enforces private system topics. Future users can provide implementation.

Tests / acceptance:

- \_\_ topics rejected for public publish/delete/query.
- Health readable only if authorized.
- Typed errors over RPC.

## 32. Runtime Shutdown Module

Priority: P1

Problem:

Shutdown must be deterministic: readiness false, reject new work, close streams, stop sources, drain workers, close chDB children.

Solution:

Extract RuntimeShutdownController.

Tests / acceptance:

- Shutdown while active plan build pending.
- Shutdown while grouped refresh in flight.
- Shutdown while chDB request pending.
- No leaked child processes.
- Streams fail with typed ServerShutdown.

## 33. chDB Child Health Contract

Priority: P1

Problem:

chDB health fields are now public. The contract should be stable and tested.

Solution:

Define ChdbHealth contract:

- status
- pid
- restarts
- pendingRequests
- lastError
- backendVersion

Tests / acceptance:

- Health updates during normal operation.
- Health degrades on child exit.
- Restart increments restarts and clears lastError on success if that is the intended behavior.
- Health topic and /health agree.

## 34. Active Raw View Keying Module

Priority: P1

Problem:

Active plan keys ignore offset/limit/fields and include where/orderBy. The cache-scope rule must be explicit and safe.

Solution:

Create ActiveRawPlanKey module:

- normalized where
- normalized orderBy
- topic-local literalStringFields assumption documented/enforced
- no offset/limit/fields in key

Tests / acceptance:

- Same where/orderBy different offset share plan.
- Different projection shares plan but view output differs.
- Different literal strictness cannot collide across topic scope.

## 35. Stable Key Encoding Module

Priority: P1

Problem:

Keys are used in maps, row identity, delta operations, duplicate checks, and SQL. String conversion mistakes can create subtle bugs.

Solution:

Create StableKey module:

- encode row id
- compare row ids
- preserve number/string distinctions if needed
- BigInt/BigDecimal if supported later

Tests / acceptance:

- id 1 and id '1' behavior is explicit.
- Delta remove/upsert keys stable across process/RPC.
- chDB worker codec preserves keys.

## 36. RowKey Module

Priority: P1

Problem:

rowKey functions are passed around. Make row identity a first-class concept.

Solution:

Create RowKey module from topic config:

- get(row)
- equals(a,b)
- encodeForWire
- decodeFromWire

Tests / acceptance:

- Missing id rejected at startup or publish depending on path.
- deleteById uses same key semantics as diffVisibleRows.
- Active index and client store agree.

## 37. Projection Module

Priority: P2

Problem:

Projection semantics affect row equality and visible no-op updates. It should be isolated.

Solution:

Create Projection module:

- project(row, fields)
- projectedRowsEqual
- visibleNonProjectedUpdateNoop

Tests / acceptance:

- Updating hidden field does not emit visible delta.
- Updating visible field emits upsert.
- Snapshot and active view projection match.

## 38. Compare Semantics Module

Priority: P1

Problem:

Sort comparison handles nulls, strings, BigDecimal, id tiebreaks, direction. It is central.

Solution:

Create CompareSemantics module:

- compareValues
- compareRowsForOrder
- stable id tiebreak
- null ordering
- string broad/literal strict rules

Tests / acceptance:

- Comparator parity against chDB for representative cases.
- Null ascending/descending.
- Case-insensitive broad strings.
- Strict literal strings.
- BigDecimal ordering.

## 39. chDB SQL Compiler Contract Tests

Priority: P1

Problem:

SQL generation bugs are dangerous. A previous concern was countSql missing WHERE in grouped branches. This deserves permanent contract tests.

Solution:

Add SQL compiler contract tests:

- raw rows SQL includes WHERE/order/limit
- raw count SQL includes WHERE
- grouped rows SQL includes WHERE/group/order/limit
- grouped count SQL counts grouped result after WHERE
- identifiers escaped

Tests / acceptance:

- Run compiler-only golden tests.
- Run real chDB execution tests proving counts match memory engine.

## 40. Worker RPC Protocol Module

Priority: P1

Problem:

Worker RPC messages, errors, metrics, and codecs cross thread/process boundaries. The protocol should be isolated.

Solution:

Create WorkerProtocol module:

- commands
- responses
- errors
- schemas
- codecs

Tests / acceptance:

- Schema decode rejects invalid protocol messages.
- BigDecimal query/mutation/result roundtrip.
- Node worker and in-process worker use same protocol types.

## 41. chDB Worker Protocol Contract

Priority: P1

Problem:

chDB child process protocol is a critical seam. It should be versioned/tested.

Solution:

Create ChdbWorkerProtocol:

- init
- applyBatch
- snapshot
- groupedRefreshSnapshot
- health
- close
- error response

Tests / acceptance:

- Invalid command rejected.
- Pending request fails on child exit.
- Health works during load.
- Shutdown waits bounded time then kills.

## 42. Runtime Source Grouping Module

Priority: P2

Problem:

Kafka/topic/source mapping will grow. Runtime should not embed all placement decisions.

Solution:

Create RuntimeSourceGraph:

- maps configured topics to source consumers
- maps topics to workers
- maps workers to chDB processes
- validates all topics before start

Tests / acceptance:

- Two topics, two workers, two chDB children.
- Missing Kafka topic fails startup.
- Reserved \_\_ topic rejected.

## 43. Demo App Contract

Priority: P2

Problem:

Demo app should prove public API, not use internal shortcuts.

Solution:

Audit orders-demo:

- imports only public packages/subpaths
- uses useLiveQuery AsyncResult
- no topic-specific generated hooks in view-server package
- uses real server path

Tests / acceptance:

- Demo build.
- Browser smoke desktop/mobile.
- Bundle grep for forbidden deps.

## 44. Documentation Source of Truth

Priority: P2

Problem:

plan.md is huge. Docs can drift from implementation.

Solution:

Create docs map:

- README: quick high-level
- docs/quickstart.md
- docs/testing.md
- docs/production-readiness.md
- docs/fault-tolerance.md
- docs/architecture.md
- docs/benchmarks.md
- docs/api-audit.md

Tests / acceptance:

- Link checker if available, or simple grep for referenced files.
- README points to authoritative docs.
- plan.md marks historical notes vs current contract.

## 45. Add CONTEXT.md

Priority: P1

Problem:

Architecture skills looked for CONTEXT.md and found none. Agents need domain language and load-bearing concepts.

Solution:

Create CONTEXT.md at repo root:

- domain vocabulary
- Module/Interface/Implementation map
- key invariants
- production architecture
- testing philosophy
- performance targets
- forbidden shortcuts

Tests / acceptance:

- Docs-only, but review for accuracy.
- Future agents can understand system without reading 20 files first.

## 46. ADRs For Load-Bearing Decisions

Priority: P1

Problem:

Decisions like mandatory chDB, one chDB child per topic, real-server tests, AsyncResult API, active plans, stale/catch-up should be recorded.

Solution:

Add docs/adr:

- ADR: chDB mandatory production runtime
- ADR: per-topic chDB child process
- ADR: real server tests with isolationId
- ADR: AsyncResult hook API
- ADR: active raw views and stale catch-up
- ADR: grouped queries chDB refresh vs incremental future

Tests / acceptance:

- Docs-only.
- ADRs include decision, context, consequences, alternatives rejected.

## 47. Public Runtime vs Internal Testing Seam

Priority: P1

Problem:

Memory backend is private but still exists. Make the seam impossible to misuse publicly.

Solution:

Audit exports and package files:

- memory backend not exported publicly
- testing-only helper names include internal/testing warning
- production config cannot select memory backend

Tests / acceptance:

- External consumer cannot import memory backend from public package.
- Internal tests can still use helper.
- Browser packages do not pull server deps.

## 48. Browser Forbidden Import Guard

Priority: P1

Problem:

This is too important to rely on manual grep.

Solution:

Create automated forbidden import test:

- scan packages/react, packages/testing browser-facing files, apps browser files
- forbid chdb, Kafka, worker_threads, fs, net, child_process, broad root core import if it drags server code

Tests / acceptance:

- Test fails if forbidden import added.
- CI runs it.
- External consumer bundle grep stays green.

## 49. Release Health Gate Module

Priority: P1

Problem:

Release readiness is currently a checklist and many commands. Make it executable.

Solution:

Create release gate script:

- check
- Effect LSP diagnostics
- tests
- build
- pack dry run
- external consumer smoke
- deployment smoke
- benchmark smoke
- policy scans
- optional 1M soak

Tests / acceptance:

- pnpm run release:gate runs required steps.
- CI can run a lighter mode.
- Output summarizes pass/fail and artifact paths.

## 50. Capacity Profile Matrix

Priority: P0

Problem:

We need explicit capacity promises and known limits. 1M passed; 10M currently does not. That must be visible.

Solution:

Create capacity matrix doc and artifacts:

- 100k dev profile
- 1M alpha profile
- 10M target profile
- raw-only subscriptions
- grouped subscriptions
- mixed publish/delta/delete
- active plan on/off/skipped
- chDB grouped refresh

Tests / acceptance:

- Each profile has a command.
- Each profile has latest result and artifact path.
- 10M profile is marked blocked until it reaches mutation phase.
- After active-plan admission fix, rerun 10M and update status.

## Suggested Work Order

Start with the tasks that unblock 10M and reduce risk:

1. Task 5: Active Plan Admission Policy.
2. Task 28: Capacity Soak Runner visibility.
3. Task 4: ActivePlanCoordinator extraction.
4. Task 2: MutationStore extraction.
5. Task 3: SnapshotReconciler extraction.
6. Task 6 and 7: FanoutQueue and SubscriptionRegistry.
7. Task 8: GroupedRefreshCoordinator.
8. Task 12 and 39: query/chDB parity suites.
9. Task 45 and 46: CONTEXT.md and ADRs.
10. Task 49 and 50: release gate and capacity matrix.

The fastest path to production is not adding features. It is making these Modules deep enough that the next performance bug has one obvious owner.
