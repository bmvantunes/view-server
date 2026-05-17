# Realtime Materialized View Server - Clean-Room Implementation Plan

> Status note, May 17 2026: this file is the historical clean-room implementation plan and
> bootstrap source of truth. The current implementation contracts now live in
> `view-server/README.md`, `view-server/CONTEXT.md`, `view-server/docs/architecture.md`,
> `view-server/docs/adr/`, and the focused operations/testing/release docs under
> `view-server/docs/`.

This document is the handoff plan for building a fresh repository for the view server. The current prototype repository was valuable for discovery, benchmarking, query semantics, Effect RPC experiments, browser tests, worker design, ClickHouse/chDB experiments, Kafka experiments, and client ergonomics. The new repository should use the lessons, not inherit the baggage.

Prototype reference root:

```text
/Users/bruno/projects/performance-filters-sort/view-server-effect
```

Additional local Effect references:

```text
/Users/bruno/projects/effect-smol
/Users/bruno/projects/t3code
```

The agent implementing the new repo should inspect these references before coding. In particular, use `/Users/bruno/projects/effect-smol` as the primary source of truth for current Effect v4 beta patterns, Effect RPC, websocket layers, NDJSON serialization, RpcTest/in-memory protocol testing, `@effect/vitest`, worker/drainable-worker patterns, and current package conventions. Use `/Users/bruno/projects/t3code` for app-level Effect usage and pragmatic project wiring. Use the old `view-server-effect` only as a prototype/reference, not as something to copy wholesale.

## Executive Summary

Build a TypeScript + Effect v4 beta realtime materialized view server.

The system ingests Kafka topics, keeps each topic isolated in its own worker, stores topic data in authoritative worker memory, mirrors each topic into its own chDB child process for fast initial snapshots, and serves live subscriptions over one multiplexed Effect RPC WebSocket using NDJSON.

The core product promise:

```text
Kafka topic -> topic worker -> authoritative in-memory topic store -> active materialized view -> Effect RPC websocket -> React hook
```

The system is not a generic database. It is a realtime UI projection engine optimized for tables/grids where users normally view small windows such as 0-50 or 50-150 rows, while the backend may ingest hundreds of thousands of messages per second.

Important architectural split:

```text
chDB / ClickHouse:
  Fast snapshot accelerator.
  Great for initial query, subscription changes, groupBy snapshots, and sanity checks.

Worker memory + custom incremental engine:
  Authoritative live state.
  Maintains active subscriptions.
  Emits minimal deltas.
  Guarantees no snapshot/delta gaps.
```

Use `@platformatic/kafka` as the Kafka client in the new implementation, even though one prototype benchmark favored KafkaJS for our local pure-consume shape. Keep the Kafka client behind an adapter so the decision remains reversible if future production benchmarks force it.

Use latest stable `@platformatic/kafka`, latest stable `chdb`, latest Effect v4 beta, and matching latest v4 beta `@effect/sql-clickhouse`.

## Non-Negotiable Principles

Use Effect-first, actually Effect-only for runtime architecture:

```text
Effect v4 beta
Effect RPC websocket transport
Effect errors on the wire
Effect Schema
Effect layers/services
Effect testing style where applicable
```

Do not build or keep a legacy custom WebSocket protocol.

Do not introduce a compatibility runtime for non-Effect consumers.

Do not create a separate HTTP polling metrics path as the primary model. The internal metrics UI should consume the same public hook/client API through the health/metrics topic. Basic HTTP `/healthz` and `/readyz` endpoints are still useful for load balancers and orchestration.

Do not allow user publishing to private internal topics such as `__view_server_health`.

Do not expose mutable metadata APIs. Metadata such as `totalRows`, versions, and source timestamps must be derived by the server.

Do not include a subscription `mode`. A subscription always means:

```text
snapshot first, then deltas forever until unsubscribe
```

One-shot reads are `client.query(topic, query)`, not a subscription mode.

## Dependency Policy

At project creation time, install latest compatible versions:

```text
effect: latest v4 beta
@effect/sql-clickhouse: latest matching v4 beta
@platformatic/kafka: latest stable
chdb: latest stable
@effect/vitest or current Effect v4 testing package: latest matching version
vite / vite-plus: current project standard
vitest: current compatible version
typescript: latest stable compatible with Effect v4
```

Kafka client decision:

```text
Use @platformatic/kafka as the production Kafka adapter.
Do not use KafkaJS in the implementation except as an optional benchmark-only adapter.
```

Snapshot backend decision:

```text
Use chDB, the in-memory ClickHouse engine, as the initial snapshot/query-change backend.
Wrap chDB behind a SnapshotBackend interface.
Keep the abstraction narrow enough that real ClickHouse, DuckDB, or another engine can be benchmarked and swapped later.
```

Effect decision:

```text
Use current Effect v4 beta APIs.
Use Effect RPC websocket support.
Use NDJSON serialization.
Use Effect errors across the wire.
Avoid old Effect v3 APIs and custom websocket plumbing.
```

## Observability / Spans

Use Effect-native tracing everywhere meaningful. Prefer:

```ts
Effect.fn("view-server.<area>.<operation>")(function* (...) {
  yield* Effect.annotateCurrentSpan({
    "view_server.topic": topic,
  })

  return yield* operation(...)
})
```

Use `Effect.fn("view-server.<area>.<operation>")` for service boundaries, RPC handlers, worker commands, Kafka batch handling, chDB snapshot queries, backend flushes, fanout, and subscription lifecycle work.

Use `Effect.withSpan("view-server.<area>.<operation>")` only for local/ad hoc blocks inside larger functions.

Use `Effect.annotateCurrentSpan(...)` for runtime attributes:

```text
view_server.topic
view_server.subscription_id
view_server.request_id
view_server.worker_version
view_server.backend_version
view_server.batch_size
view_server.rows
view_server.total_rows
view_server.kafka.partition
view_server.kafka.offset
view_server.kafka.lag
```

Do not create spans per row in the firehose path. Span per batch, query, snapshot, delta fanout, RPC request, worker command, and backend flush. Tiny pure helpers should stay plain functions or use `Effect.fnUntraced` if they must return Effect.

## Naming

The working product name can be:

```text
Realtime Materialized View Server
```

Short code/package name can remain:

```text
view-server
```

The term "view server" is fine internally, but public documentation should emphasize:

```text
realtime materialized views
live query windows
multiplexed subscriptions
incremental fanout
```

## Reference Files In Prototype

Use these prototype files as conceptual references:

```text
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/config.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/protocol.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/rpc.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/client.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/topic-worker-core.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/topic-worker.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/topic-workers.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/worker-protocol.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/chdb-query-backend.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/chdb-query-backend.test.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/rpc-inmemory.test.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/rpc-server.test.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/topic-worker-core.test.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/client-hook.browser.test.tsx
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/e2e.browser.test.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/system-topics.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/metrics-ui.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/metrics-ui-hook-app.tsx
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/view-server.bench.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/subscription-window.bench.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/runtime.bench.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/kafka-client-consume-spike.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/clickhouse-kafka-decode-vs-forward-spike.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/clickhouse-effect-sql-spike.ts
```

Use `/Users/bruno/projects/effect-smol` to verify:

```text
Effect RPC current API
Effect websocket layers
NDJSON layer naming and usage
RpcTest / in-memory protocol patterns
@effect/vitest usage
worker/drainable worker patterns
Effect v4 package names and imports
```

## High-Level Architecture

The new repository should have four major runtime zones.

### 1. Gateway Process

Responsibilities:

```text
Load defineConfig.
Normalize config.
Start one topic worker per topic.
Start Effect RPC websocket server.
Route Subscribe/Unsubscribe/Query/Publish/DeltaPublish/Health to workers.
Serve health/readiness endpoints.
Serve internal metrics UI.
Own auth decisions before commands reach workers.
Own client connection lifecycle.
Handle graceful shutdown.
```

The gateway does not own topic rows. It is a router/control plane.

### 2. Topic Worker

Responsibilities:

```text
Consume Kafka topic.
Decode Kafka records.
Validate rows with Effect Schema.
Maintain authoritative in-memory row store.
Maintain id index.
Maintain active subscription materializations.
Maintain dependency field index.
Maintain chDB in-memory mirror.
Maintain version log.
Emit snapshots/deltas to gateway.
Emit topic metrics.
Commit Kafka offsets according to policy.
```

Each topic worker is independent. No joins. No cross-topic subscriptions. No shared data structures.

### 3. chDB Snapshot Backend

Responsibilities:

```text
Mirror topic rows in in-memory ClickHouse.
Compile query language to SQL.
Return candidate snapshots quickly.
Return totalRows.
Return backendVersion for version fencing.
Support raw queries and grouped aggregate queries.
```

chDB is not authoritative. Worker memory is authoritative.

### 4. Client Runtime

Responsibilities:

```text
Open one Effect RPC WebSocket connection.
Use multiplexed RPC streams for many subscriptions.
Expose typed query API.
Expose typed publish/deltaPublish API.
Expose React useLiveQuery.
Hydrate initialData from SSR/TanStack Query.
Apply snapshots and deltas through a store.
Reconnect and resubscribe.
Surface Effect wire errors as typed client errors.
```

## Data Consistency Model

This is critical.

Worker memory is authoritative. chDB can be behind. chDB snapshots cannot be emitted directly unless they are fenced to a known worker version.

Problem:

```text
Kafka -> worker memory
Kafka -> batched chDB mirror
Subscribe -> chDB snapshot
```

chDB may be 10, 20, or 1000 messages behind worker memory. If we emit a stale chDB snapshot and then start live deltas from current time, the client can miss rows or see impossible state.

Correct invariant:

```text
The first snapshot and all following deltas must represent one continuous version stream.
No gaps.
No double-applies.
No stale snapshot followed by newer deltas.
```

Use the pro version: **version fence**.

Types:

```ts
type WorkerVersion = bigint;

type SnapshotBackendResult<Row> = {
  rows: Row[];
  totalRows: number;
  backendVersion: WorkerVersion;
};

type MutationLogEntry<Row> = {
  version: WorkerVersion;
  kind: "insert" | "update" | "delete";
  id: string | number;
  before?: Row;
  after?: Row;
  changedFields: ReadonlySet<string>;
};
```

Worker mutation flow:

```text
1. Kafka message or batch arrives.
2. Worker decodes and validates.
3. Worker applies mutation to authoritative memory.
4. Worker increments workerVersion.
5. Worker appends mutation to bounded mutation log.
6. Worker asynchronously/batched flushes rows to chDB.
7. After chDB flush succeeds, record chdbVersion = highest flushed workerVersion.
```

Subscribe flow:

```text
1. Capture targetVersion = current workerVersion.
2. Ask chDB for candidate snapshot.
3. chDB returns backendVersion.
4. If backendVersion === targetVersion, emit the chDB snapshot.
5. If backendVersion < targetVersion, replay mutation log from backendVersion + 1 through targetVersion onto the candidate snapshot.
6. If replay is unsupported or the mutation log no longer covers the full gap, fallback to computing the snapshot from authoritative worker memory.
7. Emit snapshot with version = targetVersion.
8. Set subscription.lastVersion = targetVersion.
9. Emit future deltas only for versions > targetVersion.
```

chDB never decides freshness. Worker decides freshness.

Snapshot backend contract:

```ts
interface SnapshotBackend<Row> {
  init(args: {
    topic: string;
    idField: string;
    schema: TopicSchema;
    rows: readonly VersionedRow<Row>[];
    version: WorkerVersion;
  }): Effect.Effect<void, SnapshotBackendError>;

  applyBatch(args: {
    mutations: readonly MutationLogEntry<Row>[];
    highestVersion: WorkerVersion;
  }): Effect.Effect<void, SnapshotBackendError>;

  snapshot<Query>(args: {
    query: Query;
    targetVersion: WorkerVersion;
  }): Effect.Effect<SnapshotBackendResult<Row>, SnapshotBackendError>;

  close(): Effect.Effect<void>;
}
```

The actual `snapshot` may ignore `targetVersion` internally at first and return `backendVersion = currentFlushedVersion`. The worker will reconcile or fallback.

Replay rules:

```text
If query is raw filter/sort/page and mutation log gap is small, replay onto candidate rows and re-sort/re-page.
If query is grouped and replay implementation is proven correct, replay group states.
If replay is complex or unsafe, fallback to worker-memory snapshot.
Never emit guessed state.
```

Important fallback:

```text
if backendVersion < targetVersion and mutationLog does not contain every version in the gap:
  compute full snapshot from worker memory
```

This keeps correctness independent of chDB flush lag.

## defineConfig Specification

`defineConfig` is the only source of truth for topics.

Minimal example:

```ts
import { Schema, Effect } from "effect";
import { defineConfig, KafkaSource } from "@view-server/core";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
  qty: Schema.Number,
  status: Schema.Literal("OPEN", "CLOSED", "PARTIAL", "FILLED"),
  updatedAt: Schema.BigInt,
});

export default defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
      source: KafkaSource({
        brokers: ["127.0.0.1:9092"],
        topic: "orders",
        groupId: "view-server-orders",
        decode: decodeOrderFromKafkaRecord,
      }),
    },
  },
  rpc: {
    serialization: "ndjson",
  },
  worker: {
    maxQueueDepth: 100_000,
  },
});
```

Topic config:

```ts
type TopicConfig<Row> = {
  id: keyof Row & string;
  schema: Schema.Schema<Row>;
  schemaVersion?: number;
  migrate?: (row: unknown, context: MigrationContext) => Effect.Effect<Row, ViewServerError>;
  source?: TopicSource<Row>;
  snapshot?: {
    flushBatchSize?: number;
    flushIntervalMs?: number;
    maxVersionLagBeforeMemoryFallback?: number;
  };
};
```

Schema is mandatory. No schema-less topics.

Reasons schema is mandatory:

```text
Type-safe queries.
Type-safe publisher.
Type-safe generated hooks.
Runtime validation.
ClickHouse/chDB schema generation.
String/number/bigint/boolean filter semantics.
Future indexes.
Serialization.
Schema evolution.
```

The topic should still accept any business shape as long as it is represented by Effect Schema. The server itself should remain topic-agnostic; all knowledge comes from config/schema.

Source config:

```ts
type KafkaSource<Row> = {
  _tag: "KafkaSource";
  brokers: readonly string[];
  topic: string;
  groupId: string;
  decode(record: KafkaConsumerRecord): Effect.Effect<KafkaSourceMessage<Row>, ViewServerError>;
  commitPolicy?: "after-ingest" | "none";
  maxIngestRetries?: number;
};
```

Kafka source messages:

```ts
type KafkaSourceMessage<Row> =
  | Row
  | { type: "publish"; row: Row }
  | { type: "delta-publish"; patch: Partial<Row> & Pick<Row, IdField> }
  | { type: "delete"; id: string | number };
```

Do not hardcode business topics such as `orders` inside runtime code.

## Query Language

Remove `mode`.

A subscription always returns snapshot followed by deltas.

One-shot query:

```ts
client.query("orders", query);
```

Live subscription:

```ts
client.subscribe("orders", query, callbacks);
```

React:

```ts
const result = useLiveQuery("orders", query, initialData);
```

Raw query:

```ts
type RawQuery<TTopic, TFields = FieldProjection<TTopic>> = {
  fields: TFields;
  where?: FilterNode<TTopic>;
  orderBy?: OrderBy<TTopic>;
  offset?: number;
  limit?: number;
};
```

Grouped query:

```ts
type GroupedQuery<TTopic, TGroupBy, TAggregates> = {
  groupBy: TGroupBy;
  aggregates: TAggregates;
  where?: FilterNode<TTopic>;
  orderBy?: OrderByGrouped<TTopic, TGroupBy, TAggregates>;
  offset?: number;
  limit?: number;
};
```

Default pagination:

```text
offset defaults to 0
limit defaults to 50
limit should be clamped to 50 by default unless config explicitly permits more
```

Reason:

```text
Real UI grids should almost never receive more than 50-100 rows.
The whole architecture is optimized for tiny visible windows over huge topics.
```

## Filter Semantics

Supported number/bigint filters:

```text
equals
not_equals
greater_than
greater_than_or_equal
less_than
less_than_or_equal
one_of
```

Supported string filters:

```text
equals
not_equals
contains
starts_with
one_of
```

Supported boolean filters:

```text
equals
not_equals
one_of
```

Combinators:

```text
and
or
```

String runtime semantics:

```text
String equals is case-insensitive for broad string fields.
String one_of is case-insensitive for broad string fields.
String contains is case-insensitive.
String starts_with is case-insensitive.
String sorting is case-insensitive.
```

Type-level literal semantics:

```text
If schema says status: "OPEN" | "CLOSED", then only "OPEN" or "CLOSED" are accepted by TypeScript.
"open", "closed", "CLOSeD" are invalid.
```

This means:

```text
Runtime can normalize broad string fields.
Literal union types remain strict at compile time.
```

Dates:

```text
Represent dates/timestamps as bigint nanosecond epochs internally.
Effect can serialize BigInt over the wire.
Do not introduce Date object semantics in the core engine.
```

## Sort Semantics

Raw sorting:

```text
Apply user orderBy fields in order.
For string fields, compare case-insensitively.
For null/undefined, asc puts nulls first; desc puts nulls last.
Always add id asc as final deterministic tiebreaker unless id is already present.
```

Example ascending string order:

```text
aaaaaaa
AAAAAAAAAAAAAAA
b
CCCCCC
```

Do not use default JavaScript `.sort()` string behavior directly. It produces uppercase-before-lowercase ordering that is wrong for this product.

Grouped sorting:

```text
Can sort by groupBy fields or aggregate aliases.
String ordering should be case-insensitive.
Stable deterministic fallback should exist.
```

## Aggregations

Supported aggregate functions:

```text
count
count_distinct
sum
avg
min
max
string_concat
string_concat_distinct
```

Use `aggFunc`, not `op`.

Examples:

```ts
aggregates: {
  orders: { aggFunc: "count", field: "id" },
  traders: { aggFunc: "count_distinct", field: "traderId" },
  totalQty: { aggFunc: "sum", field: "qty" },
  avgPrice: { aggFunc: "avg", field: "price" },
  minPrice: { aggFunc: "min", field: "price" },
  maxPrice: { aggFunc: "max", field: "price" },
  texts: { aggFunc: "string_concat", field: "text", joiner: ",", sort: "asc" },
  uniqueVenues: { aggFunc: "string_concat_distinct", field: "venue", joiner: "|", sort: "desc" }
}
```

String aggregation:

```text
string_concat keeps duplicates.
string_concat_distinct removes duplicates.
sort asc/desc controls aggregate string order.
asc should be default for distinct if a stable order is needed.
```

GroupBy case sensitivity:

```text
Take groupBy values at face value.
Do not case-normalize group keys unless explicitly added as a future query option.
```

## totalRows Semantics

Every snapshot and meaningful delta includes `totalRows`.

Definition:

```text
Raw query:
  totalRows = count after where filters before offset/limit.

Grouped query:
  totalRows = number of groups after where/groupBy/having semantics before offset/limit.
```

`totalRows` is a number only. Never send the full dataset just to compute total rows.

Important live behavior:

```text
If new data changes totalRows but does not change the visible page, emit a delta with totalRows updated and no row operations.
```

Example:

```text
Trader is viewing old orders sorted by createdAt asc.
New orders arrive at the end and do not affect visible page.
Grid rows should not rerender.
Counter should update from 1,000,111 to 1,000,112.
```

This was a major product requirement because systems like AMPS often force two subscriptions: one for rows, one for counts. This server should provide both in one subscription.

## RPC API

Use Effect RPC only.

Reference prototype:

```text
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/rpc.ts
```

Required RPCs:

```text
Subscribe
Unsubscribe
Query
Publish
DeltaPublish
Health
```

Suggested group:

```ts
const ViewServerRpcs = RpcGroup.make(
  Rpc.make("Subscribe", {
    payload: SubscribePayload,
    success: SubscriptionEvent,
    error: ViewServerError,
    stream: true,
  }),
  Rpc.make("Unsubscribe", {
    payload: UnsubscribePayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("Query", {
    payload: QueryPayload,
    success: QueryResponse,
    error: ViewServerError,
  }),
  Rpc.make("Publish", {
    payload: PublishPayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("DeltaPublish", {
    payload: DeltaPublishPayload,
    success: Schema.Void,
    error: ViewServerError,
  }),
  Rpc.make("Health", {
    payload: HealthPayload,
    success: HealthResponse,
    error: ViewServerError,
  }),
);
```

Transport:

```text
Effect RPC websocket layer.
NDJSON serialization.
One physical websocket per browser/client.
RPC multiplexing for all subscriptions.
```

No raw websocket protocol. No handmade message envelope outside Effect RPC.

Errors:

```text
Use TaggedError classes.
Let Effect errors travel on the wire.
Do not parse strings to recover error types.
```

Example errors:

```text
MissingTopic
MissingTopicId
InvalidQuery
InvalidFilter
InvalidPublish
Unauthorized
WorkerUnavailable
SnapshotBackendLagExceeded
SnapshotBackendFailed
KafkaIngestFailed
SchemaDecodeFailed
VersionGap
```

## Wire Events

Subscription stream events:

```ts
type SnapshotEvent<Row> = {
  type: "snapshot";
  requestId: string;
  rows: Row[];
  meta: {
    version: string;
    totalRows: number;
    backendVersion?: string;
    serverTime: number;
  };
};

type DeltaEvent<Row> = {
  type: "delta";
  requestId: string;
  ops: DeltaOperation<Row>[];
  meta: {
    fromVersion: string;
    toVersion: string;
    totalRows: number;
    sourceUpdatedAt?: number | bigint;
    serverTime: number;
  };
};
```

Delta operations:

```ts
type DeltaOperation<Row> =
  | { type: "upsert"; row: Row; index?: number }
  | { type: "patch"; key: string | number; changes: Partial<Row>; index?: number }
  | { type: "remove"; key: string | number };
```

The client store should not rely on metadata mutation from tests. Metadata is server-derived.

## Client API

Expose one canonical hook name:

```text
useLiveQuery
```

Do not keep legacy subscription hook names alongside `useLiveQuery` unless strictly temporary during a migration. The public React hook API should standardize on `useLiveQuery`.

Client creation:

```ts
const client = createViewServerClient(config, {
  url: "wss://example.com/view-server",
});
```

Query:

```ts
const initialData = await Effect.runPromise(client.query("orders", query));
```

React:

```tsx
const result = hooks.useLiveQuery("orders", query, initialData);
```

Generated topic helpers:

```ts
client.topics.orders.query(query);
client.topics.orders.subscribe(query, callbacks);
hooks.topics.orders.useLiveQuery(query, initialData);
```

Publisher:

```ts
await Effect.runPromise(client.publish("orders", row));
await Effect.runPromise(client.deltaPublish("orders", { id: "o-1", price: 123 }));
```

Type safety:

```text
publish requires full row according to schema.
deltaPublish requires id plus partial row.
query fields/order/filter are typed by topic schema.
aggregates are typed by field type.
literal string fields remain strict.
```

SSR/TanStack Query:

```tsx
loader: async ({ context }) => {
  await context.queryClient.ensureQueryData(viewServerQuery(client, "orders", query));
};

component: () => {
  const initialData = useSuspenseQuery(viewServerQuery(client, "orders", query)).data;
  const result = hooks.useLiveQuery("orders", query, initialData);
  const value = AsyncResult.getOrElse(result, () => ({
    rows: initialData.rows,
    totalRows: initialData.totalRows,
    status: "connecting",
    connection: { connected: false, attempt: 0 },
  }));
  return <Grid rows={value.rows} totalRows={value.totalRows} />;
};
```

The important model:

```text
client.query gives initial data for SSR/loader.
useLiveQuery hydrates from initialData and then connects to live stream.
```

`initialData` is not rows-only. It must include the query result rows and exact `totalRows`, so SSR-prefilled grids can render pagination totals correctly before the websocket snapshot arrives.

React hooks should return Effect's `AsyncResult` directly. The success value should include `rows`, `totalRows`, and connection/status metadata. Reconnecting and stale-refresh states should be represented with `AsyncResult.waiting(...)`, so UI can keep showing stale data while the live subscription refreshes.

Canonical success value shape:

```ts
type LiveQueryValue<T> = {
  readonly rows: readonly T[];
  readonly totalRows: number;
  readonly status: "connecting" | "live" | "reconnecting" | "stale";
  readonly connection: {
    readonly connected: boolean;
    readonly attempt: number;
    readonly lastConnectedAt?: number;
    readonly lastDisconnectedAt?: number;
  };
};
```

## React Store Behavior

Use a store around `useSyncExternalStore` or equivalent. Avoid doing expensive delta shuffling directly in render.

Use `startTransition` when applying incoming snapshot/delta state to React.

The prototype tested worker-offloaded client subscriptions and found they were not worth keeping. Keep only normal `useLiveQuery` in the new repo unless future browser benchmarks prove otherwise.

Do not introduce `useLiveQueryWorker` at first.

Client store responsibilities:

```text
Apply snapshot.
Apply delta ops.
Handle totalRows-only deltas.
Detect version gaps.
Reconnect and resubscribe.
Expose status transitions.
Avoid rerender when only invisible metadata has not changed.
```

Statuses:

```text
connecting
stale
reconnecting
live
```

## Topic Worker Internals

Reference:

```text
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/topic-worker-core.ts
```

The worker core should be testable in-process without actual worker threads.

State:

```ts
type TopicWorkerState<Row> = {
  topic: string;
  idField: keyof Row & string;
  version: WorkerVersion;
  rows: Row[];
  idIndex: Map<string | number, number>;
  activeSubscriptions: Map<string, CompiledSubscription<Row>>;
  dependencyIndex: Map<string, Set<string>>;
  mutationLog: RingBuffer<MutationLogEntry<Row>>;
  snapshotBackend: SnapshotBackend<Row>;
  metrics: TopicMetrics;
};
```

Important worker commands:

```text
init
subscribe
unsubscribe
query
publish
delta-publish
delete-by-id
append-batch
replace-for-test
get-rows-for-test
shutdown
crash-for-test
```

Important worker events:

```text
ready
snapshot
delta
query-result
error
metrics
```

Subscription compilation:

```text
Normalize query.
Ensure id projection for raw queries.
Collect dependency fields.
Collect index candidates.
Compile custom execution plan.
Prepare chDB SQL plan if possible.
```

Dependency pruning:

```text
For updates, only evaluate subscriptions whose dependency fields changed.
For inserts, all subscriptions may be affected because a new row can enter any filter/window.
For deletes, subscriptions depending on id/window/order/filter may be affected.
```

Dependency fields include:

```text
id field
projected fields
filter fields
sort fields
groupBy fields
aggregate fields
```

## Active Top-K / Window Maintenance

Core idea:

```text
Initial snapshot can come from chDB.
After that, maintain the active materialized window in memory.
```

For raw sorted windows:

```text
Maintain enough materialized prefix to answer offset + limit.
Example: offset 50, limit 100 -> maintain top 150 rows for that query.
On insert/update/delete, adjust this prefix if possible.
Emit visible window rows offset..offset+limit.
```

If a row changes sort position:

```text
Remove old version from materialized prefix if present.
Evaluate new row against filter.
Insert new version into sorted materialized prefix if it belongs.
If movement may affect prefix boundary and we lack enough data, recompute from worker memory or ask chDB with version fence.
```

Pathological example to test:

```text
Sort desc by name.
Top visible row "alex" changes to "John".
The row may move to position 20 or 30.
The engine must remove it from old position and place it correctly, shifting other rows.
```

For grouped queries:

```text
Maintain group aggregate states per active subscription when feasible.
For inserts, update one group.
For deletes/updates, update old and new group.
Sort grouped rows and page.
If unsafe, recompute from worker memory or chDB with fence.
```

Be conservative:

```text
Correctness first.
Fast path only when proven safe.
Fallback is allowed.
```

Current implementation note:

````text
Raw non-grouped subscriptions compile an ActiveRawView after the initial snapshot.
ActiveRawView reuses query-engine filter, projection, comparator, null ordering, case-insensitive string, BigDecimal, and id tiebreak helpers.
Grouped subscriptions intentionally do not use the raw ActiveRawView path yet. Initial grouped snapshots use the normal version-fenced chDB snapshot path in production, with private memory fallback only inside internal test/fault-injection paths. During relevant mutations, grouped subscriptions are marked stale/dirty and receive advisory coalescible status events instead of recomputing the grouped query on every publish. A debounced refresh snapshot recomputes the grouped result, advances the subscription version, and restores live state. Refresh snapshots keep totalRows as the grouped row count after filters and aggregation. While stale, status events carry the last known grouped totalRows until the refresh snapshot lands. Grouped refresh captures a row-array snapshot under the topic worker gate, computes grouped aggregation cooperatively outside the gate, and then installs the refresh under the gate only if no newer relevant mutation made the result stale. If newer relevant mutations arrive while a refresh is computing, the stale refresh is discarded and another debounced refresh is scheduled.
The production chDB mirror is per topic worker: Topic A owns chDB child A, Topic B owns chDB child B. There is no shared global chDB child process. This keeps IPC queues, memory pressure, backend version fencing, failure, restart, and shutdown boundaries per topic.
ActiveRawView.applyMutation classifies changes as noop, totalRowsOnly, or changed so the worker avoids materializing and diffing the visible page unless rows actually changed.
Active view correctness tests compare every incremental snapshot with executeRawQuery across null sorting, case-insensitive matching, literal strict matching, id tiebreaks, deletes, threshold crossing, sorted movement, offset windows, visible projected value updates, visible non-projected no-ops, and totalRows-only changes.
Active view fuzz tests run deterministic random insert/update/delete sequences against both array and block indexes, comparing each step with executeRawQuery across comparator-equal ids, offset windows, nulls, and case-insensitive strings.
packages/core/bench/active-view.bench.ts compares full recompute vs active view updates for random updates, hot-key updates, irrelevant-field updates, threshold crossings, sorted movement, and totalRows-only inserts with row/subscription/page-size/scenario/index env knobs. The benchmark validates ordered row ids and key values separately from the timed active update loop. Set VS_ACTIVE_VIEW_MEMORY=1 and run node with --expose-gc to measure retained heap/RSS after active-view build while plans/views are still strongly referenced.
Use VS_ACTIVE_VIEW_BASELINE=0 for large active-path timing when full recompute is too slow to finish interactively. Use VS_ACTIVE_VIEW_VALIDATE=0 only for pure timing after correctness has already been checked. Use VS_ACTIVE_VIEW_SCENARIOS=hot-key-updates,sorted-row-movement and VS_ACTIVE_VIEW_INDEXES=array,blocks to target specific comparisons.
Use VS_ACTIVE_VIEW_MUTATIONS=0 to measure active-view build time separately from mutation updates.
ActiveRawView now has an internal sorted index choice. The array backend remains available for benchmark comparison, but the runtime default is the block-list backend. The block list uses comparator binary search across block maxima, exact-key scans inside comparator-equal ranges, split/merge block maintenance, and offset/limit slicing across blocks.
Raw subscriptions now share ActiveRawPlan instances by normalized where + orderBy. Offset/limit and selected fields remain per-subscription windows over the shared plan. Fanout applies each mutation once per shared plan, then each subscription computes its own noop/totalRowsOnly/changed transition.
ActiveRawPlan keys intentionally exclude schema-derived execution options such as literalStringFields. This is safe only because active plan caches are per topic worker, and each topic worker has exactly one schema/options set. Do not move this cache to a global cross-topic scope without including those options in the key.
ActiveRawPlan construction is asynchronous, bounded, and cooperative. A new subscription receives its version-fenced snapshot immediately. While a shared active plan is queued or building, pending subscriptions do not full-recompute on every publish. Relevant mutations mark the subscription dirty and enqueue a typed status event with status stale, latest cheap totalRows, and AsyncResult waiting semantics on the client. Stale status events are advisory state transitions, not a per-mutation event stream, and they may be coalesced under pressure. Worker memory remains authoritative. Plan builds yield during filter scan, byte-limit estimate, chunk sort, and merge phases so the topic worker can keep processing publish/Kafka/fanout/health/unsubscribe work during large builds. Active build snapshots copy the row array once at build start; publish/update/delete mutate the current topic row array in place afterward, so build snapshots remain stable without paying a 1M-row copy per publish. Deletes swap-remove from the topic row array and update only affected id-index entries, avoiding a full id-index rebuild per delete. When the build finishes, the worker catches the plan up from the build snapshot version through the mutation log, then atomically switches still-subscribed request ids to the active plan and emits one refresh snapshot for dirty subscriptions. If the mutation log no longer covers the gap or the build is discarded, dirty subscriptions get one memory snapshot refresh and then stay on the memory fallback path. Builders are limited by worker.activePlanBuildConcurrency, default 1. Unsubscribed pending builds are discarded before activation.
packages/core/bench/active-plan-responsiveness.bench.ts measures operation and metrics latency while a cooperative active-plan build is in progress. Defaults are 1M rows, 50 operations, page size 50, operation publish, and the benchmark logs row generation, worker seeding, build-observed timing, operation/metrics latency percentiles, stale status count, snapshot count, and delta count. The benchmark accepts VS_ACTIVE_PLAN_RESPONSIVENESS_OPERATION=publish|deltaPublish|deleteById and VS_ACTIVE_PLAN_RESPONSIVENESS_CHUNK_SIZE for experiments. Before dirty/catch-up plus row storage fixes, a 1M-row run with 5 measured operations and default chunk size showed metrics p99 roughly 0.14ms while publish p99 was roughly 299.03ms. After dirty/catch-up, linear initial seeding, mutable hot rows, and one copied build snapshot, a 1M-row run with 5 measured operations showed worker seeding around 1.48s, build observed around 1.82s, metrics p99 roughly 0.08ms, and publish p99 roughly 1.00ms. A 1M-row, 1k-operation run with default chunk size showed 980/1000 samples while the plan was pending/building and 490 coalesced stale status events for each operation mode. Publish showed operation p50/p95/p99/max roughly 0.17/0.42/0.53/1.07ms and metrics p99 roughly 0.12ms. deltaPublish showed operation p50/p95/p99/max roughly 0.21/0.48/0.80/2.01ms and metrics p99 roughly 0.13ms. deleteById originally did not finish 1k deletes after more than 2 minutes because it rebuilt the full id index per delete; after swap-remove delete storage, deleteById showed operation p50/p95/p99/max roughly 0.20/0.44/0.97/12.35ms and metrics p99 roughly 0.14ms. Each 1k run emitted 1 catch-up snapshot and 10 deltas after activation.
packages/core/bench/grouped-responsiveness.bench.ts measures grouped-query mutation responsiveness with configurable aggregate counts. Defaults are 1M rows, 1k deltaPublish operations, grouped aggregate counts 10/50/100, debounce 50ms, operation pause 1ms, and page limit 50. The benchmark logs operation/metrics latency percentiles, stale status count, snapshot count, delta count, final rows/version, and max subscription lag. It accepts VS_GROUPED_RESPONSIVENESS_ROWS, VS_GROUPED_RESPONSIVENESS_OPERATIONS, VS_GROUPED_RESPONSIVENESS_OPERATION=publish|deltaPublish|deleteById, VS_GROUPED_RESPONSIVENESS_AGGREGATES=10,50,100, VS_GROUPED_RESPONSIVENESS_DEBOUNCE_MS, VS_GROUPED_RESPONSIVENESS_OPERATION_PAUSE_MS, and VS_GROUPED_RESPONSIVENESS_LIMIT.
packages/core/bench/grouped-refresh-overlap.bench.ts specifically measures publish responsiveness while grouped refresh computation overlaps with active mutations. It reports publish operation percentiles and start-gap percentiles because same-thread refresh freezes can delay the next publish before the publish call starts. It accepts VS_GROUPED_REFRESH_OVERLAP_BACKEND=memory|chdb. A before-fix 1M-row, 20-publish, 100-aggregate run with debounce 1ms showed operation p99/max roughly 2.09/2.09ms but startGap p95/p99/max roughly 4917/5296/5296ms, proving the topic worker event loop was frozen by gated grouped refresh. After cooperative/off-gate memory grouped refresh, the same shape showed operation p99/max roughly 1.41/1.41ms, startGap p95/p99/max roughly 25.90/26.24/26.24ms, one catch-up snapshot, settled=true, settleMs roughly 12.1s, and maxSubscriptionLagVersions 0. chDB grouped refresh is now off the topic worker thread through the worker-backed chDB snapshot backend. The topic worker captures request/version, the chDB worker runs the snapshot query, and the result is accepted only when backendVersion exactly matches the requested version and no newer dirty version exists; otherwise the result is discarded/rescheduled or falls back to cooperative memory. The chDB worker protocol explicitly encodes Effect BigDecimal values across rows, query filters, mutations, and snapshot results so structured clone does not erase decimal identity. A 250k-row, 20-publish, 100-aggregate chDB overlap run showed initial snapshot roughly 155ms, operation p99/max roughly 1.05/1.05ms, startGap p95/p99/max roughly 1.73/5.03/5.03ms, settled=true, settleMs roughly 198ms, and maxSubscriptionLagVersions 0. A 1M-row chDB run with the same shape showed worker seeding roughly 12.01s, initial snapshot roughly 473ms, operation p99/max roughly 3.07/3.07ms, startGap p95/p99/max roughly 6.67/23.31/23.31ms, settled=true, settleMs roughly 1.06s, and maxSubscriptionLagVersions 0. The previous synchronous chDB start-gap spike around 564ms is gone; remaining caveat is initial chDB worker seeding/transfer cost for very large in-memory topic state.
Benchmark entrypoints for active-view, active-plan responsiveness, grouped responsiveness, and grouped refresh overlap write JSON artifacts through packages/core/bench/benchmark-artifacts.ts. By default artifacts go under packages/core/bench/.artifacts, and VS_BENCH_ARTIFACT overrides the output path. VS_BENCH_BASELINE points at a previous artifact and compares matching case/metric pairs. Lower-is-better metrics fail when current > baseline * (1 + VS_BENCH_REGRESSION_TOLERANCE), with the tolerance defaulting to 0.10. VS_BENCH_REGRESSION_MIN_DELTA_MS can downgrade tiny millisecond-only regressions to warnings, so noisy sub-threshold timing movement does not become a hard failure. VS_BENCH_REGRESSION_METRICS can restrict comparisons to selected hot metrics. Metrics that are higher-is-better set lowerIsBetter=false and are skipped by regression failure. When GITHUB_STEP_SUMMARY is present, artifact comparison appends a Markdown summary table with current value, baseline value, delta, status, and artifact path.
GitHub Actions benchmark CI starts with tiny smoke shapes only. .github/workflows/benchmarks.yml compares generated artifacts against checked-in smoke baselines under packages/core/bench/baselines/ci-smoke, uploads generated artifacts, appends the GitHub Actions step summary, and defaults to reporting-only via VS_BENCH_BLOCKING=0. Set VS_BENCH_BLOCKING=1 once the smoke baselines are stable enough to block PRs. docs/benchmarks.md documents vp run core#bench:compare for local comparison and vp run core#bench:refresh-baselines for refreshing the checked-in smoke baselines. This is intentionally not a full benchmark platform; larger shapes remain manual until we have a policy for artifact retention and PR gating.
Topic worker state-machine review notes live in docs/worker-state-machine-review.md. The hardening coverage now includes duplicate request-id replacement, transient backend snapshot failure/recovery, websocket reconnect while stale, unsubscribe during active-plan build and grouped refresh, mutation-log gap during active-plan catch-up, stale grouped refresh discard/reschedule, stale-status/catch-up queue pressure, and a mixed-load soak test. packages/core/tests/worker-soak.test.ts defaults to a CI-safe shape and can be scaled manually with VS_WORKER_SOAK_ROWS=1000000, VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=250, VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=20, VS_WORKER_SOAK_MUTATIONS=10000, VS_WORKER_SOAK_TIMEOUT_MS=900000, and VS_WORKER_SOAK_SUMMARY_PATH=/private/tmp/view-server-worker-soak-1m-summary.json.
DX checkpoint: the root README now starts with a practical quickstart instead of the starter template. docs/quickstart.md covers defineConfig, server startup, publishing, and React useLiveQuery, and states that production runtime requires chDB. docs/operations.md documents stale/waiting AsyncResult semantics, totalRows, version fencing, chDB fallback after startup, active plan limits, health metrics, benchmark summaries, and troubleshooting. docs/hello-production.md shows the smallest Kafka + chDB + metrics UI wiring. docs/testing.md documents the recommended app-testing path: real View Server, real websocket RPC, chDB runtime, required isolationId, testing publisher helpers, scoped test queries, and explicit test:server:start/test:server:stop scripts. apps/website is now the orders-demo app: it defines an orders topic, starts a local Effect RPC websocket server with deterministic publishes, and renders raw orders plus grouped desk metrics through createViewServerReact/useLiveQuery.

Release candidate packaging checkpoint: public package entrypoints are explicit, runtime imports point at built dist files, and type entries point at source files for config-derived TypeScript inference. @view-server/core root reexports public client/config/errors/kafka/query/runtime/snapshot APIs; RPC, websocket, platformatic Kafka, chDB, and Node worker APIs are behind explicit subpaths; worker internals, chDB worker protocols/codecs, query-engine internals, and testing helpers are not root exports. chDB is a required peer for production runtime/server consumers. @effect/platform-node and @platformatic/kafka remain optional peers for their node-only subpaths. React/browser and testing browser bundles must not import chDB, Kafka, worker threads, fs, or net. @view-server/react exports createViewServerReact/createViewServerHooks/browser websocket helpers and the metrics UI, and imports public core subpaths only. @view-server/testing exports inMemoryViewServer, isolatedInMemoryViewServer, makeTestingBrowserWebsocketClient, and createTestingViewServerReact. Package metadata requires Node >=26, ESM, sideEffects false, explicit exports, dist+src files, and peer dependencies for shared runtime libraries. Public import smoke tests, type-level API tests, package metadata audits, and release docs live in the repo; pnpm run pack:dry-run builds core/react/testing and runs pnpm pack --dry-run for each publishable package.
Constrained grouped benchmark run with 1M rows, 20 deltaPublish operations, aggregate counts 10/50/100, debounce 50ms, and operationPauseMs=0:
  aggregates=10: initial snapshot 1.19s, operation p50/p95/p99/max roughly 0.30/0.55/2.24/2.24ms, metrics p99 0.14ms, staleStatusCount 12, snapshotCount 5, maxSubscriptionLagVersions 0.
  aggregates=50: initial snapshot 4.46s, operation p50/p95/p99/max roughly 0.24/0.46/0.55/0.55ms, metrics p99 0.12ms, staleStatusCount 10, snapshotCount 5, maxSubscriptionLagVersions 0.
  aggregates=100: initial snapshot 7.22s, operation p50/p95/p99/max roughly 0.33/0.61/0.76/0.76ms, metrics p99 0.09ms, staleStatusCount 12, snapshotCount 5, maxSubscriptionLagVersions 0.
The short-burst operation latency stays low because grouped subscriptions are dirty/refreshed rather than recomputed per mutation, but grouped refresh wall time is already large at 50/100 aggregates. The next grouped performance step should reduce refresh frequency/work or move grouped aggregation to chDB/incremental maintenance before treating these snapshots as production firehose-ready.
Health metrics expose active plan observability: activePlanCount, activeViewCount, activePlanRows, activePlanIndexEstimatedBytes, activePlanBuildQueueDepth, activePlanBuildingCount, activePlanPendingCount, activePlanBuildMs, activePlanBuildMsTotal, activePlanBuildMsMax, and activePlanFallbackCount. activePlanRows means indexed rows summed across active plans, not unique physical topic rows. activePlanIndexEstimatedBytes is a sorted-index estimate, not full row object heap usage.
Worker guardrails can cap active plan growth with worker.maxActivePlans and worker.maxActivePlanEstimatedBytes. Existing shared plans can still be reused. If a new distinct raw where + orderBy plan would exceed the limit, that subscription falls back to the authoritative memory recompute path instead of failing. Health reports degraded while active plan limits are near, or while fallback subscriptions are active.
maxActivePlanEstimatedBytes now uses a cheap minimum-size admission check before queueing impossible plans, then uses a filter-only preflight index-byte estimate in the background builder before constructing the candidate ActiveRawPlan. It short-circuits as soon as the candidate would exceed the remaining byte budget. Count-limit fallback is still the cheapest guardrail because it avoids even that filter scan.

Large active-only benchmark notes from 250k rows, 250 subscriptions, 500 mutations:

```text
Full recompute baseline did not finish the first scenario after roughly 11 minutes, so large runs should use VS_ACTIVE_VIEW_BASELINE=0.
Before binary removal lookup, pageSize=50 active update timings were roughly:
  hot-key-updates: 73.5s
  sorted-row-movement: 128.0s
After binary removal lookup, targeted active-only timings were:
  hot-key-updates pageSize=50: 29.6s
  sorted-row-movement pageSize=50: 41.3s
  hot-key-updates pageSize=100: 30.1s
  sorted-row-movement pageSize=100: 41.1s
Page size has little effect here. The remaining cost is sorted set maintenance, especially array splice shifting.
After block-list sorted storage with blockSize=1024, targeted active-only timings were:
  array hot-key-updates pageSize=50: 25.0s
  array sorted-row-movement pageSize=50: 35.2s
  blocks hot-key-updates pageSize=50: 1.6s
  blocks sorted-row-movement pageSize=50: 5.2s
  array hot-key-updates pageSize=100: 28.4s
  array sorted-row-movement pageSize=100: 39.3s
  blocks hot-key-updates pageSize=100: 1.7s
  blocks sorted-row-movement pageSize=100: 3.8s
Block list is the current runtime default.
Before shared ActiveRawPlan construction, build time was large because every active subscription built its own filtered sorted index.
Build-only benchmark with blocks, 250k rows, 250 subscriptions, 0 mutations, before sharing:
  pageSize=50: 46.9s
  pageSize=100: 49.9s
Page size has little impact on build. Subscription startup cost is dominated by scanning/filtering/sorting per subscription.
After shared ActiveRawPlan construction:
  same-plan build pageSize=50: per-subscription 13.7s, shared 100ms
  same-plan build pageSize=100: per-subscription 15.6s, shared 50ms
  ten-plans build pageSize=50: per-subscription 56.7s, shared 2.3s
  ten-plans build pageSize=100: per-subscription 57.5s, shared 3.2s
  same-plan hot-key 500 mutations pageSize=50: per-subscription update 1.1s, shared update 204ms
  same-plan sorted movement 500 mutations pageSize=50: per-subscription update 1.6s, shared update 406ms
  ten-plans shared hot-key 500 mutations pageSize=50: build 2.3s, update 293ms
  ten-plans shared sorted movement 500 mutations pageSize=50: build 2.1s, update 397ms
Shared plan benchmarks validate ordered row/key checksums. Shared plans reduce sorted indexes from one per subscription to one per normalized where + orderBy plan, so same-plan has 1 sorted index instead of 250 and ten-plans has 10 instead of 250.
Unique-plan stress benchmark with blocks, 250k rows, 250 subscriptions, 0 mutations, pageSize=50, queryShape=unique-plans:
  no guardrail: build 93.9s, activePlanCount 250, activeIndexBytes 1,001,842,976
  maxActivePlans=10: build 6.0s, activePlanCount 10, activeFallbackCount 240, activeIndexBytes 40,078,496
  maxActivePlanEstimatedBytes=1 before preflight: build 80.5s, activePlanCount 0, activeFallbackCount 250, activeFallbackBuildMs 80.4s
  maxActivePlanEstimatedBytes=1 after preflight: build 28.5ms, activePlanCount 0, activeFallbackCount 250, activeFallbackBuildMs 0ms, activeFallbackEstimateMs 0.77ms
This confirms plan explosion is dangerous, count-limit fallback is cheap, and byte-limit fallback now protects retained memory without paying candidate sort/index build CPU. Realistic byte limits may still pay filter-scan admission cost, so maxActivePlans remains the primary production guardrail.
Retained heap benchmark with --expose-gc, blocks, 250k rows, 250 subscriptions, 0 mutations, pageSize=50:
  same-plan: activePlanCount 1, activeIndexBytes 1,335,992, heapDelta 4.1MB, rssDelta 29.7MB
  ten-plans: activePlanCount 10, activeIndexBytes 20,719,744, heapDelta 53.8MB, rssDelta 95.3MB
  unique-plans with maxActivePlans=10: activePlanCount 10, activeFallbackCount 240, activeIndexBytes 40,078,496, heapDelta 91.8MB, rssDelta 83.4MB
  unique-plans no guardrail: activePlanCount 250, activeIndexBytes 1,001,842,976, heapDelta 2.33GB, rssDelta 2.24GB
  unique-plans with maxActivePlanEstimatedBytes=1 after preflight: activePlanCount 0, activeFallbackCount 250, activeIndexBytes 0, heapDelta 161KB, rssDelta 3.0MB
activePlanIndexEstimatedBytes is directionally useful, but full retained heap is much higher because plans also retain row-id maps, block arrays, and per-view state.
Retained heap benchmark with --expose-gc, blocks, 1M rows, 250 subscriptions, 0 mutations, pageSize=50:
  same-plan: build 481ms, activePlanCount 1, activeIndexBytes 5,343,800, heapDelta 15.1MB, rssDelta 58.6MB
  ten-plans: build 11.9s, activePlanCount 10, activeIndexBytes 82,790,336, heapDelta 213.9MB, rssDelta 268.9MB
  unique-plans with maxActivePlans=10: build 26.0s, activePlanCount 10, activeFallbackCount 240, activeIndexBytes 160,312,160, heapDelta 361.7MB, rssDelta 356.5MB
The heap multiplier remains roughly 2.3x to 2.8x over activePlanIndexEstimatedBytes at 1M rows. Production sizing should be driven primarily by maxActivePlans, with maxActivePlanEstimatedBytes treated as a lower-bound retained-index guard.
````

## chDB Snapshot Backend Details

Prototype reference:

```text
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/chdb-query-backend.ts
```

New implementation improvements:

```text
Generate ClickHouse schema from Effect Schema instead of runtime inference where possible.
Keep `__version` column.
Keep id column.
Keep scalar columns only initially.
Use Memory engine.
Batch inserts.
Track flushed version.
```

Suggested table:

```sql
CREATE TABLE topic_rows (
  __version UInt64,
  id String,
  symbol String,
  price Float64,
  qty Float64,
  updatedAt Int64
) ENGINE = Memory
```

For deletes:

Options:

```text
Simpler v1:
  Rebuild chDB table periodically or on deletes if deletes are rare.

Better:
  Use tombstones/versioned rows and query latest row per id.

Best later:
  Use replacing strategy if supported cleanly by chDB memory usage.
```

Do not overcomplicate deletes before measuring actual workload.

Snapshot SQL rules:

```text
Raw:
  SELECT projected fields
  WHERE filters
  ORDER BY user order + id asc
  LIMIT limit OFFSET offset

Raw count:
  SELECT count()
  WHERE filters

Grouped:
  SELECT group fields + aggregate expressions
  WHERE filters
  GROUP BY group fields
  ORDER BY user order/group fallback
  LIMIT/OFFSET

Grouped count:
  SELECT count() FROM (grouped query without limit)
```

String SQL semantics:

```text
lower(toString(field)) for string filters/sorts.
ClickHouse functions for startsWith/position.
arrayStringConcat for string_concat.
groupUniqArray for string_concat_distinct.
uniqExact for count_distinct.
```

## Kafka Ingestion

Use `@platformatic/kafka`.

Keep this behind an interface:

```ts
interface KafkaTopicConsumer {
  run(args: {
    topic: string;
    groupId: string;
    onBatch(batch: KafkaRecordBatch): Effect.Effect<void, ViewServerError>;
  }): Effect.Effect<void, ViewServerError>;
}
```

Reasons:

```text
We are intentionally choosing Platformatic.
Prototype local benchmarks did not prove it faster in our shape.
Adapter lets future benchmark data swap the client without touching worker logic.
```

Ingestion flow:

```text
Consume Kafka in batches.
Decode records.
Validate with Effect Schema.
Convert to publish/delta/delete mutations.
Apply to worker memory.
Append to mutation log.
Schedule chDB flush.
Commit offsets after successful memory ingest and queued/persisted mirror update depending on policy.
Update Kafka lag metrics.
```

Commit policy:

```text
after-ingest:
  commit after worker memory has applied the batch and mutation log contains the versions.

none:
  useful for tests or external offset management.
```

If exactly-once durability is required later, this needs a separate persistence design. Current view server is a live cache/projection engine, not the system of record.

Kafka lag metrics:

```text
partition
current offset
end offset / high watermark
lag per partition
total lag
max lag
last consumed timestamp if available
```

Current implementation note:

```text
view_server.kafka.lag is available on Kafka ingest/commit spans.
Runtime health and health topic rows consume KafkaBatchMetrics from ingestion.
Platformatic lag monitoring currently supplies total lag, max lag, partition count, and last consumed offset; lastKafkaEndOffset is populated only when the batch/record exposes an end offset or high watermark.
```

## Backpressure

Backpressure is mandatory.

Worker queue config:

```ts
worker: {
  maxQueueDepth: 100_000;
}
```

When queue depth exceeds thresholds:

```text
Mark topic degraded.
Expose queue depth in health topic.
Optionally slow Kafka consumption.
Optionally reject external Publish/DeltaPublish.
Never silently drop Kafka data unless explicitly configured for lossy mode.
```

Current implementation note:

```text
worker.maxQueueDepth is propagated into in-process and Node worker threads.
Health metrics report active subscription backlog as queueDepth and mark the topic degraded when that backlog exceeds maxQueueDepth.
Health metrics also report logical subscription lag as maxSubscriptionLagVersions and totalSubscriptionLagVersions because queueDepth counts physical queued events, while delta coalescing can compress many versions into one queued delta.
When a subscription backlog would exceed maxQueueDepth, the worker fails the stream with BackpressureExceeded.
Generated clients treat BackpressureExceeded as retryable and resubscribe with a fresh request id.
Generated clients ignore subscription events whose requestId does not match the active subscription attempt before invoking user handlers.
Real websocket E2E coverage verifies typed BackpressureExceeded over Effect RPC NDJSON, generated-client resubscribe with a changed requestId, health cleanup for the failed subscription, and post-resubscribe deltas.
```

WebSocket/client backpressure:

```text
If a client cannot keep up, buffer up to a limit.
After limit, close the subscription/client with typed backpressure error.
Client can reconnect and resubscribe.
```

Delta coalescing:

```text
Do not emit more often than useful for UI.
Can coalesce deltas per subscription over a small interval if needed.
But version ordering must remain correct.
```

Current implementation note:

```text
worker.deltaCoalescing defaults to true.
Queued deltas for the same subscription are coalesced into one version-contiguous delta by preserving the first fromVersion, latest toVersion, latest totalRows, and ordered delta ops.
The existing maxQueueDepth safety rail still bounds coalesced version span; backpressure retry tests can disable coalescing with worker.deltaCoalescing = false to prove the hard failure path.
Health topic rows expose maxSubscriptionLagVersions and totalSubscriptionLagVersions so queueDepth is not the only slow-subscriber signal.
```

## System Topics and Metrics UI

Internal topic:

```text
__view_server_health
```

It should be injected by config normalization, not user-defined.

Rows might include:

```ts
type ViewServerHealthRow = {
  id: string;
  kind: "server" | "topic";
  topic?: string;
  rows: number;
  subscribers: number;
  queueDepth: number;
  maxSubscriptionLagVersions: number;
  totalSubscriptionLagVersions: number;
  workerLagP95Ms: number;
  deltaFanoutP95Ms: number;
  publishLatencyP95Ms: number;
  snapshotLatencyP95Ms: number;
  chdbSnapshotLatencyP95Ms: number;
  kafkaLagTotal: number;
  kafkaLagMax: number;
  kafkaPartitions: number;
  lastKafkaOffset: number;
  lastKafkaEndOffset: number;
  rssMb: number;
  status: "ready" | "degraded" | "stopping";
  updatedAt: bigint;
};
```

The internal metrics UI should use the same public generated hooks:

```ts
hooks.useLiveQuery("__view_server_health", query);
```

This guarantees anything visible internally can also be reproduced by users in their own UI.

Reject all user publish/deltaPublish attempts to `__*` topics.

## Auth

Config:

```ts
auth: {
  authorizeConnection ? context : Effect.Effect<boolean, ViewServerError>;
  authorizePublish ? context : Effect.Effect<boolean, ViewServerError>;
  authorizeQuery ? context : Effect.Effect<boolean, ViewServerError>;
}
```

Context should include:

```text
topic
operation
query or row
transport
connection identity/headers if available
```

Auth belongs in the gateway before worker command routing.

## Testing Plan

Use tests as product specification. Do not add empty coverage tests.

Testing philosophy:

```text
Prefer high-value end-to-end and integration tests over a huge pile of low-value unit tests.
Unit tests are still correct where they make sense:
  pure query normalization
  comparator/sort semantics
  SQL compiler escaping
  version-log edge cases
  small deterministic algorithms

But the goal is to prove the actual product:
  defineConfig -> in-memory/worker server -> Effect RPC -> useLiveQuery -> browser UI behavior
```

The most important tests should exercise the view server as users actually use it.

Use `@effect/vitest`.

Reason:

```text
It makes Effect tests much cleaner.
It avoids manual Effect.runPromise noise everywhere.
It provides Effect-aware test helpers such as it.effect / it.layer-style patterns.
It pairs naturally with Effect layers, RpcTest, TestClock, scopes, and resource cleanup.
```

Before implementing tests, inspect `/Users/bruno/projects/effect-smol` for the exact current Effect v4 testing style and API names.

Preferred shape:

```ts
import { it } from "@effect/vitest"
import { expect } from "vitest"

it.effect("subscribes and receives snapshot then delta", () =>
  Effect.gen(function* () {
    const server = yield* inMemoryViewServer(config)
    const snapshot = yield* server.client.query("orders", query)
    expect(snapshot).toEqual(...)
  })
)
```

For shared server/client layers, prefer layer-based tests:

```ts
it.layer(ViewServerTestLayer)("view server rpc", (it) => {
  it.effect("streams subscription events", () =>
    Effect.gen(function* () {
      const client = yield* ViewServerTestClient;
      // test real Effect RPC path
    }),
  );
});
```

Primary test style:

```text
Vitest browser mode.
Real generated client/hooks.
In-memory view server test runtime.
Publish/deltaPublish through public APIs.
Assertions against rendered UI and hook state.
```

Coverage target:

```text
100% lines.
100% branches.
100% functions.
100% statements.
```

Use Vitest coverage strict mode:

```ts
coverage: {
  thresholds: {
    100: true
  }
}
```

Do not game coverage with meaningless tests. If coverage forces awkward tests, prefer improving API/testability or excluding genuinely unreachable generated/build glue with a clear comment.

Reference prototype tests:

```text
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/chdb-query-backend.test.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/topic-worker-core.test.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/rpc-inmemory.test.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/rpc-server.test.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/client-hook.browser.test.tsx
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/e2e.browser.test.ts
```

Test layers:

```text
Unit:
  Query normalization, comparator behavior, sort behavior, aggregate behavior.
  Keep these focused and valuable.

Worker in-process:
  Publish/delta/delete/subscribe/query without worker thread.

RPC in-memory:
  Effect RpcTest/in-memory protocol where possible.

Real websocket:
  Small number of full transport tests using Effect RPC websocket layer.

Browser:
  Vite browser mode for React hook behavior.
  This should be the main confidence layer for client-visible behavior.

Storybook:
  Components using real hooks against an in-memory view server.
  Storybook interaction tests through the Vitest addon.

Kafka:
  Integration-gated real Kafka tests.

Bench:
  Vitest bench and standalone Effect load programs.
```

Important test cases:

```text
Snapshot followed by deltas with no mode field.
Initial snapshot version fence exactly equal to workerVersion.
chDB behind by 1, 10, 1000 versions and replay succeeds.
chDB behind beyond mutation log and worker-memory fallback is used.
No deltas emitted below or equal to snapshot version.
No missed rows between snapshot and first delta.
String equals case-insensitive for broad strings.
String one_of case-insensitive for broad strings.
Literal union strings remain compile-time strict.
String contains and starts_with case-insensitive.
String sort case-insensitive.
Nulls top asc, bottom desc.
Numbers and bigints comparisons.
Boolean filters/sorts.
Three-column sort where first two values tie.
All requested sort columns equal and id asc final tiebreaker.
Update moves row from top to deep position.
Update moves row into visible window.
Insert pushes all visible rows out.
Rows entering initially empty filtered window.
Delete visible row.
Delete invisible row but totalRows changes.
totalRows-only delta does not churn visible rows.
Grouped totalRows after pagination.
Grouped query with every aggregate family.
string_concat keeps duplicates.
string_concat_distinct removes duplicates.
string aggregate ordering asc/desc.
GroupBy treats case variants as distinct groups.
BigInt epoch nanos survives RPC wire.
Reconnect and resubscribe.
Worker crash/degraded metric.
Private __ topics reject user writes.
```

Browser hook tests should include:

```text
Hydrates initialData before live snapshot arrives.
Receives live deltas.
Updates totalRows without replacing data.
Reconnects after socket close.
Handles large but realistic windows.
```

## Testing Module / In-Memory View Server

The package should export a testing module that makes user tests easy.

Suggested export:

```ts
import { inMemoryViewServer } from "@view-server/testing";
```

Purpose:

```text
Let users spin up a fully functional view server in tests without Kafka, real sockets, Docker, or external ClickHouse.
Make Vitest browser mode tests simple.
Make Storybook stories/tests simple.
Use the same public client/hooks API users use in production.
```

Example Vitest browser mode usage:

```tsx
const server = await inMemoryViewServer(config).start();
const hooks = createViewServerHooks(config, {
  client: server.client,
});

await server.publish("orders", [
  { id: "o-1", symbol: "AAPL", price: 100 },
  { id: "o-2", symbol: "MSFT", price: 120 },
]);

render(<OrdersGrid hooks={hooks} />);
await expect.element(screen.getByText("AAPL")).toBeInTheDocument();
```

Example Storybook usage:

```tsx
export const LiveOrders = {
  render: () => <OrdersGrid />,
  loaders: [
    async () => {
      const server = await inMemoryViewServer(config).start();
      await server.publish("orders", seedOrders);
      return { server };
    },
  ],
  play: async ({ canvas, loaded }) => {
    await loaded.server.deltaPublish("orders", { id: "o-1", price: 101 });
    await expect(canvas.getByText("101")).toBeInTheDocument();
  },
};
```

Storybook requirements:

```text
Use Storybook for real UI examples.
Use the Storybook Vitest addon for interaction tests.
Use inMemoryViewServer in stories instead of mocks whenever practical.
Prove that users can build and test UI components with the same generated hooks.
```

Provider split:

```text
ViewServerProvider:
  Production provider.
  Receives url.
  Uses production config/client factory from app setup.
  Does not accept isolationId.
  Does not know about testing isolation.
  Never mutates queries for test scoping.

StorybookViewServerProvider:
  Exported from @view-server/testing.
  Requires isolationId.
  Connects to a real View Server test URL from Storybook setup.
  Adds story isolation.

VitestViewServerProvider:
  Exported from @view-server/testing.
  Receives no props in normal usage.
  Reads test server url through Vitest inject/provide setup.
  Reads current test isolation id from Vitest beforeEach context registered by setup.
  Adds test isolation.

TestingViewServerProvider:
  Lower-level provider exported by createTestingViewServerReact(config) from @view-server/testing.
  Requires url and isolationId.
  Uses the same generated hooks/client shape.
  Injects isolation into Effect RPC query/subscribe payloads before they reach the server.
```

Example:

```tsx
// production
render(
  <ViewServerProvider url="wss://api.myapp.com/view-server">
    <OrdersGrid />
  </ViewServerProvider>,
);

// vitest browser mode
test("orders update live", async (ctx) => {
  render(
    <VitestViewServerProvider>
      <OrdersGrid />
    </VitestViewServerProvider>,
  );
});

// storybook preview.tsx
export const decorators = [
  (Story, context) => (
    <StorybookViewServerProvider isolationId={isolationIdFromStorybook(context)}>
      <Story />
    </StorybookViewServerProvider>
  ),
];
```

Testing isolation policy:

```text
The production provider must not accept isolationId.
VitestViewServerProvider should not require props.
StorybookViewServerProvider should require isolationId because Storybook context is naturally available in preview decorators.
Vitest isolation id should be derived from Vitest context in setup/beforeEach.
Storybook isolation id should be derived from Storybook context/story id.
Manual fallback can use crypto.randomUUID().
```

Suggested helpers:

```ts
function isolationIdFromVitest(ctx: TestContext): string {
  return [ctx.task.file?.name, ctx.task.id, ctx.task.name, ctx.task.retry ?? 0].join("::");
}

function isolationIdFromStorybook(context: StoryContext): string {
  return `${context.id}:${context.viewMode}`;
}
```

Vitest dynamic server URL setup:

```ts
// vitest.global-setup.ts, Node side
export default async function setup(project) {
  const server = await startSharedInMemoryViewServer({
    config,
    port: 0,
  });

  project.provide("VIEW_SERVER_TEST_URL", server.url);

  return async () => {
    await server.close();
  };
}
```

Vitest browser setup:

```ts
// vitest.browser.setup.ts
import { beforeEach, inject } from "vitest";
import { configureVitestViewServer, isolationIdFromVitest } from "@view-server/testing/vitest";

configureVitestViewServer({
  url: inject("VIEW_SERVER_TEST_URL"),
});

beforeEach((ctx) => {
  configureVitestViewServer({
    isolationId: isolationIdFromVitest(ctx),
  });
});
```

The exact Vitest `provide`/`inject` API should be verified against the installed Vitest version, but this is the intended model:

```text
global setup starts one server on port 0
global setup provides dynamic URL
browser setup injects URL
beforeEach registers current test isolation id
VitestViewServerProvider reads both from testing runtime context
```

Important concurrency caveat:

```text
Verify Vitest browser mode isolation before relying on a module-global current isolation id.
If tests in the same browser JS realm can run concurrently, a module-global isolation id can race.
In that case, use a helper such as renderWithViewServer(ctx, ui) that creates a per-render scoped React context, or require isolationId explicitly for concurrent tests.
Zero-prop VitestViewServerProvider is acceptable only if the test runner setup guarantees one active test context per browser realm, or if the provider can read isolation from a per-render/test-scoped context safely.
```

Preferred fallback if zero-prop is unsafe:

```tsx
test("orders update live", async (ctx) => {
  renderWithViewServer(ctx, <OrdersGrid />);
});
```

Where `renderWithViewServer` does:

```text
derive isolation id from ctx
create scoped testing client/context
render VitestViewServerProvider internally
```

Testing isolation implementation:

```text
Prefer one real shared View Server process for app/browser E2E tests.
Each test/story gets an isolation id.
Testing provider/client helpers attach isolationId to publish/deltaPublish rows and patches.
Testing provider/client helpers add an isolationId filter to query/subscribe.
Rows from one test/story must never appear in another test/story.
Cleanup should delete rows for the isolation id after test/story teardown where practical.
```

Required topic representation:

```text
Each test topic schema must include isolationId: Schema.String.
Production ViewServerProvider must not accept isolationId.
TestingViewServerProvider requires isolationId.
Testing useLiveQuery injects where isolationId == current isolationId.
Testing publisher/client helpers add isolationId to rows and patches automatically.
Hook result rows do not include isolationId unless the query explicitly selects it.
deleteById remains id-only, so tests using deleteById should use globally unique ids per isolation.
```

Reason:

```text
This allows one shared real View Server to run parallel Vitest browser tests and Storybook interaction tests while keeping production runtime semantics. The private in-memory helper remains available for package/browser tests where native chDB cannot run.
```

Testing module behavior:

```text
No Kafka required.
No external chDB/ClickHouse required for package/browser helper tests.
Use in-memory worker core only through private testing helpers.
Expose real websocket testing clients/providers for app E2E/UI tests.
Supports publish, deltaPublish, delete, query, subscribe, and health.
Supports deterministic clock/version helpers for tests.
```

The testing API should make the easy path the correct path:

```text
Tests should publish through public APIs.
Tests should subscribe through public hooks/client.
Tests should not mutate private worker state or event metadata.
```

### How inMemoryViewServer Should Use Effect RPC Test Helpers

The in-memory view server should not fake the client API and should not bypass RPC handlers. It should wire the real Effect RPC client to the real Effect RPC server handlers through Effect's in-memory/RpcTest protocol layer.

The exact Effect v4 API names must be verified in `/Users/bruno/projects/effect-smol` before implementation, but the intended pattern is:

```ts
import { Effect, Layer, Scope } from "effect";
import { RpcClient, RpcServer, RpcTest } from "effect/unstable/rpc";

const TestRpcLayer = RpcClient.layerProtocol(RpcTest.layerProtocol).pipe(
  Layer.provide(RpcServer.layer(ViewServerRpcs).pipe(Layer.provide(ViewServerHandlersLive))),
);
```

Conceptually:

```text
React hook / generated client
  -> real RpcClient.make(ViewServerRpcs)
  -> RpcTest in-memory protocol
  -> real RpcServer.layer(ViewServerRpcs)
  -> real view-server handlers
  -> real in-process topic worker core
```

That means tests cover:

```text
RPC payload schemas.
RPC success schemas.
RPC error schemas.
Stream behavior.
Effect error transport.
Subscription lifecycle.
The same handler code used by real websockets.
```

But tests avoid:

```text
real websocket server
real browser websocket limits
Kafka
Docker
external ClickHouse
timing flakes from networking
```

Suggested implementation shape:

```ts
export type InMemoryViewServer<TConfig extends ViewServerConfig> = {
  readonly client: ViewServerGeneratedClient<TConfig>;
  readonly hooks: ViewServerGeneratedHooks<TConfig>;
  publish<TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    rows: TopicRowFromConfig<TConfig, TTopic> | readonly TopicRowFromConfig<TConfig, TTopic>[],
  ): Promise<void>;
  deltaPublish<TTopic extends TopicName<TConfig>>(
    topic: TTopic,
    patch: TopicPatchFromConfig<TConfig, TTopic>,
  ): Promise<void>;
  query<TTopic extends TopicName<TConfig>, TQuery extends QueryForTopic<TConfig, TTopic>>(
    topic: TTopic,
    query: TQuery,
  ): Promise<InferQueryResult<TConfig, TTopic, TQuery>>;
  close(): Promise<void>;
};
```

Construction:

```ts
export function inMemoryViewServer<const TConfig extends ViewServerConfig>(
  config: TConfig,
  options?: {
    initialRows?: Partial<{
      [TTopic in keyof TConfig["topics"]]: readonly TopicRowFromConfig<TConfig, TTopic>[];
    }>;
    useChdbSnapshotBackend?: boolean;
    clock?: TestClockLike;
  },
): Effect.Effect<InMemoryViewServer<TConfig>, ViewServerError, Scope.Scope>;
```

Runtime layering:

```text
1. Normalize config.
2. Create in-process topic worker core per topic.
3. Seed initialRows through the same publish/append path.
4. Build ViewServerHandlersLive using those workers.
5. Build RpcServer.layer(ViewServerRpcs) from handlers.
6. Build RpcClient.layerProtocol(RpcTest.layerProtocol).
7. Create generated client/hooks from the real RpcClient.
8. Return testing handle.
```

Important: `inMemoryViewServer` should still use public operations internally where practical:

```text
server.publish(...) should call client.publish(...) or the same handler path.
server.deltaPublish(...) should call client.deltaPublish(...) or the same handler path.
server.query(...) should call client.query(...).
```

Do not expose shortcuts like:

```text
server.worker.rows.push(...)
server.applyDeltaMeta(...)
server.setTotalRows(...)
```

Those recreate the bad testing pattern where tests can create impossible states.

Vitest browser mode usage should feel like this:

```tsx
import { inMemoryViewServer } from "@view-server/testing";
import { render, screen } from "@testing-library/react";
import { expect, test } from "vitest";

test("grid receives live updates", async () => {
  await Effect.scoped(
    Effect.gen(function* () {
      const server = yield* inMemoryViewServer(config, {
        initialRows: {
          orders: [
            { id: "o-1", symbol: "AAPL", price: 100 },
            { id: "o-2", symbol: "MSFT", price: 120 },
          ],
        },
      });

      render(<OrdersGrid hooks={server.hooks} />);

      await expect.element(screen.getByText("AAPL")).toBeInTheDocument();

      yield* Effect.promise(() => server.deltaPublish("orders", { id: "o-1", price: 101 }));

      await expect.element(screen.getByText("101")).toBeInTheDocument();
    }),
  );
});
```

If Effect's current test helper is named differently than `RpcTest.layerProtocol`, use the current name from `effect-smol`, but preserve the architecture:

```text
real RpcClient + in-memory protocol + real RpcServer + real handlers + in-process workers
```

Do not replace this with a fake client object unless a specific test is intentionally unit-level.

## Benchmark Plan

Benchmark philosophy:

```text
Benchmark real hot paths.
Keep runs long enough, ideally 5-10 seconds.
Avoid drawing conclusions from 300ms micro timings.
Fail regression on >=10% degradation for important cases.
Write JSON artifacts.
Use numbers to guide decisions except the explicit Platformatic Kafka product decision.
```

Benchmark layers:

```text
1. Kafka pure consume:
   Kafka -> client -> count bytes/checksum.

2. Kafka decode:
   Kafka -> client -> decode protobuf/json -> count rows.

3. Kafka ingest:
   Kafka -> client -> decode -> worker memory apply -> mutation log.

4. chDB insert:
   worker batch -> chDB insert.

5. chDB snapshot:
   raw filter/sort/page and grouped aggregates.

6. Version fence:
   chDB snapshot + replay gap sizes 1/10/1000.

7. Live top-k:
   insert/update/delete against active subscriptions.

8. Full pipeline:
   Kafka -> worker -> subscription delta -> RPC client.
```

Scale targets:

```text
250k rows
1M rows
10M rows
eventually 50M rows if local machine can handle it
250 subscriptions
1000 subscriptions
1500 subscriptions
page size 50 normal
page size 100 normal
large windows only as stress: 10k, 25k, 50k, 100k+
```

Important benchmark scenarios:

```text
High-selectivity filters.
Low-selectivity filters.
Multiple filters on different fields.
Sort/filter/page only.
GroupBy with count/sum/avg/min/max/count_distinct/string aggs.
100 aggregate functions.
Irrelevant field updates.
Hot key updates.
Threshold crossing updates.
Rows outside current page changing totalRows only.
Version-fence replay vs memory fallback.
chDB fresh vs chDB behind.
```

Prototype benchmark references:

```text
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/view-server.bench.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/subscription-window.bench.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/runtime.bench.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/kafka-client-consume-spike.ts
/Users/bruno/projects/performance-filters-sort/view-server-effect/src/clickhouse-kafka-decode-vs-forward-spike.ts
```

Known Kafka benchmark nuance:

```text
In the prototype, KafkaJS eachBatch was faster than @platformatic/kafka for our local pure consume benchmark.
The new product decision is still @platformatic/kafka.
Therefore, keep the adapter isolated and benchmark continuously.
```

Prototype benchmark context that should guide expectations:

```text
Pure Kafka consume was tested separately from decode and ClickHouse insert.
KafkaJS eachBatch was much faster than our tested @platformatic/kafka paths locally.
@platformatic/kafka v1.34.0 did not fix that local result versus v2.0.1.
This does not overrule the product decision to use @platformatic/kafka, but it means the adapter boundary is not optional.

ClickHouse/chDB-style engines were excellent for initial snapshots and grouped aggregate snapshots.
The custom worker engine was strongest for incremental live fanout and small visible windows.
The hybrid model exists because databases are pull/snapshot engines, while this product needs push/incremental subscription semantics.

Client-side worker subscriptions were tested and should not be kept in v1 unless new browser benchmarks prove a benefit.
Use one normal useLiveQuery path first.
```

## Performance Design Notes

Do not maintain active subscription work when there are no subscribers.

If a topic has zero subscriptions:

```text
Still ingest Kafka.
Still update authoritative memory.
Still flush to chDB mirror.
Do not maintain subscription materializations.
Do not maintain per-subscription dependency entries.
```

This is the major simplification from using chDB for initial snapshots.

When a user subscribes:

```text
Use chDB to get fast initial candidate snapshot.
Fence/reconcile with worker version.
Create active top-k/materialization only for that subscription.
```

When the user changes filters/sorts/page:

```text
Unsubscribe old query.
Destroy old materialization.
Query chDB for new candidate snapshot.
Fence/reconcile.
Create new materialization.
```

When the user unsubscribes:

```text
Destroy materialization.
Remove dependency index entries.
If no subscriptions remain, worker returns to cheap ingest/mirror mode.
```

This avoids building and maintaining indexes for every possible query upfront.

## Generated Code / DX

Config should generate or infer:

```text
Topic map.
Typed client.
Typed publisher.
Typed hooks.
Typed query helpers.
Topic-specific helpers.
```

Example:

```ts
const { client, hooks } = createViewServer(config, {
  url: "ws://localhost:4050/rpc",
});

hooks.useLiveQuery("orders", {
  fields: { id: true, symbol: true, price: true },
  where: { field: "status", comparator: "equals", value: "OPEN" },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 50,
});

client.topics.orders.publish(row);
client.topics.orders.deltaPublish({ id: "o-1", price: 123 });
```

Generated topic-specific helpers are nice, but not required for v1 if they slow implementation. The important part is that the generic API is type safe from config.

## Project Structure Proposal

Suggested new repo structure:

```text
src/
  config/
    define-config.ts
    normalize-config.ts
    schema-introspection.ts

  protocol/
    query.ts
    filters.ts
    aggregates.ts
    events.ts
    types.ts

  rpc/
    rpcs.ts
    server.ts
    client.ts
    serialization.ts
    errors.ts

  worker/
    topic-worker-core.ts
    topic-worker-thread.ts
    topic-worker-pool.ts
    worker-protocol.ts
    mutation-log.ts
    materialized-subscription.ts
    version-fence.ts

  snapshot/
    snapshot-backend.ts
    chdb-backend.ts
    clickhouse-sql.ts
    schema-to-clickhouse.ts

  kafka/
    platformatic-consumer.ts
    record-decoders.ts
    lag.ts

  client/
    create-client.ts
    live-query-store.ts
    react.ts
    query-options.ts

  server/
    gateway.ts
    health.ts
    metrics-topic.ts
    metrics-ui.ts
    auth.ts

  benchmarks/
    ...

  tests/
    ...
```

Keep prototype/spike code out of production `src` where possible. Use `benchmarks/` or `spikes/` for optional experiments.

## Implementation Order

### Phase 0 - Read References

Before coding:

```text
Read effect-smol Effect RPC tests/examples.
Read effect-smol websocket/NDJSON layers.
Read effect-smol worker/drainable worker examples if present.
Read prototype config/protocol/rpc/client/worker/chdb files.
```

Do not assume Effect APIs from memory. Verify v4 beta APIs locally.

### Phase 1 - Core Types

Implement:

```text
defineConfig
schema-required topic config
topic map inference
query protocol without mode
filter/aggregate/sort types
error TaggedErrors
```

Add type tests for:

```text
literal string strictness
numeric filters only on numbers/bigints
string filters only on strings
sum/avg only on numeric fields
min/max on comparable fields
deltaPublish requires id
```

### Phase 2 - chDB Snapshot Backend

Implement:

```text
schema-to-ClickHouse columns
chDB session service
table creation
batch apply
raw query SQL compile
group query SQL compile
totalRows
backendVersion
```

Tests:

```text
chDB snapshots match expected query semantics.
Special string escaping.
BigInt handling.
Grouped aggregates.
totalRows.
```

### Phase 3 - Worker Core In-Process

Implement:

```text
authoritative memory store
workerVersion
mutation log
subscribe/query/publish/delta/delete
version-fenced snapshot flow
dependency pruning
basic incremental raw insert fast path
fallback to memory snapshot
metrics emission
```

Tests:

```text
all core query edge cases
version fence with chDB lag
fallback when mutation log gap missing
```

### Phase 4 - Effect RPC In-Memory

Implement:

```text
RpcGroup
handlers
in-memory RpcTest tests
client query/subscribe/publish
Effect errors over wire
```

Tests:

```text
Subscribe gets snapshot then delta.
Query returns snapshot.
Publish/deltaPublish typed errors.
Unsubscribe stops deltas.
```

### Phase 5 - Real WebSocket RPC

Implement:

```text
Effect RPC websocket server
NDJSON serialization
single multiplexed client websocket
reconnect/resubscribe
```

Tests:

```text
real websocket subscribe
multiple concurrent subscriptions over one socket
error propagation
reconnect
```

### Phase 6 - React Client

Implement:

```text
live query store
useLiveQuery
initialData hydration
totalRows
startTransition/useSyncExternalStore
TanStack Query helper if useful
```

Browser tests:

```text
hydration
snapshot
delta
totalRows-only
reconnect
sorting edge cases
```

### Phase 7 - Real Worker Threads

Implement:

```text
topic worker thread wrapper
topic worker pool
worker lifecycle
shutdown
crash handling
queue depth
worker lag metrics
```

Keep worker core testable without actual worker threads.

### Phase 8 - Kafka Adapter

Implement:

```text
@platformatic/kafka consumer adapter
batch decode
schema validation
lag metrics
commit policy
backpressure behavior
```

Integration tests behind env flag.

### Phase 9 - Metrics UI

Implement:

```text
__view_server_health system topic
internal metrics React app
use public generated hooks
reject external writes to __ topics
```

### Phase 10 - Benchmarks / Regression Gate

Implement:

```text
Vitest bench for core algorithms.
Standalone load scripts for Kafka/RPC/full pipeline.
JSON benchmark artifacts.
Regression checker with 10% threshold.
```

## Production Concerns

Graceful shutdown:

```text
Stop accepting new connections.
Stop new subscriptions.
Flush chDB/mutation state as needed.
Commit Kafka offsets if policy allows.
Close RPC streams with typed shutdown error.
Terminate workers.
```

Worker crash:

```text
Mark topic degraded.
Optionally restart worker.
Rehydrate from Kafka/chDB if possible.
Clients receive typed error or reconnect.
Metrics show degraded state.
```

Memory:

```text
Memory is an explicit tradeoff.
Rows live in worker memory.
chDB mirror also uses memory.
Active subscriptions use additional memory.
Expose memory metrics.
Benchmark 1000+ subscriptions and 10M rows.
```

Horizontal scaling:

```text
Future defineConfig can assign topics to machines.
Nginx/load balancer can route by topic path if needed.
No cross-topic queries makes this feasible.
```

## Explicit Decisions From Prototype

Keep:

```text
Effect v4.
Effect RPC websocket multiplexing.
NDJSON.
Effect Schema mandatory.
defineConfig as source of truth.
Typed query language.
Typed useLiveQuery.
totalRows in same subscription.
chDB for initial snapshots.
Worker-owned authoritative memory.
Version fence consistency model.
Metrics via public hook/system topic.
Benchmarks as first-class.
```

Remove/simplify:

```text
No query/subscription mode.
No legacy websocket protocol.
No custom websocket test helpers.
No duplicate hook names.
No client worker subscription path initially.
No schema-less topics.
No public metadata mutation.
No maintaining active subscription indexes when there are no subscriptions.
```

Use despite benchmark concerns:

```text
@platformatic/kafka
```

Let numbers guide:

```text
chDB vs custom path split.
Which queries use chDB.
When to fallback.
Batch sizes.
Replay gap thresholds.
RPC serialization tuning.
```

## Final Mental Model

The server is a realtime projection engine, not just a database wrapper.

chDB gives fast snapshots:

```text
subscription starts -> chDB candidate snapshot -> version fence -> emit snapshot
```

Worker memory gives correct live state:

```text
Kafka mutation -> authoritative memory -> active materialization -> delta
```

Effect RPC gives clean typed transport:

```text
one websocket -> many subscriptions -> typed errors -> NDJSON
```

The version fence is the correctness line. Never emit a snapshot unless it is fenced to the version from which future deltas will continue.

## Final Hardening Notes Added After Review

This section exists to remove ambiguity for the next agent. Treat it as binding.

### Atomic Version Fence Details

The subscribe flow must not race concurrent Kafka writes.

Correct subscription snapshot algorithm:

```text
1. Enter the topic worker command queue.
2. Capture targetVersion from the serialized worker state.
3. Build or request the candidate snapshot for that targetVersion.
4. Reconcile candidate snapshot to targetVersion if chDB is behind.
5. Register subscription.lastVersion = targetVersion.
6. Emit snapshot.
7. Process later mutations as deltas with version > targetVersion.
```

The key is that worker commands are serialized per topic. A subscription command should observe a single worker version. Kafka mutations queued after it should not be included in the snapshot; they should become deltas. Kafka mutations queued before it should be included through chDB snapshot, replay, or memory fallback.

This means the worker command queue is part of the correctness model, not just an implementation detail.

Do not do this:

```text
capture version
await random async work while mutations keep modifying state
register subscription against stale assumptions
```

If the worker must await chDB during subscribe, either:

```text
Use a serialized command model where state mutation does not interleave with the subscribe critical section.
```

or:

```text
Capture targetVersion and a stable mutation-log boundary, allow later mutations to queue, and only set lastVersion once reconciliation to targetVersion is complete.
```

The simpler v1 recommendation is:

```text
One serialized topic command loop.
Do not interleave state mutations inside a subscribe command.
Keep chDB snapshot fast.
If chDB is slow or behind too far, fallback to memory rather than blocking indefinitely.
```

### Versioned chDB Mirror

The chDB mirror should be explicitly version-aware.

Minimum viable design:

```text
Worker keeps currentFlushedVersion.
Every chDB flush is a batch of mutations up to highestVersion.
After successful flush, currentFlushedVersion = highestVersion.
SnapshotBackendResult.backendVersion = currentFlushedVersion.
```

If chDB stores only current rows, the backend version is still valid if every mutation through that version has been applied. The version does not need to be a row column for correctness, but storing `__version` is useful for debugging, future compaction, and versioned/tombstone strategies.

For updates/deletes in chDB, choose one clear strategy:

```text
Strategy A, simplest:
  For early implementation, if update/delete volume is low, apply memory correctly and periodically rebuild chDB mirror.

Strategy B, better:
  Store latest current table and apply ALTER/DELETE-like operations if chDB supports them acceptably.

Strategy C, robust analytical mirror:
  Store versioned rows with tombstones and query latest row per id.
```

The worker-memory path must remain correct regardless of chDB mirror strategy.

### Query and Subscription API Must Not Contain Mode

If any type or RPC payload contains:

```text
mode: "snapshot_and_delta"
```

that is a regression from this plan. Delete it.

There are only two operations:

```text
query:
  one-shot snapshot/read.

subscribe:
  snapshot followed by deltas.
```

### Public Package Exports

The new repo should have a clear package surface. Suggested exports:

```text
@view-server/core
  defineConfig
  KafkaSource
  EffectSource
  createViewServer
  createViewServerClient
  createViewServerHooks
  createViewServerPublisher
  ViewServer errors
  Query/filter/aggregate types

@view-server/react
  React hook helpers if split from core

@view-server/node
  Node server/runtime entrypoints if split from core
```

Avoid leaking prototype internals from public exports:

```text
worker command internals
chDB implementation details
test helpers
benchmark-only adapters
```

### CLI / Local Dev Experience

The new repo should include a simple local dev path:

```text
view-server dev --config ./view-server.config.ts
view-server generate --config ./view-server.config.ts
view-server bench ...
```

Use Effect CLI and/or Effect Schema for CLI/env parsing. Environment variables are startup inputs, not runtime mysteries.

Startup env policy:

```text
If a required environment variable is missing, malformed, or semantically invalid, crash during startup.
Do not try to recover from missing existential config.
Do not start a partially configured server.
Do not defer required env validation until the first request/subscription.
```

Examples of required startup values may include:

```text
Kafka brokers
Kafka consumer group prefix
RPC host/port
auth secrets if auth is enabled
ClickHouse/chDB mode flags if configurable
metrics/public URL config if required by deployment
```

Use Effect Schema to decode and validate env:

```ts
const Env = Schema.Struct({
  KAFKA_BROKERS: Schema.NonEmptyString,
  VIEW_SERVER_PORT: Schema.NumberFromString,
  VIEW_SERVER_RPC_PATH: Schema.NonEmptyString,
});
```

Prefer a typed config layer:

```text
raw process env -> Effect Schema decode -> typed Env service -> server layers
```

Include a dev publisher for demos/tests:

```ts
const publisher = createViewServerPublisher(config, { url });
await publisher.publish("orders", row);
await publisher.deltaPublish("orders", { id: "o-1", price: 123 });
```

A TCP publisher is optional but useful for local ingestion demos. If implemented, it should still route through the same worker publish/deltaPublish code path and schema validation. It must not bypass authorization or internal topic protections in production mode.

### Topic Placement / Future Multi-Machine Scaling

The config should leave room for topic placement without implementing distributed routing in v1.

Possible future shape:

```ts
defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
      placement: {
        group: "hot-orders",
        shardBy: "id",
        replicas: 2,
      },
    },
  },
});
```

Do not implement this fully now. Just avoid designing APIs that make it impossible. The current no-joins/no-cross-topic rule is what makes horizontal topic placement feasible later.

### Schema Evolution

Schema evolution should be explicitly planned, even if v1 is simple.

Topic config supports:

```ts
schemaVersion?: number
migrate?: (row, context) => Effect.Effect<Row, ViewServerError>
```

Kafka decoder should be able to return:

```text
decoded row
source schema version if known
source timestamp if known
partition/offset metadata
```

Migration order:

```text
decode Kafka record
apply migrate if needed
validate final row with current Effect Schema
apply to memory/chDB
```

Metrics should expose schema decode failures and migration failures per topic.

### Protobuf / Binary Ingestion

The system should not require JSON Kafka messages.

Kafka records may be:

```text
Protobuf
JSON
Avro later
custom binary later
```

Decoder is topic-owned:

```ts
decode(record): Effect.Effect<KafkaSourceMessage<Row>, ViewServerError>
```

This keeps transformations in TypeScript/Effect where needed and preserves flexibility, even if ClickHouse can decode some binary formats itself.

### Effect SQL ClickHouse Role

Use `@effect/sql-clickhouse` where it fits:

```text
real ClickHouse integration
typed Effect service wrapper
benchmarking direct ClickHouse paths
future external ClickHouse mirror
```

Do not force chDB through `@effect/sql-clickhouse` if chDB requires its own session API. Wrap chDB in an Effect service manually.

### Startup Validation

The app should validate its world before accepting traffic.

Fail-fast startup checks:

```text
1. Config file loads.
2. Config validates against Effect Schema/types.
3. Required env vars validate.
4. Topic names are valid and unique.
5. No user-defined topic starts with reserved prefix "__".
6. Every topic has an id field and mandatory schema.
7. Every configured Kafka source can connect to Kafka.
8. Every configured Kafka topic exists.
9. Kafka topic partition metadata can be read.
10. chDB snapshot backend can initialize required tables.
11. RPC server can bind host/port.
```

Kafka topic existence is mandatory:

```text
If config references Kafka topic "orders" and that topic does not exist, startup fails.
Do not auto-create production Kafka topics by default.
Do not start the app and wait for the topic to appear.
```

Optional dev-only behavior:

```text
allowAutoCreateTopicsForDev?: true
```

If this exists, it must be visibly dev-only and off by default.

Rationale:

```text
Missing Kafka topics, missing env vars, invalid auth secrets, and invalid config are existential failures.
Failing immediately is cheaper and safer than running a server that silently drops data or accepts subscriptions against impossible topics.
```

### Acceptance Criteria For V1

The new repo is not "done" until these are true:

```text
1. defineConfig requires schema and id for every topic.
2. Query types have no mode field.
3. Effect RPC WebSocket works with NDJSON and multiplexed subscriptions.
4. A browser can hold one websocket and many subscriptions.
5. Kafka ingestion through @platformatic/kafka reaches a topic worker.
6. Topic worker memory is authoritative.
7. chDB provides initial candidate snapshots.
8. Version fence prevents stale chDB snapshots from causing missed rows.
9. useLiveQuery returns Effect AsyncResult; success contains rows, totalRows, and connection/status metadata.
10. totalRows updates can arrive without visible row churn.
11. Private __ topics cannot be externally published.
12. Internal metrics UI uses the same public hook/client API.
13. Browser tests cover sorting/filtering/pagination edge cases.
14. Worker tests cover chDB lag/replay/fallback.
15. Benchmarks produce JSON artifacts and can fail on 10% regressions for selected hot paths.
16. Startup validates required env vars through Effect Schema or Effect CLI and fails fast on missing/invalid values.
17. Startup verifies configured Kafka topics exist before accepting traffic.
18. Vitest coverage thresholds use 100% for lines, branches, functions, and statements.
19. The package exports an inMemoryViewServer testing helper.
20. Vitest browser mode tests prove useLiveQuery against the in-memory view server.
21. Storybook stories/tests use the in-memory view server and the Storybook Vitest addon.
```

### Things To Avoid In The New Repo

Avoid these prototype traps:

```text
Do not keep old /ws custom protocol.
Do not hand-roll websocket test clients if Effect RPC test layers can do it.
Do not let tests mutate private metadata.
Do not hardcode orders/cars in runtime code.
Do not make metrics a separate bespoke protocol.
Do not build client worker subscriptions before benchmarks prove they help.
Do not optimize chDB delete strategy before live correctness exists.
Do not trust a chDB snapshot without a worker version fence.
Do not maintain active query materializations when there are no subscribers.
Do not make groupBy case-insensitive accidentally.
Do not use JavaScript default string sort.
Do not let a broad string runtime rule weaken literal union compile-time strictness.
```

## Fault Injection / Production Chaos

Production hardening must prove failure behavior, not only happy paths.

Fault suites should cover:

```text
1. Kafka and Effect sources can pause, fail, resume, duplicate batches, and stop mid-batch.
2. Source failure marks topic readiness degraded.
3. Shutdown completes even when a source is stuck or failed.
4. Kafka commits happen only after memory ingest and mirror enqueue policy are safe.
5. chDB snapshot, grouped refresh, and applyBatch failures never make chDB authoritative.
6. chDB failure marks the topic degraded and later successful backend work can recover health.
7. Hung snapshot operations are interruptible and do not poison worker memory.
8. Real websocket reconnect storms do not leak subscribers, queues, active plans, builds, or grouped refreshes.
9. Backpressure reaches clients as typed BackpressureExceeded and generated clients retry with a fresh request id.
10. Optional --expose-gc soak mode can assert retained heap growth with an env threshold.
```

Expected operator docs:

```text
docs/fault-tolerance.md documents Kafka down, chDB behind/down, websocket reconnect storms, slow clients, shutdown, and memory pressure behavior.
```

## External Consumer Smoke

Release-candidate packaging must be tested from outside the monorepo with actual packed tarballs.

Required smoke:

```text
1. Pack @view-server/core, @view-server/react, and @view-server/testing into /private/tmp/view-server-packs.
2. Install those tarballs into a fresh temp project.
3. Node consumer imports only public core subpaths, starts a real runtime with chDB, publishes, queries, subscribes, checks typed errors, and shuts down.
4. React/Vite consumer imports only @view-server/react, renders useLiveQuery with AsyncResult, builds a production bundle, and greps the bundle for forbidden server deps.
5. Testing consumer imports @view-server/testing and runs a browser-mode in-memory helper smoke without bundling chDB/Kafka, plus documents the real-server TestingViewServerProvider isolation path.
6. Docs record exact commands, expected output, optional peer requirements, and known limitations.
```

Production runtime consumers must install chDB. React/browser bundles and testing browser helpers must not import or bundle chDB, Kafka, worker threads, fs, or net. @effect/platform-node and @platformatic/kafka stay optional peers for their node-only subpaths.

## Deployment Artifact Smoke

Release-candidate deployment must also prove the built repo can run as a container artifact.

Required smoke:

```text
1. Dockerfile uses Node 26 and prepares pnpm 11.0.9 through Corepack.
2. Docker build installs native chDB build/runtime prerequisites and runs pnpm install --frozen-lockfile without --ignore-scripts.
3. docker-compose.production-smoke.yml exposes the server port and healthchecks /ready.
4. scripts/deployment-smoke.sh runs compose up --build, waits for readiness, runs a host client against the real Effect RPC websocket, verifies raw/grouped query plus publish/delta/delete, and shuts down compose.
5. docs/deployment-smoke.md records commands, env vars, expected output, and limitations.
```

This smoke intentionally exercises chDB because production runtime requires chDB startup. Kafka remains optional for the demo container, but the snapshot backend is not memory-only and must not use an `--ignore-scripts` install shortcut.

## Code Quality Rules

The implementation should read like careful human-written code.

Comments:

```text
Avoid obvious comments.
Do not add comments that restate the code.
Use comments only for non-obvious design constraints, correctness invariants, performance tradeoffs, or dangerous edge cases.
Good comment: explains why a version fence cannot be skipped.
Bad comment: "increment counter" above `counter += 1`.
```

Coverage:

```text
Never fake coverage.
Never lower coverage targets.
Never change coverage thresholds to make CI pass.
Never add ignore-coverage comments to hide untested code.
Never add meaningless tests just to execute a line.
If a line is hard to test, improve the design/testability or ask before making an exception.
```

Casts and type safety:

```text
Avoid `as`.
Avoid `as unknown as`.
Avoid `any`.
Avoid non-null assertions unless there is a very tight local invariant.
Prefer better type modeling, Effect Schema decoding, discriminated unions, branded types, and narrow helper functions.
If a cast seems unavoidable, stop and reassess the design.
If still unavoidable, ask before adding it.
```

Decision rule:

```text
If in doubt, ask.
Do not assume away correctness.
Do not weaken type safety.
Do not weaken tests.
Do not weaken coverage.
Do not hide uncertainty with comments or casts.
```
