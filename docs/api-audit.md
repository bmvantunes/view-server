# Release Candidate API Audit

This audit describes the package surface that is intended to be usable by applications. Anything not listed here is implementation detail, even if it exists in `src`.

Package runtime exports point at built `dist` files. Type entries also point at built `.d.mts` files so downstream TypeScript users do not need the repo's internal `.ts` import settings. Dry-run tarballs include `dist` and `src`; the packages are ESM-only and require Node 26.

## Packages

Published packages:

- `@view-server/core`
- `@view-server/react`
- `@view-server/testing`

Private workspace packages and apps:

- `@view-server/utils`
- `metrics`
- `orders-demo`
- root `view-server` workspace

## `@view-server/core`

Root `@view-server/core` reexports the public `client`, `config`, `errors`, `kafka`, `query`, `runtime`, and `snapshot` entrypoints. It intentionally does not reexport RPC server handlers, topic worker internals, active-view internals, query-engine internals, chDB worker protocol/codecs, or testing helpers.

### `@view-server/core/config`

Runtime exports:

- `EffectSource`
- `KafkaSource`
- `RESERVED_TOPIC_PREFIX`
- `VIEW_SERVER_HEALTH_TOPIC`
- `ViewServerHealthRowSchema`
- `defineConfig`
- `isReservedTopic`
- `normalizeConfig`

Type exports:

- `AuthorizationContext`
- `EffectSourceConfig`
- `EffectSourceContext`
- `HttpPath`
- `IdValue`
- `KafkaConsumerRecord`
- `KafkaSourceConfig`
- `KafkaSourceMessage`
- `MigrationContext`
- `NormalizedTopicConfigMap`
- `NormalizedViewServerConfig`
- `ReadableTopicName`
- `ReadableTopicRowFromConfig`
- `RowObject`
- `SystemTopicName`
- `TopicConfig`
- `TopicConfigByName`
- `TopicConfigMap`
- `TopicIdFieldFromConfig`
- `TopicIdFromConfig`
- `TopicName`
- `TopicPatchFromConfig`
- `TopicRowFromConfig`
- `TopicSource`
- `ViewServerAuth`
- `ViewServerConfig`
- `ViewServerHealthRow`

### `@view-server/core/client`

Runtime exports:

- `LiveQueryStore`
- `applyDeltaOperations`
- `createViewServerClient`
- `queryResultToRuntimeRows`
- `rowKeyForTypedQuery`
- `runtimeRowsToQueryResult`

Type exports:

- `ActiveSubscription`
- `LiveQueryConnection`
- `LiveQueryInitialData`
- `LiveQueryLifecycle`
- `LiveQueryLifecycleEvent`
- `LiveQueryLifecycleHandler`
- `LiveQueryListener`
- `LiveQueryResult`
- `LiveQueryStatus`
- `LiveQueryValue`
- `RpcClientForViewServer`
- `ViewServerClient`
- `ViewServerRpcTransport`

### `@view-server/core/query`

Runtime exports:

- `groupRowKey`
- `isRuntimeGroupedQuery`
- `rowKeyByField`
- `rowKeyForQuery`
- `stableStringify`

Type exports:

- `AggregateDefinition`
- `AggregateMap`
- `BooleanComparator`
- `BooleanField`
- `Comparator`
- `ComparatorValue`
- `ComparableField`
- `DeltaEvent`
- `DeltaOperation`
- `FieldOf`
- `FieldPredicate`
- `FieldProjection`
- `FilterNode`
- `GroupedQuery`
- `GroupByFields`
- `InferGroupedResult`
- `InferQueryResult`
- `InferRawResult`
- `InferReadableQueryResult`
- `InferredResult`
- `LiveQueryStatusEvent`
- `NumberComparator`
- `NumericField`
- `OrderBy`
- `OrderByGrouped`
- `Predicate`
- `Query`
- `QueryForReadableTopic`
- `QueryForTopic`
- `QueryResponse`
- `RawQuery`
- `RuntimeAggregateDefinition`
- `RuntimeAggregateMap`
- `RuntimeComparator`
- `RuntimeFilterNode`
- `RuntimeGroupedQuery`
- `RuntimeQuery`
- `RuntimeRawQuery`
- `RuntimeRow`
- `RuntimeRowKey`
- `RuntimeRowKeyFn`
- `SnapshotEvent`
- `SortDirection`
- `StringComparator`
- `StringField`
- `SubscriptionEvent`
- `TopicMap`

### `@view-server/core/errors`

Runtime and type exports:

- `BackpressureExceeded`
- `InvalidConfig`
- `InvalidFilter`
- `InvalidPublish`
- `InvalidQuery`
- `InvalidStartupEnv`
- `KafkaIngestFailed`
- `MissingTopic`
- `MissingTopicId`
- `SchemaDecodeFailed`
- `ServerShutdown`
- `SnapshotBackendFailed`
- `SnapshotBackendLagExceeded`
- `SubscriptionClosed`
- `TransportError`
- `Unauthorized`
- `VersionGap`
- `ViewServerError`
- `WorkerUnavailable`
- `backpressureExceeded`
- `invalidConfig`
- `invalidFilter`
- `invalidPublish`
- `invalidQuery`
- `invalidStartupEnv`
- `isViewServerError`
- `kafkaIngestFailed`
- `missingTopic`
- `missingTopicId`
- `schemaDecodeFailed`
- `serverShutdown`
- `snapshotBackendFailed`
- `transportError`
- `unauthorized`
- `versionGap`
- `workerUnavailable`

### `@view-server/core/runtime`

Runtime exports:

- `RawViewServerProductionEnv`
- `RawViewServerStartupEnv`
- `ViewServerRuntime`
- `ViewServerStartupEnvService`
- `decodeViewServerProductionEnv`
- `decodeViewServerStartupEnv`
- `layerViewServerHealthRoutes`
- `layerViewServerRuntime`
- `layerViewServerStartupEnv`
- `loadViewServerProductionConfigFromEnv`
- `makeViewServerRuntime`

Type exports:

- `HealthResponse`
- `RawViewServerProductionEnv`
- `RawViewServerStartupEnv`
- `ViewServerProductionConfig`
- `ViewServerProductionEnv`
- `ViewServerRuntimeOptions`
- `ViewServerRuntimeShape`
- `ViewServerStartupEnv`

### `@view-server/core/rpc`

Runtime exports:

- `RpcDeltaEvent`
- `RpcDeltaMeta`
- `RpcDeltaOperation`
- `RpcDeltaPublishPayload`
- `RpcDeleteByIdPayload`
- `RpcHealthPayload`
- `RpcHealthResponse`
- `RpcHealthTopic`
- `RpcLiveQueryStatusEvent`
- `RpcPublishPayload`
- `RpcQuery`
- `RpcQueryPayload`
- `RpcQueryResponse`
- `RpcRow`
- `RpcRows`
- `RpcSnapshotEvent`
- `RpcSnapshotMeta`
- `RpcSubscribePayload`
- `RpcSubscriptionEvent`
- `RpcUnsubscribePayload`
- `RpcWireValue`
- `ViewServerRpcs`
- `fromWireRow`
- `fromWireRows`
- `toWireRow`
- `wireQueryResponse`
- `wireSubscriptionEvent`

Type exports:

- `RpcDeltaPublishPayload`
- `RpcDeleteByIdPayload`
- `RpcHealthPayload`
- `RpcHealthResponse`
- `RpcPublishPayload`
- `RpcQueryPayload`
- `RpcQueryResponse`
- `RpcSubscribePayload`
- `RpcSubscriptionEvent`
- `RpcUnsubscribePayload`
- `RpcWireValue`

### `@view-server/core/kafka`

Runtime exports:

- `decodeJsonRecord`
- `decodeKafkaRecordJson`
- `decodeProtobufDecimal`
- `protobufDecimalToBigDecimal`
- `unscaledDecimalToBigDecimal`

Type exports:

- `KafkaBatchMetrics`
- `KafkaRecordBatch`
- `KafkaTopicConsumer`
- `KafkaTopicConsumerRunArgs`
- `KafkaTopicVerificationArgs`
- `KafkaTopicVerifier`
- `ProtobufDecimalInput`

### `@view-server/core/snapshot`

Runtime exports:

- none

Type exports:

- `SnapshotBackend`
- `SnapshotBackendResult`
- `VersionedRow`

## Node-Only Core Subpaths

These exports are intentionally public but must stay behind explicit Node-only subpaths:

- `@view-server/core/rpc/websocket`: `layerNodeWebsocketRpcClient`, `layerViewServerWebsocketProtocol`, `layerViewServerWebsocketProtocolRoute`, `layerViewServerWebsocketServer`, `makeNodeWebsocketClient`
- `@view-server/core/kafka/platformatic`: `createPlatformaticKafkaConsumerFactory`, `createPlatformaticKafkaTopicConsumer`, `createPlatformaticKafkaTopicVerifier`, `platformaticKafkaTopicConsumerOptions`, plus `PlatformaticKafkaConsumerFactoryOptions`, `PlatformaticKafkaTopicConsumerOptions`, and `PlatformaticKafkaTopicVerifierOptions`
- `@view-server/core/snapshot/chdb`: `createChdbSnapshotBackend`, `createChdbSnapshotBackendFactory`, plus `ChdbSnapshotBackendOptions`
- `@view-server/core/worker/node`: `makeNodeThreadTopicWorkerHostFactory`, plus `NodeThreadTopicWorkerHostFactoryOptions`
- `@view-server/core/internal/testing`: `makeInternalTestingViewServerRuntime`, plus `InternalTestingViewServerRuntimeOptions`. This subpath exists only so `@view-server/testing` can keep the memory backend in test helpers without exposing a production backend choice.

The package does not export `./worker`, `./worker/core`, `./snapshot/chdb-query-worker-entry`, `./snapshot/snapshot-backend`, `./rpc/server`, or user-facing memory backend helpers. The `./internal/testing` subpath is reserved for `@view-server/testing`.

## `@view-server/react`

Runtime exports:

- `ViewServerMetricsDashboard`
- `createViewServerHooks`
- `createViewServerReact`
- `layerBrowserWebsocketRpcClient`
- `makeBrowserWebsocketClient`
- `metricsDashboardCss`
- `viewServerHealthQuery`

Type exports:

- `ViewServerHooks`
- `ViewServerMetricsHooks`
- `ViewServerMetricsRow`

The React package imports public core subpaths only. It must not import Node-only APIs such as `node:worker_threads`, `chdb`, `@platformatic/kafka`, `fs`, or `net`.

## `@view-server/testing`

Runtime exports:

- `createTestingViewServerReact`
- `inMemoryViewServer`
- `isolatedInMemoryViewServer`
- `makeTestingBrowserWebsocketClient`
- `readyUrlForRpcUrl`
- `realViewServerTestHarness`

Type exports:

- `InMemoryViewServer`
- `InMemoryViewServerOptions`
- `IsolatedInMemoryViewServer`
- `IsolatedInMemoryViewServerOptions`
- `MissingIsolationTopics`
- `RealViewServerTestHarness`
- `RealViewServerTestHarnessOptions`
- `RequireIsolationId`
- `TestingViewServerClient`
- `TopicPatchWithoutIsolation`
- `TopicRowWithoutIsolation`

The testing package is intentionally separate from `@view-server/core`; test helpers must not leak through the core root export.

## Package Metadata Policy

Public packages must have:

- `type: "module"`
- `sideEffects: false`
- `engines.node: ">=26.0.0"`
- explicit `exports`
- `files` including `dist` and `src`
- peer dependencies for shared runtime libraries (`effect`, `react`, `@view-server/core`, `@view-server/react`) instead of bundling duplicate copies
- required `chdb` peer dependency for production runtime
- optional peer dependencies for unrelated Node-only integrations (`@effect/platform-node`, `@platformatic/kafka`) so browser consumers are not forced to install websocket server or Kafka integrations
