# Hello Production Config

This is the smallest production-shaped wiring: Kafka source, chDB snapshot backend, websocket RPC, health topic, and metrics UI.

It is intentionally a wiring example, not a deployment guide.

## Config Module

```ts
import * as Schema from "effect/Schema";
import { KafkaSource, decodeJsonRecord, defineConfig } from "@view-server/core";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
  quantity: Schema.Number,
  updatedAt: Schema.Number,
});

type OrderRow = typeof Order.Type;

export const viewServerConfig = defineConfig({
  rpc: {
    path: "/rpc",
    serialization: "ndjson",
  },
  worker: {
    maxQueueDepth: 512,
    mutationLogSize: 100_000,
    deltaCoalescing: true,
    maxActivePlans: 64,
    maxActivePlanEstimatedBytes: 512 * 1024 * 1024,
    activePlanBuildConcurrency: 1,
    groupedRefreshDebounceMs: 100,
  },
  topics: {
    orders: {
      id: "id",
      schema: Order,
      snapshot: {
        backend: "chdb",
        flushBatchSize: 10_000,
        flushIntervalMs: 100,
      },
      source: KafkaSource<OrderRow, "id">({
        brokers: ["127.0.0.1:9092"],
        topic: "orders",
        groupId: "view-server-orders",
        commitPolicy: "after-ingest",
        decode: decodeJsonRecord({ topic: "orders", schema: Order }),
      }),
    },
  },
});
```

## Server

```ts
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import * as Layer from "effect/Layer";
import { createServer } from "node:http";
import { createPlatformaticKafkaConsumerFactory } from "@view-server/core/kafka/platformatic";
import { layerViewServerRuntime } from "@view-server/core";
import { layerViewServerWebsocketServer } from "@view-server/core/rpc/websocket";
import { createChdbSnapshotBackendFactory } from "@view-server/core/snapshot/chdb";
import { viewServerConfig } from "./view-server.config";

const RuntimeLayer = layerViewServerRuntime(viewServerConfig, {
  kafkaConsumerFactory: createPlatformaticKafkaConsumerFactory({
    clientIdPrefix: "view-server",
    batchSize: 1_000,
    lagMonitoringIntervalMs: 1_000,
  }),
  snapshotBackendFactory: createChdbSnapshotBackendFactory(),
});

const ServerLayer = layerViewServerWebsocketServer("/rpc").pipe(
  Layer.provide(RuntimeLayer),
  Layer.provide(NodeHttpServer.layer(createServer, { host: "0.0.0.0", port: 3000 })),
);

Layer.launch(ServerLayer).pipe(NodeRuntime.runMain);
```

## Metrics UI

The metrics app consumes the internal health topic through the same websocket RPC path:

```bash
VITE_VIEW_SERVER_RPC_URL=ws://127.0.0.1:3000/rpc vp run metrics#dev
```

Useful probes:

```bash
curl http://127.0.0.1:3000/health
curl http://127.0.0.1:3000/ready
```

## Notes

- Worker memory is authoritative.
- chDB is a snapshot accelerator and must be exact-version fenced.
- Kafka lag is captured from the Platformatic consumer lag monitor when available.
- Active plan limits should be set before exposing arbitrary user-defined sort/filter combinations.
- Use `maxActivePlans` as the primary guardrail. The byte estimate is a lower-bound index estimate.
