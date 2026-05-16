# Quickstart

This is the shortest local loop: define one topic, start the Effect RPC websocket server, publish rows, and render a browser grid with `useLiveQuery`.

Production/runtime startup requires chDB. There is no public memory-vs-chDB backend choice; memory is only used by private package testing helpers.

## Run The Demo

Terminal 1:

```bash
vp run orders-demo#server
```

Terminal 2:

```bash
vp run orders-demo#dev
```

The server listens on `ws://127.0.0.1:3000/rpc` by default. The browser app reads `VITE_VIEW_SERVER_RPC_URL` when you need a different URL.

## Define A Topic

`defineConfig` is the source of truth. Topic schemas, row ids, source config, snapshot settings, and generated/client types derive from it.

```ts
import * as Schema from "effect/Schema";
import { defineConfig } from "@view-server/core/config";
import type { RawQuery } from "@view-server/core/query";

export const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
  quantity: Schema.Number,
});

type OrderRow = typeof Order.Type;

export const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

export const ordersQuery = {
  fields: {
    id: true,
    symbol: true,
    price: true,
    quantity: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 50,
} satisfies RawQuery<
  OrderRow,
  {
    readonly id: true;
    readonly symbol: true;
    readonly price: true;
    readonly quantity: true;
  }
>;
```

## Start The Server

```ts
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import * as Layer from "effect/Layer";
import { createServer } from "node:http";
import { layerViewServerRuntime } from "@view-server/core";
import { layerViewServerWebsocketServer } from "@view-server/core/rpc/websocket";
import { config } from "./view-server";

const RuntimeLayer = layerViewServerRuntime(config, {
  initialRows: {
    orders: [{ id: "o-1", symbol: "AAPL", price: 210, quantity: 20 }],
  },
});

const ServerLayer = layerViewServerWebsocketServer("/rpc").pipe(
  Layer.provide(RuntimeLayer),
  Layer.provide(NodeHttpServer.layer(createServer, { host: "127.0.0.1", port: 3000 })),
);

Layer.launch(ServerLayer).pipe(NodeRuntime.runMain);
```

If chDB cannot initialize, runtime startup fails before the websocket server is ready.

## Publish Rows

Use the typed client or call the runtime from an Effect source/Kafka adapter. The demo server publishes deterministic rows in `apps/website/src/server.ts`.

```ts
yield *
  runtime.publish("orders", {
    id: "o-2",
    symbol: "NVDA",
    price: 890,
    quantity: 10,
  });

yield *
  runtime.deltaPublish("orders", {
    id: "o-2",
    price: 901,
  });
```

## Render React

`useLiveQuery` returns Effect's `AsyncResult` directly. Success values include `rows`, `totalRows`, `status`, and connection metadata.

```tsx
import { AsyncResult } from "effect/unstable/reactivity";
import { createViewServerReact } from "@view-server/react";
import { config, ordersQuery } from "./view-server";

const { ViewServerProvider, useLiveQuery } = createViewServerReact(config);

function OrdersGrid() {
  const result = useLiveQuery("orders", ordersQuery);

  return AsyncResult.match(result, {
    onInitial: () => <p>Connecting</p>,
    onFailure: () => <p>Subscription failed</p>,
    onSuccess: ({ value }) => (
      <div>
        <p>
          {value.status} / {value.totalRows} rows
        </p>
        {value.rows.map((row) => (
          <p key={row.id}>{row.symbol}</p>
        ))}
      </div>
    ),
  });
}

export function App() {
  return (
    <ViewServerProvider url="ws://127.0.0.1:3000/rpc">
      <OrdersGrid />
    </ViewServerProvider>
  );
}
```
