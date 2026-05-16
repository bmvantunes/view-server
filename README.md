# View Server

Realtime materialized views for live UI grids. View Server ingests topic rows, keeps worker memory authoritative, uses Effect RPC over websocket + NDJSON for subscriptions, and exposes React `useLiveQuery` as an `AsyncResult`.

The core loop is:

```text
defineConfig -> topic worker -> snapshot + live deltas -> useLiveQuery
```

## Quickstart

Run the orders demo server in one terminal:

```bash
vp run orders-demo#server
```

Run the browser app in another terminal:

```bash
vp run orders-demo#dev
```

Open the Vite URL and watch the orders grid plus grouped desk metrics update from the real Effect RPC websocket path.

The demo code lives in `apps/website/src`:

- `view-server.ts` defines the topic schema, `defineConfig`, raw query, grouped query, and seed rows.
- `server.ts` starts the Effect websocket server and publishes deterministic updates.
- `App.tsx` uses `createViewServerReact(config)` and `useLiveQuery`.

## Minimal Shape

```ts
import * as Schema from "effect/Schema";
import { defineConfig, type RawQuery } from "@view-server/core";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
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

export const query = {
  fields: {
    id: true,
    symbol: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 50,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly symbol: true; readonly price: true }>;
```

React:

```tsx
import { AsyncResult } from "effect/unstable/reactivity";
import { createViewServerReact } from "@view-server/react";
import { config, query } from "./view-server";

const { ViewServerProvider, useLiveQuery } = createViewServerReact(config);

function OrdersGrid() {
  const result = useLiveQuery("orders", query);

  return AsyncResult.match(result, {
    onInitial: () => <p>Connecting</p>,
    onFailure: () => <p>Subscription failed</p>,
    onSuccess: ({ value }) => (
      <table>
        <tbody>
          {value.rows.map((row) => (
            <tr key={row.id}>
              <td>{row.symbol}</td>
              <td>{value.totalRows}</td>
            </tr>
          ))}
        </tbody>
      </table>
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

## Docs

- `CONTEXT.md` is the compact architecture and domain map for future agents.
- `docs/quickstart.md` shows the run-server, publish, and browser flow.
- `docs/operations.md` documents stale/waiting semantics, `totalRows`, version fencing, health metrics, active plan limits, and troubleshooting.
- `docs/hello-production.md` shows a Kafka + chDB + metrics UI configuration.
- `docs/benchmarks.md` covers benchmark artifact output, summaries, and baseline refresh.
- `docs/capacity-matrix.md` records the 100k, 1M, and 10M capacity profiles and latest artifacts.
- `docs/api-audit.md` lists the release-candidate package exports and private internals.
- `docs/release-checklist.md` lists the release-candidate validation and dry-run steps.
- `docs/worker-state-machine-review.md` records the hardening checklist and soak shape.
- `docs/adr/` records the load-bearing architecture decisions.

## Validation

```bash
vp check
vp run -r test
vp run -r build
```
