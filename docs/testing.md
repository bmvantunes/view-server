# Testing

Production runtime uses chDB. The memory backend exists only as private test infrastructure for package and browser tests where native chDB cannot run.

## App UI And E2E Tests

Recommended app-level browser tests should run against a real View Server process and isolate test data with `isolationId`.

Use this path for app correctness:

- real View Server runtime
- real Effect RPC websocket
- chDB-backed production runtime
- `TestingViewServerProvider` with required `isolationId`
- test publisher/client helpers that inject `isolationId`
- live queries automatically scoped by `isolationId`

Do not rely on `inMemoryViewServer` behavior for app correctness. It exists for package/browser helper tests only.

## Start And Stop A Test Server

The demo repo includes explicit scripts for a local real-server UI test loop:

```bash
pnpm run test:server:start
```

This starts `orders-demo` on `VIEW_SERVER_PORT`, default `3100`, waits for `/ready`, writes the process id to `.view-server-test-server.pid`, and logs to `.view-server-test-server.log`.

```bash
pnpm run test:server:stop
```

For app repos, keep the same convention but point the script at the app's real View Server config module.

Each test topic schema must include:

```ts
isolationId: Schema.String;
```

Use the testing-only React factory from `@view-server/testing`:

```tsx
import { createTestingViewServerReact } from "@view-server/testing";
import { config, ordersQuery } from "./view-server";

const { TestingViewServerProvider, useLiveQuery } = createTestingViewServerReact(config);

function OrdersGrid() {
  const result = useLiveQuery("orders", ordersQuery);
  // AsyncResult.match(...)
}

export function TestApp() {
  return (
    <TestingViewServerProvider url="ws://127.0.0.1:3100/rpc" isolationId="test-run-42">
      <OrdersGrid />
    </TestingViewServerProvider>
  );
}
```

`TestingViewServerProvider` requires `isolationId`. Production `ViewServerProvider` does not accept it.

The testing transport automatically:

- injects `where isolationId == current isolationId` into query and subscription payloads
- adds `isolationId` to publish rows and delta patches sent through testing helpers
- leaves the health topic unfiltered

`deleteById` cannot carry an `isolationId` because the production command is id-only. Use globally unique ids per test when exercising deletes.

## Package And Browser Tests

`inMemoryViewServer` and `isolatedInMemoryViewServer` are testing helpers. They are allowed to use the private memory backend because they are not production runtime paths.

Use `isolatedInMemoryViewServer` when a browser-mode package test needs the same isolation behavior without starting a real websocket server:

```ts
const server =
  yield *
  isolatedInMemoryViewServer(config, {
    isolationId: "test-a",
  });

await server.publish("orders", {
  id: "o-1",
  symbol: "AAPL",
  price: 100,
});
```

The row passed to `publish` omits `isolationId`; the helper adds it before sending the payload.

## Production Boundary

Do not document or expose memory as a deployment backend. Production startup requires chDB to initialize successfully. React/browser bundles must not import chDB, Kafka, worker threads, `fs`, or `net`.
