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

Do not rely on in-memory View Server behavior for app correctness. Memory-backed helpers are package-internal test infrastructure only; public app tests should use a real View Server process.

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

For tests that need a repeatable server lifecycle wrapper, use the real-server harness:

```ts
import { realViewServerTestHarness, readyUrlForRpcUrl } from "@view-server/testing";

const rpcUrl = "ws://127.0.0.1:3100/rpc";

const harness =
  yield *
  realViewServerTestHarness(config, {
    rpcUrl,
    readyUrl: readyUrlForRpcUrl(rpcUrl),
    isolationId: "test-run-42",
    start: Effect.promise(() => startTestServer()),
    stop: Effect.promise(() => stopTestServer()),
  });

await harness.publish("orders", {
  id: "test-run-42:o-1",
  symbol: "AAPL",
  price: 100,
});
```

The harness only shuts down the server when a `stop` effect is provided. If the test points at a
shared Docker or already-running server, omit `start` and `stop`; the harness still waits for
`readyUrl`, creates an isolated websocket client, and injects `isolationId` into test publishes and
queries.

## Package And Browser Tests

View Server's own package/browser tests may use private in-memory helpers where native chDB cannot
run, but those helpers are not exported from `@view-server/testing`. External app/browser tests
should use `realViewServerTestHarness`, `makeTestingBrowserWebsocketClient`, or
`createTestingViewServerReact` against a real websocket server.

The testing client/provider path injects `isolationId` into publish and patch payloads and scopes
queries by `where isolationId == current isolationId`. The row passed to public testing helpers
omits `isolationId`; the helper adds it before sending the payload.

## Production Boundary

Do not document or expose memory as a deployment backend. Production startup requires chDB to initialize successfully. React/browser bundles must not import chDB, Kafka, worker threads, `fs`, or `net`.
