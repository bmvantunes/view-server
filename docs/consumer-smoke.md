# External Consumer Smoke

This smoke verifies the packages as real tarballs from a fresh project outside the monorepo. It is intentionally separate from workspace tests so package exports, peer dependencies, browser bundles, and testing helpers are exercised through the same paths an application would use.

Use Node 26 and pnpm 11.0.9.

## Pack Tarballs

From the repo root:

```bash
rm -rf /private/tmp/view-server-packs
mkdir -p /private/tmp/view-server-packs
vp run core#build
vp run react#build
vp run testing#build
pnpm --filter @view-server/core pack --pack-destination /private/tmp/view-server-packs
pnpm --filter @view-server/react pack --pack-destination /private/tmp/view-server-packs
pnpm --filter @view-server/testing pack --pack-destination /private/tmp/view-server-packs
```

Expected tarballs:

```text
/private/tmp/view-server-packs/view-server-core-0.0.0.tgz
/private/tmp/view-server-packs/view-server-react-0.0.0.tgz
/private/tmp/view-server-packs/view-server-testing-0.0.0.tgz
```

## Create Consumer Project

```bash
SMOKE_DIR=/private/tmp/view-server-consumer-smoke/rc-$(date +%Y%m%d-%H%M%S)
mkdir -p "$SMOKE_DIR/src" "$SMOKE_DIR/tests"
cd "$SMOKE_DIR"
corepack enable
corepack prepare pnpm@11.0.9 --activate
```

Create a minimal `package.json` with the tarballs:

```json
{
  "type": "module",
  "packageManager": "pnpm@11.0.9",
  "scripts": {
    "node:smoke": "node --experimental-strip-types src/node-smoke.ts",
    "build": "vite build",
    "bundle:grep": "node src/bundle-grep.mjs",
    "test": "vitest run --config vitest.browser.config.ts"
  },
  "dependencies": {
    "@view-server/core": "file:/private/tmp/view-server-packs/view-server-core-0.0.0.tgz",
    "@view-server/react": "file:/private/tmp/view-server-packs/view-server-react-0.0.0.tgz",
    "@view-server/testing": "file:/private/tmp/view-server-packs/view-server-testing-0.0.0.tgz",
    "effect": "4.0.0-beta.65",
    "react": "19.2.3",
    "react-dom": "19.2.3"
  },
  "devDependencies": {
    "@effect/platform-browser": "4.0.0-beta.65",
    "@types/node": "^24.10.3",
    "@types/react": "^19.2.7",
    "@types/react-dom": "^19.2.3",
    "@vitejs/plugin-react": "^5.1.2",
    "@vitest/browser-playwright": "^4.1.5",
    "playwright": "^1.57.0",
    "typescript": "^5.9.3",
    "vite": "^8.0.13",
    "vitest": "^4.1.5"
  }
}
```

If pnpm prompts about ignored build scripts from transitive dependencies, keep the decision explicit in the temp project:

```yaml
# pnpm-workspace.yaml
onlyBuiltDependencies:
  - msgpackr-extract
allowBuilds:
  - msgpackr-extract
```

`msgpackr-extract` comes from the Effect dependency graph. Browser bundles should not import `chdb`, `@platformatic/kafka`, or `@effect/platform-node`. Production server/runtime consumers must install `chdb`; browser-only and internal memory testing paths must not bundle chDB into client assets.

```bash
pnpm install --config.confirmModulesPurge=false
pnpm why @platformatic/kafka
pnpm why @effect/platform-node
```

The Kafka and platform-node `pnpm why` commands should print nothing for the browser/testing smoke unless the temp project intentionally exercises node websocket server helpers. `chdb` is expected when the temp project exercises a real server runtime.

## Shared Consumer Config

`src/view-server.ts`:

```ts
import * as Schema from "effect/Schema";
import { defineConfig } from "@view-server/core/config";
import type { RawQuery } from "@view-server/core/query";

export const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
  desk: Schema.Literal("ny", "ldn"),
});

export const Trade = Schema.Struct({
  id: Schema.String,
  orderId: Schema.String,
  qty: Schema.Number,
});

type OrderRow = typeof Order.Type;

export const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
    trades: {
      id: "id",
      schema: Trade,
    },
  },
});

export const ordersQuery = {
  fields: {
    id: true,
    symbol: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 50,
} satisfies RawQuery<
  OrderRow,
  {
    readonly id: true;
    readonly symbol: true;
    readonly price: true;
  }
>;
```

## Node Consumer Smoke

The Node smoke should import only public package entrypoints:

```text
@view-server/core/client
@view-server/core/config
@view-server/core/errors
@view-server/core/query
@view-server/core/rpc
@view-server/core/runtime
```

It should:

- create a runtime with two topics
- publish `orders` and `trades`
- query `orders`
- subscribe to `orders`
- receive a snapshot and a delta
- close the subscription
- verify health has zero subscribers
- verify an invalid runtime publish returns a typed `ViewServerError`
- close the runtime

Run:

```bash
pnpm exec tsc --noEmit
pnpm run node:smoke
```

Expected result: both commands exit 0.

## React / Vite Consumer Smoke

The React smoke should import only `@view-server/react` plus the consumer config and use the factory API:

```tsx
import { AsyncResult } from "effect/unstable/reactivity";
import { createViewServerReact } from "@view-server/react";
import { config, ordersQuery } from "./view-server";

const hooks = createViewServerReact(config);

export function App() {
  return (
    <hooks.ViewServerProvider url="ws://127.0.0.1:3000/rpc">
      <Orders />
    </hooks.ViewServerProvider>
  );
}

function Orders() {
  const initialData = {
    rows: [{ id: "o-1", symbol: "AAPL", price: 100 }],
    totalRows: 1,
  };
  const result = hooks.useLiveQuery("orders", ordersQuery, initialData);

  return AsyncResult.match(result, {
    onInitial: () => <p>connecting</p>,
    onFailure: ({ error }) => <p>{error._tag}</p>,
    onSuccess: ({ value }) => (
      <p>
        {value.status}:{value.totalRows}:{value.rows.map((row) => row.symbol).join(",")}
      </p>
    ),
  });
}
```

Run:

```bash
pnpm run build
pnpm run bundle:grep
```

The bundle grep should fail if built browser assets contain server-only imports:

```text
node:worker_threads
worker_threads
@platformatic/kafka
node:fs
node:net
from "chdb"
from 'chdb'
```

Expected result: Vite builds successfully and the grep exits 0.

## Testing Consumer Smoke

The testing smoke should import `@view-server/testing` from the tarball and run a browser-mode test against `inMemoryViewServer`. It should not require production chDB or Kafka dependencies.

For app-level UI/E2E tests, prefer a real View Server process plus the testing-only `TestingViewServerProvider` from `@view-server/testing`. It requires an `isolationId`, injects `where isolationId == current isolationId` into live queries, and adds `isolationId` to rows and patches sent through testing helpers. Each test topic schema must include `isolationId: Schema.String`.

Run:

```bash
pnpm run test
```

Expected result: the browser-mode test passes.

## Known Limitations

- Browser bundle checks do not prove a production websocket server, Kafka consumer, or chDB backend. Use `docs/deployment-smoke.md` for the real server artifact smoke.
- Node-only subpaths have explicit optional peers:
  - `@view-server/core/rpc/websocket` and `@view-server/core/worker/node` require `@effect/platform-node`.
  - production runtime and `@view-server/core/snapshot/chdb` require `chdb`.
  - `@view-server/core/kafka/platformatic` requires `@platformatic/kafka`.
- The browser bundle grep is a coarse artifact check. It is useful for catching accidental server imports, but package export tests and Vite build errors are still the stronger checks.
- The temp app should never import from monorepo source paths. Any source-path import means the package surface is incomplete.
