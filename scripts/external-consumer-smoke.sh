#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
PACK_DIR="${VS_CONSUMER_SMOKE_PACK_DIR:-/private/tmp/view-server-packs-${RUN_ID}}"
SMOKE_ROOT="${VS_CONSUMER_SMOKE_ROOT:-/private/tmp/view-server-consumer-smoke}"
SMOKE_DIR="${SMOKE_ROOT}/rc-${RUN_ID}"

mkdir -p "$PACK_DIR" "$SMOKE_DIR/src" "$SMOKE_DIR/tests"

cd "$ROOT_DIR"
vp run core#build
vp run react#build
vp run testing#build
pnpm --filter @view-server/core pack --pack-destination "$PACK_DIR"
pnpm --filter @view-server/react pack --pack-destination "$PACK_DIR"
pnpm --filter @view-server/testing pack --pack-destination "$PACK_DIR"

cd "$SMOKE_DIR"

cat >package.json <<EOF
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
    "@view-server/core": "file:${PACK_DIR}/view-server-core-0.0.0.tgz",
    "@view-server/react": "file:${PACK_DIR}/view-server-react-0.0.0.tgz",
    "@view-server/testing": "file:${PACK_DIR}/view-server-testing-0.0.0.tgz",
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
    "vite-plus": "^0.1.14",
    "vitest": "^4.1.5"
  },
  "pnpm": {
    "onlyBuiltDependencies": [
      "chdb",
      "msgpackr-extract",
      "protobufjs"
    ]
  }
}
EOF

cat >pnpm-workspace.yaml <<'EOF'
packages:
  - .
onlyBuiltDependencies:
  - chdb
  - msgpackr-extract
  - protobufjs
allowBuilds:
  - chdb
  - msgpackr-extract
  - protobufjs
EOF

cat >.npmrc <<'EOF'
only-built-dependencies[]=chdb
only-built-dependencies[]=msgpackr-extract
only-built-dependencies[]=protobufjs
EOF

cat >index.html <<'EOF'
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>View Server Consumer Smoke</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/App.tsx"></script>
  </body>
</html>
EOF

cat >tsconfig.json <<'EOF'
{
  "compilerOptions": {
    "jsx": "react-jsx",
    "lib": ["DOM", "DOM.Iterable", "ES2024"],
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "noEmit": true,
    "allowImportingTsExtensions": true,
    "skipLibCheck": true,
    "strict": true,
    "target": "ES2024",
    "types": ["node", "vite/client"]
  },
  "include": ["src", "tests", "vitest.browser.config.ts", "vite.config.ts"]
}
EOF

cat >vite.config.ts <<'EOF'
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [react()],
});
EOF

cat >vitest.browser.config.ts <<'EOF'
import { playwright } from "@vitest/browser-playwright";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "vite-plus/test": "vitest",
    },
  },
  test: {
    include: ["tests/**/*.browser.tsx"],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium" }],
      headless: true,
    },
  },
});
EOF

cat >src/view-server.ts <<'EOF'
import * as Schema from "effect/Schema";
import { defineConfig } from "@view-server/core/config";
import type { RawQuery } from "@view-server/core/query";

export const Order = Schema.Struct({
  id: Schema.String,
  isolationId: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
  desk: Schema.Literals(["ny", "ldn"]),
});

export const Trade = Schema.Struct({
  id: Schema.String,
  isolationId: Schema.String,
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
EOF

cat >src/node-smoke.ts <<'EOF'
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { makeViewServerRuntime } from "@view-server/core/runtime";
import { config, ordersQuery } from "./view-server.ts";

const program = Effect.scoped(
  Effect.gen(function* () {
    const runtime = yield* makeViewServerRuntime(config);
    yield* runtime.publish("orders", {
      id: "o-1",
      isolationId: "node-smoke",
      symbol: "AAPL",
      price: 100,
      desk: "ny",
    });
    yield* runtime.publish("trades", {
      id: "t-1",
      isolationId: "node-smoke",
      orderId: "o-1",
      qty: 10,
    });

    const result = yield* runtime.query("orders", ordersQuery);
    if (result.totalRows !== 1 || result.rows[0]?.symbol !== "AAPL") {
      return yield* Effect.die(new Error("query result mismatch"));
    }

    const events = yield* runtime
      .subscribe("node-smoke-sub", "orders", ordersQuery)
      .pipe(Stream.toQueue({ capacity: 8 }));

    const snapshot = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
    if (snapshot.type !== "snapshot") {
      return yield* Effect.die(new Error("expected snapshot"));
    }
    yield* runtime.publish("orders", {
      id: "o-2",
      isolationId: "node-smoke",
      symbol: "MSFT",
      price: 200,
      desk: "ny",
    });
    const delta = yield* Queue.take(events).pipe(Effect.timeout("1 second"));
    if (delta.type !== "delta") {
      return yield* Effect.die(new Error("expected delta"));
    }
    yield* runtime.unsubscribe("node-smoke-sub");
    yield* Effect.sleep("20 millis");
    const health = yield* runtime.health;
    const subscribers = Object.values(health.topics).reduce(
      (total, topic) => total + topic.subscribers,
      0,
    );

    if (subscribers !== 0) {
      return yield* Effect.die(new Error("subscription lifecycle mismatch"));
    }
    yield* runtime.close;
  }),
);

await Effect.runPromise(program);
process.stdout.write("node consumer smoke passed\n");
EOF

cat >src/App.tsx <<'EOF'
import { AsyncResult } from "effect/unstable/reactivity";
import { createRoot } from "react-dom/client";
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
  const result = hooks.useLiveQuery("orders", ordersQuery, {
    rows: [{ id: "o-1", symbol: "AAPL", price: 100 }],
    totalRows: 1,
  });

  return AsyncResult.match(result, {
    onInitial: () => <p>connecting</p>,
    onFailure: () => <p>error</p>,
    onSuccess: ({ value }) => (
      <p>
        {value.status}:{value.totalRows}:{value.rows.map((row) => row.symbol).join(",")}
      </p>
    ),
  });
}

createRoot(document.getElementById("root")!).render(<App />);
EOF

cat >src/bundle-grep.mjs <<'EOF'
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

const forbidden = [
  "node:worker_threads",
  "worker_threads",
  "@platformatic/kafka",
  "node:fs",
  "node:net",
  "from \"chdb\"",
  "from 'chdb'",
];

async function files(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const found = [];
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      found.push(...(await files(path)));
    } else {
      found.push(path);
    }
  }
  return found;
}

for (const file of await files("dist")) {
  const text = await readFile(file, "utf8");
  for (const token of forbidden) {
    if (text.includes(token)) {
      process.stderr.write(`forbidden browser bundle token ${token} in ${file}\n`);
      process.exit(1);
    }
  }
}

process.stdout.write("browser bundle grep passed\n");
EOF

cat >tests/testing.browser.tsx <<'EOF'
import { describe, expect, test } from "vite-plus/test";
import {
  createTestingViewServerReact,
  makeTestingBrowserWebsocketClient,
  readyUrlForRpcUrl,
  realViewServerTestHarness,
} from "@view-server/testing";
import { config } from "../src/view-server";

describe("packed testing package", () => {
  test("imports real-server testing helpers from the public package", () => {
    const testingReact = createTestingViewServerReact(config);
    expect(typeof testingReact.TestingViewServerProvider).toBe("function");
    expect(typeof testingReact.useLiveQuery).toBe("function");
    expect(typeof makeTestingBrowserWebsocketClient).toBe("function");
    expect(typeof realViewServerTestHarness).toBe("function");
    expect(readyUrlForRpcUrl("ws://127.0.0.1:3100/rpc")).toBe("http://127.0.0.1:3100/ready");
  });
});
EOF

if command -v corepack >/dev/null 2>&1; then
  corepack enable
  corepack prepare pnpm@11.0.9 --activate
fi
if ! pnpm install --config.confirmModulesPurge=false; then
  pnpm approve-builds --all
  pnpm install --config.confirmModulesPurge=false
fi
pnpm exec tsc --noEmit
pnpm run node:smoke
pnpm run build
pnpm run bundle:grep
pnpm exec playwright install chromium
pnpm run test

printf 'external consumer smoke passed in %s\n' "$SMOKE_DIR"
