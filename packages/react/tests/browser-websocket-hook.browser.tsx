import { Effect, Schema } from "effect";
import React from "react";
import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, inject, test } from "vite-plus/test";
import { defineConfig, type RawQuery } from "@view-server/core";
import {
  ViewServerProvider,
  createViewServerHooks,
  makeBrowserWebsocketClient,
  useSubscription,
  type ViewServerHooks,
} from "../src/index.ts";

type OrderRow = {
  readonly id: string;
  readonly symbol: string;
  readonly price: number;
};

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

const config = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: Order,
    },
  },
});

const roots: Root[] = [];

afterEach(() => {
  unmountAll();
});

describe("browser websocket useSubscription", () => {
  test("ViewServerProvider creates a browser websocket client from url and config", async () => {
    const url = inject("viewServerWsUrl");
    const runId = `PROVIDER-${crypto.randomUUID()}`;
    const pageQuery = makeQuery(runId, 2);

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeBrowserWebsocketClient<typeof config>(url, config);
          yield* client.publish("orders", {
            id: `${runId}-1`,
            symbol: `${runId}-A`,
            price: 100,
          });

          renderProviderGrid(url, pageQuery);

          yield* Effect.promise(() => waitForText(`provider:${runId}-1:${runId}-A:100`));
          expect(document.body.textContent).toContain("provider:rows=1");
          yield* Effect.sync(unmountAll);
        }),
      ),
    );
  });

  test("renders live hook updates over the real Effect RPC websocket", async () => {
    const url = inject("viewServerWsUrl");
    const runId = `HOOK-${crypto.randomUUID()}`;
    const pageQuery = makeQuery(runId, 2);
    const topQuery = makeQuery(runId, 1);

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeBrowserWebsocketClient<typeof config>(url);
          const hooks = createViewServerHooks(client);

          yield* client.publish("orders", {
            id: `${runId}-1`,
            symbol: `${runId}-A`,
            price: 100,
          });
          yield* client.publish("orders", {
            id: `${runId}-2`,
            symbol: `${runId}-B`,
            price: 200,
          });

          renderGrids(hooks, pageQuery, topQuery);

          yield* Effect.promise(() => waitForText(`page:${runId}-2:${runId}-B:200`));
          yield* Effect.promise(() => waitForText(`top:${runId}-2:${runId}-B:200`));
          expect(document.body.textContent).toContain("page:rows=2");
          expect(document.body.textContent).toContain("top:rows=2");

          yield* client.publish("orders", {
            id: `${runId}-3`,
            symbol: `${runId}-C`,
            price: 300,
          });

          yield* Effect.promise(() => waitForText(`page:${runId}-3:${runId}-C:300`));
          yield* Effect.promise(() => waitForText(`top:${runId}-3:${runId}-C:300`));
          expect(document.body.textContent).toContain("page:rows=3");
          expect(document.body.textContent).toContain("top:rows=3");
          expect(document.body.textContent).toContain(`page:${runId}-2:${runId}-B:200`);
          expect(document.body.textContent).not.toContain(`page:${runId}-1:${runId}-A:100`);
          expect(document.body.textContent).not.toContain(`top:${runId}-2:${runId}-B:200`);

          yield* Effect.sync(unmountAll);
        }),
      ),
    );
  });
});

function makeQuery(prefix: string, limit: number) {
  return {
    fields: {
      id: true,
      symbol: true,
      price: true,
    },
    where: {
      field: "symbol",
      comparator: "starts_with",
      value: prefix,
    },
    orderBy: [{ field: "price", direction: "desc" }],
    limit,
  } satisfies RawQuery<
    OrderRow,
    { readonly id: true; readonly symbol: true; readonly price: true }
  >;
}

function renderGrids(
  hooks: ViewServerHooks<typeof config>,
  pageQuery: ReturnType<typeof makeQuery>,
  topQuery: ReturnType<typeof makeQuery>,
) {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() =>
    root.render(
      <div>
        <OrdersGrid hooks={hooks} label="page" query={pageQuery} />
        <OrdersGrid hooks={hooks} label="top" query={topQuery} />
      </div>,
    ),
  );
}

function renderProviderGrid(url: string, query: ReturnType<typeof makeQuery>) {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() =>
    root.render(
      <ViewServerProvider url={url} config={config}>
        <ProviderOrdersGrid query={query} />
      </ViewServerProvider>,
    ),
  );
}

function ProviderOrdersGrid(props: { readonly query: ReturnType<typeof makeQuery> }) {
  const result = useSubscription<typeof config, "orders", ReturnType<typeof makeQuery>>(
    "orders",
    props.query,
  );
  return (
    <div>
      <div>provider:rows={result.totalRows}</div>
      {result.data.map((row) => (
        <div key={row.id}>
          provider:{row.id}:{row.symbol}:{row.price}
        </div>
      ))}
    </div>
  );
}

function OrdersGrid(props: {
  readonly hooks: ViewServerHooks<typeof config>;
  readonly label: string;
  readonly query: ReturnType<typeof makeQuery>;
}) {
  const result = props.hooks.useSubscription("orders", props.query);
  return (
    <div>
      <div>
        {props.label}:status={result.status}
      </div>
      <div>
        {props.label}:rows={result.totalRows}
      </div>
      {result.data.map((row) => (
        <div key={row.id}>
          {props.label}:{row.id}:{row.symbol}:{row.price}
        </div>
      ))}
    </div>
  );
}

function unmountAll(): void {
  for (const root of roots.splice(0)) {
    root.unmount();
  }
  document.body.innerHTML = "";
}

async function waitForText(text: string): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < 10_000) {
    if (document.body.textContent?.includes(text)) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for text: ${text}`);
}
