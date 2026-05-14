import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { AsyncResult } from "effect/unstable/reactivity";
import React from "react";
import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, inject, test } from "vite-plus/test";
import {
  defineConfig,
  type RawQuery,
  type RuntimeRow,
  type SubscriptionEvent,
  type ViewServerClient,
} from "@view-server/core";
import {
  createViewServerReact,
  createViewServerHooks,
  makeBrowserWebsocketClient,
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

const viewServerReact = createViewServerReact(config);
const { ViewServerProvider, useLiveQuery } = viewServerReact;

const roots: Root[] = [];
const scriptedQuery = makeQuery("SCRIPTED", 2);
const scriptedRenderStates: ScriptedRenderState[] = [];

type ScriptedRenderState = {
  readonly status: string;
  readonly waiting: boolean;
  readonly totalRows: number;
  readonly rows: readonly string[];
};

afterEach(() => {
  scriptedRenderStates.splice(0);
  unmountAll();
});

describe("browser useLiveQuery", () => {
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
          const client = yield* makeBrowserWebsocketClient<typeof config>(url, config);
          const hooks = createViewServerHooks(client, config);

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

  test("surfaces stale catch-up status through useLiveQuery while keeping rows visible", async () => {
    const hooks = createViewServerHooks(scriptedLiveQueryClient(), config);
    renderScriptedGrid(hooks);

    await waitForCondition(
      () =>
        scriptedRenderStates.some(
          (state) => state.status === "live" && !state.waiting && state.totalRows === 3,
        ),
      "refreshed live state",
    );
    const initialIndex = scriptedRenderStates.findIndex(
      (state) =>
        state.status === "live" &&
        !state.waiting &&
        state.totalRows === 2 &&
        state.rows.join(",") === "SCRIPTED-2,SCRIPTED-1",
    );
    const staleIndex = scriptedRenderStates.findIndex(
      (state, index) =>
        index > initialIndex &&
        state.status === "stale" &&
        state.waiting &&
        state.totalRows === 3 &&
        state.rows.join(",") === "SCRIPTED-2,SCRIPTED-1",
    );
    const refreshedIndex = scriptedRenderStates.findIndex(
      (state, index) =>
        index > staleIndex &&
        state.status === "live" &&
        !state.waiting &&
        state.totalRows === 3 &&
        state.rows.join(",") === "SCRIPTED-2,SCRIPTED-1",
    );
    expect(initialIndex).toBeGreaterThanOrEqual(0);
    expect(staleIndex).toBeGreaterThan(initialIndex);
    expect(refreshedIndex).toBeGreaterThan(staleIndex);
    expect(document.body.textContent).toContain("scripted:SCRIPTED-2:SCRIPTED-B:200");
    expect(document.body.textContent).toContain("scripted:SCRIPTED-1:SCRIPTED-A:100");
    expect(document.body.textContent).not.toContain("scripted:SCRIPTED-3:SCRIPTED-C:50");
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
      <ViewServerProvider url={url}>
        <ProviderOrdersGrid query={query} />
      </ViewServerProvider>,
    ),
  );
}

function renderScriptedGrid(hooks: ViewServerHooks<typeof config>) {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() => root.render(<ScriptedOrdersGrid hooks={hooks} />));
}

function ProviderOrdersGrid(props: { readonly query: ReturnType<typeof makeQuery> }) {
  const result = useLiveQuery("orders", props.query);
  return AsyncResult.match(result, {
    onInitial: () => <div>provider:rows=0</div>,
    onFailure: () => <div>provider:status=error</div>,
    onSuccess: ({ value }) => (
      <div>
        <div>provider:rows={value.totalRows}</div>
        {value.rows.map((row) => (
          <div key={row.id}>
            provider:{row.id}:{row.symbol}:{row.price}
          </div>
        ))}
      </div>
    ),
  });
}

function OrdersGrid(props: {
  readonly hooks: ViewServerHooks<typeof config>;
  readonly label: string;
  readonly query: ReturnType<typeof makeQuery>;
}) {
  const result = props.hooks.useLiveQuery("orders", props.query);
  return AsyncResult.match(result, {
    onInitial: () => (
      <div>
        <div>{props.label}:status=connecting</div>
        <div>{props.label}:rows=0</div>
      </div>
    ),
    onFailure: () => (
      <div>
        <div>{props.label}:status=error</div>
      </div>
    ),
    onSuccess: ({ value }) => (
      <div>
        <div>
          {props.label}:status={value.status}
        </div>
        <div>
          {props.label}:rows={value.totalRows}
        </div>
        {value.rows.map((row) => (
          <div key={row.id}>
            {props.label}:{row.id}:{row.symbol}:{row.price}
          </div>
        ))}
      </div>
    ),
  });
}

function ScriptedOrdersGrid(props: { readonly hooks: ViewServerHooks<typeof config> }) {
  const result = props.hooks.useLiveQuery("orders", scriptedQuery);
  React.useEffect(() => {
    if (!AsyncResult.isSuccess(result)) {
      return;
    }
    scriptedRenderStates.push({
      status: result.value.status,
      waiting: result.waiting,
      totalRows: result.value.totalRows,
      rows: result.value.rows.map((row) => row.id),
    });
  }, [result]);
  return AsyncResult.match(result, {
    onInitial: () => (
      <div>
        <div>scripted:status=connecting</div>
        <div>scripted:waiting=true</div>
        <div>scripted:rows=0</div>
      </div>
    ),
    onFailure: () => <div>scripted:status=error</div>,
    onSuccess: ({ value }) => (
      <div>
        <div>scripted:status={value.status}</div>
        <div>scripted:waiting={String(result.waiting)}</div>
        <div>scripted:rows={value.totalRows}</div>
        {value.rows.map((row) => (
          <div key={row.id}>
            scripted:{row.id}:{row.symbol}:{row.price}
          </div>
        ))}
      </div>
    ),
  });
}

function scriptedLiveQueryClient(): ViewServerClient<typeof config> {
  return {
    query: () => Effect.die(new Error("scripted client query is not used")),
    subscribe: (_topic, _query, onEvent) =>
      Effect.gen(function* () {
        const requestId = "scripted-live-query";
        const timers = scheduleScriptedLiveQueryEvents(requestId, onEvent);
        const clearTimers = () => {
          for (const timer of timers) {
            clearTimeout(timer);
          }
          timers.clear();
        };
        yield* Effect.addFinalizer(() => Effect.sync(clearTimers));
        return {
          requestId,
          close: Effect.sync(clearTimers),
        };
      }),
    publish: () => Effect.void,
    deltaPublish: () => Effect.void,
    deleteById: () => Effect.void,
    health: () => Effect.die(new Error("scripted client health is not used")),
    createStore: () => Effect.die(new Error("scripted client createStore is not used")),
  };
}

function scheduleScriptedLiveQueryEvents(
  requestId: string,
  onEvent: (event: SubscriptionEvent<readonly RuntimeRow[]>) => Effect.Effect<void>,
): Set<ReturnType<typeof setTimeout>> {
  const timers = new Set<ReturnType<typeof setTimeout>>();
  const visibleRows = [
    { id: "SCRIPTED-2", symbol: "SCRIPTED-B", price: 200 },
    { id: "SCRIPTED-1", symbol: "SCRIPTED-A", price: 100 },
  ];
  const schedule = (delay: number, event: SubscriptionEvent<readonly RuntimeRow[]>) => {
    const timer = setTimeout(() => {
      void Effect.runPromise(onEvent(event));
      timers.delete(timer);
    }, delay);
    timers.add(timer);
  };
  schedule(0, {
    type: "snapshot",
    requestId,
    rows: visibleRows,
    meta: {
      version: "0",
      totalRows: 2,
      serverTime: Date.now(),
    },
  });
  schedule(100, {
    type: "status",
    requestId,
    status: "stale",
    meta: {
      version: "1",
      totalRows: 3,
      serverTime: Date.now(),
    },
  });
  schedule(200, {
    type: "snapshot",
    requestId,
    rows: visibleRows,
    meta: {
      version: "1",
      totalRows: 3,
      serverTime: Date.now(),
    },
  });
  return timers;
}

function unmountAll(): void {
  for (const root of roots.splice(0)) {
    root.unmount();
  }
  document.body.innerHTML = "";
}

async function waitForText(text: string): Promise<void> {
  await waitForCondition(() => Boolean(document.body.textContent?.includes(text)), text);
}

async function waitForCondition(
  condition: () => boolean | Promise<boolean>,
  label = "condition",
  timeoutMs = 10_000,
): Promise<void> {
  const startedAt = Date.now();
  while (Date.now() - startedAt < timeoutMs) {
    if (await condition()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for ${label}`);
}
