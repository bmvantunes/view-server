import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { AsyncResult } from "effect/unstable/reactivity";
import React from "react";
import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, test } from "vite-plus/test";
import type { LiveQueryInitialData } from "@view-server/core/client";
import type { ViewServerRpcTransport } from "@view-server/core/client";
import { defineConfig } from "@view-server/core/config";
import type {
  InferQueryResult,
  RawQuery,
  RuntimeQuery,
  RuntimeRow,
  SubscriptionEvent,
} from "@view-server/core/query";
import { fromWireRow, wireQueryResponse } from "@view-server/core/rpc";
import { readyUrlForRpcUrl, realViewServerTestHarness } from "../src/index.ts";
import {
  inMemoryViewServer,
  isolatedInMemoryViewServer,
  type InMemoryViewServer,
  type IsolatedInMemoryViewServer,
} from "./fixtures/in-memory.ts";
import {
  createTestingViewServerClientFromTransport,
  validateTestingIsolationId,
} from "../src/testing-isolation.ts";

type OrderRow = {
  readonly id: string;
  readonly symbol: string;
  readonly price: number;
};

type IsolatedOrderRow = OrderRow & {
  readonly isolationId: string;
};

type TradeRow = {
  readonly id: string;
  readonly region: string;
  readonly trader: string;
  readonly qty: number;
};

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

const IsolatedOrder = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
  isolationId: Schema.String,
});

const Trade = Schema.Struct({
  id: Schema.String,
  region: Schema.String,
  trader: Schema.String,
  qty: Schema.Number,
});

const config = defineConfig({
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

const isolatedConfig = defineConfig({
  topics: {
    orders: {
      id: "id",
      schema: IsolatedOrder,
    },
  },
});

const query = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 2,
} satisfies RawQuery<OrderRow, { id: true; price: true }>;

type OrdersGridRows = InferQueryResult<typeof config, "orders", typeof query>;

const pagedQuery = {
  fields: {
    id: true,
    symbol: true,
    price: true,
  },
  where: {
    field: "symbol",
    comparator: "contains",
    value: "A",
  },
  orderBy: [{ field: "price", direction: "asc" }],
  offset: 1,
  limit: 2,
} satisfies RawQuery<OrderRow, { id: true; symbol: true; price: true }>;

const stringQuery = {
  fields: {
    id: true,
    symbol: true,
    price: true,
  },
  where: {
    field: "symbol",
    comparator: "one_of",
    value: ["aapl", "msft", "aabbbb"],
  },
  orderBy: [{ field: "symbol", direction: "asc" }],
  limit: 10,
} satisfies RawQuery<OrderRow, { id: true; symbol: true; price: true }>;

const groupedQuery = {
  groupBy: ["region"],
  aggregates: {
    trades: { aggFunc: "count", field: "id" },
    traders: { aggFunc: "count_distinct", field: "trader" },
    totalQty: { aggFunc: "sum", field: "qty" },
  },
  orderBy: [{ field: "totalQty", direction: "desc" }],
  limit: 5,
} satisfies import("@view-server/core").GroupedQuery<
  TradeRow,
  ["region"],
  {
    trades: { aggFunc: "count"; field: "id" };
    traders: { aggFunc: "count_distinct"; field: "trader" };
    totalQty: { aggFunc: "sum"; field: "qty" };
  }
>;

const isolatedQuery = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 5,
} satisfies RawQuery<IsolatedOrderRow, { id: true; price: true }>;

const roots: Root[] = [];
const rowRenderCounts = new Map<string, number>();

afterEach(() => {
  for (const root of roots.splice(0)) {
    root.unmount();
  }
  rowRenderCounts.clear();
  document.body.innerHTML = "";
});

describe("useLiveQuery browser mode", () => {
  test("renders initial snapshot and live deltas from inMemoryViewServer", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* inMemoryViewServer(config, {
            initialRows: {
              orders: [
                { id: "o-1", symbol: "AAPL", price: 100 },
                { id: "o-2", symbol: "MSFT", price: 200 },
              ],
            },
          });
          const initialResult = yield* Effect.promise(() => server.query("orders", query));
          expect(initialResult.rows[0]?.id).toBe("o-2");
          expect(initialResult.totalRows).toBe(2);
          const initialHealth = yield* Effect.promise(() => server.health());
          expect(initialHealth.topics.orders.rows).toBe(2);
          const firstEvent = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
          yield* server.subscribe("orders", query, (event) =>
            Deferred.succeed(firstEvent, event).pipe(Effect.asVoid),
          );
          const snapshot = yield* Deferred.await(firstEvent).pipe(Effect.timeout("1 second"));
          expect(snapshot.type).toBe("snapshot");

          renderGrid(server);

          yield* Effect.promise(() => waitForText("o-2:200"));
          expect(document.body.textContent).toContain("rows=2");

          yield* Effect.promise(() =>
            server.publish("orders", { id: "o-3", symbol: "NVDA", price: 300 }),
          );
          yield* Effect.promise(() => waitForText("o-3:300"));
          expect(document.body.textContent).toContain("rows=3");

          const renderCountsBeforeTotalOnly = new Map(rowRenderCounts);
          yield* Effect.promise(() =>
            server.publish("orders", { id: "o-0", symbol: "IBM", price: 50 }),
          );
          yield* Effect.promise(() => waitForText("rows=4"));
          expect(document.body.textContent).toContain("o-3:300");
          expect(document.body.textContent).toContain("o-2:200");
          expect(document.body.textContent).not.toContain("o-0:50");
          expect(rowRenderCounts.get("o-3")).toBe(renderCountsBeforeTotalOnly.get("o-3"));
          expect(rowRenderCounts.get("o-2")).toBe(renderCountsBeforeTotalOnly.get("o-2"));

          yield* Effect.promise(() => server.deleteById("orders", "o-3"));
          yield* Effect.promise(() =>
            waitForCondition(
              () =>
                document.body.textContent?.includes("rows=3") === true &&
                document.body.textContent?.includes("o-3:300") === false,
              "deleted row to leave the visible window",
            ),
          );
          expect(document.body.textContent).toContain("o-2:200");
          expect(document.body.textContent).toContain("o-1:100");
        }),
      ),
    );
  });

  test("hydrates initialData before the live snapshot replaces it", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* inMemoryViewServer(config, {
            initialRows: {
              orders: [{ id: "o-2", symbol: "MSFT", price: 200 }],
            },
          });

          renderGrid(server, { rows: [{ id: "o-1", price: 100 }], totalRows: 25 });

          expect(document.body.textContent).toContain("o-1:100");
          expect(document.body.textContent).toContain("rows=25");
          expect(document.body.textContent).toContain("stale");
          expect(document.body.textContent).toContain("waiting=true");

          yield* Effect.promise(() => waitForText("o-2:200"));
          expect(document.body.textContent).toContain("rows=1");
          expect(document.body.textContent).toContain("live");
          expect(document.body.textContent).toContain("waiting=false");
          expect(document.body.textContent).not.toContain("o-1:100");
        }),
      ),
    );
  });

  test("renders sorted filtered paginated windows", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* inMemoryViewServer(config, {
            initialRows: {
              orders: [
                { id: "o-1", symbol: "AAPL", price: 100 },
                { id: "o-2", symbol: "MSFT", price: 200 },
                { id: "o-3", symbol: "AMZN", price: 150 },
                { id: "o-4", symbol: "NVDA", price: 300 },
              ],
            },
          });

          renderPagedGrid(server);

          yield* Effect.promise(() => waitForText("o-3:AMZN:150"));
          expect(document.body.textContent).toContain("o-4:NVDA:300");
          expect(document.body.textContent).toContain("rows=3");
          expect(document.body.textContent).not.toContain("o-1:AAPL:100");
          expect(document.body.textContent).not.toContain("o-2:MSFT:200");

          yield* Effect.promise(() =>
            server.publish("orders", { id: "o-5", symbol: "ADBE", price: 125 }),
          );

          yield* Effect.promise(() => waitForText("o-5:ADBE:125"));
          expect(document.body.textContent).toContain("o-3:AMZN:150");
          expect(document.body.textContent).toContain("rows=4");
          expect(document.body.textContent).not.toContain("o-4:NVDA:300");
        }),
      ),
    );
  });

  test("renders case-insensitive string filters and deterministic string sorting", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* inMemoryViewServer(config, {
            initialRows: {
              orders: [
                { id: "o-3", symbol: "MSFT", price: 200 },
                { id: "o-2", symbol: "AAPL", price: 125 },
                { id: "o-1", symbol: "aapl", price: 100 },
                { id: "o-4", symbol: "aaBBBB", price: 90 },
                { id: "o-5", symbol: "ZZZ", price: 300 },
              ],
            },
          });

          renderStringGrid(server);

          yield* Effect.promise(() => waitForText("o-4:aaBBBB:90"));
          expect(stringRows()).toEqual([
            "o-4:aaBBBB:90",
            "o-1:aapl:100",
            "o-2:AAPL:125",
            "o-3:MSFT:200",
          ]);
          expect(document.body.textContent).toContain("rows=4");
          expect(document.body.textContent).not.toContain("o-5:ZZZ:300");
        }),
      ),
    );
  });

  test("refreshes grouped aggregate subscriptions using groupBy fields as stable row keys", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* inMemoryViewServer(config, {
            initialRows: {
              trades: [
                { id: "t-1", region: "us", trader: "bruno", qty: 30 },
                { id: "t-2", region: "us", trader: "ana", qty: 10 },
                { id: "t-3", region: "eu", trader: "john", qty: 25 },
              ],
            },
          });

          renderGroupedGrid(server);

          yield* Effect.promise(() => waitForText("us:2:2:40"));
          expect(groupedRows()).toEqual(["us:2:2:40", "eu:1:1:25"]);
          expect(document.body.textContent).toContain("rows=2");

          yield* Effect.promise(() => server.deltaPublish("trades", { id: "t-3", qty: 50 }));
          yield* Effect.promise(() => waitForText("eu:1:1:50"));
          expect(groupedRows()).toEqual(["eu:1:1:50", "us:2:2:40"]);

          yield* Effect.promise(() =>
            server.publish("trades", { id: "t-4", region: "apac", trader: "zara", qty: 60 }),
          );
          yield* Effect.promise(() => waitForText("apac:1:1:60"));
          expect(groupedRows()).toEqual(["apac:1:1:60", "eu:1:1:50", "us:2:2:40"]);
          expect(document.body.textContent).toContain("rows=3");
        }),
      ),
    );
  });

  test("testing isolation injects isolationId into publishes and live queries", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const server = yield* isolatedInMemoryViewServer(isolatedConfig, {
            isolationId: "test-a",
          });

          yield* Effect.promise(() =>
            server.publish("orders", { id: "o-1", symbol: "AAPL", price: 100 }),
          );
          const initial = yield* Effect.promise(() => server.query("orders", isolatedQuery));
          expect(initial.rows).toEqual([{ id: "o-1", price: 100 }]);
          expect(initial.totalRows).toBe(1);

          renderIsolatedGrid(server);
          yield* Effect.promise(() => waitForText("o-1:100"));
          expect(document.body.textContent).toContain("rows=1");

          yield* Effect.promise(() => server.deltaPublish("orders", { id: "o-1", price: 125 }));
          yield* Effect.promise(() => waitForText("o-1:125"));
          expect(document.body.textContent).toContain("rows=1");
        }),
      ),
    );
  });

  test("testing isolation scopes two clients over one transport", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const rows: RuntimeRow[] = [];
        const transport = fakeIsolatedTransport(rows);
        const clientA = createTestingViewServerClientFromTransport(
          transport,
          isolatedConfig,
          "test-a",
        ).client;
        const clientB = createTestingViewServerClientFromTransport(
          transport,
          isolatedConfig,
          "test-b",
        ).client;

        yield* clientA.publish("orders", { id: "a-1", symbol: "AAPL", price: 100 });
        yield* clientB.publish("orders", { id: "b-1", symbol: "MSFT", price: 200 });
        yield* clientA.deltaPublish("orders", { id: "a-1", price: 125 });

        const a = yield* clientA.query("orders", isolatedQuery);
        const b = yield* clientB.query("orders", isolatedQuery);

        expect(rows).toEqual([
          { id: "a-1", symbol: "AAPL", price: 125, isolationId: "test-a" },
          { id: "b-1", symbol: "MSFT", price: 200, isolationId: "test-b" },
        ]);
        expect(a.rows).toEqual([{ id: "a-1", price: 125 }]);
        expect(a.totalRows).toBe(1);
        expect(b.rows).toEqual([{ id: "b-1", price: 200 }]);
        expect(b.totalRows).toBe(1);
        expect(() => validateTestingIsolationId(" ")).toThrow(/isolationId is required/);
      }),
    );
  });

  test("real server harness uses scoped isolation helpers without owning browser internals", async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const rows: RuntimeRow[] = [];
          let starts = 0;
          let stops = 0;
          const harness = yield* realViewServerTestHarness(isolatedConfig, {
            rpcUrl: "ws://127.0.0.1:3100/rpc",
            isolationId: "harness-a",
            transport: fakeIsolatedTransport(rows),
            start: Effect.sync(() => {
              starts += 1;
            }),
            stop: Effect.sync(() => {
              stops += 1;
            }),
          });

          yield* Effect.promise(() =>
            harness.publish("orders", { id: "o-1", symbol: "AAPL", price: 100 }),
          );
          const result = yield* Effect.promise(() => harness.query("orders", isolatedQuery));
          yield* Effect.promise(() => harness.close());
          yield* Effect.promise(() => harness.close());

          expect(starts).toBe(1);
          expect(stops).toBe(1);
          expect(harness.isolationId).toBe("harness-a");
          expect(result.rows).toEqual([{ id: "o-1", price: 100 }]);
          expect(rows).toEqual([
            { id: "o-1", symbol: "AAPL", price: 100, isolationId: "harness-a" },
          ]);
          expect(readyUrlForRpcUrl("wss://example.test/rpc")).toBe("https://example.test/ready");
        }),
      ),
    );
  });
});

function renderGrid(
  server: InMemoryViewServer<typeof config>,
  initialData?: LiveQueryInitialData<OrdersGridRows[number]>,
) {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() => root.render(<OrdersGrid server={server} initialData={initialData} />));
}

function renderPagedGrid(server: InMemoryViewServer<typeof config>) {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() => root.render(<PagedOrdersGrid server={server} />));
}

function renderStringGrid(server: InMemoryViewServer<typeof config>) {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() => root.render(<StringOrdersGrid server={server} />));
}

function renderGroupedGrid(server: InMemoryViewServer<typeof config>) {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() => root.render(<GroupedTradesGrid server={server} />));
}

function renderIsolatedGrid(server: IsolatedInMemoryViewServer<typeof isolatedConfig>) {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() => root.render(<IsolatedOrdersGrid server={server} />));
}

function fakeIsolatedTransport(rows: RuntimeRow[]): ViewServerRpcTransport {
  return {
    Query: (payload) =>
      Effect.succeed(
        wireQueryResponse({
          rows: projectRows(rows, isolationIdFromQuery(payload.query)),
          totalRows: projectRows(rows, isolationIdFromQuery(payload.query)).length,
          version: "0",
        }),
      ),
    Subscribe: () => Stream.empty,
    Unsubscribe: () => Effect.void,
    Publish: (payload) =>
      Effect.sync(() => {
        rows.push(fromWireRow(payload.row));
      }),
    DeltaPublish: (payload) =>
      Effect.sync(() => {
        const patch = fromWireRow(payload.patch);
        const id = patch.id;
        const index = rows.findIndex(
          (row) => row.id === id && row.isolationId === patch.isolationId,
        );
        if (index >= 0) {
          rows[index] = { ...rows[index], ...patch };
        }
      }),
    DeleteById: () => Effect.void,
    Health: () => Effect.succeed({ ok: true, topics: {} }),
  };
}

function projectRows(rows: readonly RuntimeRow[], isolationId: string | undefined): RuntimeRow[] {
  return rows
    .filter((row) => row.isolationId === isolationId)
    .map((row) => ({
      id: row.id,
      price: row.price,
    }));
}

function isolationIdFromQuery(query: RuntimeQuery): string | undefined {
  return isolationIdFromWhere(query.where);
}

function isolationIdFromWhere(where: RuntimeQuery["where"]): string | undefined {
  if (where === undefined) {
    return undefined;
  }
  if ("conditions" in where) {
    for (const condition of where.conditions) {
      const isolationId = isolationIdFromWhere(condition);
      if (isolationId !== undefined) {
        return isolationId;
      }
    }
    return undefined;
  }
  return where.field === "isolationId" && where.comparator === "equals"
    ? String(where.value)
    : undefined;
}

function OrdersGrid(props: {
  readonly server: InMemoryViewServer<typeof config>;
  readonly initialData?: LiveQueryInitialData<OrdersGridRows[number]> | undefined;
}) {
  const result = props.server.hooks.useLiveQuery("orders", query, props.initialData);
  return AsyncResult.match(result, {
    onInitial: () => (
      <div>
        <div>connecting</div>
        <div>waiting=true</div>
        <div>rows=0</div>
      </div>
    ),
    onFailure: () => <div>error</div>,
    onSuccess: ({ value }) => (
      <div>
        <div>{value.status}</div>
        <div>waiting={String(result.waiting)}</div>
        <div>rows={value.totalRows}</div>
        {value.rows.map((row) => (
          <OrderLine key={row.id} row={row} />
        ))}
      </div>
    ),
  });
}

const OrderLine = React.memo(function OrderLine(props: { readonly row: OrdersGridRows[number] }) {
  rowRenderCounts.set(props.row.id, (rowRenderCounts.get(props.row.id) ?? 0) + 1);
  return (
    <div>
      {props.row.id}:{props.row.price}
    </div>
  );
});

function PagedOrdersGrid(props: { readonly server: InMemoryViewServer<typeof config> }) {
  const result = props.server.hooks.useLiveQuery("orders", pagedQuery);
  return AsyncResult.match(result, {
    onInitial: () => (
      <div>
        <div>connecting</div>
        <div>rows=0</div>
      </div>
    ),
    onFailure: () => <div>error</div>,
    onSuccess: ({ value }) => (
      <div>
        <div>{value.status}</div>
        <div>rows={value.totalRows}</div>
        {value.rows.map((row) => (
          <div key={row.id}>
            {row.id}:{row.symbol}:{row.price}
          </div>
        ))}
      </div>
    ),
  });
}

function StringOrdersGrid(props: { readonly server: InMemoryViewServer<typeof config> }) {
  const result = props.server.hooks.useLiveQuery("orders", stringQuery);
  return AsyncResult.match(result, {
    onInitial: () => (
      <div>
        <div>connecting</div>
        <div>rows=0</div>
      </div>
    ),
    onFailure: () => <div>error</div>,
    onSuccess: ({ value }) => (
      <div>
        <div>{value.status}</div>
        <div>rows={value.totalRows}</div>
        {value.rows.map((row) => (
          <div key={row.id} data-string-row="">
            {row.id}:{row.symbol}:{row.price}
          </div>
        ))}
      </div>
    ),
  });
}

function GroupedTradesGrid(props: { readonly server: InMemoryViewServer<typeof config> }) {
  const result = props.server.hooks.useLiveQuery("trades", groupedQuery);
  return AsyncResult.match(result, {
    onInitial: () => (
      <div>
        <div>connecting</div>
        <div>rows=0</div>
      </div>
    ),
    onFailure: () => <div>error</div>,
    onSuccess: ({ value }) => (
      <div>
        <div>{value.status}</div>
        <div>rows={value.totalRows}</div>
        {value.rows.map((row) => (
          <div key={row.region} data-grouped-row="">
            {row.region}:{row.trades}:{row.traders}:{row.totalQty}
          </div>
        ))}
      </div>
    ),
  });
}

function IsolatedOrdersGrid(props: {
  readonly server: IsolatedInMemoryViewServer<typeof isolatedConfig>;
}) {
  const result = props.server.hooks.useLiveQuery("orders", isolatedQuery);
  return AsyncResult.match(result, {
    onInitial: () => (
      <div>
        <div>connecting</div>
        <div>rows=0</div>
      </div>
    ),
    onFailure: () => <div>error</div>,
    onSuccess: ({ value }) => (
      <div>
        <div>{value.status}</div>
        <div>rows={value.totalRows}</div>
        {value.rows.map((row) => (
          <div key={row.id}>
            {row.id}:{row.price}
          </div>
        ))}
      </div>
    ),
  });
}

function stringRows(): string[] {
  return Array.from(document.querySelectorAll("[data-string-row]"), (row) => row.textContent ?? "");
}

function groupedRows(): string[] {
  return Array.from(
    document.querySelectorAll("[data-grouped-row]"),
    (row) => row.textContent ?? "",
  );
}

async function waitForText(text: string): Promise<void> {
  await waitForCondition(() => Boolean(document.body.textContent?.includes(text)), text);
}

async function waitForCondition(
  condition: () => boolean | Promise<boolean>,
  label = "condition",
  timeoutMs = 5_000,
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
