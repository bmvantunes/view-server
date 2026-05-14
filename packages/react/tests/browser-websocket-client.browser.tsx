import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { describe, expect, inject, test } from "vite-plus/test";
import {
  defineConfig,
  type RawQuery,
  type RuntimeRow,
  type SubscriptionEvent,
} from "@view-server/core";
import { makeBrowserWebsocketClient } from "../src/index.ts";

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

describe("browser websocket client", () => {
  test("multiplexes subscriptions over one browser WebSocket", async () => {
    const url = inject("viewServerWsUrl");
    const runId = `CLIENT-${crypto.randomUUID()}`;
    const pageQuery = makeQuery(runId, 2);
    const topQuery = makeQuery(runId, 1);
    expect(url).toMatch(/^ws:\/\/127\.0\.0\.1:\d+\/rpc$/);
    await expectWebSocketOpens(url);

    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const client = yield* makeBrowserWebsocketClient<typeof config>(url, config);
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
          const result = yield* client.query("orders", pageQuery).pipe(Effect.timeout("1 second"));
          expect(result.rows[0]?.id).toBe(`${runId}-2`);

          const pageSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
          const topSnapshot = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
          const pageDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();
          const topDelta = yield* Deferred.make<SubscriptionEvent<readonly RuntimeRow[]>>();

          const pageSeen = { count: 0 };
          const topSeen = { count: 0 };

          const pageSubscription = yield* client.subscribe("orders", pageQuery, (event) =>
            Effect.sync(() => {
              pageSeen.count += 1;
              return pageSeen.count;
            }).pipe(
              Effect.flatMap((count) =>
                count === 1
                  ? Deferred.succeed(pageSnapshot, event)
                  : count === 2
                    ? Deferred.succeed(pageDelta, event)
                    : Effect.void,
              ),
              Effect.asVoid,
            ),
          );
          const topSubscription = yield* client.subscribe("orders", topQuery, (event) =>
            Effect.sync(() => {
              topSeen.count += 1;
              return topSeen.count;
            }).pipe(
              Effect.flatMap((count) =>
                count === 1
                  ? Deferred.succeed(topSnapshot, event)
                  : count === 2
                    ? Deferred.succeed(topDelta, event)
                    : Effect.void,
              ),
              Effect.asVoid,
            ),
          );

          const firstPage = yield* Deferred.await(pageSnapshot).pipe(Effect.timeout("1 second"));
          const firstTop = yield* Deferred.await(topSnapshot).pipe(Effect.timeout("1 second"));
          expect(firstPage.type).toBe("snapshot");
          expect(firstTop.type).toBe("snapshot");

          yield* client.publish("orders", {
            id: `${runId}-3`,
            symbol: `${runId}-C`,
            price: 300,
          });

          const nextPage = yield* Deferred.await(pageDelta).pipe(Effect.timeout("1 second"));
          const nextTop = yield* Deferred.await(topDelta).pipe(Effect.timeout("1 second"));
          expect(nextPage.type).toBe("delta");
          expect(nextTop.type).toBe("delta");
          if (nextPage.type === "delta" && nextTop.type === "delta") {
            expect(nextPage.meta.totalRows).toBe(3);
            expect(nextTop.meta.totalRows).toBe(3);
            expect(nextPage.ops.some((operation) => operation.type === "upsert")).toBe(true);
            expect(nextTop.ops.some((operation) => operation.type === "upsert")).toBe(true);
          }

          yield* pageSubscription.close;
          yield* topSubscription.close;
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

async function expectWebSocketOpens(url: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const socket = new WebSocket(url);
    const timeout = setTimeout(() => {
      socket.close();
      reject(new Error(`Timed out opening ${url}`));
    }, 1_000);
    socket.addEventListener(
      "open",
      () => {
        clearTimeout(timeout);
        socket.close();
        resolve();
      },
      { once: true },
    );
    socket.addEventListener(
      "error",
      () => {
        clearTimeout(timeout);
        reject(new Error(`Failed to open ${url}`));
      },
      { once: true },
    );
  });
}
