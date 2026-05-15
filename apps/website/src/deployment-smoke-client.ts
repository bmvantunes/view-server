import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import { makeNodeWebsocketClient } from "@view-server/core/rpc/websocket";
import {
  ordersByDeskQuery,
  ordersDemoConfig,
  ordersWindowQuery,
  type OrderRow,
} from "./view-server.ts";

const httpUrl = process.env.VS_DEPLOYMENT_SMOKE_HTTP_URL ?? "http://127.0.0.1:3100";
const rpcUrl = process.env.VS_DEPLOYMENT_SMOKE_RPC_URL ?? "ws://127.0.0.1:3100/rpc";

const smokeRow = {
  id: "deployment-smoke-order",
  symbol: "SMOKE",
  desk: "LDN",
  status: "open",
  price: 1_000_000,
  quantity: 100,
  notional: 100_000_000,
  updatedAt: 1_900_000_000_000,
} satisfies OrderRow;

const updatedSmokePatch = {
  id: smokeRow.id,
  price: 1_000_001,
  quantity: 100,
  notional: 100_000_100,
  updatedAt: smokeRow.updatedAt + 1,
} satisfies Partial<OrderRow> & Pick<OrderRow, "id">;

const program = Effect.fn("view-server.deployment_smoke.client")(function* () {
  yield* assertHttpReady();

  const client = yield* makeNodeWebsocketClient(rpcUrl, ordersDemoConfig);
  const events = yield* Queue.unbounded<string>();
  const subscription = yield* client.subscribe("orders", ordersWindowQuery, (event) =>
    Queue.offer(events, event.type),
  );

  const firstEvent = yield* Queue.take(events).pipe(Effect.timeout("5 seconds"));
  if (firstEvent !== "snapshot") {
    return yield* Effect.die(new Error(`expected snapshot event, received ${firstEvent}`));
  }

  const initial = yield* client.query("orders", ordersWindowQuery);
  if (initial.totalRows <= 0 || initial.rows.length === 0) {
    return yield* Effect.die(new Error("orders query returned no rows"));
  }

  const grouped = yield* client.query("orders", ordersByDeskQuery);
  if (grouped.totalRows <= 0 || grouped.rows.length === 0) {
    return yield* Effect.die(new Error("grouped orders query returned no rows"));
  }

  yield* client.publish("orders", smokeRow);
  yield* client.deltaPublish("orders", updatedSmokePatch);

  const liveEvent = yield* Queue.take(events).pipe(Effect.timeout("5 seconds"));
  if (liveEvent !== "delta" && liveEvent !== "status" && liveEvent !== "snapshot") {
    return yield* Effect.die(new Error(`unexpected live event ${liveEvent}`));
  }

  const afterPublish = yield* client.query("orders", ordersWindowQuery);
  if (!afterPublish.rows.some((row) => row.id === smokeRow.id)) {
    return yield* Effect.die(new Error("published smoke row was not visible through RPC query"));
  }

  yield* client.deleteById("orders", smokeRow.id);
  yield* subscription.close;
  yield* Effect.sleep("100 millis");

  const health = yield* client.health();
  if (!health.ok) {
    return yield* Effect.die(new Error("deployment smoke health is not ready"));
  }
  const ordersHealth = health.topics.orders;
  if (ordersHealth === undefined || ordersHealth.subscribers !== 0) {
    return yield* Effect.die(new Error("deployment smoke left subscribers behind"));
  }

  yield* Effect.logInfo(
    `deployment smoke passed rows=${ordersHealth.rows} version=${ordersHealth.version}`,
  );
});

await Effect.runPromise(Effect.scoped(program()));

function assertHttpReady(): Effect.Effect<void> {
  return Effect.tryPromise({
    try: async () => {
      const response = await fetch(`${httpUrl}/ready`);
      if (!response.ok) {
        throw new Error(`ready endpoint returned ${response.status}`);
      }
    },
    catch: (error: unknown) => error,
  }).pipe(
    Effect.mapError((error) =>
      error instanceof Error ? error : new Error(`ready endpoint failed: ${String(error)}`),
    ),
    Effect.orDie,
  );
}
