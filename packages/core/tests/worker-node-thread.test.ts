import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import type { RawQuery } from "../src/protocol/index.ts";
import { makeNodeThreadTopicWorkerHostFactory } from "../src/worker/topic-worker-node-host.ts";
import config from "./fixtures/node-worker-config.ts";

type OrderRow = {
  readonly id: string;
  readonly symbol: string;
  readonly price: number;
};

const query = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 2,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

describe("node topic worker host", () => {
  it.effect("imports the defineConfig module in the worker thread and streams live events", () =>
    Effect.gen(function* () {
      const makeWorker = makeNodeThreadTopicWorkerHostFactory({
        configModuleUrl: new URL("./fixtures/node-worker-config.ts", import.meta.url),
        workerEntryUrl: new URL("../src/worker/topic-worker-node-entry.ts", import.meta.url),
        snapshotBackend: "memory",
      });
      const worker = yield* makeWorker("orders", config.topics.orders, {
        initialRows: [
          { id: "o-1", symbol: "AAPL", price: 100 },
          { id: "o-2", symbol: "MSFT", price: 200 },
        ],
      });

      const events = yield* worker
        .subscribe("worker-sub", query)
        .pipe(Stream.toQueue({ capacity: 16 }));

      const snapshot = yield* Queue.take(events).pipe(Effect.timeout("2 seconds"));
      expect(snapshot.type).toBe("snapshot");
      if (snapshot.type !== "snapshot") {
        throw new Error("Expected snapshot");
      }
      expect(snapshot.meta.totalRows).toBe(2);
      expect(snapshot.rows).toEqual([
        { id: "o-2", price: 200 },
        { id: "o-1", price: 100 },
      ]);

      yield* worker.publish({ id: "o-3", symbol: "NVDA", price: 300 });

      const delta = yield* Queue.take(events).pipe(Effect.timeout("2 seconds"));
      expect(delta.type).toBe("delta");
      if (delta.type !== "delta") {
        throw new Error("Expected delta");
      }
      expect(delta.meta.totalRows).toBe(3);
      expect(
        delta.ops.some((operation) => operation.type === "upsert" && operation.row.id === "o-3"),
      ).toBe(true);
    }).pipe(Effect.scoped),
  );
});
