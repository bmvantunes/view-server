import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  createViewServerClient,
  defineConfig,
  InvalidQuery,
  KafkaSource,
  type RawQuery,
} from "@view-server/core";
import { createViewServerClient as createViewServerClientFromClient } from "@view-server/core/client";
import { defineConfig as defineConfigFromConfig } from "@view-server/core/config";
import { invalidQuery } from "@view-server/core/errors";
import { decodeJsonRecord } from "@view-server/core/kafka";
import { createPlatformaticKafkaConsumerFactory } from "@view-server/core/kafka/platformatic";
import { rowKeyForQuery } from "@view-server/core/query";
import { ViewServerRpcs } from "@view-server/core/rpc";
import { layerViewServerWebsocketServer } from "@view-server/core/rpc/websocket";
import { makeViewServerRuntime } from "@view-server/core/runtime";
import type { SnapshotBackend } from "@view-server/core/snapshot";
import { createChdbSnapshotBackendFactory } from "@view-server/core/snapshot/chdb";
import { makeNodeThreadTopicWorkerHostFactory } from "@view-server/core/worker/node";

const Order = Schema.Struct({
  id: Schema.String,
  symbol: Schema.String,
  price: Schema.Number,
});

type OrderRow = typeof Order.Type;

const query = {
  fields: {
    id: true,
    price: true,
  },
  orderBy: [{ field: "price", direction: "desc" }],
  limit: 5,
} satisfies RawQuery<OrderRow, { readonly id: true; readonly price: true }>;

describe("public package API smoke", () => {
  it.effect("imports core root and explicit public subpaths only", () =>
    Effect.sync(() => {
      const config = defineConfig({
        topics: {
          orders: {
            id: "id",
            schema: Order,
          },
        },
      });

      expect(defineConfigFromConfig(config)).toBe(config);
      expect(createViewServerClientFromClient).toBe(createViewServerClient);
      expect(invalidQuery("orders", "bad query")).toBeInstanceOf(InvalidQuery);
      expect(rowKeyForQuery(query, "id")({ id: "o-1", price: 1 })).toBe("o-1");
      expect(typeof makeViewServerRuntime).toBe("function");
      expect(typeof ViewServerRpcs).toBe("function");
      expect(typeof decodeJsonRecord).toBe("function");
      expect(typeof KafkaSource).toBe("function");
      const _snapshotBackendTypeOnly: SnapshotBackend | undefined = undefined;
      expect(_snapshotBackendTypeOnly).toBeUndefined();
      expect(typeof layerViewServerWebsocketServer).toBe("function");
      expect(typeof createPlatformaticKafkaConsumerFactory).toBe("function");
      expect(typeof createChdbSnapshotBackendFactory).toBe("function");
      expect(typeof makeNodeThreadTopicWorkerHostFactory).toBe("function");
    }),
  );
});
