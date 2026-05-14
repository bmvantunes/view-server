import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { decodeViewServerStartupEnv } from "../src/server/index.ts";

describe("startup env validation", () => {
  it.effect("decodes required startup environment through Effect Schema", () =>
    Effect.gen(function* () {
      const env = yield* decodeViewServerStartupEnv({
        KAFKA_BROKERS: "kafka-1:9092, kafka-2:9092",
        VIEW_SERVER_PORT: "8080",
        VIEW_SERVER_RPC_PATH: "/rpc",
      });

      expect(env.kafkaBrokers).toEqual(["kafka-1:9092", "kafka-2:9092"]);
      expect(env.rpcPort).toBe(8080);
      expect(env.rpcPath).toBe("/rpc");
    }),
  );

  it.effect("fails fast when required startup environment is missing or invalid", () =>
    Effect.gen(function* () {
      const missing = yield* decodeViewServerStartupEnv({
        VIEW_SERVER_PORT: "8080",
        VIEW_SERVER_RPC_PATH: "/rpc",
      }).pipe(Effect.flip);
      expect(missing._tag).toBe("InvalidStartupEnv");

      const invalidPort = yield* decodeViewServerStartupEnv({
        KAFKA_BROKERS: "kafka-1:9092",
        VIEW_SERVER_PORT: "70000",
        VIEW_SERVER_RPC_PATH: "/rpc",
      }).pipe(Effect.flip);
      expect(invalidPort._tag).toBe("InvalidStartupEnv");

      const invalidPath = yield* decodeViewServerStartupEnv({
        KAFKA_BROKERS: "kafka-1:9092",
        VIEW_SERVER_PORT: "8080",
        VIEW_SERVER_RPC_PATH: "rpc",
      }).pipe(Effect.flip);
      expect(invalidPath._tag).toBe("InvalidStartupEnv");
      expect(invalidPath.variable).toBe("VIEW_SERVER_RPC_PATH");
    }),
  );
});
