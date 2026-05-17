import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/client.ts",
      "src/config.ts",
      "src/errors.ts",
      "src/internal/testing.ts",
      "src/kafka.ts",
      "src/query.ts",
      "src/rpc.ts",
      "src/runtime.ts",
      "src/snapshot.ts",
      "src/kafka/platformatic-consumer.ts",
      "src/rpc/websocket.ts",
      "src/snapshot/chdb-backend.ts",
      "src/worker/topic-worker-node-host.ts",
      "src/worker/topic-worker-node-entry.ts",
      "src/snapshot/chdb-query-worker-entry.ts",
    ],
    dts: {
      tsgo: true,
    },
    exports: false,
  },
  test: {
    coverage: {
      thresholds: {
        100: true,
      },
    },
  },
  lint: {
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  fmt: {},
});
