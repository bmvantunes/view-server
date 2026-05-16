import { playwright } from "@vitest/browser-playwright";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";

const coreSource = fileURLToPath(new URL("../core/src", import.meta.url));

export default {
  plugins: [react()],
  optimizeDeps: {
    force: true,
    include: [
      "@effect/platform-browser",
      "effect/Cause",
      "effect/BigDecimal",
      "effect/Context",
      "effect/Deferred",
      "effect/Effect",
      "effect/Exit",
      "effect/Fiber",
      "effect/Layer",
      "effect/Option",
      "effect/Queue",
      "effect/Schema",
      "effect/SchemaGetter",
      "effect/Scope",
      "effect/Semaphore",
      "effect/Stream",
      "effect/unstable/http",
      "effect/unstable/reactivity",
      "effect/unstable/rpc",
      "effect/unstable/rpc/RpcClient",
      "effect/unstable/rpc/RpcSerialization",
      "effect/unstable/rpc/RpcServer",
    ],
    exclude: ["@view-server/core", "@view-server/react"],
  },
  resolve: {
    tsconfigPaths: true,
    alias: {
      "@view-server/core": coreSource,
      "vite-plus/test": "vitest",
    },
  },
  test: {
    globalSetup: ["./tests/browser-rpc.global-setup.ts"],
    include: ["tests/**/*.browser.tsx"],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium" }],
      headless: true,
      api: {
        strictPort: false,
      },
    },
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
};
