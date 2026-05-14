import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

const reactSource = fileURLToPath(new URL("../react/src/index.ts", import.meta.url));

export default defineConfig({
  plugins: [react()],
  optimizeDeps: {
    force: true,
    include: [
      "@effect/platform-browser",
      "effect/unstable/http",
      "effect/unstable/reactivity",
      "effect/unstable/rpc",
      "effect/unstable/rpc/RpcClient",
      "effect/unstable/rpc/RpcSerialization",
      "effect/unstable/rpc/RpcServer",
    ],
    exclude: ["@view-server/core", "@view-server/react", "@view-server/testing"],
  },
  resolve: {
    alias: {
      "@view-server/react": reactSource,
      "vite-plus/test": "vitest",
    },
  },
  test: {
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
    coverage: {
      thresholds: {
        100: true,
      },
    },
  },
});
