import { playwright } from "@vitest/browser-playwright";
import { createRunnableDevEnvironment, type ResolvedConfig } from "vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";

const reactSource = fileURLToPath(new URL("../../packages/react/src/index.ts", import.meta.url));

function createSsrDevEnvironment(name: string, resolvedConfig: ResolvedConfig) {
  return createRunnableDevEnvironment(name, resolvedConfig);
}

export default {
  environments: {
    ssr: {
      dev: {
        createEnvironment: createSsrDevEnvironment,
      },
    },
  },
  plugins: [tanstackStart({ vite: { installDevServerMiddleware: true } }), viteReact()],
  optimizeDeps: {
    force: true,
    include: [
      "@effect/platform-browser",
      "effect/unstable/http",
      "effect/unstable/rpc",
      "effect/unstable/rpc/RpcClient",
      "effect/unstable/rpc/RpcSerialization",
      "effect/unstable/rpc/RpcServer",
      "react",
      "react-dom",
      "react-dom/client",
    ],
    exclude: ["@view-server/core", "@view-server/react"],
  },
  resolve: {
    tsconfigPaths: true,
    dedupe: ["react", "react-dom"],
    alias: {
      "@view-server/react": reactSource,
      "vite-plus/test": "vitest",
    },
  },
  test: {
    globalSetup: ["../../packages/react/tests/browser-rpc.global-setup.ts"],
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
