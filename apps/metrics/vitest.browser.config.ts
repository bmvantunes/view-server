import { playwright } from "@vitest/browser-playwright";
import { createRunnableDevEnvironment, type ResolvedConfig } from "vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";

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
    include: ["effect/unstable/http", "react", "react-dom", "react-dom/client"],
  },
  resolve: {
    tsconfigPaths: true,
    dedupe: ["react", "react-dom"],
    alias: {
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
