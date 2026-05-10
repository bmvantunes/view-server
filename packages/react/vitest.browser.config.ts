import { playwright } from "@vitest/browser-playwright";
import react from "@vitejs/plugin-react";

export default {
  plugins: [react()],
  optimizeDeps: {
    include: ["effect/unstable/http"],
  },
  resolve: {
    alias: {
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
