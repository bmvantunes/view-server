import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

export default defineConfig({
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
