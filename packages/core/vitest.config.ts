import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    tsconfigPaths: true,
  },
  test: {
    include: ["tests/**/*.test.ts"],
    // Core tests spawn real websocket servers, worker threads, and chDB child processes.
    // Running files serially avoids cross-file resource contention hiding as timeout flakes.
    fileParallelism: false,
    testTimeout: 30_000,
    coverage: {
      thresholds: {
        100: true,
      },
    },
  },
});
