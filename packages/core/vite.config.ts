import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: ["src/index.ts", "src/worker/topic-worker-node-entry.ts"],
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
