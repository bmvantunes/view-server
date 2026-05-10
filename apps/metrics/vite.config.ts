import { createRunnableDevEnvironment, defineConfig, type ResolvedConfig } from "vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";

import viteReact from "@vitejs/plugin-react";

function createSsrDevEnvironment(name: string, resolvedConfig: ResolvedConfig) {
  return createRunnableDevEnvironment(name, resolvedConfig);
}

const config = defineConfig({
  environments: {
    ssr: {
      dev: {
        createEnvironment: createSsrDevEnvironment,
      },
    },
  },
  resolve: { tsconfigPaths: true },
  plugins: [tanstackStart({ vite: { installDevServerMiddleware: true } }), viteReact()],
});

export default config;
