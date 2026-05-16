import { defineConfig } from "@view-server/core/config";

export const metricsViewServerConfig = defineConfig({
  topics: {},
});

export type MetricsViewServerConfig = typeof metricsViewServerConfig;

export function resolveViewServerRpcUrl(): string {
  const explicitUrl = import.meta.env.VITE_VIEW_SERVER_RPC_URL;
  if (typeof explicitUrl === "string" && explicitUrl.length > 0) {
    return explicitUrl;
  }

  const configuredPath = import.meta.env.VITE_VIEW_SERVER_RPC_PATH;
  const path =
    typeof configuredPath === "string" && configuredPath.length > 0
      ? withLeadingSlash(configuredPath)
      : "/rpc";

  if (typeof window === "undefined") {
    return `ws://127.0.0.1:3000${path}`;
  }

  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}${path}`;
}

function withLeadingSlash(path: string): string {
  return path.startsWith("/") ? path : `/${path}`;
}
