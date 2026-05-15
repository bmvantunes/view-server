import { describe, expect, test } from "vite-plus/test";
import {
  createViewServerHooks,
  createViewServerReact,
  layerBrowserWebsocketRpcClient,
  makeBrowserWebsocketClient,
  ViewServerMetricsDashboard,
  viewServerHealthQuery,
} from "@view-server/react";

describe("react public package API", () => {
  test("imports hooks, provider factories, browser client, and metrics UI from the package root", () => {
    expect(typeof createViewServerHooks).toBe("function");
    expect(typeof createViewServerReact).toBe("function");
    expect(typeof layerBrowserWebsocketRpcClient).toBe("function");
    expect(typeof makeBrowserWebsocketClient).toBe("function");
    expect(typeof ViewServerMetricsDashboard).toBe("function");
    expect(viewServerHealthQuery.limit).toBe(50);
  });
});
