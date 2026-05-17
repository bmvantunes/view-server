import { describe, expect, test } from "vite-plus/test";
import {
  createTestingViewServerReact,
  makeTestingBrowserWebsocketClient,
  readyUrlForRpcUrl,
  realViewServerTestHarness,
} from "@view-server/testing";

describe("testing public package API", () => {
  test("imports testing helpers from the package root", () => {
    expect(typeof makeTestingBrowserWebsocketClient).toBe("function");
    expect(typeof createTestingViewServerReact).toBe("function");
    expect(typeof realViewServerTestHarness).toBe("function");
    expect(typeof readyUrlForRpcUrl).toBe("function");
  });
});
