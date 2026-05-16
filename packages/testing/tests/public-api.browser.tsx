import { describe, expect, test } from "vite-plus/test";
import {
  createTestingViewServerReact,
  inMemoryViewServer,
  isolatedInMemoryViewServer,
  makeTestingBrowserWebsocketClient,
} from "@view-server/testing";

describe("testing public package API", () => {
  test("imports testing helpers from the package root", () => {
    expect(typeof inMemoryViewServer).toBe("function");
    expect(typeof isolatedInMemoryViewServer).toBe("function");
    expect(typeof makeTestingBrowserWebsocketClient).toBe("function");
    expect(typeof createTestingViewServerReact).toBe("function");
  });
});
