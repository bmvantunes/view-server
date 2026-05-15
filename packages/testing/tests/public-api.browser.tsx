import { describe, expect, test } from "vite-plus/test";
import { inMemoryViewServer } from "@view-server/testing";

describe("testing public package API", () => {
  test("imports inMemoryViewServer from the package root", () => {
    expect(typeof inMemoryViewServer).toBe("function");
  });
});
