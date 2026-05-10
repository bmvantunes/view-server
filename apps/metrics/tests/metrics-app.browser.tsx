import React from "react";
import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, inject, test } from "vite-plus/test";
import { MetricsApp } from "../src/components/MetricsApp";

const roots: Root[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) {
    root.unmount();
  }
  document.body.innerHTML = "";
});

describe("metrics app", () => {
  test("renders health rows from the Effect RPC websocket", async () => {
    render(<MetricsApp rpcUrl={inject("viewServerWsUrl")} />);

    await expectText("View Server Metrics");
    await expectText("orders");
    expect(document.body.textContent).toContain("ready");
  });
});

function render(element: React.ReactNode): void {
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() => root.render(element));
}

async function expectText(text: string): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    if (document.body.textContent?.includes(text)) {
      expect(document.body.textContent).toContain(text);
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  expect(document.body.textContent).toContain(text);
}
