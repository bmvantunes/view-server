import { flushSync } from "react-dom";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, describe, expect, inject, test } from "vite-plus/test";
import { App } from "../src/App.tsx";

const roots: Root[] = [];

afterEach(() => {
  for (const root of roots.splice(0)) {
    root.unmount();
  }
  document.body.innerHTML = "";
  document.body.style.width = "";
});

describe("orders demo browser contract", () => {
  test("renders live raw and grouped views over the real websocket on desktop", async () => {
    renderDemo("1200px");

    await expectText("Orders Live View");
    await expectText("Open order window");
    await expectText("Grouped desk metrics");
    await expectText("order-");
    await expectText("groups");
  });

  test("renders the same real websocket data in a mobile-width container", async () => {
    renderDemo("390px");

    await expectText("Orders Live View");
    await expectText("Open order window");
    await expectText("Grouped desk metrics");
    await expectText("order-");
  });
});

function renderDemo(width: string): void {
  document.body.style.width = width;
  const host = document.createElement("div");
  document.body.append(host);
  const root = createRoot(host);
  roots.push(root);
  flushSync(() => root.render(<App rpcUrl={inject("ordersDemoWsUrl")} />));
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
