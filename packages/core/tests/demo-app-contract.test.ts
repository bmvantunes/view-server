import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const workspaceRoot = fileURLToPath(new URL("../../../", import.meta.url));

describe("orders demo contract", () => {
  it.effect("uses public package APIs and the AsyncResult live-query surface", () =>
    Effect.gen(function* () {
      const appSource = yield* readSource("apps/website/src/App.tsx");
      const configSource = yield* readSource("apps/website/src/view-server.ts");
      const serverSource = yield* readSource("apps/website/src/server.ts");
      const combined = `${appSource}\n${configSource}\n${serverSource}`;

      expect(configSource).toContain('from "@view-server/core/config"');
      expect(configSource).toContain('from "@view-server/core/query"');
      expect(appSource).toContain('from "@view-server/react"');
      expect(appSource).toContain("createViewServerReact");
      expect(appSource).toContain("useLiveQuery");
      expect(appSource).toContain("AsyncResult.match");
      expect(serverSource).toContain('from "@view-server/core/runtime"');
      expect(serverSource).toContain('from "@view-server/core/rpc/websocket"');
      expect(combined).not.toMatch(/packages\/|src\/worker|src\/snapshot|topic-worker|chdb/);
      expect(combined).not.toMatch(/useSubscription|useOrders|generated hooks/i);
    }),
  );

  it.effect("keeps the browser demo source free of node-only server dependencies", () =>
    Effect.gen(function* () {
      const browserSource = yield* readSource("apps/website/src/App.tsx");
      expect(browserSource).not.toMatch(
        /node:worker_threads|node:child_process|node:fs|node:net|@platformatic\/kafka|from ["']chdb["']/,
      );
    }),
  );
});

function readSource(path: string) {
  return Effect.sync(() => readFileSync(join(workspaceRoot, path), "utf8"));
}
