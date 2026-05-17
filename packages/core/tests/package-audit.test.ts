import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const workspaceRoot = fileURLToPath(new URL("../../../", import.meta.url));

const PackageJson = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  type: Schema.Literals(["module"]),
  files: Schema.Array(Schema.String),
  sideEffects: Schema.Boolean,
  exports: Schema.Record(Schema.String, Schema.Unknown),
  peerDependencies: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  peerDependenciesMeta: Schema.optional(
    Schema.Record(
      Schema.String,
      Schema.Struct({
        optional: Schema.Boolean,
      }),
    ),
  ),
  engines: Schema.Struct({
    node: Schema.String,
  }),
  private: Schema.optional(Schema.Boolean),
});

const PublicPackage = Schema.Struct({
  name: Schema.String,
  path: Schema.String,
  requiredExports: Schema.Array(Schema.String),
  requiredPeerDependencies: Schema.Array(Schema.String),
});

const publicPackages = [
  {
    name: "@view-server/core",
    path: "packages/core/package.json",
    requiredExports: [
      ".",
      "./client",
      "./config",
      "./errors",
      "./kafka",
      "./kafka/platformatic",
      "./query",
      "./rpc",
      "./rpc/websocket",
      "./runtime",
      "./snapshot",
      "./snapshot/chdb",
      "./worker/node",
      "./package.json",
    ],
    requiredPeerDependencies: ["effect"],
  },
  {
    name: "@view-server/react",
    path: "packages/react/package.json",
    requiredExports: [".", "./package.json"],
    requiredPeerDependencies: ["@view-server/core", "effect", "react"],
  },
  {
    name: "@view-server/testing",
    path: "packages/testing/package.json",
    requiredExports: [".", "./package.json"],
    requiredPeerDependencies: ["@view-server/core", "@view-server/react", "effect", "react"],
  },
] satisfies readonly (typeof PublicPackage.Type)[];

describe("release package audit", () => {
  for (const packageInfo of publicPackages) {
    it.effect(`${packageInfo.name} declares the release package metadata`, () =>
      Effect.gen(function* () {
        const packageJson = yield* readPackageJson(packageInfo.path);
        expect(packageJson.name).toBe(packageInfo.name);
        expect(packageJson.version).toMatch(/^\d+\.\d+\.\d+/);
        expect(packageJson.type).toBe("module");
        expect(packageJson.files).toEqual(expect.arrayContaining(["dist", "src"]));
        expect(packageJson.sideEffects).toBe(false);
        expect(packageJson.engines.node).toBe(">=26.0.0");
        expect(packageJson.private).toBeUndefined();
        expect(Object.keys(packageJson.exports)).toEqual(
          expect.arrayContaining(packageInfo.requiredExports),
        );
        expect(JSON.stringify(packageJson.exports)).toContain("./dist/");
        expect(JSON.stringify(packageJson.exports)).not.toContain("./src/");

        for (const dependency of packageInfo.requiredPeerDependencies) {
          expect(packageJson.peerDependencies?.[dependency]).toBeDefined();
        }
      }),
    );
  }

  it.effect("keeps worker and implementation internals out of root public exports", () =>
    Effect.gen(function* () {
      const corePackage = yield* readPackageJson("packages/core/package.json");
      expect(Object.keys(corePackage.exports)).not.toContain("./worker");
      expect(Object.keys(corePackage.exports)).not.toContain("./worker/core");
      expect(Object.keys(corePackage.exports)).not.toContain("./internal/testing");
      expect(Object.keys(corePackage.exports)).not.toContain("./snapshot/chdb-query-worker-entry");
      expect(Object.keys(corePackage.exports)).not.toContain("./snapshot/snapshot-backend");
      expect(Object.keys(corePackage.exports)).not.toContain("./testing");
    }),
  );

  it.effect("keeps memory snapshot backend behind the internal testing seam", () =>
    Effect.gen(function* () {
      const coreSnapshotEntry = yield* readSources(["packages/core/src/snapshot.ts"]);
      const internalSnapshotBarrel = yield* readSources(["packages/core/src/snapshot/index.ts"]);
      const nodeWorkerHost = yield* readSources([
        "packages/core/src/worker/topic-worker-node-host.ts",
      ]);
      const runtimePublicEntry = yield* readSources(["packages/core/src/runtime.ts"]);
      const testingPublicEntry = yield* readSources(["packages/testing/src/index.ts"]);

      expect(coreSnapshotEntry).not.toContain("createMemorySnapshotBackend");
      expect(internalSnapshotBarrel).not.toContain("createMemorySnapshotBackend");
      expect(nodeWorkerHost).not.toContain("TopicWorkerSnapshotBackendMode");
      expect(nodeWorkerHost).not.toContain("snapshotBackend?");
      expect(runtimePublicEntry).not.toContain("makeInternalTestingViewServerRuntime");
      expect(testingPublicEntry).not.toContain("inMemoryViewServer");
      expect(testingPublicEntry).not.toContain("@view-server/core/internal/testing");
    }),
  );

  it.effect("keeps the public chDB adapter on the per-topic worker path", () =>
    Effect.gen(function* () {
      const corePackage = yield* readPackageJson("packages/core/package.json");
      const publicChdbEntry = yield* readSources(["packages/core/src/snapshot/chdb-backend.ts"]);

      expect(Object.keys(corePackage.exports)).not.toContain("./snapshot/chdb-in-process-backend");
      expect(publicChdbEntry).not.toContain("createInProcessChdbSnapshotBackend");
      expect(publicChdbEntry).not.toContain("groupedRefreshWorker?: boolean");
      expect(publicChdbEntry).not.toContain("new Session()");
      expect(publicChdbEntry).not.toContain("sharedSession");
    }),
  );

  it.effect("keeps chDB process supervision in the supervision module", () =>
    Effect.gen(function* () {
      const publicChdbEntry = yield* readSources(["packages/core/src/snapshot/chdb-backend.ts"]);
      const supervisorEntry = yield* readSources([
        "packages/core/src/snapshot/chdb-worker-supervisor.ts",
      ]);

      expect(publicChdbEntry).not.toContain("ChdbProcessClient");
      expect(publicChdbEntry).not.toContain("encodeRuntimeQuery");
      expect(publicChdbEntry).not.toContain("restartWorkerOnUnexpectedExit !== true");
      expect(supervisorEntry).toContain("ChdbProcessClient");
      expect(supervisorEntry).toContain("restartWorkerOnUnexpectedExit !== true");
      expect(supervisorEntry).toContain("pendingRequests");
      expect(supervisorEntry).toContain("SnapshotBackendHealth");
    }),
  );

  it.effect("requires chDB for production while keeping unrelated integrations optional", () =>
    Effect.gen(function* () {
      const corePackage = yield* readPackageJson("packages/core/package.json");
      expect(corePackage.peerDependencies?.["@effect/platform-node"]).toBe("4.0.0-beta.65");
      expect(corePackage.peerDependencies?.chdb).toBe("1.6.0");
      expect(corePackage.peerDependencies?.["@platformatic/kafka"]).toBe("2.0.1");
      expect(corePackage.peerDependenciesMeta?.["@effect/platform-node"]?.optional).toBe(true);
      expect(corePackage.peerDependenciesMeta?.chdb).toBeUndefined();
      expect(corePackage.peerDependenciesMeta?.["@platformatic/kafka"]?.optional).toBe(true);
    }),
  );

  it.effect("keeps browser-facing packages free of node-only imports", () =>
    Effect.gen(function* () {
      const reactSources = yield* readSources([
        "packages/react/src/index.ts",
        "packages/react/src/metrics-ui.tsx",
        "packages/testing/src/index.ts",
        "packages/testing/src/real-server-harness.ts",
        "packages/testing/src/testing-isolation.ts",
      ]);
      expect(reactSources).not.toMatch(/node:worker_threads|node:fs|node:net/);
      expect(reactSources).not.toMatch(/from ["']chdb["']|@platformatic\/kafka/);
    }),
  );
});

function readPackageJson(path: string) {
  return Effect.sync(() => {
    const source = readFileSync(join(workspaceRoot, path), "utf8");
    return Schema.decodeUnknownSync(PackageJson)(JSON.parse(source));
  });
}

function readSources(paths: readonly string[]) {
  return Effect.sync(() => {
    const sources: string[] = [];
    for (const path of paths) {
      sources.push(readFileSync(join(workspaceRoot, path), "utf8"));
    }
    return sources.join("\n");
  });
}
