import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { readdirSync, readFileSync } from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const workspaceRoot = fileURLToPath(new URL("../../../", import.meta.url));

const forbiddenImports = new Set([
  "@effect/platform-node",
  "@platformatic/kafka",
  "@view-server/core",
  "chdb",
  "child_process",
  "fs",
  "fs/promises",
  "net",
  "node:child_process",
  "node:fs",
  "node:fs/promises",
  "node:net",
  "node:worker_threads",
  "worker_threads",
]);

const browserImportScopes = [
  {
    label: "react package",
    files: collectSourceFiles("packages/react/src"),
  },
  {
    label: "testing browser helpers",
    files: [
      "packages/testing/src/testing-isolation.ts",
      "packages/testing/src/real-server-harness.ts",
    ],
  },
  {
    label: "orders demo browser app",
    files: collectSourceFiles("apps/website/src").filter(
      (path) =>
        path !== "apps/website/src/server.ts" &&
        path !== "apps/website/src/deployment-smoke-client.ts",
    ),
  },
] as const;

describe("browser forbidden import guard", () => {
  for (const scope of browserImportScopes) {
    it.effect(`${scope.label} stays free of server-only imports`, () =>
      Effect.sync(() => {
        const violations = findForbiddenImports(scope.files);
        expect(violations).toEqual([]);
      }),
    );
  }
});

function collectSourceFiles(root: string): readonly string[] {
  const absoluteRoot = join(workspaceRoot, root);
  const files: string[] = [];
  const visit = (directory: string): void => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const absolutePath = join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(absolutePath);
      } else if (/\.(ts|tsx)$/.test(entry.name)) {
        files.push(relative(workspaceRoot, absolutePath));
      }
    }
  };
  visit(absoluteRoot);
  return files.sort();
}

function findForbiddenImports(files: readonly string[]): readonly string[] {
  const violations: string[] = [];
  for (const file of files) {
    const source = readFileSync(join(workspaceRoot, file), "utf8");
    for (const moduleName of importedModules(source)) {
      if (isForbiddenImport(moduleName)) {
        violations.push(`${file} imports ${moduleName}`);
      }
    }
  }
  return violations;
}

function importedModules(source: string): readonly string[] {
  const modules: string[] = [];
  const importPattern =
    /(?:import|export)\s+(?:type\s+)?(?:[^"'()]*?\s+from\s+)?["']([^"']+)["']|import\(\s*["']([^"']+)["']\s*\)/g;
  for (const match of source.matchAll(importPattern)) {
    const moduleName = match[1] ?? match[2];
    if (moduleName !== undefined) {
      modules.push(moduleName);
    }
  }
  return modules;
}

function isForbiddenImport(moduleName: string): boolean {
  if (forbiddenImports.has(moduleName)) {
    return true;
  }
  if (moduleName.startsWith("@effect/platform-node")) {
    return true;
  }
  return moduleName.startsWith("@view-server/core/worker");
}
