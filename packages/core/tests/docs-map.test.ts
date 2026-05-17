import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const workspaceRoot = fileURLToPath(new URL("../../../", import.meta.url));

const authoritativeDocs = [
  "docs/architecture.md",
  "docs/quickstart.md",
  "docs/testing.md",
  "docs/production-readiness.md",
  "docs/fault-tolerance.md",
  "docs/query-semantics.md",
  "docs/benchmarks.md",
  "docs/api-audit.md",
] as const;

describe("documentation source map", () => {
  it.effect("README points to every authoritative implementation doc", () =>
    Effect.gen(function* () {
      const readme = yield* readSource("README.md");

      for (const path of authoritativeDocs) {
        expect(readme).toContain(path);
        expect(existsSync(join(workspaceRoot, path))).toBe(true);
      }
      expect(readme).toContain("plan.md");
      expect(readme).toContain("historical implementation plan");
    }),
  );

  it.effect(
    "authoritative docs have headings and do not point at missing local markdown files",
    () =>
      Effect.gen(function* () {
        const allDocs = ["README.md", "CONTEXT.md", ...authoritativeDocs];

        for (const path of allDocs) {
          const source = yield* readSource(path);
          expect(source.trimStart().startsWith("#")).toBe(true);
          for (const link of markdownLinks(source)) {
            if (link.startsWith("http") || link.startsWith("#")) {
              continue;
            }
            if (!link.endsWith(".md")) {
              continue;
            }
            expect(existsSync(join(workspaceRoot, link))).toBe(true);
          }
        }
      }),
  );
});

function readSource(path: string) {
  return Effect.sync(() => readFileSync(join(workspaceRoot, path), "utf8"));
}

function markdownLinks(source: string): readonly string[] {
  return Array.from(source.matchAll(/\[[^\]]+\]\(([^)]+)\)/g), (match) => match[1] ?? "");
}
