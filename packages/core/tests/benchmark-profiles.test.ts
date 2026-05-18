import { describe, expect, it } from "@effect/vitest";
import {
  benchmarkProfileCoverageGapsEnv,
  benchmarkProfileNames,
  benchmarkProfiles,
  getBenchmarkProfile,
  listBenchmarkProfiles,
} from "../bench/benchmark-profiles.ts";

describe("benchmark profiles", () => {
  it("lists every named benchmark profile in registry order", () => {
    expect(benchmarkProfileNames).toEqual([
      "ci-smoke",
      "firehose-ci",
      "websocket-fanout",
      "dev-fast",
      "rc-1m",
      "soak-10m",
      "grouped-heavy",
      "active-plan-startup",
      "chdb-worker-overlap",
    ]);
    expect(listBenchmarkProfiles().map((profile) => profile.name)).toEqual(benchmarkProfileNames);
  });

  it("resolves every named benchmark profile from the CLI lookup", () => {
    for (const name of benchmarkProfileNames) {
      expect(getBenchmarkProfile(name)?.name).toBe(name);
    }
  });

  it("keeps firehose CI thresholds report-only and artifact-backed", () => {
    const profile = benchmarkProfiles["firehose-ci"];
    expect(profile.ciSafe).toBe(true);
    expect(profile.benchmarks.map((benchmark) => benchmark.name)).toEqual([
      "worker-mutation-batch",
      "chdb-apply-batch",
      "fanout-slow-client",
      "worker-soak-alpha-1m",
      "runtime-websocket-soak-100-client",
    ]);
    for (const benchmark of profile.benchmarks) {
      expect(benchmark.blocking).toBe(false);
      expect(benchmark.script).toMatch(/^bench\/.*\.ts$/);
      expect(benchmark.artifactFile).toMatch(/\.json$/);
      expect(benchmark.baselineFile).toMatch(/\.json$/);
      expect(benchmark.metrics).toBeDefined();
      expect(Object.keys(benchmark.env).length).toBeGreaterThan(0);
    }
  });

  it("keeps CI smoke parameters centralized and artifact-backed", () => {
    const profile = benchmarkProfiles["ci-smoke"];
    expect(profile.ciSafe).toBe(true);
    expect(profile.benchmarks.map((benchmark) => benchmark.name)).toEqual([
      "active-view",
      "active-plan-responsiveness",
      "grouped-responsiveness",
      "grouped-refresh-overlap",
      "runtime-websocket-soak",
    ]);
    for (const benchmark of profile.benchmarks) {
      expect(benchmark.script).toMatch(/^bench\/.*\.ts$/);
      expect(benchmark.artifactFile).toMatch(/\.json$/);
      expect(benchmark.baselineFile).toMatch(/\.json$/);
      expect(Object.keys(benchmark.env).length).toBeGreaterThan(0);
    }
  });

  it("documents profile coverage gaps for artifact summaries", () => {
    const profile = benchmarkProfiles["ci-smoke"];
    const gaps = benchmarkProfileCoverageGapsEnv(profile);
    expect(gaps).toContain("Active-plan responsiveness");
    expect(gaps.split("\n").length).toBe(profile.coverageGaps.length);
  });

  it("returns undefined for unknown profiles", () => {
    expect(getBenchmarkProfile("not-a-profile")).toBeUndefined();
  });
});
