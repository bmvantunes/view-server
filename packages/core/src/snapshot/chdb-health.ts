import * as Schema from "effect/Schema";
import type { WorkerVersion } from "../worker/mutation-log.ts";
import type { SnapshotBackendHealth } from "./snapshot-backend.ts";

export const ChdbHealthSchema = Schema.Struct({
  status: Schema.Literals(["ready", "degraded", "restarting", "stopped"]),
  pid: Schema.Number,
  restarts: Schema.Number,
  pendingRequests: Schema.Number,
  lastError: Schema.String,
  backendVersion: Schema.BigInt,
});

export type ChdbHealth = typeof ChdbHealthSchema.Type;

export function chdbHealthFromSnapshotBackendHealth(health: SnapshotBackendHealth): ChdbHealth {
  return {
    status: health.status,
    pid: health.pid ?? 0,
    restarts: health.restarts ?? 0,
    pendingRequests: health.pendingRequests ?? 0,
    lastError: health.lastError ?? health.message ?? "",
    backendVersion: health.backendVersion ?? 0n,
  };
}

export function chdbHealthBackendVersion(health: ChdbHealth): WorkerVersion {
  return health.backendVersion;
}
