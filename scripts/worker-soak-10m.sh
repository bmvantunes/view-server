#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"

: "${VS_WORKER_SOAK_ROWS:=10000000}"
: "${VS_WORKER_SOAK_RAW_SUBSCRIPTIONS:=250}"
: "${VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS:=0}"
: "${VS_WORKER_SOAK_MUTATIONS:=10000}"
: "${VS_WORKER_SOAK_RAW_PAGE_CYCLE:=10}"
: "${VS_WORKER_SOAK_GROUPED_DEBOUNCE_MS:=0}"
: "${VS_WORKER_SOAK_ACTIVE_PLAN_AUTO_BUILD_MAX_ROWS:=1000000}"
: "${VS_WORKER_SOAK_TIMEOUT_MS:=7200000}"
: "${VS_WORKER_SOAK_MAX_OLD_SPACE_MB:=24576}"
: "${VS_WORKER_SOAK_SUMMARY_PATH:=/private/tmp/view-server-worker-soak-10m-${TIMESTAMP}.json}"
: "${VS_WORKER_SOAK_PROGRESS_PATH:=${VS_WORKER_SOAK_SUMMARY_PATH}.progress.jsonl}"
: "${VS_WORKER_SOAK_PROGRESS_INTERVAL_MS:=60000}"

export VS_WORKER_SOAK_ROWS
export VS_WORKER_SOAK_RAW_SUBSCRIPTIONS
export VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS
export VS_WORKER_SOAK_MUTATIONS
export VS_WORKER_SOAK_RAW_PAGE_CYCLE
export VS_WORKER_SOAK_GROUPED_DEBOUNCE_MS
export VS_WORKER_SOAK_ACTIVE_PLAN_AUTO_BUILD_MAX_ROWS
export VS_WORKER_SOAK_TIMEOUT_MS
export VS_WORKER_SOAK_SUMMARY_PATH
export VS_WORKER_SOAK_PROGRESS_PATH
export VS_WORKER_SOAK_PROGRESS_INTERVAL_MS
export NODE_OPTIONS="${NODE_OPTIONS:+${NODE_OPTIONS} }--expose-gc --max-old-space-size=${VS_WORKER_SOAK_MAX_OLD_SPACE_MB}"

printf 'view-server 10M raw worker soak profile\n'
printf 'rows=%s raw=%s grouped=%s mutations=%s activePlanAutoBuildMaxRows=%s timeoutMs=%s maxOldSpaceMb=%s\n' \
  "$VS_WORKER_SOAK_ROWS" \
  "$VS_WORKER_SOAK_RAW_SUBSCRIPTIONS" \
  "$VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS" \
  "$VS_WORKER_SOAK_MUTATIONS" \
  "$VS_WORKER_SOAK_ACTIVE_PLAN_AUTO_BUILD_MAX_ROWS" \
  "$VS_WORKER_SOAK_TIMEOUT_MS" \
  "$VS_WORKER_SOAK_MAX_OLD_SPACE_MB"
printf 'summary=%s\n' "$VS_WORKER_SOAK_SUMMARY_PATH"
printf 'progress=%s\n' "$VS_WORKER_SOAK_PROGRESS_PATH"
printf 'note=grouped defaults to 0 because this worker soak uses memory fallback, not production chDB grouped refresh\n'

cd "$ROOT_DIR/packages/core"

node \
  ./node_modules/vitest/vitest.mjs \
  run \
  --config vitest.config.ts \
  tests/worker-soak.test.ts

printf '10M worker soak summary written to %s\n' "$VS_WORKER_SOAK_SUMMARY_PATH"
