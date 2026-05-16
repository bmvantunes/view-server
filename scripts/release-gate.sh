#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCOPE="${VS_RELEASE_GATE_SCOPE:-local}"
DRY_RUN="${VS_RELEASE_GATE_DRY_RUN:-0}"
INCLUDE_SOAK="${VS_RELEASE_GATE_INCLUDE_SOAK:-0}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
SUMMARY_PATH="${VS_RELEASE_GATE_SUMMARY_PATH:-/private/tmp/view-server-release-gate-${TIMESTAMP}.md}"

mkdir -p "$(dirname "$SUMMARY_PATH")"

{
  printf '# View Server Release Gate\n\n'
  printf -- '- scope: `%s`\n' "$SCOPE"
  printf -- '- dryRun: `%s`\n' "$DRY_RUN"
  printf -- '- startedAt: `%s`\n\n' "$TIMESTAMP"
  printf '| status | step | duration | command |\n'
  printf '| --- | --- | ---: | --- |\n'
} >"$SUMMARY_PATH"

run_step() {
  local name="$1"
  shift
  local started
  local finished
  local duration
  local command
  started="$(date +%s)"
  command="$*"

  printf 'release-gate: %s\n' "$name"

  if [[ "$DRY_RUN" == "1" ]]; then
    printf '| skipped | %s | 0s | `%s` |\n' "$name" "$command" >>"$SUMMARY_PATH"
    return 0
  fi

  if "$@"; then
    finished="$(date +%s)"
    duration="$((finished - started))s"
    printf '| pass | %s | %s | `%s` |\n' "$name" "$duration" "$command" >>"$SUMMARY_PATH"
    return 0
  fi

  finished="$(date +%s)"
  duration="$((finished - started))s"
  printf '| fail | %s | %s | `%s` |\n' "$name" "$duration" "$command" >>"$SUMMARY_PATH"
  printf 'release-gate failed at step "%s"\nsummary: %s\n' "$name" "$SUMMARY_PATH" >&2
  return 1
}

policy_scan() {
  local cast_pattern="as (never|any|unk""nown)"
  local console_pattern="con""sole\\."
  local node_assert_pattern="node:""assert"
  local node_test_pattern="node:""test"
  local vitest_import_pattern="from ['\\\"]vite""st['\\\"]"
  local pattern="${cast_pattern}|${console_pattern}|${node_assert_pattern}|${node_test_pattern}|${vitest_import_pattern}"
  local matches
  matches="$(
    rg -n "$pattern" packages apps scripts || true
  )"
  matches="$(
    printf '%s\n' "$matches" \
      | sed '/apps\/metrics\/src\/routeTree\.gen\.ts/d' \
      | sed '/^$/d'
  )"
  if [[ -n "$matches" ]]; then
    printf '%s\n' "$matches" >&2
    return 1
  fi
}

benchmark_artifacts_summary() {
  if [[ -d packages/core/bench/.artifacts/ci ]]; then
    {
      printf '\n## Benchmark Artifacts\n\n'
      find packages/core/bench/.artifacts/ci -maxdepth 1 -name '*.json' -print | sort | sed 's#^#- `#; s#$#`#'
    } >>"$SUMMARY_PATH"
  fi
}

cd "$ROOT_DIR"

case "$SCOPE" in
  ci|local|full) ;;
  *)
    printf 'Unknown VS_RELEASE_GATE_SCOPE=%s. Use ci, local, or full.\n' "$SCOPE" >&2
    exit 1
    ;;
esac

run_step "check" vp check
run_step "effect-lsp" pnpm exec effect-language-service diagnostics --project packages/core/tsconfig.json --format text --severity error

if [[ "$SCOPE" == "ci" ]]; then
  run_step "focused package audit" pnpm --dir packages/core exec vitest run --config vitest.config.ts tests/package-audit.test.ts tests/public-api-smoke.test.ts tests/public-api-types.test.ts
else
  run_step "tests" vp run -r test
fi

run_step "build" vp run -r build

if [[ "$SCOPE" != "ci" ]]; then
  run_step "pack dry run" pnpm run pack:dry-run
fi

run_step "benchmark smoke" env VS_BENCH_BLOCKING=0 VS_BENCH_REGRESSION_MIN_DELTA_MS=5 vp run core#bench:compare
benchmark_artifacts_summary
run_step "policy scan" policy_scan

if [[ "$SCOPE" == "full" ]]; then
  run_step "external consumer smoke" pnpm run smoke:consumer
  run_step "deployment smoke" pnpm run smoke:deployment
fi

if [[ "$INCLUDE_SOAK" == "1" ]]; then
  run_step "1m worker soak" env VS_WORKER_SOAK_ROWS=1000000 VS_WORKER_SOAK_RAW_SUBSCRIPTIONS=250 VS_WORKER_SOAK_GROUPED_SUBSCRIPTIONS=20 VS_WORKER_SOAK_MUTATIONS=10000 VS_WORKER_SOAK_TIMEOUT_MS=900000 vp run core#test -- tests/worker-soak.test.ts
fi

printf '\nrelease-gate passed\nsummary: %s\n' "$SUMMARY_PATH"
