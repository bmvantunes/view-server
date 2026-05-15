#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/docker-compose.production-smoke.yml"
PROJECT_NAME="${VS_DEPLOYMENT_SMOKE_PROJECT:-view-server-production-smoke}"
HOST_PORT="${VIEW_SERVER_DEPLOYMENT_SMOKE_PORT:-3100}"
HTTP_URL="${VS_DEPLOYMENT_SMOKE_HTTP_URL:-http://127.0.0.1:${HOST_PORT}}"
RPC_URL="${VS_DEPLOYMENT_SMOKE_RPC_URL:-ws://127.0.0.1:${HOST_PORT}/rpc}"

cleanup() {
  local status=$?
  if [[ $status -ne 0 ]]; then
    docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" logs --no-color app || true
  fi
  docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down --remove-orphans >/dev/null 2>&1 || true
  exit "$status"
}

trap cleanup EXIT

docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" up --build -d

for attempt in $(seq 1 90); do
  if node -e "fetch('${HTTP_URL}/ready').then((response) => process.exit(response.ok ? 0 : 1)).catch(() => process.exit(1))" >/dev/null 2>&1; then
    break
  fi

  if [[ "$attempt" == "90" ]]; then
    printf 'deployment smoke readiness timed out for %s\n' "$HTTP_URL" >&2
    exit 1
  fi

  sleep 1
done

VS_DEPLOYMENT_SMOKE_HTTP_URL="$HTTP_URL" \
VS_DEPLOYMENT_SMOKE_RPC_URL="$RPC_URL" \
  node --experimental-strip-types "$ROOT_DIR/apps/website/src/deployment-smoke-client.ts"

docker compose -f "$COMPOSE_FILE" -p "$PROJECT_NAME" down --remove-orphans
trap - EXIT
