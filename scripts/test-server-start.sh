#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${VS_TEST_SERVER_PID_FILE:-$ROOT_DIR/.view-server-test-server.pid}"
LOG_FILE="${VS_TEST_SERVER_LOG_FILE:-$ROOT_DIR/.view-server-test-server.log}"
HOST="${VIEW_SERVER_HOST:-127.0.0.1}"
PORT="${VIEW_SERVER_PORT:-3100}"
READY_URL="http://${HOST}:${PORT}/ready"

if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE")"
  if kill -0 "$existing_pid" >/dev/null 2>&1; then
    printf 'view-server test server already running pid=%s url=ws://%s:%s/rpc\n' "$existing_pid" "$HOST" "$PORT"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

nohup bash -c '
  cd "$1"
  exec env \
    VIEW_SERVER_HOST="$2" \
    VIEW_SERVER_PORT="$3" \
    VIEW_SERVER_DEMO_ROWS="$4" \
    VIEW_SERVER_DEMO_PUBLISH_INTERVAL_MS="$5" \
    pnpm --filter orders-demo run server
' bash \
  "$ROOT_DIR" \
  "$HOST" \
  "$PORT" \
  "${VIEW_SERVER_DEMO_ROWS:-256}" \
  "${VIEW_SERVER_DEMO_PUBLISH_INTERVAL_MS:-250}" \
  >"$LOG_FILE" 2>&1 &

server_pid=$!
printf '%s\n' "$server_pid" >"$PID_FILE"

for attempt in $(seq 1 90); do
  if curl -sS "$READY_URL" >/dev/null 2>&1; then
    printf 'view-server test server ready pid=%s url=ws://%s:%s/rpc log=%s\n' "$server_pid" "$HOST" "$PORT" "$LOG_FILE"
    exit 0
  fi

  if ! kill -0 "$server_pid" >/dev/null 2>&1; then
    printf 'view-server test server exited before readiness; log follows:\n' >&2
    cat "$LOG_FILE" >&2
    rm -f "$PID_FILE"
    exit 1
  fi

  if [[ "$attempt" == "90" ]]; then
    printf 'view-server test server readiness timed out for %s; log follows:\n' "$READY_URL" >&2
    cat "$LOG_FILE" >&2
    rm -f "$PID_FILE"
    exit 1
  fi

  sleep 1
done
