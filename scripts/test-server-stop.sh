#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${VS_TEST_SERVER_PID_FILE:-$ROOT_DIR/.view-server-test-server.pid}"

if [[ ! -f "$PID_FILE" ]]; then
  printf 'view-server test server is not running\n'
  exit 0
fi

server_pid="$(cat "$PID_FILE")"
if ! kill -0 "$server_pid" >/dev/null 2>&1; then
  rm -f "$PID_FILE"
  printf 'view-server test server pid file was stale\n'
  exit 0
fi

kill "$server_pid"

for _ in $(seq 1 30); do
  if ! kill -0 "$server_pid" >/dev/null 2>&1; then
    rm -f "$PID_FILE"
    printf 'view-server test server stopped pid=%s\n' "$server_pid"
    exit 0
  fi
  sleep 1
done

printf 'view-server test server did not stop gracefully; sending SIGKILL pid=%s\n' "$server_pid" >&2
kill -9 "$server_pid" >/dev/null 2>&1 || true
rm -f "$PID_FILE"
