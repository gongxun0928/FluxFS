#!/usr/bin/env bash
# Smoke: start MetaMaster, ping + create via remote mount path (meta only).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TMP="$(mktemp -d /tmp/fluxfs-meta-rpc.XXXXXX)"
META_DIR="$TMP/meta"
LISTEN="127.0.0.1:55051"
cleanup() {
  if [[ -n "${META_PID:-}" ]] && kill -0 "$META_PID" 2>/dev/null; then
    kill "$META_PID" 2>/dev/null || true
    wait "$META_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

cargo build -q -p fluxfs-metamaster -p fluxfs
./target/debug/fluxfs-metamaster --data-dir "$META_DIR" --listen "$LISTEN" \
    --allow-insecure-dev &
META_PID=$!
sleep 0.5
./target/debug/fluxfs meta-ping --addr "$LISTEN" --allow-insecure-dev
echo "meta rpc: ok"
