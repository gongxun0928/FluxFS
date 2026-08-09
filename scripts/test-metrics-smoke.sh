#!/usr/bin/env bash
# Smoke: MetaMaster --metrics-listen serves Prometheus text after one RPC.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

TMP="$(mktemp -d /tmp/fluxfs-metrics-smoke.XXXXXX)"
META_DIR="$TMP/meta"
LISTEN="127.0.0.1:55061"
METRICS="127.0.0.1:9101"
cleanup() {
  if [[ -n "${META_PID:-}" ]] && kill -0 "$META_PID" 2>/dev/null; then
    kill "$META_PID" 2>/dev/null || true
    wait "$META_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

cargo build -q -p fluxfs-metamaster -p fluxfs
./target/debug/fluxfs-metamaster \
  --data-dir "$META_DIR" \
  --listen "$LISTEN" \
  --metrics-listen "$METRICS" \
  --allow-insecure-dev &
META_PID=$!
sleep 0.5
./target/debug/fluxfs meta-ping --addr "$LISTEN" --allow-insecure-dev

BODY="$(curl -fsS "http://${METRICS}/metrics")"
echo "$BODY" | grep -E -q 'fluxfs_meta_rpc_total [1-9]'
echo "$BODY" | grep -q 'fluxfs_meta_rpc_latency_ms_bucket'
echo "$BODY" | grep -q 'fluxfs_gc_tombstone_pending'
echo "$BODY" | grep -q 'fluxfs_flush_intent_pending'
echo "$BODY" | grep -q 'fluxfs_worker_available_bytes_min'
echo "$BODY" | grep -q 'fluxfs_repair_pass_total'
echo "metrics smoke: ok"
