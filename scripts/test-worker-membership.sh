#!/usr/bin/env bash
set -euo pipefail

if [[ ! -c /dev/fuse ]] || ! command -v fusermount3 >/dev/null 2>&1; then
    echo "SKIP: /dev/fuse or fusermount3 unavailable" >&2
    exit 77
fi

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
count_chunks() { "$repo_dir/scripts/count-pack-chunks.sh" "$1"; }
test_root=$(mktemp -d -t fluxfs-membership.XXXXXX)
mount_dir="$test_root/mnt"
base_port=$((32000 + ($$ % 16000)))
meta_port=$base_port
worker1_port=$((base_port + 1))
worker2_port=$((base_port + 2))
worker3_port=$((base_port + 3))
worker4_port=$((base_port + 4))
meta_pid=""; worker1_pid=""; worker2_pid=""; worker3_pid=""; worker4_pid=""; mount_pid=""

cleanup() {
    fusermount3 -u "$mount_dir" 2>/dev/null || true
    for pid in "$mount_pid" "$meta_pid" "$worker1_pid" "$worker2_pid" "$worker3_pid" "$worker4_pid"; do
        [[ -z "$pid" ]] || kill "$pid" 2>/dev/null || true
        [[ -z "$pid" ]] || wait "$pid" 2>/dev/null || true
    done
    rm -rf -- "$test_root"
}
trap cleanup EXIT

wait_port() {
    local port=$1
    for _ in $(seq 1 100); do
        timeout 0.1 bash -c "</dev/tcp/127.0.0.1/$port" 2>/dev/null && return 0
        sleep 0.05
    done
    return 1
}

start_meta() {
    "$repo_dir/target/debug/fluxfs-metamaster" --listen "127.0.0.1:$meta_port" \
        --data-dir "$test_root/meta" --allow-insecure-dev >"$test_root/meta.log" 2>&1 &
    meta_pid=$!
    wait_port "$meta_port"
}

start_worker() {
    local id=$1 port=$2 domain=$3 dir=$4 log=$5
    "$repo_dir/target/debug/fluxfs-chunkworker" --worker-id "$id" \
        --listen "127.0.0.1:$port" --advertise-endpoint "http://127.0.0.1:$port" \
        --failure-domain "$domain" --capacity-bytes 1073741824 \
        --heartbeat-interval-secs 1 --lease-secs 4 \
        --meta-addr "http://127.0.0.1:$meta_port" --data-dir "$dir" \
        --allow-insecure-dev >"$log" 2>&1 &
    LAST_PID=$!
    wait_port "$port"
}

start_mount() {
    "$repo_dir/target/debug/fluxfs" mount --no-ufs --data-dir "$test_root/client" \
        --mountpoint "$mount_dir" --meta-addr "http://127.0.0.1:$meta_port" \
        --allow-insecure-dev \
        >"$test_root/mount.log" 2>&1 &
    mount_pid=$!
    for _ in $(seq 1 100); do
        mountpoint -q "$mount_dir" && return 0
        if ! kill -0 "$mount_pid" 2>/dev/null; then
            cat "$test_root/mount.log" >&2
            wait "$mount_pid"
        fi
        sleep 0.05
    done
    return 1
}

mkdir -p "$mount_dir"
cargo build --manifest-path "$repo_dir/Cargo.toml" -p fluxfs -p fluxfs-metamaster -p fluxfs-chunkworker
start_meta
start_worker 11 "$worker1_port" rack-a "$test_root/worker-11" "$test_root/worker-11.log"; worker1_pid=$LAST_PID
start_worker 22 "$worker2_port" rack-b "$test_root/worker-22" "$test_root/worker-22.log"; worker2_pid=$LAST_PID
start_worker 33 "$worker3_port" rack-c "$test_root/worker-33" "$test_root/worker-33.log"; worker3_pid=$LAST_PID
start_mount

dd if=/dev/urandom of="$test_root/expected" bs=1M count=9 status=none
cp "$test_root/expected" "$mount_dir/membership.bin"
cmp "$test_root/expected" "$mount_dir/membership.bin"
total=$(($(count_chunks "$test_root/worker-11") + $(count_chunks "$test_root/worker-22") + $(count_chunks "$test_root/worker-33")))
test "$total" -ge 6

# Lose one topology member. After its lease expires, reads remain available and
# the membership-refreshed client repairs onto the two live failure domains.
kill "$worker1_pid"; wait "$worker1_pid" 2>/dev/null || true; worker1_pid=""
sleep 5
cmp "$test_root/expected" "$mount_dir/membership.bin"

# Add a stable new ID at a new endpoint; no remount or endpoint flag is used.
start_worker 44 "$worker4_port" rack-d "$test_root/worker-44" "$test_root/worker-44.log"; worker4_pid=$LAST_PID
sleep 2
for i in $(seq 1 32); do printf 'topology-%04d\n' "$i" >"$mount_dir/new-$i"; done
test "$(count_chunks "$test_root/worker-44")" -ge 1

# Membership is in the Raft state machine/snapshot-backed Heed data, so a Meta
# restart and discovery-only remount retain stable IDs/endpoints.
kill "$meta_pid"; wait "$meta_pid" 2>/dev/null || true; meta_pid=""
start_meta
fusermount3 -u "$mount_dir"; wait "$mount_pid"; mount_pid=""
start_mount
cmp "$test_root/expected" "$mount_dir/membership.bin"
test "$(cat "$mount_dir/new-32")" = "topology-0032"
fusermount3 -u "$mount_dir"; wait "$mount_pid"; mount_pid=""

echo "dynamic Worker membership: registration, lease expiry, discovery, topology refresh, Meta restart: ok"
