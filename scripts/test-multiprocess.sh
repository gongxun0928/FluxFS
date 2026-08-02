#!/usr/bin/env bash
set -euo pipefail

if [[ ! -c /dev/fuse ]]; then
    echo "SKIP: /dev/fuse is unavailable" >&2
    exit 77
fi
if ! command -v fusermount3 >/dev/null 2>&1; then
    echo "SKIP: fusermount3 is unavailable" >&2
    exit 77
fi

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
test_root=$(mktemp -d -t fluxfs-multiprocess.XXXXXX)
mount_dir="$test_root/mnt"
base_port=$((30000 + ($$ % 19000)))
meta_port=$base_port
worker0_port=$((base_port + 1))
worker1_port=$((base_port + 2))
worker2_port=$((base_port + 3))
meta_pid=""
worker0_pid=""
worker1_pid=""
worker2_pid=""
mount_pid=""

cleanup() {
    fusermount3 -u "$mount_dir" 2>/dev/null || true
    for pid in "$mount_pid" "$meta_pid" "$worker0_pid" "$worker1_pid" "$worker2_pid"; do
        if [[ -n "$pid" ]]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    rm -rf -- "$test_root"
}
trap cleanup EXIT

wait_port() {
    local port=$1
    for _ in $(seq 1 100); do
        if timeout 0.1 bash -c "</dev/tcp/127.0.0.1/$port" 2>/dev/null; then
            return 0
        fi
        sleep 0.05
    done
    echo "port $port did not become ready" >&2
    return 1
}

start_worker0() {
    "$repo_dir/target/debug/fluxfs-chunkworker" \
        --worker-id 0 --listen "127.0.0.1:$worker0_port" \
        --data-dir "$test_root/worker-0" >"$test_root/worker-0.log" 2>&1 &
    worker0_pid=$!
    wait_port "$worker0_port"
}

start_worker1() {
    "$repo_dir/target/debug/fluxfs-chunkworker" \
        --worker-id 1 --listen "127.0.0.1:$worker1_port" \
        --data-dir "$test_root/worker-1" >"$test_root/worker-1.log" 2>&1 &
    worker1_pid=$!
    wait_port "$worker1_port"
}

start_meta() {
    "$repo_dir/target/debug/fluxfs-metamaster" \
        --listen "127.0.0.1:$meta_port" --data-dir "$test_root/meta" \
        >"$test_root/meta.log" 2>&1 &
    meta_pid=$!
    wait_port "$meta_port"
}

start_mount() {
    "$repo_dir/target/debug/fluxfs" mount --no-ufs \
        --data-dir "$test_root/client" --mountpoint "$mount_dir" \
        --meta-addr "http://127.0.0.1:$meta_port" \
        --chunk-worker "http://127.0.0.1:$worker0_port" \
        --chunk-worker "http://127.0.0.1:$worker1_port" \
        --chunk-worker "http://127.0.0.1:$worker2_port" \
        >"$test_root/mount.log" 2>&1 &
    mount_pid=$!
    for _ in $(seq 1 100); do
        if mountpoint -q "$mount_dir"; then
            return 0
        fi
        if ! kill -0 "$mount_pid" 2>/dev/null; then
            cat "$test_root/mount.log" >&2
            wait "$mount_pid"
            return 1
        fi
        sleep 0.05
    done
    echo "FluxFS did not mount within 5 seconds" >&2
    return 1
}

mkdir -p "$mount_dir"
cargo build --manifest-path "$repo_dir/Cargo.toml" \
    -p fluxfs -p fluxfs-metamaster -p fluxfs-chunkworker

start_meta
start_worker0
start_worker1
"$repo_dir/target/debug/fluxfs-chunkworker" \
    --worker-id 2 --listen "127.0.0.1:$worker2_port" \
    --data-dir "$test_root/worker-2" >"$test_root/worker-2.log" 2>&1 &
worker2_pid=$!

wait_port "$worker2_port"
start_mount

printf 'multiprocess durable\n' >"$mount_dir/durable.txt"
test "$(cat "$mount_dir/durable.txt")" = "multiprocess durable"
dd if=/dev/zero of="$test_root/large.expected" bs=1M count=4 status=none
printf 'chunk-boundary-ok' >>"$test_root/large.expected"
cp "$test_root/large.expected" "$mount_dir/large.bin"
cmp "$test_root/large.expected" "$mount_dir/large.bin"
test "$(find "$test_root/worker-0/objects" -type f | wc -l)" -ge 1
test "$(find "$test_root/worker-1/objects" -type f | wc -l)" -ge 1
test "$(find "$test_root/worker-2/objects" -type f 2>/dev/null | wc -l)" -eq 0

# The initial RF=2 set is workers 0/1. With worker 0 down, reads from worker 1
# lazily copy accessed chunks to spare worker 2. Before the next write ACKs, a
# paginated inventory scrub restores every reachable authoritative chunk to RF=2.
kill "$worker0_pid"
wait "$worker0_pid" 2>/dev/null || true
worker0_pid=""
test "$(cat "$mount_dir/durable.txt")" = "multiprocess durable"
cmp "$test_root/large.expected" "$mount_dir/large.bin"
test "$(find "$test_root/worker-2/objects" -type f | wc -l)" -ge 3
printf 'repaired-to-spare\n' >>"$mount_dir/durable.txt"
test "$(tail -n 1 "$mount_dir/durable.txt")" = "repaired-to-spare"

start_worker0
printf 'worker-zero-returned\n' >>"$mount_dir/durable.txt"
test "$(tail -n 1 "$mount_dir/durable.txt")" = "worker-zero-returned"

# A second topology change paginated-repairs the mixed 0/1 and 1/2 placements
# to 0/2 before allowing a new write.
kill "$worker1_pid"
wait "$worker1_pid" 2>/dev/null || true
worker1_pid=""
test "$(tail -n 1 "$mount_dir/durable.txt")" = "worker-zero-returned"
printf 'rebalanced-to-zero-two\n' >>"$mount_dir/durable.txt"
test "$(tail -n 1 "$mount_dir/durable.txt")" = "rebalanced-to-zero-two"

# MetaMaster is a separate process too. Before OpenRaft HA lands, killing it
# makes metadata-dependent reads unavailable; reopening the same heed state
# restores service through tonic channel reconnection without remounting.
kill "$meta_pid"
wait "$meta_pid" 2>/dev/null || true
meta_pid=""
if cat "$mount_dir/durable.txt" >/dev/null 2>&1; then
    echo "read unexpectedly succeeded with MetaMaster down" >&2
    exit 1
fi
start_meta
test "$(tail -n 1 "$mount_dir/durable.txt")" = "rebalanced-to-zero-two"

fusermount3 -u "$mount_dir"
wait "$mount_pid"
mount_pid=""
start_mount
test "$(tail -n 1 "$mount_dir/durable.txt")" = "rebalanced-to-zero-two"
cmp "$test_root/large.expected" "$mount_dir/large.bin"
fusermount3 -u "$mount_dir"
wait "$mount_pid"
mount_pid=""

echo "multi-process Meta + 3 Workers + FUSE, RF=2 automatic repair/rebalance: ok"
