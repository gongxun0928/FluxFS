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
count_chunks() { "$repo_dir/scripts/count-pack-chunks.sh" "$1"; }
test_root=$(mktemp -d -t fluxfs-crash-test.XXXXXX)
data_dir="$test_root/data"
mount_dir="$test_root/mnt"
mount_pid=""

cleanup() {
    # A SIGKILLed userspace daemon can leave a disconnected FUSE mount that
    # briefly fools mountpoint(1), so always ask fusermount to detach it.
    fusermount3 -u "$mount_dir" 2>/dev/null || true
    if [[ -n "$mount_pid" ]]; then
        kill "$mount_pid" 2>/dev/null || true
        wait "$mount_pid" 2>/dev/null || true
    fi
    rm -rf -- "$test_root"
}
trap cleanup EXIT

mkdir -p "$data_dir" "$mount_dir"
cargo build --manifest-path "$repo_dir/Cargo.toml" -p fluxfs

start_mount() {
    "$repo_dir/target/debug/fluxfs" mount \
        --no-ufs \
        --data-dir "$data_dir" \
        --mountpoint "$mount_dir" &
    mount_pid=$!
    for _ in $(seq 1 100); do
        if mountpoint -q "$mount_dir"; then
            return 0
        fi
        if ! kill -0 "$mount_pid" 2>/dev/null; then
            wait "$mount_pid"
            return 1
        fi
        sleep 0.05
    done
    echo "FluxFS did not mount within 5 seconds" >&2
    return 1
}

start_mount
printf 'acknowledged before crash\n' >"$mount_dir/durable.txt"
test "$(cat "$mount_dir/durable.txt")" = "acknowledged before crash"

# An acknowledged Ephemeral write must exist on both local Worker replicas.
test "$(count_chunks "$data_dir/chunks/worker-0")" -ge 1
test "$(count_chunks "$data_dir/chunks/worker-1")" -ge 1

kill -KILL "$mount_pid"
wait "$mount_pid" 2>/dev/null || true
mount_pid=""
fusermount3 -u "$mount_dir"

# Corrupt one replica while FluxFS is down; the other must remain readable.
primary_seg=$(find "$data_dir/chunks/worker-0/segments" -name 'seg-*.dat' -type f | head -n 1)
test -n "$primary_seg"
printf 'corrupt replica' >"$primary_seg"

start_mount
test "$(cat "$mount_dir/durable.txt")" = "acknowledged before crash"
printf 'recovered\n' >>"$mount_dir/durable.txt"
test "$(tail -n 1 "$mount_dir/durable.txt")" = "recovered"
fusermount3 -u "$mount_dir"
wait "$mount_pid"
mount_pid=""

echo "SIGKILL restart + replica fallback: ok"
