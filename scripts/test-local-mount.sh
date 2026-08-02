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
test_root=$(mktemp -d -t fluxfs-mount-test.XXXXXX)
data_dir="$test_root/data"
mount_dir="$test_root/mnt"
mount_pid=""

cleanup() {
    if mountpoint -q "$mount_dir" 2>/dev/null; then
        fusermount3 -u "$mount_dir" || true
    fi
    if [[ -n "$mount_pid" ]]; then
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

stop_mount() {
    fusermount3 -u "$mount_dir"
    wait "$mount_pid"
    mount_pid=""
}

start_mount
printf 'hello fluxfs\n' >"$mount_dir/hello.txt"
mkdir "$mount_dir/dir"
printf 'nested\n' >"$mount_dir/dir/nested.txt"
test "$(cat "$mount_dir/hello.txt")" = "hello fluxfs"
test "$(cat "$mount_dir/dir/nested.txt")" = "nested"
stop_mount

# Reopen the same metadata/chunk directories and verify acknowledged data.
start_mount
test "$(cat "$mount_dir/hello.txt")" = "hello fluxfs"
test "$(cat "$mount_dir/dir/nested.txt")" = "nested"
printf 'restart-ok\n' >>"$mount_dir/hello.txt"
test "$(tail -n 1 "$mount_dir/hello.txt")" = "restart-ok"
stop_mount

echo "local mount + restart: ok"
