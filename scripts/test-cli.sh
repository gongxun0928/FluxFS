#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
test_root=$(mktemp -d -t fluxfs-cli.XXXXXX)
base_port=$((28000 + ($$ % 18000)))
meta_port=$base_port
worker0_port=$((base_port + 1))
worker1_port=$((base_port + 2))
worker2_port=$((base_port + 3))
meta_pid=""; worker0_pid=""; worker1_pid=""; worker2_pid=""

cleanup() {
    for pid in "$meta_pid" "$worker0_pid" "$worker1_pid" "$worker2_pid"; do
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
    echo "port $port did not become ready" >&2
    return 1
}

start_meta() {
    "$repo_dir/target/debug/fluxfs-metamaster" \
        --listen "127.0.0.1:$meta_port" --data-dir "$test_root/meta" \
        --allow-insecure-dev >"$test_root/meta.log" 2>&1 &
    meta_pid=$!
    wait_port "$meta_port"
}

start_worker() {
    local id=$1 port=$2 domain=$3 data_dir=$4 log=$5
    "$repo_dir/target/debug/fluxfs-chunkworker" \
        --worker-id "$id" --listen "127.0.0.1:$port" \
        --advertise-endpoint "http://127.0.0.1:$port" \
        --failure-domain "$domain" --capacity-bytes 1073741824 \
        --heartbeat-interval-secs 1 --lease-secs 30 \
        --meta-addr "http://127.0.0.1:$meta_port" \
        --data-dir "$data_dir" --allow-insecure-dev >"$log" 2>&1 &
    LAST_PID=$!
    wait_port "$port"
}

cargo build --manifest-path "$repo_dir/Cargo.toml" \
    -p fluxfs -p fluxfs-metamaster -p fluxfs-chunkworker

start_meta
start_worker 101 "$worker0_port" rack-a "$test_root/worker-0" "$test_root/worker-0.log"; worker0_pid=$LAST_PID
start_worker 202 "$worker1_port" rack-b "$test_root/worker-1" "$test_root/worker-1.log"; worker1_pid=$LAST_PID
start_worker 303 "$worker2_port" rack-c "$test_root/worker-2" "$test_root/worker-2.log"; worker2_pid=$LAST_PID

fs=("$repo_dir/target/debug/fluxfs" fs --meta-addr "http://127.0.0.1:$meta_port" --allow-insecure-dev)
admin=("$repo_dir/target/debug/fluxfs" admin --meta-addr "http://127.0.0.1:$meta_port" --allow-insecure-dev)

for _ in $(seq 1 50); do
    if "${admin[@]}" status >"$test_root/status" 2>/dev/null && grep -q 'workers_live=3/3' "$test_root/status"; then
        break
    fi
    sleep 0.1
done
grep -q 'workers_live=3/3' "$test_root/status"
"${admin[@]}" workers >"$test_root/workers"
grep -q $'101\tlive\t' "$test_root/workers"
grep -q $'202\tlive\t' "$test_root/workers"
grep -q $'303\tlive\t' "$test_root/workers"

"${fs[@]}" mkdir /left
"${fs[@]}" mkdir /right
dd if=/dev/urandom of="$test_root/expected.bin" bs=1M count=5 status=none
printf 'stream-tail\n' >>"$test_root/expected.bin"
"${fs[@]}" put "$test_root/expected.bin" /left/data.bin
"${fs[@]}" cat /left/data.bin >"$test_root/cat.bin"
cmp "$test_root/expected.bin" "$test_root/cat.bin"
"${fs[@]}" get /left/data.bin "$test_root/get.bin"
cmp "$test_root/expected.bin" "$test_root/get.bin"
"${fs[@]}" stat /left/data.bin >"$test_root/stat"
grep -q '^size=5242892$' "$test_root/stat"
"${fs[@]}" ls /left | grep -q 'data.bin$'

"${fs[@]}" mv --no-replace /left/data.bin /right/moved.bin
if "${fs[@]}" rmdir /right >/dev/null 2>&1; then
    echo "rmdir unexpectedly removed a non-empty directory" >&2
    exit 1
fi
"${fs[@]}" chmod 0600 /right/moved.bin
"${fs[@]}" chown 123:456 /right/moved.bin
"${fs[@]}" touch /right/moved.bin
"${fs[@]}" stat /right/moved.bin >"$test_root/moved-stat"
grep -q '^mode=0600$' "$test_root/moved-stat"
grep -q '^uid=123$' "$test_root/moved-stat"
grep -q '^gid=456$' "$test_root/moved-stat"

"${fs[@]}" ln /right/moved.bin /right/hard.bin
test "$("${fs[@]}" stat /right/moved.bin | sed -n 's/^inode=//p')" = \
    "$("${fs[@]}" stat /right/hard.bin | sed -n 's/^inode=//p')"
"${fs[@]}" ln --symbolic moved.bin /right/sym.bin
test "$("${fs[@]}" readlink /right/sym.bin)" = "moved.bin"
"${fs[@]}" lstat /right/sym.bin | grep -q '^type=Symlink$'
"${fs[@]}" stat /right/sym.bin | grep -q '^type=Regular$'
"${fs[@]}" setxattr --create /right/hard.bin user.cli cli-value
test "$("${fs[@]}" getxattr /right/hard.bin user.cli)" = "cli-value"
test "$("${fs[@]}" getxattr /right/sym.bin user.cli)" = "cli-value"
"${fs[@]}" listxattr /right/hard.bin | grep -q '^user.cli$'
"${fs[@]}" setxattr --no-follow /right/sym.bin user.link-self link-value
test "$("${fs[@]}" getxattr --no-follow /right/sym.bin user.link-self)" = "link-value"
"${fs[@]}" listxattr --no-follow /right/sym.bin | grep -q '^user.link-self$'
"${fs[@]}" setxattr --replace /right/hard.bin user.cli replaced
test "$("${fs[@]}" getxattr /right/moved.bin user.cli)" = "replaced"

printf 'replacement\n' >"$test_root/replacement"
printf 'source-wins\n' >"$test_root/source"
"${fs[@]}" put "$test_root/replacement" /right/destination
"${fs[@]}" put "$test_root/source" /right/source
if "${fs[@]}" mv --no-replace /right/source /right/destination >/dev/null 2>&1; then
    echo "mv --no-replace unexpectedly overwrote its destination" >&2
    exit 1
fi
"${fs[@]}" mv /right/source /right/destination
test "$("${fs[@]}" cat /right/destination)" = "source-wins"

"${fs[@]}" truncate 1024 /right/moved.bin
"${fs[@]}" stat /right/moved.bin | grep -q '^size=1024$'
"${fs[@]}" rm /right/moved.bin
test "$("${fs[@]}" cat /right/hard.bin | wc -c)" = "1024"
"${fs[@]}" removexattr /right/hard.bin user.cli
"${fs[@]}" removexattr --no-follow /right/sym.bin user.link-self
"${fs[@]}" rm /right/sym.bin
"${fs[@]}" rm /right/hard.bin
"${fs[@]}" rm /right/destination
"${fs[@]}" rmdir /right
"${fs[@]}" rmdir /left

# Namespace/data survive Meta restart and remain reachable without a FUSE mount.
"${fs[@]}" mkdir /persisted
printf 'after-restart\n' >"$test_root/persisted"
"${fs[@]}" put "$test_root/persisted" /persisted/file
"${fs[@]}" ln /persisted/file /persisted/hard
"${fs[@]}" ln --symbolic file /persisted/sym
"${fs[@]}" setxattr /persisted/file user.persist durable
kill "$meta_pid"; wait "$meta_pid" 2>/dev/null || true; meta_pid=""
start_meta
test "$("${fs[@]}" cat /persisted/file)" = "after-restart"
test "$("${fs[@]}" cat /persisted/hard)" = "after-restart"
test "$("${fs[@]}" readlink /persisted/sym)" = "file"
test "$("${fs[@]}" getxattr /persisted/hard user.persist)" = "durable"

echo "fluxfs fs/admin CLI: streaming CRUD + links/xattrs + atomic rename/rmdir + restart: ok"
