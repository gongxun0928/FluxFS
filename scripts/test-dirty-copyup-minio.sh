#!/usr/bin/env bash
set -euo pipefail

if [[ ! -c /dev/fuse ]] || ! command -v fusermount3 >/dev/null 2>&1; then
    echo "SKIP: /dev/fuse or fusermount3 unavailable" >&2
    exit 77
fi
if ! command -v docker >/dev/null 2>&1; then
    echo "SKIP: docker unavailable" >&2
    exit 77
fi

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
count_chunks() { "$repo_dir/scripts/count-pack-chunks.sh" "$1"; }
test_root=$(mktemp -d -t fluxfs-dirty-copyup.XXXXXX)
mount_dir="$test_root/mnt"
prefix="dirty-copyup-$$"
base_port=$((31000 + ($$ % 18000)))
meta_port=$base_port
worker0_port=$((base_port + 1))
worker1_port=$((base_port + 2))
worker2_port=$((base_port + 3))
minio_name="${FLUXFS_MINIO_NAME:-fluxfs-minio}"
minio_port="${FLUXFS_MINIO_PORT:-9000}"
minio_user="${FLUXFS_MINIO_USER:-minioadmin}"
minio_pass="${FLUXFS_MINIO_PASS:-minioadmin}"
minio_bucket="${FLUXFS_MINIO_BUCKET:-fluxfs}"
minio_net="${FLUXFS_MINIO_NET:-fluxfs-net}"
mc_image="${FLUXFS_MINIO_MC_IMAGE:-minio/mc:latest}"
meta_pid=""
worker0_pid=""
worker1_pid=""
worker2_pid=""
mount_pid=""

mc_sh() {
    docker run --rm --network "$minio_net" --entrypoint /bin/sh "$mc_image" -c "
mc alias set local http://${minio_name}:9000 '${minio_user}' '${minio_pass}' >/dev/null &&
$*
"
}

cleanup() {
    fusermount3 -u "$mount_dir" 2>/dev/null || true
    for pid in "$mount_pid" "$meta_pid" "$worker0_pid" "$worker1_pid" "$worker2_pid"; do
        if [[ -n "$pid" ]]; then
            kill "$pid" 2>/dev/null || true
            wait "$pid" 2>/dev/null || true
        fi
    done
    mc_sh "mc rm --recursive --force local/${minio_bucket}/${prefix} >/dev/null" 2>/dev/null || true
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

start_worker() {
    local id=$1
    local port=$2
    "$repo_dir/target/debug/fluxfs-chunkworker" \
        --worker-id "$id" --listen "127.0.0.1:$port" \
        --data-dir "$test_root/worker-$id" >"$test_root/worker-$id.log" 2>&1 &
    case "$id" in
        0) worker0_pid=$! ;;
        1) worker1_pid=$! ;;
        2) worker2_pid=$! ;;
    esac
    wait_port "$port"
}

start_meta() {
    "$repo_dir/target/debug/fluxfs-metamaster" \
        --listen "127.0.0.1:$meta_port" --data-dir "$test_root/meta" \
        >"$test_root/meta.log" 2>&1 &
    meta_pid=$!
    wait_port "$meta_port"
}

start_mount() {
    FLUXFS_UFS_ENDPOINT="http://127.0.0.1:${minio_port}" \
    FLUXFS_UFS_BUCKET="$minio_bucket" \
    FLUXFS_UFS_REGION=us-east-1 \
    FLUXFS_UFS_ACCESS_KEY="$minio_user" \
    FLUXFS_UFS_SECRET_KEY="$minio_pass" \
        "$repo_dir/target/debug/fluxfs" mount \
        --ufs "s3://${minio_bucket}/${prefix}" \
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
    echo "FluxFS UFS mount did not become ready" >&2
    return 1
}

mkdir -p "$mount_dir"
bash "$repo_dir/scripts/dev-minio.sh" >/dev/null
dd if=/dev/urandom of="$test_root/original.bin" bs=1M count=12 status=none
cp "$test_root/original.bin" "$test_root/expected.bin"
docker run --rm --network "$minio_net" --entrypoint /bin/sh \
    -v "$test_root:/test:ro" "$mc_image" -c "
mc alias set local http://${minio_name}:9000 '${minio_user}' '${minio_pass}' >/dev/null &&
mc cp /test/original.bin local/${minio_bucket}/${prefix}/large.bin >/dev/null
"

cargo build --manifest-path "$repo_dir/Cargo.toml" \
    -p fluxfs -p fluxfs-metamaster -p fluxfs-chunkworker
start_meta
start_worker 0 "$worker0_port"
start_worker 1 "$worker1_port"
start_worker 2 "$worker2_port"
start_mount

# First random write materializes only the middle 4 MiB window as a Local RF=2
# extent; bytes outside it remain pinned UFS ranges.
python3 - "$mount_dir/large.bin" "$test_root/expected.bin" <<'PY'
import os, sys
payload = b"dirty-copy-up-first"
offset = 4 * 1024 * 1024 + 37
for path in sys.argv[1:]:
    fd = os.open(path, os.O_RDWR)
    try:
        assert os.pwrite(fd, payload, offset) == len(payload)
    finally:
        os.close(fd)
PY
cmp "$test_root/expected.bin" "$mount_dir/large.bin"
test "$(count_chunks "$test_root/worker-0")" -eq 1
test "$(count_chunks "$test_root/worker-1")" -eq 1
test "$(count_chunks "$test_root/worker-2")" -eq 0

# Copy-up is write-back: the original MinIO object is untouched before flush.
mc_sh "mc cat local/${minio_bucket}/${prefix}/large.bin" >"$test_root/backing.bin"
cmp "$test_root/original.bin" "$test_root/backing.bin"

# Lose one authoritative Worker. Read repair copies the first Local chunk to the
# spare; a second random write then ACKs only after its new chunk reaches RF=2.
kill "$worker0_pid"
wait "$worker0_pid" 2>/dev/null || true
worker0_pid=""
cmp "$test_root/expected.bin" "$mount_dir/large.bin"
python3 - "$mount_dir/large.bin" "$test_root/expected.bin" <<'PY'
import os, sys
payload = b"dirty-copy-up-after-worker-loss"
offset = 8 * 1024 * 1024 + 91
for path in sys.argv[1:]:
    fd = os.open(path, os.O_RDWR)
    try:
        assert os.pwrite(fd, payload, offset) == len(payload)
    finally:
        os.close(fd)
PY
cmp "$test_root/expected.bin" "$mount_dir/large.bin"
test "$(count_chunks "$test_root/worker-2")" -ge 2

# fsync is the explicit write-back boundary: durable intent, conditional Put,
# HEAD digest verification, and metadata CAS to a clean UFS-only manifest.
python3 - "$mount_dir/large.bin" <<'PY'
import os, sys
fd = os.open(sys.argv[1], os.O_RDONLY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
PY
mc_sh "mc cat local/${minio_bucket}/${prefix}/large.bin" >"$test_root/backing-after-flush.bin"
cmp "$test_root/expected.bin" "$test_root/backing-after-flush.bin"

fusermount3 -u "$mount_dir"
wait "$mount_pid"
mount_pid=""
start_mount
cmp "$test_root/expected.bin" "$mount_dir/large.bin"
mc_sh "mc cat local/${minio_bucket}/${prefix}/large.bin" >"$test_root/backing-after-remount.bin"
cmp "$test_root/expected.bin" "$test_root/backing-after-remount.bin"
# Mount no longer stop-the-world GC's. Explicit quiesced reclaim (admin/test)
# removes superseded manifests/chunks from reachable Workers (worker-0 down).
"$repo_dir/target/debug/fluxfs" orphan-gc \
    --data-dir "$test_root/client" \
    --meta-addr "http://127.0.0.1:$meta_port" \
    --chunk-worker "http://127.0.0.1:$worker0_port" \
    --chunk-worker "http://127.0.0.1:$worker1_port" \
    --chunk-worker "http://127.0.0.1:$worker2_port"
test "$(count_chunks "$test_root/worker-1")" -eq 0
test "$(count_chunks "$test_root/worker-2")" -eq 0
fusermount3 -u "$mount_dir"
wait "$mount_pid"
mount_pid=""

echo "Dirty copy-up + fsync recovery + explicit orphan-gc + FUSE remount: ok"
