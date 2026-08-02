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
test_root=$(mktemp -d -t fluxfs-ufs-minio.XXXXXX)
mount_dir="$test_root/mnt"
data_dir="$test_root/data"
prefix="acceptance-$$"
minio_name="${FLUXFS_MINIO_NAME:-fluxfs-minio}"
minio_port="${FLUXFS_MINIO_PORT:-9000}"
minio_user="${FLUXFS_MINIO_USER:-minioadmin}"
minio_pass="${FLUXFS_MINIO_PASS:-minioadmin}"
minio_bucket="${FLUXFS_MINIO_BUCKET:-fluxfs}"
minio_net="${FLUXFS_MINIO_NET:-fluxfs-net}"
mc_image="${FLUXFS_MINIO_MC_IMAGE:-minio/mc:latest}"
mount_pid=""

mc_sh() {
    docker run --rm --network "$minio_net" --entrypoint /bin/sh "$mc_image" -c "
mc alias set local http://${minio_name}:9000 '${minio_user}' '${minio_pass}' >/dev/null &&
$*
"
}

cleanup() {
    fusermount3 -u "$mount_dir" 2>/dev/null || true
    if [[ -n "$mount_pid" ]]; then
        kill "$mount_pid" 2>/dev/null || true
        wait "$mount_pid" 2>/dev/null || true
    fi
    mc_sh "mc rm --recursive --force local/${minio_bucket}/${prefix} >/dev/null" 2>/dev/null || true
    rm -rf -- "$test_root"
}
trap cleanup EXIT

start_mount() {
    FLUXFS_UFS_ENDPOINT="http://127.0.0.1:${minio_port}" \
    FLUXFS_UFS_BUCKET="$minio_bucket" \
    FLUXFS_UFS_REGION=us-east-1 \
    FLUXFS_UFS_ACCESS_KEY="$minio_user" \
    FLUXFS_UFS_SECRET_KEY="$minio_pass" \
        "$repo_dir/target/debug/fluxfs" mount \
        --ufs "s3://${minio_bucket}/${prefix}" \
        --data-dir "$data_dir" --mountpoint "$mount_dir" \
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
printf 'external-minio-small\n' >"$test_root/small.txt"
dd if=/dev/urandom of="$test_root/large.bin" bs=1M count=8 status=none

docker run --rm --network "$minio_net" --entrypoint /bin/sh \
    -v "$test_root:/test:ro" "$mc_image" -c "
mc alias set local http://${minio_name}:9000 '${minio_user}' '${minio_pass}' >/dev/null &&
mc cp /test/small.txt local/${minio_bucket}/${prefix}/small.txt >/dev/null &&
mc cp /test/large.bin local/${minio_bucket}/${prefix}/large.bin >/dev/null
"

cargo build --manifest-path "$repo_dir/Cargo.toml" -p fluxfs
start_mount

test "$(cat "$mount_dir/small.txt")" = "external-minio-small"
dd if="$test_root/large.bin" of="$test_root/range.expected" bs=4096 skip=768 count=1 status=none
dd if="$mount_dir/large.bin" of="$test_root/range.actual" bs=4096 skip=768 count=1 status=none
cmp "$test_root/range.expected" "$test_root/range.actual"
cmp "$test_root/large.bin" "$mount_dir/large.bin"
if { printf 'read-only\n' >"$mount_dir/must-fail.txt"; } 2>/dev/null; then
    echo "External mount unexpectedly accepted a create" >&2
    exit 1
fi

fusermount3 -u "$mount_dir"
wait "$mount_pid"
mount_pid=""
start_mount
test "$(cat "$mount_dir/small.txt")" = "external-minio-small"
cmp "$test_root/large.bin" "$mount_dir/large.bin"
fusermount3 -u "$mount_dir"
wait "$mount_pid"
mount_pid=""

echo "MinIO External lazy namespace + pinned range/cache FUSE path: ok"
