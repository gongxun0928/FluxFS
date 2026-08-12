#!/usr/bin/env bash
set -euo pipefail

repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
config_dir="$repo_dir/scripts/pjdfstest"
mode="${1:-all}"
case "$mode" in
    all|ephemeral|external-minio) ;;
    *) echo "usage: $0 [all|ephemeral|external-minio]" >&2; exit 2 ;;
esac

if [[ "$mode" == all ]]; then
    status=0
    "$0" ephemeral || status=1
    "$0" external-minio || status=1
    exit "$status"
fi

# shellcheck disable=SC1091
source "$config_dir/pin.env"
cache_root="${FLUXFS_PJDFSTEST_CACHE:-$repo_dir/target/pjdfstest}"
report_dir="${FLUXFS_PJDFSTEST_REPORT_DIR:-$repo_dir/target/pjdfstest-reports}"
src_dir="$cache_root/src"
build_stamp="$cache_root/build-revision"

for command in git autoreconf automake make prove sudo fusermount3 mountpoint python3; do
    command -v "$command" >/dev/null || { echo "missing prerequisite: $command" >&2; exit 2; }
done
if [[ "$mode" == external-minio ]]; then
    for command in curl docker; do
        command -v "$command" >/dev/null || { echo "missing prerequisite: $command" >&2; exit 2; }
    done
fi
sudo -n true 2>/dev/null || {
    echo "pjdfstest must run as root; passwordless sudo is required for this harness" >&2
    exit 2
}

(cd "$config_dir" && PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -q test_report.py)

mkdir -p "$cache_root" "$report_dir"
if [[ ! -d "$src_dir/.git" ]]; then
    git clone --quiet "$PJDFSTEST_REPOSITORY" "$src_dir"
fi
if ! git -C "$src_dir" cat-file -e "$PJDFSTEST_REVISION^{commit}" 2>/dev/null; then
    git -C "$src_dir" fetch --quiet origin "$PJDFSTEST_REVISION"
fi
git -C "$src_dir" checkout --quiet --detach "$PJDFSTEST_REVISION"
test "$(git -C "$src_dir" rev-parse HEAD)" = "$PJDFSTEST_REVISION"
if [[ ! -x "$src_dir/pjdfstest" \
    || ! -f "$build_stamp" \
    || $(<"$build_stamp") != "$PJDFSTEST_REVISION" ]]; then
    (cd "$src_dir" && autoreconf -ifs && ./configure --quiet && make -s pjdfstest)
    printf '%s\n' "$PJDFSTEST_REVISION" >"$build_stamp"
fi
cargo build --manifest-path "$repo_dir/Cargo.toml" -p fluxfs
fluxfs_revision=$(git -C "$repo_dir" rev-parse HEAD)
fluxfs_dirty=false
if [[ -n $(git -C "$repo_dir" status --porcelain) ]]; then
    fluxfs_dirty=true
fi

run_mode() (
    local current_mode=$1
    local test_root mount_dir data_dir mount_pid="" minio_started=false
    test_root=$(mktemp -d -t "fluxfs-pjdfstest-${current_mode}.XXXXXX")
    chmod 0755 "$test_root"
    mount_dir="$test_root/mnt"
    data_dir="$test_root/data"
    mkdir -m 0755 "$mount_dir" "$data_dir"

    cleanup_mode() {
        sudo -n fusermount3 -u "$mount_dir" 2>/dev/null || true
        if [[ -n "$mount_pid" ]]; then
            kill "$mount_pid" 2>/dev/null || true
            wait "$mount_pid" 2>/dev/null || true
        fi
        if $minio_started; then
            FLUXFS_MINIO_NAME="fluxfs-pjdfstest-minio" \
                "$repo_dir/scripts/dev-minio.sh" stop >/dev/null 2>&1 || true
        fi
        sudo -n rm -rf -- "$test_root"
    }
    trap cleanup_mode EXIT

    local -a mount_args=(
        "$repo_dir/target/debug/fluxfs" mount
        --data-dir "$data_dir"
        --mountpoint "$mount_dir"
    )
    local -a mount_env=()
    if [[ "$current_mode" == ephemeral ]]; then
        mount_args+=(--no-ufs)
    else
        pick_free_tcp_port() {
            python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
        }
        export FLUXFS_MINIO_NAME=fluxfs-pjdfstest-minio
        export FLUXFS_MINIO_PORT="${FLUXFS_PJDFSTEST_MINIO_PORT:-$(pick_free_tcp_port)}"
        export FLUXFS_MINIO_CONSOLE_PORT="${FLUXFS_PJDFSTEST_MINIO_CONSOLE_PORT:-$(pick_free_tcp_port)}"
        while [[ "$FLUXFS_MINIO_CONSOLE_PORT" == "$FLUXFS_MINIO_PORT" ]]; do
            FLUXFS_MINIO_CONSOLE_PORT=$(pick_free_tcp_port)
        done
        export FLUXFS_MINIO_BUCKET=fluxfs-pjdfstest
        "$repo_dir/scripts/dev-minio.sh" stop >/dev/null
        minio_started=true
        "$repo_dir/scripts/dev-minio.sh" >/dev/null
        docker ps --format '{{.Names}}' | grep -Fxq "$FLUXFS_MINIO_NAME"
        export FLUXFS_UFS_ENDPOINT="http://127.0.0.1:$FLUXFS_MINIO_PORT"
        export FLUXFS_UFS_BUCKET="$FLUXFS_MINIO_BUCKET"
        export FLUXFS_UFS_REGION=us-east-1
        export FLUXFS_UFS_ACCESS_KEY="${FLUXFS_MINIO_USER:-minioadmin}"
        export FLUXFS_UFS_SECRET_KEY="${FLUXFS_MINIO_PASS:-minioadmin}"
        mount_env+=(
            "FLUXFS_UFS_ENDPOINT=$FLUXFS_UFS_ENDPOINT"
            "FLUXFS_UFS_BUCKET=$FLUXFS_UFS_BUCKET"
            "FLUXFS_UFS_REGION=$FLUXFS_UFS_REGION"
            "FLUXFS_UFS_ACCESS_KEY=$FLUXFS_UFS_ACCESS_KEY"
            "FLUXFS_UFS_SECRET_KEY=$FLUXFS_UFS_SECRET_KEY"
        )
        mount_args+=(--ufs "s3://$FLUXFS_MINIO_BUCKET")
    fi

    sudo -n env "${mount_env[@]}" "${mount_args[@]}" >"$test_root/mount.log" 2>&1 &
    mount_pid=$!
    for _ in $(seq 1 200); do
        sudo -n mountpoint -q "$mount_dir" && break
        if ! kill -0 "$mount_pid" 2>/dev/null; then
            cat "$test_root/mount.log" >&2
            wait "$mount_pid"
            return 1
        fi
        sleep 0.05
    done
    sudo -n mountpoint -q "$mount_dir" || { cat "$test_root/mount.log" >&2; return 1; }

    local runner_status=0
    sudo -n env PATH="$PATH" python3 "$config_dir/report.py" \
        --suite "$config_dir/suite-${current_mode}.json" \
        --known-fail "$config_dir/known-fail-${current_mode}.json" \
        --pjdfstest-dir "$src_dir" \
        --mountpoint "$mount_dir" \
        --report-dir "$report_dir" \
        --revision "$PJDFSTEST_REVISION" \
        --fluxfs-revision "$fluxfs_revision" \
        --fluxfs-dirty "$fluxfs_dirty" \
        --timeout-seconds "${FLUXFS_PJDFSTEST_CASE_TIMEOUT:-60}" || runner_status=$?
    sudo -n chown -R "$(id -u):$(id -g)" "$report_dir"
    cp "$test_root/mount.log" "$report_dir/pjdfstest-${current_mode}-mount.log"
    return "$runner_status"
)

run_mode "$mode"
