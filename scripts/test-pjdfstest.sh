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
git_dir="$cache_root/repository.git"

for command in git autoreconf automake make prove sudo fusermount3 mountpoint python3 tar; do
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
fluxfs_revision=$(git -C "$repo_dir" rev-parse HEAD)
fluxfs_dirty=false
if [[ -n $(git -C "$repo_dir" status --porcelain) ]]; then
    fluxfs_dirty=true
fi
if $fluxfs_dirty && [[ "${FLUXFS_PJDFSTEST_ALLOW_DIRTY:-0}" != 1 ]]; then
    echo "FluxFS worktree is dirty; commit it or set FLUXFS_PJDFSTEST_ALLOW_DIRTY=1" >&2
    exit 2
fi

mkdir -p "$cache_root" "$report_dir"
if [[ ! -f "$git_dir/HEAD" ]]; then
    git clone --quiet --bare "$PJDFSTEST_REPOSITORY" "$git_dir"
fi
if ! git --git-dir "$git_dir" cat-file -e "$PJDFSTEST_REVISION^{commit}" 2>/dev/null; then
    git --git-dir "$git_dir" fetch --quiet "$PJDFSTEST_REPOSITORY" "$PJDFSTEST_REVISION"
fi
test "$(git --git-dir "$git_dir" rev-parse "$PJDFSTEST_REVISION^{commit}")" = "$PJDFSTEST_REVISION"
source_root=$(mktemp -d -t fluxfs-pjdfstest-source.XXXXXX)
src_dir="$source_root/src"
mkdir "$src_dir"
cleanup_source() {
    rm -rf -- "$source_root"
}
trap cleanup_source EXIT
git --git-dir "$git_dir" archive "$PJDFSTEST_REVISION" | tar -x -C "$src_dir"
(cd "$src_dir" && autoreconf -ifs && ./configure --quiet && make -s pjdfstest)
cargo build --manifest-path "$repo_dir/Cargo.toml" -p fluxfs

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
            FLUXFS_MINIO_NAME="$FLUXFS_MINIO_NAME" \
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
        export FLUXFS_MINIO_NAME="fluxfs-pjdfstest-${current_mode}-$$-${RANDOM}"
        export FLUXFS_MINIO_PORT="${FLUXFS_PJDFSTEST_MINIO_PORT:-$(pick_free_tcp_port)}"
        export FLUXFS_MINIO_CONSOLE_PORT="${FLUXFS_PJDFSTEST_MINIO_CONSOLE_PORT:-$(pick_free_tcp_port)}"
        while [[ "$FLUXFS_MINIO_CONSOLE_PORT" == "$FLUXFS_MINIO_PORT" ]]; do
            FLUXFS_MINIO_CONSOLE_PORT=$(pick_free_tcp_port)
        done
        export FLUXFS_MINIO_BUCKET=fluxfs-pjdfstest
        export FLUXFS_MINIO_IMAGE="$PJDFSTEST_MINIO_IMAGE"
        export FLUXFS_MINIO_MC_IMAGE="$PJDFSTEST_MINIO_MC_IMAGE"
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
        --pin-file "$config_dir/pin.env" \
        --fluxfs-revision "$fluxfs_revision" \
        --fluxfs-dirty "$fluxfs_dirty" \
        --timeout-seconds "${FLUXFS_PJDFSTEST_CASE_TIMEOUT:-60}" || runner_status=$?
    sudo -n chown -R "$(id -u):$(id -g)" "$report_dir"
    cp "$test_root/mount.log" "$report_dir/pjdfstest-${current_mode}-mount.log"
    return "$runner_status"
)

run_mode "$mode"
