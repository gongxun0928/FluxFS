#!/usr/bin/env bash
set -euo pipefail

# Real >1 GiB acceptance for windowed Dirty copy-up and multipart UFS flush.
# The source file is sparse locally, but MinIO receives and FluxFS reconstructs
# the complete logical payload. Override the size for stress runs if desired.
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
export FLUXFS_DIRTY_FILE_MIB="${FLUXFS_LARGE_FILE_MIB:-1025}"
export FLUXFS_DIRTY_SPARSE=1
export FLUXFS_SKIP_GC_ASSERT=1
exec "$repo_dir/scripts/test-dirty-copyup-minio.sh"
