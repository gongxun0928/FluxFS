#!/usr/bin/env bash
# Start a local MinIO container as the FluxFS UFS test bed (S3-compatible).
#
# Usage:
#   bash scripts/dev-minio.sh          # start (idempotent) + ensure bucket
#   bash scripts/dev-minio.sh stop     # stop & remove container
#   bash scripts/dev-minio.sh status   # health + list bucket
#
# Env overrides:
#   FLUXFS_MINIO_NAME, FLUXFS_MINIO_PORT, FLUXFS_MINIO_CONSOLE_PORT
#   FLUXFS_MINIO_USER, FLUXFS_MINIO_PASS, FLUXFS_MINIO_BUCKET, FLUXFS_MINIO_NET

set -euo pipefail

NAME="${FLUXFS_MINIO_NAME:-fluxfs-minio}"
PORT="${FLUXFS_MINIO_PORT:-9000}"
CONSOLE_PORT="${FLUXFS_MINIO_CONSOLE_PORT:-9001}"
USER="${FLUXFS_MINIO_USER:-minioadmin}"
PASS="${FLUXFS_MINIO_PASS:-minioadmin}"
BUCKET="${FLUXFS_MINIO_BUCKET:-fluxfs}"
NET="${FLUXFS_MINIO_NET:-fluxfs-net}"
IMAGE="${FLUXFS_MINIO_IMAGE:-minio/minio:latest}"
MC_IMAGE="${FLUXFS_MINIO_MC_IMAGE:-minio/mc:latest}"

cmd="${1:-start}"

ensure_net() {
  docker network inspect "$NET" >/dev/null 2>&1 || docker network create "$NET" >/dev/null
}

mc_sh() {
  # mc config is per-container; always alias + command in one shot.
  docker run --rm --network "$NET" --entrypoint /bin/sh "$MC_IMAGE" -c "
mc alias set local http://${NAME}:9000 '${USER}' '${PASS}' >/dev/null &&
$*
"
}

start() {
  ensure_net
  if docker ps -a --format '{{.Names}}' | grep -qx "$NAME"; then
    if ! docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
      docker start "$NAME" >/dev/null
    fi
    docker network connect "$NET" "$NAME" 2>/dev/null || true
  else
    docker run -d \
      --name "$NAME" \
      --network "$NET" \
      -p "${PORT}:9000" \
      -p "${CONSOLE_PORT}:9001" \
      -e "MINIO_ROOT_USER=${USER}" \
      -e "MINIO_ROOT_PASSWORD=${PASS}" \
      "$IMAGE" server /data --console-address ':9001' >/dev/null
  fi

  for _ in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${PORT}/minio/health/live" >/dev/null; then
      break
    fi
    sleep 0.3
  done
  curl -sf "http://127.0.0.1:${PORT}/minio/health/live" >/dev/null \
    || { echo "minio health check failed on :${PORT}" >&2; exit 1; }

  mc_sh "mc mb -p local/${BUCKET} >/dev/null"

  cat <<EOF
MinIO ready for FluxFS UFS tests.

  API:      http://127.0.0.1:${PORT}
  Console:  http://127.0.0.1:${CONSOLE_PORT}
  Bucket:   ${BUCKET}
  Access:   ${USER} / ${PASS}

Export for OpenDAL / fluxfs:
  export FLUXFS_UFS_ENDPOINT=http://127.0.0.1:${PORT}
  export FLUXFS_UFS_BUCKET=${BUCKET}
  export FLUXFS_UFS_REGION=us-east-1
  export FLUXFS_UFS_ACCESS_KEY=${USER}
  export FLUXFS_UFS_SECRET_KEY=${PASS}

Smoke:
  cargo run -p fluxfs -- ufs-check
EOF
}

stop() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  echo "stopped ${NAME}"
}

status() {
  if ! docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
    echo "${NAME}: not running"
    exit 1
  fi
  curl -sf "http://127.0.0.1:${PORT}/minio/health/live" >/dev/null \
    && echo "health: ok" || echo "health: fail"
  ensure_net
  mc_sh "mc ls local/${BUCKET}"
}

case "$cmd" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  *)
    echo "usage: $0 [start|stop|status]" >&2
    exit 2
    ;;
esac
