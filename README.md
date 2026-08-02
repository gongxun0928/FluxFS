# FluxFS

Unified write-cache (JuiceFS-like) + transparent UFS read (Alluxio-like) filesystem — Rust MVP.

**Repo:** https://github.com/gongxun0928/FluxFS

## Status

The current MVP can mount a local Ephemeral (`--no-ufs`) filesystem. Metadata
is persisted with heed; authoritative chunks are acknowledged after two local
replicas durably store and checksum them. UFS I/O is OpenDAL (local FS + S3/MinIO
via `scripts/dev-minio.sh` / `fluxfs ufs-check`); External objects are lazily
imported into a read-only FUSE namespace and read with pinned, bounded Range GETs.
Basic create/read/write, random write,
truncate, mkdir/readdir, unlink, unmount/remount, process-crash recovery, and
single-replica read fallback are executable. Dirty copy-up and safe UFS
write-back remain next-stage work.

The same Ephemeral path also runs as five localhost processes: one MetaMaster,
three ChunkWorkers, and one FUSE/client. Meta and chunk traffic use tonic/TCP;
worker-0/1 form the initial RF=2 set and worker-2 is a repair spare. A Worker
topology change triggers a checksum-valid inventory sweep before the next write;
missing replicas are copied to healthy Workers until RF=2 is restored.
Meta writes pass through an OpenRaft single-voter state machine. Vote/log live
under `meta/raft/`; inode mutations and SM `last_applied` commit in one MetaStore
write txn. Snapshots export/import full inode/dentry/manifest state. This is
durable single-voter recovery, not multi-voter metadata HA.

Design: [`docs/mvp-v0.1.md`](docs/mvp-v0.1.md) · Alpha gates: [`docs/alpha-checklist.md`](docs/alpha-checklist.md)

## Workspace

```
crates/
  types/    shared inode/dentry/chunk types
  meta/     MetaStore trait, heed backend, openraft single-voter wiring
  proto/    tonic/protobuf Meta and ChunkWorker contracts
  metamaster/ independent heed + tonic metadata process
  chunk/    RF=2 authoritative disk store + evictable foyer facade
  chunkworker/ independent durable chunk process
  ufs/      OpenDAL adapter + bounded range cache/single-flight prefetch
  client/   internal API (not public SDK)
  fuse/     Ephemeral FUSE operations
  fluxfs/   co-located binary + CLI
```

## Quick start

```bash
cargo test --workspace
cargo run -p fluxfs -- info
cargo run -p fluxfs -- smoke --data-dir /tmp/fluxfs-smoke

# Automated local acceptance (exit 77 when FUSE is unavailable)
./scripts/test-local-mount.sh
./scripts/test-local-crash-restart.sh
./scripts/test-multiprocess.sh
./scripts/test-ufs-minio.sh

# Ephemeral local mount (no UFS)
mkdir -p /tmp/fluxfs-data /tmp/fluxfs-mnt
cargo run -p fluxfs -- mount --no-ufs --data-dir /tmp/fluxfs-data --mountpoint /tmp/fluxfs-mnt
# other terminal:
#   echo hi > /tmp/fluxfs-mnt/hi.txt && cat /tmp/fluxfs-mnt/hi.txt
# unmount:
#   fusermount3 -u /tmp/fluxfs-mnt

# UFS test bed (MinIO) + OpenDAL smoke
bash scripts/dev-minio.sh   # prints FLUXFS_UFS_* exports
export FLUXFS_UFS_ENDPOINT=http://127.0.0.1:9000
export FLUXFS_UFS_BUCKET=fluxfs
export FLUXFS_UFS_REGION=us-east-1
export FLUXFS_UFS_ACCESS_KEY=minioadmin
export FLUXFS_UFS_SECRET_KEY=minioadmin
cargo run -p fluxfs -- ufs-check

# Read-only External mount over the test bucket
cargo run -p fluxfs -- mount --ufs s3://fluxfs --data-dir /tmp/fluxfs-external-data --mountpoint /tmp/fluxfs-mnt
```

## Locked product boundaries (alpha)

- `external-consistency = best-effort` for out-of-band UFS mutation
- dentry + inode namespace; External lazy + rebuildable TTL cache
- Dirty/Ephemeral default RF=2; Clean/External cache RF=1
- Ephemeral via `--no-ufs`; no nested mounts; External write → copy-up → Dirty
- Dirty/Ephemeral write + whole-object flush cap: 1 GiB; External large reads via Range GET
