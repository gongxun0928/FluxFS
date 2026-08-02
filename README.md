# FluxFS

Unified write-cache (JuiceFS-like) + transparent UFS read (Alluxio-like) filesystem — Rust MVP.

**Repo:** https://github.com/gongxun0928/FluxFS

## Status

The current MVP can mount a local Ephemeral (`--no-ufs`) filesystem. Metadata
is persisted with heed; authoritative chunks are acknowledged after two local
replicas durably store and checksum them. Basic create/read/write, random write,
truncate, mkdir/readdir, unlink, unmount/remount, process-crash recovery, and
single-replica read fallback are executable. OpenRaft replication and UFS-backed
lazy read/write-back remain next-stage work.

Design: [`docs/mvp-v0.1.md`](docs/mvp-v0.1.md) · Alpha gates: [`docs/alpha-checklist.md`](docs/alpha-checklist.md)

## Workspace

```
crates/
  types/    shared inode/dentry/chunk types
  meta/     MetaStore trait, heed backend, openraft stubs
  chunk/    RF=2 authoritative disk store + evictable foyer facade
  ufs/      OpenDAL adapter
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

# Ephemeral local mount (no UFS)
mkdir -p /tmp/fluxfs-data /tmp/fluxfs-mnt
cargo run -p fluxfs -- mount --no-ufs --data-dir /tmp/fluxfs-data --mountpoint /tmp/fluxfs-mnt
# other terminal:
#   echo hi > /tmp/fluxfs-mnt/hi.txt && cat /tmp/fluxfs-mnt/hi.txt
# unmount:
#   fusermount3 -u /tmp/fluxfs-mnt
```

## Locked product boundaries (alpha)

- `external-consistency = best-effort` for out-of-band UFS mutation
- dentry + inode namespace; External lazy + rebuildable TTL cache
- Dirty/Ephemeral default RF=2; Clean/External cache RF=1
- Ephemeral via `--no-ufs`; no nested mounts; External write → copy-up → Dirty
- Dirty/Ephemeral write + whole-object flush cap: 1 GiB; External large reads via Range GET
