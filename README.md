# FluxFS

Unified write-cache (JuiceFS-like) + transparent UFS read (Alluxio-like) filesystem — Rust MVP.

**Repo:** https://github.com/gongxun0928/FluxFS

## Status

Week-1 skeleton: MetaStore (heed) + ChunkStore (disk/foyer facade) + OpenDAL UFS + internal CLI smoke. FUSE crate stubbed. openraft types declared for MetaMaster.

Design: [`docs/mvp-v0.1.md`](docs/mvp-v0.1.md) · Alpha gates: [`docs/alpha-checklist.md`](docs/alpha-checklist.md)

## Workspace

```
crates/
  types/    shared inode/dentry/chunk types
  meta/     MetaStore trait, heed backend, openraft stubs
  chunk/    ChunkStore trait, disk + foyer hybrid facade
  ufs/      OpenDAL adapter
  client/   internal API (not public SDK)
  fuse/     FUSE skeleton
  fluxfs/   co-located binary + CLI
```

## Quick start

```bash
cargo test --workspace
cargo run -p fluxfs -- info
cargo run -p fluxfs -- smoke --data-dir /tmp/fluxfs-smoke

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
