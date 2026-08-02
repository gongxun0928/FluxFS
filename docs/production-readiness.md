# Production readiness gap analysis

This document separates verified MVP behavior from temporary designs that must
not be mistaken for a production architecture. It is a prioritization input,
not a production-readiness claim.

## Current verified state

- FUSE supports Ephemeral files and lazy External namespace/read access.
- External random writes perform chunk-aligned read-modify-write. Each touched
  4 MiB window becomes an RF=2 Local extent; untouched ranges remain pinned to
  the imported UFS object.
- New chunks are durable before one metadata state-machine transaction stores
  the immutable manifest and CAS-switches the inode head. A failed CAS can leave
  unreachable data, but cannot expose a partial head.
- The localhost topology is one durable OpenRaft voter, three ChunkWorkers, and
  RF=2 authoritative chunks with a repair spare.
- MinIO/FUSE acceptance covers mixed reads, the backing object remaining
  unchanged before flush, one Worker loss, repair to the spare, and remount.

This is a strong local alpha slice. It does not yet provide metadata HA, UFS
write-back, full POSIX behavior, multi-tenant security, or production operations.

## P0: redesign or complete before production

### Correctness and availability

1. **Metadata consensus is single-voter.** `StubNetwork` cannot contact another
   voter, membership is fixed, and reads go directly to heed. Production needs
   a real multi-voter OpenRaft network, membership changes, snapshot transfer,
   and leader-aware ReadIndex or proven lease reads. Direct local reads become
   unsafe as soon as multiple voters exist.
2. **Client retries are not idempotent.** Raft orders requests but does not
   deduplicate a timeout retry submitted as a new request. Every mutation needs
   a stable client/session/request ID, result retention, and deterministic IDs.
3. **Write paths are inconsistent.** The co-located mount can mutate heed
   directly while the remote path uses Raft. Production must route every
   authoritative mutation through the same state-machine contract.
4. **Namespace transactions are incomplete.** External import currently creates
   an Ephemeral inode and then converts it through separate manifest/inode calls.
   Rename, unlink, and data-head CAS are not one serializable namespace model.
   Add atomic import and directory/inode concurrency control.
5. **UFS flush and recovery are absent.** Production needs durable FLUSH_INTENT,
   reconstruct/Put/verify, conditional publication, generation CAS, replay after
   crash, and explicit DirtyConflict handling. Never mark Clean merely because a
   Put returned success.
6. **Garbage collection is absent.** CAS losers and superseded manifests/chunks
   intentionally leave orphans today. Add reference tracking, safe epochs, GC,
   reconciliation, and deletion retry before unbounded service operation.
7. **Error contracts are lossy.** Some typed tonic errors are reconstructed from
   strings. Use structured error details with stable wire codes and retryability.

### Scale and resource control

1. **heed is a bounded single-writer MVP engine.** Fixed map sizes and the
   current key layout need workload benchmarks and a migration path to a
   partitioned/LSM metadata design without leaking engine types through VFS APIs.
2. **Manifests and snapshots are whole-object structures.** Extents are a linear
   `Vec` serialized as JSON, and Raft snapshots serialize all metadata into one
   in-memory buffer. Use versioned binary schemas, indexed extent trees, and
   streaming/checkpoint snapshots with incremental transfer.
3. **Worker membership and repair are fixed and synchronous.** Exactly three
   endpoints are configured by the client. Topology changes trigger full
   inventories and checksum reads before writes continue. Production needs a
   placement/membership service, failure domains, capacity-aware selection,
   throttled background scrub/repair, pagination, and admission control.
4. **Chunk layout is one file per object.** Directory fsync is correct for the
   alpha but filesystem inode count and small-file amplification will dominate
   at scale. Evaluate packfiles/segments, compaction, and checksum indexes.
5. **The Foyer store is a placeholder.** It is currently a mutex-protected
   in-memory map, not a production HybridCache. Clean data needs bounded DRAM/SSD
   policies and eviction; authoritative Dirty data must remain outside an
   evictable cache.

### Security and operability

1. Meta, Worker, and client RPCs have no authentication, authorization, or TLS.
   Add workload identity, encrypted transport, tenant/mount authorization,
   credential rotation, and audit logs.
2. There are no production SLO signals: add metrics, distributed traces, request
   IDs, health/readiness, repair/flush lag, capacity and conflict alerts.
3. Define schema compatibility, rolling upgrade/downgrade, backup/restore, and
   disaster recovery. Validate with multi-machine chaos, long soak, deterministic
   simulation, and linearizability/fault testing rather than localhost scripts.

## P1: can follow a controlled alpha, but is still temporary

- Ephemeral random writes rebuild the whole file; replace this with the same
  extent-tree RMW machinery used by Dirty files.
- The UFS range cache is process-local FIFO with fixed two-part prefetch. Add
  shared bounded DRAM/SSD cache, adaptive streams, retry/backoff, and global I/O
  concurrency budgets.
- Lazy namespace path state is an in-memory map and imported metadata has no real
  TTL/event invalidation. Make cache entries discardable and persistent open
  handles stable across refresh/restart.
- The synchronous FUSE adapter uses blocking runtime bridges. Production should
  pool async clients, batch/coalesce I/O, propagate cancellation, and control
  backpressure.
- External create/unlink/truncate remain read-only, and rename, fsync durability,
  locks, links, symlinks, xattrs, mmap, permissions/ACLs, and complete errno
  behavior are not implemented. Freeze the supported POSIX contract before a
  general-purpose claim.

## Intentional boundary

`external-consistency = best-effort` is a product choice, not an implementation
accident. Pinned ETags and conditional reads detect many out-of-band replacements
and return a conflict instead of silently mixing bytes, but non-versioned UFS
cannot recover an overwritten old base. Production documentation and telemetry
must make this boundary visible; deployments requiring strong cross-writer
consistency need versioned objects or exclusive external-writer control.
