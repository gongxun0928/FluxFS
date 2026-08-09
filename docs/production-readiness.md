# Production readiness gap analysis

This document separates the verified MVP from the work still required for a
production claim. See [MVP implementation status](mvp-status.md) for the current
metadata, data-path, GC, watermark, and local I/O-engine design.

## Verified MVP controls

- All authoritative metadata mutations use one OpenRaft state-machine contract.
  Request IDs have a deterministic, durable retention ledger.
- Chunk RF acknowledgement precedes a generation-CAS manifest commit. Durable
  pre-Put reservations and GC tombstones close the writer/delete race.
- UFS write-back uses a durable intent, conditional bounded publication,
  size/digest verification, generation CAS, and startup reconciliation.
- Worker membership has stable IDs, leases, failure domains, capacity-weighted
  placement, paginated inventory, throttled repair, and durable delete retry
  accounting.
- Chunk storage uses checksummed append-only segments with background
  compaction. Clean data has bounded Foyer DRAM+SSD caching; authoritative Dirty
  data does not depend on eviction-prone cache state.
- RPCs support mTLS workload identity/authorization, structured errors,
  request-ID spans, Prometheus metrics, bounded queues/semaphores, and separate
  low-priority GC admission.
- Meta schema gates, portable streaming snapshots, legacy snapshot decoding,
  and crash-durable snapshot file publication are implemented.

These controls and the localhost FUSE/MinIO acceptance suite make a useful
restricted MVP. They do not establish production availability or scale.

## P0 before production

### Metadata availability and consistency

MetaMaster remains one durable voter with a stub Raft network. Production needs
a real multi-voter transport, membership changes, quorum fault testing,
snapshot transfer between nodes, leader routing, and linearizable reads
(`ReadIndex` or a proven lease-read protocol). Direct local state reads must not
be introduced when multiple voters exist.

### Capacity and resource control

Worker `available_bytes` is an administrative registration value rather than
live free-space telemetry. Add periodic disk sampling, reserved headroom,
hysteretic high/low watermarks, write admission, cache/packfile accounting, and
operator alerts. Add per-tenant/per-mount quotas and validate behavior when a
disk fills during a Put or compaction.

The current heed qualification covers the committed local benchmark and keeps
the engine behind MetaStore boundaries. It is not evidence for billion-inode or
high-concurrency targets. Run target-hardware scale, concurrency, recovery, and
snapshot-install workloads before deciding whether to add an LSM engine. See
[metadata engine qualification](meta-engine.md).

Manifests have indexed extents but each immutable manifest is still serialized
as one value. Raft snapshot transfer streams records, yet snapshot creation must
still scan the full MetaStore. Large fragmented-file and billion-entry designs
need paged manifests, incremental/checkpointed snapshots, reference indexes,
and explicit memory/I/O budgets.

### Local I/O path

Packfile reads are synchronous `seek` + `read_exact` calls executed through
`spawn_blocking`. The current whole-chunk `ChunkStore` API does not support
positional reads, caller-owned buffers, cancellation, or an async completion
engine. Introduce a lower positional I/O interface before claiming `pread` or
`io_uring`, then benchmark both under realistic queue depth and FUSE workloads.

### Security and operations

mTLS and workload authorization exist, but production still needs certificate
issuance/revocation operations, audit retention, secret rotation procedures,
tenant isolation review, rate limits, and external security testing.

Define rolling upgrade/downgrade, backup/restore, disaster recovery, SLOs,
alert routing, dashboards, runbooks, and safe operator controls. Validate them
with multi-machine chaos, long soak, rolling failures, clock skew, disk-full and
network-partition tests rather than only localhost scripts.

## P1 / controlled-alpha boundaries

- Lazy External namespace entries lack a complete TTL/event invalidation model.
  Open handles need explicit identity and refresh rules.
- External create/unlink/truncate and many POSIX operations (locks, links,
  symlinks, xattrs, mmap, ACLs, and complete errno behavior) are not supported.
  Freeze and test the advertised POSIX contract before general use.
- The synchronous FUSE bridge and owned `Vec<u8>` data path add copies and
  blocking transitions. Add batching/coalescing, deadlines/cancellation, buffer
  pools, and end-to-end adaptive admission based on measurement.
- Request-ID spans provide per-RPC cross-process correlation, not a full
  OpenTelemetry trace graph or production collector/export pipeline.
- Incomplete object-store multipart uploads after a process crash require a
  backend lifecycle policy. Old object versions and their retention are also
  UFS/operator-owned.

## Intentional boundary

`external-consistency = best-effort` is a product choice. Pinned ETags and
conditional operations detect many out-of-band replacements and return a
conflict instead of silently mixing bytes, but a non-versioned UFS cannot
recover an overwritten old base. Deployments requiring strong cross-writer
consistency need object versioning or exclusive external-writer control.
