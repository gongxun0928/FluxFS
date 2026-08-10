# FluxFS MVP implementation status

Status: verified implementation description as of 2026-08-10. This document
describes what is in `main`; it is not a production-readiness claim. The
original [v0.1 design](mvp-v0.1.md) is retained as historical context.

## 1. Architecture and metadata persistence

FluxFS has three runtime roles:

- MetaMaster is the authoritative metadata service. All authoritative mutations
  pass through OpenRaft and a single state-machine API.
- ChunkWorkers store content-addressed 4 MiB chunks in append-only pack
  segments. Dirty/Ephemeral data uses RF=2 by default.
- The FUSE/client process combines Meta RPCs, ChunkWorker RPCs, and an OpenDAL
  UFS adapter. A mount may be Ephemeral (`--no-ufs`) or UFS-backed.

MetaMaster currently has one voter and a stub Raft network. It provides durable
restart recovery, but not metadata high availability.

Metadata uses two heed/LMDB environments:

| Environment | Named databases | Contents |
|---|---|---|
| MetaStore | `inodes`, `dentries`, `manifests`, `client_requests`, `meta` | inode and namespace state, immutable manifests, request deduplication results, write reservations, GC tombstones, worker membership, flush/GC state, schema and state-machine markers |
| RaftStore (`raft/`) | `raft_logs`, `raft_meta` | OpenRaft entries, vote, committed index, and last-purged index |

A normal Raft entry applies its business mutation and advances
`last_applied`/membership in the same MetaStore write transaction. The Raft log
lives in the second environment; OpenRaft replays entries after a crash and the
state-machine marker prevents duplicate application.

An inode records independent metadata, data-head, and UFS generations plus its
locality, UFS locator/version, immutable manifest ID, and optional durable flush
intent. A manifest is one versioned LMDB value containing an ordered extent
tree. Extents are either Worker-owned `Local` chunks or pinned `UfsRange`s.
This representation makes range lookup/update indexed, but very large and
highly fragmented manifests are still serialized as whole values.

Mutation request IDs have a durable 24-hour result ledger. The 10,000-entry
soft cap never evicts an unexpired result: when the in-window ledger is full, a
new mutation fails with `Busy` before taking effect. Time is stamped by the
leader into the replicated request, so state-machine replay is deterministic.

Raft snapshots stream length-delimited records instead of building one full
in-memory blob. Builds and installs use unique temporary files, `sync_all`,
atomic rename, and parent-directory fsync. Startup removes stale managed build,
incoming, installed, and legacy snapshot artifacts; old monolithic JSON
snapshots remain readable.

## 2. Data read and write paths

### Reads

The FUSE read callback asks the client for the inode and its current immutable
manifest, selects overlapping extents, and assembles the requested range:

- `Local` extents are read from the RF ChunkWorker set. A corrupt/missing copy
  falls back to a healthy replica and background repair restores placement.
- `UfsRange` extents use bounded OpenDAL Range GETs. Reads are split into 1 MiB
  parts, fetched in parallel with single-flight suppression and bounded
  prefetch, and keyed by object version in the cache.
- Clean/External bytes may be promoted into the bounded Foyer DRAM+SSD cache.
  Dirty authoritative chunks remain packfile-backed and are not made dependent
  on an evictable cache.

A Dirty file can therefore read its modified windows from Local chunks and its
untouched windows from the pinned UFS object while preserving read-after-write
for writes made through FluxFS.

### Writes

Random writes and truncate operate in 4 MiB windows. For every touched window,
the client reads the previous bytes, overlays the modification, hashes the new
content, and replaces that window in a new immutable manifest. There is no
artificial 1 GiB file-size limit.

The commit order is:

1. Persist a Meta reservation for every Local chunk referenced by the proposed
   manifest.
2. Durably put each staged content-addressed chunk to the required RF=2 Worker
   set.
3. Submit one Raft mutation that generation-CASes the inode, stores the new
   manifest, and consumes the reservation atomically.
4. A failed CAS aborts the reservation; it never exposes a partial new head.

Writing an External file uses the same path and changes only touched windows to
Local extents, producing a sparse Dirty copy-up. Untouched extents keep the
imported UFS version pin.

FUSE `setattr` sends size, mode, owner, and explicit timestamps through one
client operation. Size-changing requests publish the new manifest and POSIX
attributes in one inode generation CAS; metadata-only requests use a restricted
CAS that cannot replace data or lifecycle fields. Writes and size changes are
rejected once an inode is in `DirtyConflict`, while a metadata-only update may
still proceed. The mount advertises `noatime`: reads do not create metadata
writes, but explicit `utimens` updates persist.

`fsync` performs crash-recoverable write-back:

1. Persist a `FlushIntent` for an immutable head generation.
2. Stream that generation to UFS with bounded multipart/conditional publication.
3. Verify the published size and BLAKE3 digest with HEAD/readback metadata.
4. Generation-CAS the inode to Clean and clear the intent. A version mismatch
   becomes `DirtyConflict` rather than silent last-writer-wins.

Startup replays unfinished intents. A process crash before multipart completion
cannot publish a partial object, but the object store must have an incomplete
multipart lifecycle policy to reclaim abandoned parts.

### Namespace, SDK, FUSE, and CLI

The authoritative namespace supports transactional `rmdir` and rename through
the MetaStore, OpenRaft state machine, snapshot-safe heed transaction, tonic
contract, and remote client. Rename moves a dentry and updates both parent
generations atomically, supports destination replacement and no-replace,
rejects file/directory type mismatches, refuses a non-empty destination
directory, and prevents moving a directory into its own subtree. `unlink`
rejects directories and `rmdir` rejects regular or non-empty targets.

FUSE wires those operations to real `rename(2)` and `rmdir(2)` callbacks.
Normal rename and `RENAME_NOREPLACE` are supported; exchange and whiteout are
explicitly rejected. The Rust client exposes validated absolute-path CRUD plus
bounded 4 MiB `Read`/`Write` streaming helpers, so tools do not buffer whole
files in memory.

`fluxfs fs` uses the same remote client and membership-discovered RF=2 Workers
without requiring a FUSE mount. It provides file transfer, namespace CRUD, and
basic numeric POSIX attributes. `put` streams into a temporary inode and makes
the new file visible with one atomic rename; a failed transfer best-effort
removes the temporary name. `fluxfs admin status|workers` is read-only. Both
command groups use the existing TLS flags and are exercised with a distinct
client-admin mTLS identity. Until shell/user identity mapping exists, CLI
creates use bootstrap owner `0:0`; `chown` accepts numeric IDs.

Imported/UFS dentries are not silently changed by the namespace-only path:
unlink, rmdir, rename, and replacement of imported entries fail closed. The
mount-free CLI has no UFS publication adapter; this preserves the existing
External consistency boundary instead of pretending a metadata-only delete or
rename modified the backing object.

## 3. Garbage collection

Physical data deletion is asynchronous. Mount readiness and foreground writes
do not wait for a stop-the-world sweep. The mount runs a background thread with
bounded 32-item passes; it sleeps 5 seconds when idle and 200 ms after making
progress.

| Garbage source | How it becomes reclaimable | Reclamation path |
|---|---|---|
| Client crash after Worker Put but before metadata commit | The durable pre-Put reservation protects the chunk for 15 minutes. After replicated expiry, the chunk is no longer protected. | Inventory-driven GC creates a durable tombstone, deletes each recorded Worker target, persists acknowledgements, then finalizes the tombstone. |
| Unlink with `nlink` reaching zero | The inode is removed and its active reservations are aborted, so its manifest and chunks become unreachable. | The same background manifest/chunk reachability scan and tombstone workflow reclaims them. |
| Overwrite or truncate | The inode CAS points to a new immutable manifest; superseded manifests and chunks are no longer reachable from any current inode. | The same bounded background pass deletes unreachable manifests and tombstones zero-reference chunks. |

Reservations and tombstones survive restart and snapshot restore. Creating a
tombstone fences a concurrent reservation of that content address. Delete
targets are initialized durably; an unavailable Worker remains pending and is
retried after it reconnects, so Meta does not forget a failed replica delete.

Worker `DeleteChunk` removes the pack index entry and evicts any cache entry.
The bytes in an append-only segment are reclaimed later by background packfile
compaction (the default 300-second check compacts when more than one segment
exists) or the admin compaction command.

## 4. Watermarks and backpressure

The MVP has bounded admission controls, but it does not yet have a complete
live-disk high/low-watermark controller.

| Boundary | Default | Behaviour at the boundary |
|---|---:|---|
| ChunkWorker foreground semaphore | 128 operations | fail fast with gRPC `RESOURCE_EXHAUSTED`, which the client maps to typed `Busy` |
| ChunkWorker low-priority GC semaphore | 1 operation | same mapping, independently of foreground permits |
| Remote chunk client queue | 64 operations | `try_send` returns `Busy`; memory does not grow without bound |
| Remote GC queue | 8 operations | same fail-fast behavior on a separate queue |
| Placement minimum | advertised `available_bytes >= 4 MiB` | workers below the threshold or with expired leases are excluded; insufficient RF candidates returns `Busy` |
| Write reservation lifetime | 15 minutes | bounded replicated expiry during GC passes |
| Background GC pass | 32 items | bounded work per scheduling tick |

FUSE maps `Busy` to `EAGAIN`; production code does not contain the acceptance
script's 8-by-250-ms retry loop. RF=2 acknowledgement requires both replicas.
After one replica is lost, existing data remains readable but new authoritative
writes pause until placement/repair again provides RF=2.

MetaMaster exports advertised capacity minimum/sum and
`fluxfs_worker_capacity_low`, while ChunkWorkers export in-flight gauges.
Latency histograms cover success and error paths. RPC clients propagate
`x-fluxfs-request-id`, and Meta/Chunk servers include the ID and operation in
span lifecycle output. This is useful per-RPC cross-process correlation, not a
full OpenTelemetry trace tree or collector.

`available_bytes` is currently the registration/admin value, not sampled free
space. Disk fill/drain therefore does not automatically update placement, and
there are no percentage high/low waterlines or per-tenant quotas yet.

## 5. I/O abstraction and `pread`/`io_uring`

The current Worker I/O abstraction does **not** yet support selecting `pread`
or `io_uring` as an engine.

`ChunkStore` abstracts logical whole-chunk operations, but it is synchronous and
returns owned byte vectors. `PackStore` opens `std::fs::File`, seeks, and calls
`read_exact`; ChunkWorker runs those calls through `spawn_blocking`. There is no
positional-read trait, caller-provided buffer API, async completion model, or
`io_uring` dependency. Foyer currently uses its POSIX synchronous engine.

Supporting both engines cleanly requires a second boundary below `ChunkStore`:

1. Introduce a positional `ChunkIoEngine`/pack-reader interface with explicit
   offset and caller-owned buffers; implement `pread`/`FileExt::read_at` first.
2. Keep logical chunk lookup, checksum, replication, and cache policy above that
   interface.
3. Add an asynchronous submission/completion form for an `io_uring` engine and
   a blocking adapter for the positional engine. Hiding `io_uring` behind the
   current synchronous `Vec<u8>` method would preserve the blocking bottleneck.
4. Benchmark queue depth, buffer ownership, cancellation, and FUSE-to-Worker
   concurrency before choosing the default.

OpenDAL already abstracts UFS backends, but it does not make the local packfile
path `io_uring`-capable. This is an explicit post-MVP performance item rather
than a capability claimed by the current implementation.

## Current production boundaries

- Meta is a durable single voter and remains a metadata SPOF.
- External consistency under out-of-band UFS mutation is deliberately
  best-effort; pinned tokens detect many conflicts but cannot recover an old
  non-versioned object.
- `available_bytes` is administrative rather than live disk telemetry.
- Very large fragmented manifests and global metadata snapshots still have
  scale costs despite indexed extents and streaming transfer.
- Directory-cycle validation for rename scans the moved subtree inside the
  atomic metadata transaction; very large directory trees need an indexed or
  otherwise bounded ancestry design before production scale.
- Managed namespace hard links and symbolic links are supported end to end.
  Extended attributes use a durable side table (64 KiB/value, 64/inode,
  256 KiB total/inode), and Linux POSIX ACL blobs round-trip and inherit as
  `system.posix_acl_access/default`. ACL permission enforcement is deliberately
  not claimed until FluxFS has an authenticated UID/GID identity model.
- Remaining POSIX scope includes locking, mmap coherence, open-unlink lifetime,
  and permission enforcement. Multi-machine chaos/soak, operational upgrades/DR,
  complete capacity control, and positional/async local I/O also remain production work.
- Open-unlink lifetime is not yet guaranteed: final-name unlink currently reaps
  the inode. Durable session references versus daemon-local grace collection is
  an explicit performance/correctness decision, not a completed capability.
