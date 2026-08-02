# FluxFS MVP v0.1 Design (cursor-agent)

Status: proposal for discussion with @ubuntu-cc / @gongxun  
Date: 2026-08-02  
Repo: https://github.com/gongxun0928/FluxFS

## 1. Feasibility (on the 4 scenarios)

**Yes — implementable**, if we treat them as one inode namespace + per-inode `LocalityState` (as @ubuntu-cc sketched), not as two separate filesystems glued later.

| Scenario | Feasibility | MVP? |
|---|---|---|
| 1 Write → FluxFS chunk cache, async flush whole object to UFS | Feasible; write amp accepted | **In** |
| 2 Read miss → lazy load from UFS, same chunk layout | Feasible (Alluxio-like) | **In** (read-only external first) |
| 3 Read-your-writes via dirty flag | Feasible; needs inode state on hot path + client cache | **In** |
| 4 Ephemeral no-UFS (shuffle/logs) | Feasible as mount flag | **In** (single-node/demo OK; multi-replica later) |

Hard constraint we keep explicit: **strong POSIX rename/create atomicity only for data FluxFS owns** (Dirty/Clean/Ephemeral). Pure External (UFS is truth) stays **best-effort** until promote/copy-up.

## 2. Architecture (single kernel)

```
Client (FUSE / Rust SDK)
        │
        ▼
┌─────────────────────────────┐
│ MetaMaster (1 primary +     │  ← Mantle-inspired: durable btree/LSM,
│  N lease-read mirrors)      │     Raft/WAL, follower lease read later
└─────────────┬───────────────┘
              │ inode / dir / locality / chunk map
              ▼
┌─────────────────────────────┐
│ ChunkWorker (N)             │  ← local disk/SSD; hash-addressed chunks
│  - put/get/replicate        │     Dirty/Ephemeral: RF=3 (config)
│  - serve client I/O         │     Clean/External cache: RF=1
└─────────────┬───────────────┘
              │ flush / hydrate
              ▼
┌─────────────────────────────┐
│ UFS Adapter (S3/OSS/Local)  │  ← object key = logical object or hash
└─────────────────────────────┘
```

**Shared (one copy of code):**
- Inode + directory index
- Chunk store API (`put/get/delete` by `ChunkId = content hash`)
- Manifest / txn commit for metadata mutations
- LocalityState machine on each inode

**Not separate mounts in MVP:** one mount; UFS optional (`--ufs s3://bucket` vs `--ephemeral`). Behavior = LocalityState, not mount class.

## 3. LocalityState (MVP subset)

Steady states alone are not enough — need a durable flush intent:

```
Dirty      — has unflushed FluxFS chunks (write cache)
Flushing   — durable flush-intent recorded (generation + idempotency key)
Clean      — flushed; cache optional, UFS has full object
External   — UFS authoritative; FluxFS holds read cache chunks
Ephemeral  — no UFS; FluxFS is sole store
```

Per-inode durable fields (from @ubuntu-gpt56):
- `generation`
- `ufs_etag` / base version
- `extent_root`
- `flush_attempt` / idempotency key

**Clean commit point = metadata CAS after successful Put**, not the Put return itself. Old flusher must not mark a newer generation Clean.

MVP transitions:
- `create/write` → Dirty (or Ephemeral if mount ephemeral); bumps generation as needed
- `read` miss on External → hydrate chunks, stay External (etag-bound extents)
- `read` on Dirty/Flushing → serve from ChunkWorkers (scenario 3 / R-A-W)
- `flush`: Dirty → Flushing (WAL intent) → PutObject → **CAS metadata → Clean**
- External write: **copy-up → Dirty** (default)

Defer: UFS bucket notification invalidation (use TTL / explicit invalidate in MVP).

## 4. Chunk & object model (unify write/read cache)

- Fixed chunk size e.g. **4 MiB** (configurable), JuiceFS-like.
- `ChunkId = blake3(content)` (or xxhash64 for MVP speed + blake3 later).
- File = ordered list of `(offset, ChunkId, len)` in inode extent map (MetaMaster).
- **Write path:** client buffers → chunk → ChunkWorker put → update extent map (txn) → Dirty.
- **Flush path:** read extents in object-aligned range → stream assemble → `PutObject` → update inode `ufs_key` + state Clean → GC unreferenced chunks async.
- **Hydrate path:** `GetObject` range/full → split to same chunk size → put ChunkWorkers → fill extent map → External.

This is the reuse point Gongxun asked for: **one chunk layout for write cache and read cache**.

## 5. POSIX scope for MVP

| Op | MVP |
|---|---|
| lookup / getattr / readdir | Yes |
| create / mkdir / unlink / rmdir | Yes |
| open / read / write / truncate | Yes |
| rename (same FS, non-External) | Yes (atomic via MetaMaster txn) |
| rename involving External without copy-up | **No** → copy-up first or EXDEV |
| hardlink / xattr / flock full | Defer |
| fsync | Flush that inode’s dirty extents (best-effort durability) |

## 6. What we take from references (MVP-level)

From **Mantle** (as cited; PDF should be checked into repo later):
- Single MetaMaster process first; durable tree + WAL
- Compact dir entries goal (engineering target, not 80B/dir on day 1)
- Defer: multi-mirror lease-read, 1.8M lookups/s tuning

From **CFS / MantleX blogs**:
- Namespace evolution mindset: start simple, don’t pretend UFS+POSIX are one truth
- Single-machine → distributed path: code MetaMaster behind a trait so RF=1 → Raft later

From **Alluxio**:
- Lazy namespace materialization for External
- Transparent read hydrate

From **JuiceFS**:
- Chunked random write + async compaction to object storage

## 7. MVP deliverables — revised “restricted alpha” (@ubuntu-gpt56)

Greenfield promising FUSE+SDK+WAL+RF=3+two UFS+External/copy-up+Ephemeral+full crash matrix in 4–6 weeks is **demo-only**. Restricted alpha cut:

**Must keep:**
1. Single MetaMaster leader + WAL
2. **One** S3-compatible UFS (LocalFS for unit tests only)
3. **One** client entry: FUSE (SDK later) *or* SDK-first if FUSE slips — pick one
4. Fixed chunk size + file size caps; whole-object flush; per-inode coarse lock
5. Dirty R-A-W, generation-safe flush, crash recovery
6. External hydrate + TTL/explicit invalidate; copy-up on write
7. Ephemeral mount flag

**Replica policy for alpha:** if RF=3 Dirty/Ephemeral is a core claim, **do not** treat it as W6 optional — cut SDK/second backend instead. If RF=3 is not core for alpha, ship RF=1 first.

**Explicitly out:** HA/Raft, EC, online GC, broad POSIX, bucket notify, second UFS product, dual client surfaces

### Verification gates (merge @ubuntu-gpt56)
- Durable fields: generation, UFS etag/base version, extent root, flush idempotency key
- Deterministic fault hooks around: chunk quorum, manifest/WAL, hydrate HEAD→GET→publish, flush snapshot/reassemble/multipart/Put/CAS/GC, recovery/repair mid-flight; cover kill, worker loss, timeout, lost/duplicate response
- Concurrent schedules: dual write+read, write vs flush, dual flusher, fsync vs bg flush, hydrate vs UFS mutate, dual copy-up, truncate/unlink/rename vs flush, evict/repair vs dirty read
- Property tests: no loss of acked write; External extents etag-bound; old flusher cannot Clean new generation; Ephemeral makes zero UFS calls; idempotent retry; GC never deletes live/pinned chunks; history linearizable to manifest commit
- CI: small model + fault hooks per PR; nightly stress later; ops metrics: dirty age/backlog, flush retry, under-replication, orphan bytes, hash mismatch, recovery time

## 8. Milestone plan (restricted alpha, ~6 weeks with 3–4 eng; 8–12 solo)

| Week | Goal |
|---|---|
| W1 | Skeleton + WAL create/lookup + LocalityState fields (incl. Flushing intent) |
| W2 | Dirty/Ephemeral write/read + R-A-W; RF decision locked |
| W3 | Flush Dirty→Flushing→Put→CAS Clean; crash recovery |
| W4 | External hydrate + copy-up; TTL invalidate |
| W5 | FUSE (or SDK) demo + fault-hook tests for core paths |
| W6 | Stabilize invariants; soak; freeze alpha API |

## 9. Decisions (see also §12)

**Locked (@gongxun msg f7d88680):**
1. Replica: Dirty/Ephemeral default RF=2; Clean/External default RF=1 (optional multi for bandwidth)
2. Ephemeral: `mount --no-ufs`; no nested mounts
3. External write: auto copy-up → Dirty
4. Ephemeral is mainline (simplify later only if blocked)

**Still open:**
- Alpha client: FUSE vs SDK first?
- Flush triggers: size + time + fsync?
- Versioned UFS required for External partial copy-up?

## 10. Risk register (MVP)

| Risk | Mitigation |
|---|---|
| Flush write amp / object rewrite | Accept; size flush batches; document as accelerator |
| MetaMaster SPOF | Explicit MVP limit; trait for Raft later |
| Stale External after out-of-band UFS edit | TTL; document best-effort |
| FUSE POSIX gaps | SDK-first demo if FUSE slips |

---

## 11. Merged from @ubuntu-gpt56 (msgs 85d997c9 / 712ce00a / accdba87)

### Orthogonal state model (replaces naive 4-enum for implementation)

```
BackingMode = UfsBacked | Ephemeral
DataState   = UfsClean | Dirty | DirtyConflict | Ephemeral
OpState     = None | Hydrating(range, token) | Flushing(flush_intent)
Residency   = per extent/chunk (Absent | Fetching | Resident)
```

- Product labels Dirty/Clean/External/Ephemeral OK for UX; **implementation must not use a single inode enum alone**.
- `External` → provenance `origin=Imported` only; correctness same as UfsClean (UFS version is truth, cache droppable).
- Hydrate is **per-extent**, not whole-inode state flip.

### Commit order (hard)
1. Write: chunk RF quorum + fdatasync → then `DATA_COMMIT` WAL → then ack
2. Flush: durable `FLUSH_INTENT` → UFS put (versioned/conditional) → then `UFS_COMMIT` metadata CAS → Clean only if `head_gen` still matches snapshot gen

### Key invariants (selected)
- No ack before chunk quorum + metadata txn
- Single immutable manifest per read linearization point
- Dirty reconstructible from local RF chunks + pinned immutable UFS base version
- Partial copy-up requires versioned UFS read; else full materialize on first partial write
- Old flusher cannot Clean a newer generation
- Ephemeral never enters flushable Dirty via generic write rule
- Safe GC only after no live refs + grace

### Alpha protocol choices
- Per-inode serialize write vs flush in MVP
- Managed: prefer immutable generation object key + metadata pointer for atomic visibility
- External canonical-key write-back: FluxFS-internal strong consistency; external observers best-effort unless pointer/manifest
- `fsync`: seal gen g, DATA_COMMIT, sync flush g, return after UFS receipt + UFS_COMMIT
- Ephemeral fsync: Meta+chunk RF only (or EOPNOTSUPP for UFS durability claims)

### Full tables
See thread msgs 712ce00a (transitions + invariants) and accdba87 (write/flush protocol + fault matrix) in #FluxFS:e8398b75.

---

## 12. Decisions locked by @gongxun (msg f7d88680, 2026-08-02)

| # | Decision |
|---|---|
| Q1 Replica | Dirty/Ephemeral default **RF=2** (configurable; EC later). Clean/External default **RF=1**, optionally multi-replica for read bandwidth. |
| Q2 Ephemeral mount | Mount-level `mount --no-ufs` only. **No nested mounts** under an existing UFS mount (simplify). |
| Q3 External write | **(a) auto copy-up → Dirty** (user-transparent). |
| Q4 Ephemeral scope | **Mainline** capability (shuffle/spill); metadata engine same as other modes. If too hard, may ship as side-effect first — prefer mainline. |

Still open for @gongxun:
- Client surface for alpha: FUSE vs SDK first?
- External/UFS: require versioned read + conditional write, or accept full materialize on first partial write?

## 13. Proposed stack freeze (@gongxun db07a974 / task #3)

Awaiting explicit "栈冻结" from @gongxun; agent recommendation = accept.

| Layer | Choice | MVP plan |
|---|---|---|
| Consensus / MetaMaster | [openraft](https://github.com/databendlabs/openraft) | Single-voter first; Storage + StateMachine for inode/dentry/manifest; scale voters later |
| ChunkWorker cache | [foyer](https://github.com/foyer-rs/foyer) | Hybrid memory+SSD default behind `ChunkStore` trait |
| UFS | [OpenDAL](https://github.com/apache/opendal) + thin wrapper | Prefetch + parallel Range GET; patterns from [ZeroFS](https://github.com/Barre/ZeroFS) (read path), no full fork |
| Tests | ZeroFS-inspired (owner @ubuntu-gpt56 task #3) | Fault inject / integration layout; W1 baseline benches owned with design |

Refs:
- openraft usage article: https://mp.weixin.qq.com/s/yxPsJo8-QPUE4FvB6akYAQ

## 14. Three decisions for @gongxun (@ubuntu-cc 915ab49b)

| # | Options | cursor-agent recommendation |
|---|---|---|
| D1 UFS crate | OpenDAL vs object_store | **OpenDAL** (Gongxun specified); document tradeoff |
| D2 Meta engine | heed vs slatedb | **heed + MetaStore trait**; slatedb later (different HA model) |
| D3 Topology | distributed Master/Worker vs ZeroFS single-process | **crate-split Master/Worker; W1 co-located single binary**; split processes later — avoid rewrite trap |

Locked with cc: openraft, foyer+ChunkStore trait, ZeroFS-inspired tests (gpt56 #3).

## 15. Stack freeze (executed 2026-08-02)

Per @gongxun: proceed with agent-recommended defaults without blocking.

| Layer | Choice | W1 status |
|---|---|---|
| Consensus | openraft | Single-voter writes reach a heed-backed state machine; Raft log/snapshot durability and multi-voter HA remain |
| Meta engine | heed + `MetaStore` trait | create/lookup/readdir/put_inode working |
| Chunk cache | foyer + `ChunkStore` trait | Disk durable + hybrid facade (HybridCache async wire-up next) |
| UFS | OpenDAL | local FS + S3 features; head/range/write_full |
| Client | internal API | shared by CLI; public SDK / get_by_key → v0.2 |
| Access | FUSE + internal CLI | Ephemeral create/read/write/readdir/unlink/truncate mounts locally |
| Topology | crate-split, multi-process localhost path | MetaMaster + 3 ChunkWorkers + FUSE/client communicate over tonic/TCP |
| External consistency | best-effort | documented; no full materialize on partial write |

### W1 exit commands

```bash
cargo test -p fluxfs-meta -p fluxfs-chunk
cargo run -p fluxfs -- info
cargo run -p fluxfs -- smoke --data-dir /tmp/fluxfs-smoke
```
