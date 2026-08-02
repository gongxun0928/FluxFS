# FluxFS Restricted Alpha — Must / Must-not checklist

Owner: @cursor-agent (merge of @ubuntu-cc + @ubuntu-gpt56 + @gongxun Q1–Q4)  
Date: 2026-08-02  
Status: ready for @gongxun confirm on A/B defaults below

## Locked product decisions

| Item | Decision |
|---|---|
| Replica | Dirty/Ephemeral default **RF=2**; Clean/External default **RF=1** (optional multi for read BW) |
| Ephemeral | `mount --no-ufs`; **no nested mounts** |
| External write | auto **copy-up → Dirty** |
| Ephemeral role | mainline (same meta engine) |
| State model | implement `BackingMode × DataState × OpState × per-extent Residency` (not single 4-enum) |
| Commit order | chunk quorum → DATA_COMMIT → ack; FLUSH_INTENT → UFS durable → UFS_COMMIT/CAS |

## Proposed defaults (agent consensus — confirm)

| Item | Proposal |
|---|---|
| **A. Client** | **FUSE + internal CLI/test client**; public SDK → v0.2 |
| **B. UFS** | **S3-compatible with versioning** (dev: MinIO); LocalFS for unit tests only; require versioned read + conditional replace for partial copy-up, else full materialize + no external concurrent mutate |

## Alpha MUST

### MetaMaster
- [ ] Single leader + durable WAL (`fdatasync`)
- [ ] Inode fields: uuid, head_gen, head_root, ufs_gen/locator/version, flush_intent, origin, backing_mode
- [ ] Dir create/lookup/readdir/unlink/rename (same FS; External rename → copy-up or EXDEV)
- [ ] Per-inode serialize write vs flush
- [ ] Recovery: replay WAL; rebuild Dirty from `head_gen > ufs_gen`; reconcile flush intents

### ChunkWorker
- [ ] Content-addressed chunks; fixed size (e.g. 4MiB)
- [ ] Dirty/Ephemeral write ack only after **RF=2 durable** (both replicas `fdatasync`)
- [ ] On replica loss: reads OK if one remains; **pause new writes** to that chunk until repaired (no silent RF=1 write unless explicit danger flag)
- [ ] Clean/External cache RF=1

### UFS / Flusher
- [ ] One S3-compatible backend (+ LocalFS test double)
- [ ] Whole-object flush from immutable generation snapshot
- [ ] Managed path: prefer immutable generation object key + metadata pointer
- [ ] External: pin VersionId; conditional replace on flush; conflict → DirtyConflict (no silent LWW)
- [ ] Clean commit = metadata CAS after Put success (never Put return alone)
- [ ] Old flusher cannot Clean a newer head_gen

### Client
- [ ] FUSE mount: `--ufs s3://...` or `--no-ufs`
- [ ] Internal Rust client API shared by FUSE + tests (not public SDK yet)
- [ ] Read-your-writes: serve Dirty/Flushing from manifest overlay
- [ ] `fsync`: seal gen g → flush g → return after UFS_COMMIT (Ephemeral: Meta+RF only)

### Scenarios (Gongxun 1–4)
- [ ] 1 Write cache → async flush to UFS
- [ ] 2 External read hydrate (same chunk layout)
- [ ] 3 Dirty R-A-W
- [ ] 4 Ephemeral `--no-ufs`

### Verification (gate)
- [ ] Fault hooks: WAL, chunk quorum, hydrate stages, flush intent/Put/CAS, recovery
- [ ] Properties: no loss of acked write; etag/version-bound External extents; generation-safe Clean; Ephemeral zero UFS calls; idempotent retry; safe GC
- [ ] Concurrent schedules: write vs flush, dual flusher, hydrate vs UFS mutate, copy-up races

## Alpha MUST NOT

- [ ] Meta HA / Raft / lease-read mirrors
- [ ] Public SDK as v0.1 deliverable
- [ ] Second production UFS product (e.g. HDFS)
- [ ] Nested mounts / mount trees
- [ ] EC, compression, encryption
- [ ] Online aggressive GC as correctness path (deferred GC + grace OK)
- [ ] Full POSIX (xattr, flock, mmap edge, ACL)
- [ ] Bucket notification invalidation (TTL / explicit invalidate only)
- [ ] Default RF=1 writes for Dirty/Ephemeral
- [ ] Auto-open GitHub PRs (push fork branch only unless @gongxun says so)

## Week-1 skeleton (after A/B confirm)

Push to `gongxun0928/FluxFS` branch only:
```
fluxfs/
  crates/
    meta/      # WAL + inode stubs
    chunk/     # put/get RF=2 stubs
    ufs/       # S3+LocalFS traits
    client/    # internal API
    fuse/      # mount skeleton
  docs/
    mvp-v0.1.md
    alpha-checklist.md
```

W1 exit: create/lookup via meta WAL + single-process chunk put/get smoke; plus baseline benches (native UFS HEAD, External cold/warm lookup, path-depth × dir-size) — report numbers, no Mantle/µs SLO promises.
EOF
## Review amendments (@ubuntu-cc msg 3d6c2e40) — accepted

1. **Meta store (alpha)**: use **heed (LMDB)** for embedded ACID. B1's
   [qualification workload](meta-engine.md) keeps it while the measured gate
   passes; target-scale/concurrent-write evidence, not engine preference,
   triggers a RocksDB/LSM evaluation.
2. **File size cap (alpha)**: e.g. **1 GiB** max to avoid multipart flush complexity (single Put path).
3. **ChunkWorker topology**: deploy **3 workers** in alpha so RF=2 can tolerate 1 failure; W2 must exercise 3-node failure modes.
4. **proptest in W1**: pin property-test deps in skeleton; invariants must be enforceable from day 1, not bolted on in W3.

## Refinements (@ubuntu-gpt56 + @ubuntu-cc, msgs fb2ca008 / f1501f6f)

1. **1 GiB cap is NOT global file size**: applies only to **Dirty/Ephemeral writes and whole-object flush/copy-up**. External read must support large objects via **Range GET** (core transparent-access promise). Over-cap writes → clear capability error.

2. **MetaStore trait in W1**: heed is alpha **default**, not a frozen engine choice. Freeze trait boundary: create/lookup/update/reopen/recovery; **no engine types leak** into inode/manifest API (so later LSM/Mantle path is swap, not rewrite).

3. **3 workers + RF=2**: after one failure → **still readable**; **not** still RF=2 writable. Pause authoritative (Dirty/Ephemeral) new writes on under-replicated chunks until repaired.

4. **Verification triad from W1**: proptest **and** deterministic fault hooks **and** reference model — proptest does not replace the other two.

## B locked (@gongxun msg 9f36a89f) — external consistency is best-effort

**Product stance:** FluxFS does **not** guarantee consistency under concurrent external UFS mutation of the same object.
- Without versioning: document torn-file risk if sparse Dirty still refs live UFS key
- With versioning: document that write conflicts / races with external writers can still occur; not guaranteed

**Alpha engineering implication:**
- Do **not** hard-require bucket versioning for v0.1
- Implement opportunistic safety when cheap (ETag pin, If-Match on flush if backend supports) but treat as best-effort
- Docs must state: concurrent external rewrite of an object while open/Dirty in FluxFS is unsupported / not consistent
- FluxFS-internal multi-client consistency (via MetaMaster) remains in scope

### Refined wording (@ubuntu-gpt56 + locked)

Mode name: **`external-consistency = best-effort`**

- Guarantees apply only when **all access goes through FluxFS** (read-after-write, atomic manifest, acknowledged writes durable).
- Out-of-band UFS mutation is out of scope; TTL/explicit refresh eventually observes Clean/External changes — no instant visibility promise.
- ETag/If-Match: best-effort conflict detection + clear errors when supported; never silent overwrite when detectable.

## External partial write (v0.1 simplify, @ubuntu-cc)

Given `external-consistency = best-effort`: **no full materialize** on External partial write.
- Write dirty extents into chunk write-cache directly; sparse Dirty may still ref live UFS for unread ranges
- Document torn-file / mixed-version risk under out-of-band UFS mutation
- Saves W2/W3 complexity; revisitable if product later wants stronger External semantics

## A — agent consensus (awaiting @gongxun)

@ubuntu-cc + @ubuntu-gpt56 + @cursor-agent: **FUSE + internal CLI**, public SDK later.

## Namespace (awaiting @gongxun ack; agent consensus with @ubuntu-cc)

- **dentry + inode required** for FUSE/POSIX alpha (cannot skip)
- All LocalityStates share one model
- External: lazy import on lookup/LIST; **persist** dentry/inode + TTL/explicit refresh
- Object-flat / no-dentry surface: out of alpha

## Namespace persistence tiers (@ubuntu-gpt56 efc5afea — adopted)

- Mount root (UFS endpoint/bucket/prefix/policy): durable
- Lazy dentry→inode: on lookup/readdir; no recursive pre-import of whole bucket
- Dirty/Ephemeral + FluxFS create/rename/unlink dentries: authoritative durable
- Pure External discoveries: TTL cache only; discardable & rebuildable from UFS (not SoT)
- Clean/External: durable `UfsObject{key,size,etag/mtime}`; hydrated chunks in evictable cache index (not authoritative manifest); Dirty extent overlay only on partial write
- inode/nodeid stable for open handle lifetime
- alpha: no hardlinks; minimal link_count for stat

## Point-lookup product fork (awaiting @gongxun; @ubuntu-cc b6fb368c)

- Dual surface agreed: FUSE/dentry for POSIX; optional `get_by_key` bypass for known-key (shared inode/chunk)
- Mantle ref: 1.8M lookups/s includes path resolve; alpha hot <10μs / cold <1ms plausible with heed
- Deep paths: packed dir pages (Mantle 80B/dir style), not inherent dentry flaw
- W1 scope:
  - FUSE-repeated-IO primary → MetaStore + chunk smoke; get_by_key → v0.2
  - known-key primary → W1 must include internal get_by_key alongside FUSE

### Perf stance (@ubuntu-gpt56 bf20a6d4)

- Do NOT promise Mantle 1.8M/s or hot<10µs/cold<1ms for FluxFS alpha
- Point lookup: hash key (parent,name); readdir: separate ordered index
- External cold with full relative path: compose key → one HEAD + lazy inode
- W1 must benchmark: native UFS HEAD vs External cold/warm vs path depth / dir size

## Stack (proposed freeze — await @gongxun)

- MetaMaster: **openraft** (single voter in W1)
- ChunkWorker hybrid cache: **foyer** behind ChunkStore trait
- UFS: **OpenDAL** + prefetch/parallel Range; ZeroFS as read-path reference only
- Tests: ZeroFS-inspired → @ubuntu-gpt56 task #3; W1 baseline benches still required
