# FluxFS MVP test strategy

The MVP test pyramid starts with an executable reference model and named crash
boundaries, then runs the same contracts against real implementations. Tests
must print a seed before execution and accept `FLUXFS_TEST_SEEDS` for exact
replay. A failing random seed is a test artifact, not just log trivia.

## Layers

1. **Model/property tests** — generate create, lookup, write, flush, fsync,
   rename, unlink, evict, crash, and recover operations against a small oracle.
2. **Component contract suites** — every `MetaStore`, evictable `CacheStore`,
   authoritative `ReplicaStore`, and `UfsAdapter` implementation runs the same
   behavioral suite. UFS cases are capability-gated (range read, version read,
   conditional write, multipart, list); unsupported behavior must return a
   typed capability error.
3. **Crash/failpoint tests** — restart at each named boundary in
   `fluxfs_testkit::failpoints`; the `fail` registry is process-global, so these
   tests run serially.
4. **Distributed simulation** — use OpenRaft's Turmoil-style deterministic
   network and a serial client oracle. First prove safety under partitions,
   delay, duplication, leader crash, and snapshot install; then heal the world
   and prove liveness. Followers do not claim lease reads until a separate
   lease/fencing design is tested.
5. **Real integration** — MinIO + three MetaMasters + three ChunkWorkers + FUSE.
   Kill and pause each service, partition Meta and Worker traffic separately,
   inject disk errors, corrupt one replica, and restart from cold disks.
6. **POSIX conformance** — run a versioned allowlist of pjdfstest cases on the
   FUSE mount. Add selected xfstests only after basic namespace, open/unlink,
   fsync, rename, truncate, permissions, and timestamp behavior is stable.
7. **Soak/performance** — replay known seeds on every PR; run fresh-seed DST and
   fault campaigns nightly. Benchmark native UFS HEAD against External cold and
   warm lookup across path depth and directory size before setting an SLO.

## Required safety invariants

- Every acknowledged write survives process and leader restart.
- DATA_COMMIT never references an authoritative chunk below its configured
  durable replication factor (RF=2 by default).
- Dirty and Ephemeral authoritative chunks are pinned and never cache-evicted.
- Clean is visible only after UFS_COMMIT for the inode's current generation.
- A stale flusher cannot mark a newer head Clean.
- Retries are idempotent across client, Raft, chunk replication, and UFS publish.
- GC deletes only unreachable and unpinned chunks; accounting reconciles with
  an authoritative scan.
- Clean reads do not combine extents from different UFS object tokens.
- External consistency is best-effort: detectable ETag/version conflicts are
  reported, never silently overwritten, but out-of-band mutation is outside the
  through-FluxFS consistency guarantee.
- An open inode retains stable identity for the lifetime of its FUSE handle.

## pjdfstest integration

The first CI job follows the proven ZeroFS shape: install FUSE 3 and pjdfstest,
start MinIO, start FluxFS, mount it, wait with `mountpoint`, run an explicit test
allowlist, and always unmount/kill services. Start with cases matching the
declared alpha surface instead of copying ZeroFS's mature exclusions. Each
excluded upstream test needs a tracked reason: unsupported-by-scope, known bug,
or environment limitation. Pin the pjdfstest revision so upstream changes do
not silently change the gate.

Initial suites: basic open/read/write/stat, mkdir/rmdir, rename within one
mount, unlink/open-handle lifetime, truncate, fsync, permission checks, and
64-bit timestamps. Exclude hard links, nested/cross-mount rename, mmap coherence,
file locking, xattrs, and other semantics explicitly outside v0.1.

## MVP staging and gates

- **W1:** `cargo fmt`, `cargo clippy --workspace --all-targets`, and
  `cargo test --workspace`; executable reference model, named failpoints, seed
  replay convention, and benchmark matrix exist.
- **W2:** Dirty/Ephemeral RF=2 read/write tests plus one real three-node Raft
  partition/leader-crash test. Single-voter wiring is not sufficient evidence.
- **W3:** flush/fsync/crash matrix proves publish and generation-CAS ordering.
- **W4:** External hydrate/copy-up tests cover range reads and object-token
  changes for versioned and non-versioned UFS backends.
- **W5:** FUSE integration and the declared pjdfstest allowlist pass.
- **W6:** nightly deterministic soak, Jepsen-style HA faults, and baselines with
  environment metadata. Performance numbers remain measurements, not promises.

Useful upstream patterns: [OpenRaft's test suite](https://github.com/databendlabs/openraft),
[ZeroFS deterministic/failpoint/POSIX tests](https://github.com/Barre/ZeroFS),
[Foyer fuzzy tests](https://github.com/foyer-rs/foyer), and
[OpenDAL capability-gated behavior tests](https://github.com/apache/opendal).
