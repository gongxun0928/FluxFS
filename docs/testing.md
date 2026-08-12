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

The reproducible runner is `scripts/test-pjdfstest.sh`. It checks out
pjdfstest revision `85a8aea9e685999ef0540392fd80535f873d7ff7`, builds it from
source, mounts FluxFS as root, runs each selected `.t` file independently, and
always tears down the mount and its isolated MinIO container. Required host
tools are FUSE 3, Git, Autoconf/Automake, Make, Perl `prove`, Python 3, and
passwordless `sudo`; the External lane also requires Docker and curl.
The pjdfstest source is exported from the pinned Git object into a fresh
temporary build tree on every lane; the MinIO server and client images are
digest-pinned in `scripts/pjdfstest/pin.env`.

Run the lanes separately or together:

```bash
scripts/test-pjdfstest.sh ephemeral
scripts/test-pjdfstest.sh external-minio
scripts/test-pjdfstest.sh all
```

JSON, JUnit XML, raw TAP, and mount logs are written under
`target/pjdfstest-reports/`. Reports record the pinned pjdfstest revision,
FluxFS commit and dirty state, plus SHA-256 digests of the suite and known-fail
documents. A pin upgrade is a dedicated review change and must regenerate both
lanes' baselines. Each case has a 60-second default timeout, overridable with
`FLUXFS_PJDFSTEST_CASE_TIMEOUT`. A dirty FluxFS worktree is rejected by default;
local harness development may opt in explicitly with
`FLUXFS_PJDFSTEST_ALLOW_DIRTY=1` and the report will record that state.

The gate is fail-closed: suite and known-fail entries must name exact test
files, wildcard/duplicate/orphan entries are rejected, an unexpected failure
blocks, and an unexpected pass also blocks until its obsolete exclusion is
removed. A known failure with `reason=bug` remains blocking; only individually
reviewed `deferred` or `env-limit` entries are non-blocking. Even those must
match the exact expected number of non-TODO `not ok` lines, and every such line
must match its allowed TAP failure signature. This prevents a new failure from
hiding behind one expected substring or a deferred matrix from silently
shrinking. Each exclusion also records a category and concrete detail.

The initial Ephemeral suite has 42 files: 26 pass and 16 are exact deferred
cases. Green coverage includes mkdir/rmdir, rename, hard/symbolic links,
unlink, open flags/modes, truncate/ftruncate, subsecond and post-2038
timestamps, and `unlink/14.t` open-after-final-unlink semantics. The 16
deferred files contain special-file matrices or full UID/GID permission
enforcement checks outside v0.1. The separate External-MinIO lane has 11 exact
expected failures documenting the intentional fail-closed namespace-mutation
contract; it is a negative contract baseline, not a claim that External
namespace mutation works.

Still outside this first allowlist are mmap coherence, file locking, special
files, ACL permission enforcement, cross-mount behavior, and pjdfstest coverage
for Linux xattrs/ACL storage and fsync crash recovery. Those remain covered by
FluxFS-native integration tests until a suitable upstream or supplemental
conformance suite is added.

## MVP staging and gates

- **W1:** `cargo fmt`, `cargo clippy --workspace --all-targets`, and
  `cargo test --workspace`; executable reference model, named failpoints, seed
  replay convention, and benchmark matrix exist.
- **W2:** Dirty/Ephemeral RF=2 read/write tests plus one real three-node Raft
  partition/leader-crash test. Single-voter wiring is not sufficient evidence.
- **W3:** flush/fsync/crash matrix proves publish and generation-CAS ordering.
  `scripts/test-large-file-minio.sh` is the real 1025 MiB gate: bounded copy-up,
  multipart ETag, conditional fsync publication, byte comparison, and remount.
- **W4:** External hydrate/copy-up tests cover range reads and object-token
  changes for versioned and non-versioned UFS backends.
- **W5:** FUSE integration and the declared pjdfstest allowlist pass.
- **W6:** nightly deterministic soak, Jepsen-style HA faults, and baselines with
  environment metadata. Performance numbers remain measurements, not promises.

Useful upstream patterns: [OpenRaft's test suite](https://github.com/databendlabs/openraft),
[ZeroFS deterministic/failpoint/POSIX tests](https://github.com/Barre/ZeroFS),
[Foyer fuzzy tests](https://github.com/foyer-rs/foyer), and
[OpenDAL capability-gated behavior tests](https://github.com/apache/opendal).
