# Metadata engine qualification

FluxFS keeps metadata callers behind `MetaStore`. The current production path
continues to use heed/LMDB until a repeatable target workload fails its gate;
an unconditional RocksDB swap would add compaction and operational complexity
without evidence that it fixes the limiting layer.

## Reproducible workload

Run an optimized, empty-database benchmark:

```bash
cargo run -p fluxfs-meta --release --example meta_workload -- \
  --files 100000 --operations 1000000 --map-size-gib 4
```

The runner loads a flat directory, then executes a deterministic 70% pathname
lookup / 20% inode read / 10% inode mutation mix. It reports JSON containing
the exact dimensions, p99 operation latency, mixed throughput, database size,
schema version, reopen time, gate, and decision. `--path` may point at an empty
directory on the target production storage; omitting it uses a temporary local
directory. The LMDB map size is configurable and is virtual address capacity,
not preallocated disk usage.

The initial gate is deliberately conservative and machine-independent enough
to catch gross regressions: at least 20k mixed operations/s, lookup p99 no more
than 2ms, mutation p99 no more than 10ms, and reopen no more than 30s. Before a
production launch, rerun on target hardware at 1M, 10M, and the expected inode
count, with concurrent readers and the Raft/RPC end-to-end benchmark. Tighten
the gate from measured service SLOs; never compare results with different
dataset, storage, build profile, or concurrency as if they were equivalent.

## 2026-08-02 local baseline

On the development host, release mode with 100k files and 1M mixed operations
reported:

- load: 67,086 operations/s
- mixed: 241,496 operations/s
- lookup p99: 4us; inode-read p99: 2us; mutation p99: 12us
- reopen: below 1ms; database files: 64,745,472 bytes

This passes the provisional gate by a wide margin. The decision for B1 is to
retain heed now, not to claim that heed is proven at billion-inode scale. A
future LSM implementation is triggered when the target-size run fails a gate,
LMDB's single-writer tail latency violates the service SLO under measured write
concurrency, or operational constraints make its map/checkpoint model unsuitable.

## Schema and engine migration contract

Every Meta database now carries `meta_schema_version`. An unmarked legacy
database is version 0; the current v2 path applies the v1 marker step and the v2
extent-tree encoding step transactionally on open. A newer schema is rejected,
so an older binary cannot silently downgrade or corrupt it.
Each future version must add one explicit step to `migration_path` plus upgrade,
reopen, snapshot/restore, and future-version rejection tests.

Engine replacement uses the OpenRaft state-machine snapshot as the portable
export/import boundary. Engine-specific handles and key/value types remain
private to the backend; VFS, FUSE, client, inode, dentry, and manifest APIs use
only `MetaStore` and FluxFS domain types. A new engine must pass the existing
MetaStore contract/recovery suite, import a snapshot produced by the prior
engine, emit a snapshot that the current version can reinstall, and then pass
the same workload gate before it can become the default.
