# Manifest extent tree

Manifest schema v2 replaces the public linear extent vector with
`ExtentTree`, an ordered `BTreeMap` keyed by logical byte offset. Range
positioning and exact lookup are `O(log n)`, followed by `O(k)` work for the
overlapping extents. `replace_range` consumes the immutable snapshot and
updates only overlapping tree entries, avoiding a full linear scan or clone;
the resulting manifest remains a new immutable generation.

The JSON wire form is explicitly versioned:

```json
{"version":1,"entries":[...]}
```

Readers also accept the legacy bare extent array, so schema v1 databases are
upgraded lazily without rewriting every manifest at startup. New writes use
the tree form, the Meta schema marker advances to v2, and older binaries reject
the newer database rather than silently downgrading it. Unknown extent-tree
wire versions and duplicate logical offsets are rejected.

The tree preserves the existing invariants: offsets are ordered, extents do not
overlap, UFS range splits preserve the pinned version and object offset, Local
partial overlap still requires caller-side read-modify-write, and the manifest
root digest is independent of JSON representation. Tests cover legacy/new wire
compatibility, unknown versions, duplicate offsets, split/cover/adjacency, and
tail lookup/replacement in a 100,000-extent manifest.

The MetaStore still persists one serialized immutable manifest blob. Streaming
Raft snapshots and separately persisted/checkpointed tree pages are B3 scope;
this change removes the CPU-side linear lookup/update behavior without claiming
that whole-blob snapshot transfer is solved.
