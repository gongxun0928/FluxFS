//! Chunk storage: trait boundary + simple disk backend.
//!
//! foyer hybrid cache is available behind [`FoyerChunkStore`] for MVP memory+SSD.

mod disk;
mod foyer_store;
mod remote;
mod replicated;
mod store;

pub use disk::DiskChunkStore;
pub use foyer_store::FoyerChunkStore;
pub use remote::{RemoteReplicatedChunkStore, RepairReport, DEFAULT_MAX_PENDING_CHUNK_OPS};
pub use replicated::{PutReceipt, ReplicaHealth, ReplicatedChunkStore};
pub use store::ChunkStore;
