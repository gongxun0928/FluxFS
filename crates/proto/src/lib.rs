//! Wire contracts. Generated prost/tonic types live under [`meta::v1`].
//! Domain conversion helpers live in [`meta_codec`] — call sites should use
//! `fluxfs_types`, never raw prost messages inside VFS/Manifest logic.

pub mod meta {
    pub mod v1 {
        tonic::include_proto!("fluxfs.meta.v1");
    }
}

pub mod chunk {
    pub mod v1 {
        tonic::include_proto!("fluxfs.chunk.v1");
    }
}

pub mod meta_codec;

pub use chunk::v1::chunk_worker_client::ChunkWorkerClient;
pub use chunk::v1::chunk_worker_server::{ChunkWorker, ChunkWorkerServer};
pub use meta::v1::meta_service_client::MetaServiceClient;
pub use meta::v1::meta_service_server::{MetaService, MetaServiceServer};
