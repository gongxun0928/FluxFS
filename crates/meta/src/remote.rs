//! Sync [`MetaStore`] façade over tonic MetaService (for FUSE / CLI).

use crate::store::MetaStore;
use fluxfs_proto::meta::v1::{
    AbortChunkReservationRequest, AcknowledgeGcDeletesRequest, BeginFlushRequest, BeginGcRequest,
    CommitFlushRequest, CommitInodeManifestRequest, CommitInodeManifestReservedRequest,
    CreateRequest, CurrentGcPlanRequest, ExpireChunkReservationsRequest, FailFlushConflictRequest,
    FinalizeGcTombstonesRequest, FinishGcRequest, GetInodeRequest, GetManifestRequest,
    GetWorkerMembershipRequest, ImportExternalRequest, InitializeGcDeleteTargetsRequest,
    ListFlushIntentsRequest, ListGcTombstonesRequest, LookupRequest, PutInodeRequest,
    PutManifestRequest, ReaddirRequest, RegisterWorkerRequest, ReserveChunksRequest,
    TombstoneGcBatchRequest, UnlinkRequest,
};
use fluxfs_proto::meta_codec::{
    decode_dentries, decode_flush_intents, decode_gc_batch, decode_gc_plan, decode_gc_tombstones,
    decode_inode, decode_manifest, decode_worker_membership, encode_chunk_ids, encode_flush_intent,
    encode_gc_delete_acks, encode_inode, encode_manifest, encode_ufs_object,
    encode_worker_registration, encode_worker_targets, file_type_to_wire, flux_from_status,
};
use fluxfs_proto::MetaServiceClient;
use fluxfs_types::{
    ChunkId, Dentry, FileType, FlushId, FlushIntent, FluxError, GcBatch, GcLeaseId, GcPlan,
    GcTombstone, Inode, InodeId, Manifest, ManifestId, RequestOpId, Result, UfsObject,
    WorkerMembership, WorkerRegistration, WorkerTargetId, WriteTicketId, ROOT_INODE,
};
use std::future::Future;
use std::sync::Mutex;
use tokio::runtime::{Handle, Runtime};
use tonic::transport::Channel;

pub struct RemoteMetaStore {
    /// Owned runtime only when constructed outside an existing Tokio context.
    rt: Option<Runtime>,
    client: Mutex<MetaServiceClient<Channel>>,
}

impl RemoteMetaStore {
    /// Plaintext connect (tests only — production dials via [`Self::connect_tls`]).
    ///
    /// Refuses `http://` unless `insecure_dev` is true, to prevent silent
    /// plaintext downgrade in production. Bare `host:port` is treated as
    /// plaintext and also requires `insecure_dev`.
    pub fn connect(addr: impl AsRef<str>, insecure_dev: bool) -> Result<Self> {
        Self::connect_tls(addr, None, insecure_dev)
    }

    /// Connect with optional TLS (task #30 C1 Phase 2).
    ///
    /// - `tls=Some(opts)`: builds a tonic `ClientTlsConfig` from `opts` and
    ///   dials with TLS + mTLS client identity.
    /// - `tls=None`: plaintext. `insecure_dev` must be true.
    pub fn connect_tls(
        addr: impl AsRef<str>,
        tls: Option<fluxfs_tls::ClientTlsOptions>,
        insecure_dev: bool,
    ) -> Result<Self> {
        use tonic::transport::Endpoint;

        let addr = addr.as_ref().to_string();
        let tls_enabled = tls.is_some();
        let url = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr
        } else if tls_enabled {
            format!("https://{addr}")
        } else {
            format!("http://{addr}")
        };
        let gate = fluxfs_tls::InsecureDev::allow(insecure_dev);
        gate.check_endpoint(&url)
            .map_err(|e| FluxError::Meta(e.to_string()))?;
        gate.check_scheme_matches_tls(&url, tls_enabled)
            .map_err(|e| FluxError::Meta(e.to_string()))?;

        let build_endpoint = || -> Result<Endpoint> {
            let mut endpoint = Endpoint::from_shared(url.clone())
                .map_err(|e| FluxError::Meta(format!("meta endpoint: {e}")))?;
            if let Some(opts) = tls.as_ref() {
                let cfg = opts
                    .build_config_blocking()
                    .map_err(|e| FluxError::Meta(e.to_string()))?;
                if let Some(cfg) = cfg {
                    endpoint = endpoint
                        .tls_config(cfg)
                        .map_err(|e| FluxError::Meta(format!("meta tls_config: {e}")))?;
                }
            }
            Ok(endpoint)
        };

        let (rt, client) = if let Ok(handle) = Handle::try_current() {
            let endpoint = build_endpoint()?;
            let _ = &handle;
            let client = MetaServiceClient::new(endpoint.connect_lazy());
            (None, client)
        } else {
            let rt = Runtime::new().map_err(|e| FluxError::Meta(e.to_string()))?;
            let endpoint = build_endpoint()?;
            let client = MetaServiceClient::new(endpoint.connect_lazy());
            (Some(rt), client)
        };

        Ok(Self {
            rt,
            client: Mutex::new(client),
        })
    }

    fn client(&self) -> Result<MetaServiceClient<Channel>> {
        self.client
            .lock()
            .map(|g| g.clone())
            .map_err(|_| FluxError::Meta("meta client lock poisoned".into()))
    }

    fn block_on<T>(&self, fut: impl Future<Output = T>) -> T {
        if let Ok(handle) = Handle::try_current() {
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else if let Some(rt) = &self.rt {
            rt.block_on(fut)
        } else {
            // Constructed under a runtime that has since exited — spin a one-shot.
            let rt = Runtime::new().expect("create temporary runtime");
            rt.block_on(fut)
        }
    }
}

impl MetaStore for RemoteMetaStore {
    fn root(&self) -> InodeId {
        ROOT_INODE
    }

    fn get_inode(&self, id: InodeId) -> Result<Inode> {
        let mut c = self.client()?;
        let resp = self
            .block_on(async { c.get_inode(GetInodeRequest { id }).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&resp.inode_json)
    }

    fn register_worker(&self, registration: &WorkerRegistration) -> Result<WorkerMembership> {
        let mut c = self.client()?;
        let response = self
            .block_on(async {
                c.register_worker(RegisterWorkerRequest {
                    registration_json: encode_worker_registration(registration)?,
                    request_id: RequestOpId::random().to_hex(),
                })
                .await
                .map_err(flux_from_status)
            })?
            .into_inner();
        decode_worker_membership(&response.membership_json)
    }

    fn worker_membership(&self) -> Result<WorkerMembership> {
        let mut c = self.client()?;
        let response = self
            .block_on(async {
                c.get_worker_membership(GetWorkerMembershipRequest {})
                    .await
                    .map_err(flux_from_status)
            })?
            .into_inner();
        decode_worker_membership(&response.membership_json)
    }

    fn lookup(&self, parent: InodeId, name: &str) -> Result<Inode> {
        let mut c = self.client()?;
        let resp = self
            .block_on(async {
                c.lookup(LookupRequest {
                    parent,
                    name: name.to_string(),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&resp.inode_json)
    }

    fn create_cas(
        &self,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
        file_type: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<Inode> {
        let mut c = self.client()?;
        let resp = self
            .block_on(async {
                c.create(CreateRequest {
                    parent,
                    name: name.to_string(),
                    file_type: file_type_to_wire(file_type),
                    mode,
                    uid,
                    gid,
                    request_id: RequestOpId::random().to_hex(),
                    expected_parent_generation: expected_parent_generation.unwrap_or(0),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&resp.inode_json)
    }

    fn readdir(&self, dir: InodeId) -> Result<Vec<Dentry>> {
        let mut c = self.client()?;
        let resp = self
            .block_on(async { c.readdir(ReaddirRequest { dir }).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_dentries(&resp.dentries_json)
    }

    fn put_inode(&self, inode: &Inode) -> Result<()> {
        let inode_json = encode_inode(inode)?;
        let mut c = self.client()?;
        self.block_on(async { c.put_inode(PutInodeRequest { inode_json }).await })
            .map_err(flux_from_status)?;
        Ok(())
    }

    fn put_manifest(&self, manifest: &Manifest) -> Result<ManifestId> {
        let manifest_json = encode_manifest(manifest)?;
        let mut c = self.client()?;
        let resp = self
            .block_on(async { c.put_manifest(PutManifestRequest { manifest_json }).await })
            .map_err(flux_from_status)?
            .into_inner();
        Ok(ManifestId(resp.manifest_id))
    }

    fn commit_inode_manifest_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode> {
        let inode_json = encode_inode(inode)?;
        let manifest_json = encode_manifest(manifest)?;
        let mut c = self.client()?;
        let resp = self
            .block_on(async {
                c.commit_inode_manifest(CommitInodeManifestRequest {
                    expected_generation,
                    inode_json,
                    manifest_json,
                    request_id: op_id.to_hex(),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&resp.inode_json)
    }

    fn reserve_chunks(
        &self,
        ticket: WriteTicketId,
        inode: InodeId,
        expected_generation: u64,
        chunks: &[ChunkId],
    ) -> Result<()> {
        let mut c = self.client()?;
        let chunks_json = encode_chunk_ids(chunks)?;
        self.block_on(async {
            c.reserve_chunks(ReserveChunksRequest {
                ticket: ticket.0,
                inode,
                expected_generation,
                chunks_json,
                request_id: RequestOpId::random().to_hex(),
            })
            .await
        })
        .map_err(flux_from_status)?;
        Ok(())
    }

    fn abort_chunk_reservation(&self, ticket: WriteTicketId) -> Result<()> {
        let mut c = self.client()?;
        self.block_on(async {
            c.abort_chunk_reservation(AbortChunkReservationRequest {
                ticket: ticket.0,
                request_id: RequestOpId::random().to_hex(),
            })
            .await
        })
        .map_err(flux_from_status)?;
        Ok(())
    }

    fn expire_chunk_reservations(&self, max_to_expire: usize) -> Result<()> {
        let mut c = self.client()?;
        self.block_on(async {
            c.expire_chunk_reservations(ExpireChunkReservationsRequest {
                max_to_expire: max_to_expire.try_into().unwrap_or(u64::MAX),
            })
            .await
        })
        .map_err(flux_from_status)?;
        Ok(())
    }

    fn commit_inode_manifest_reserved_with_id(
        &self,
        op_id: RequestOpId,
        ticket: WriteTicketId,
        expected_generation: u64,
        inode: &Inode,
        manifest: &Manifest,
    ) -> Result<Inode> {
        let mut c = self.client()?;
        let inode_json = encode_inode(inode)?;
        let manifest_json = encode_manifest(manifest)?;
        let response = self
            .block_on(async {
                c.commit_inode_manifest_reserved(CommitInodeManifestReservedRequest {
                    ticket: ticket.0,
                    expected_generation,
                    inode_json,
                    manifest_json,
                    request_id: op_id.to_hex(),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&response.inode_json)
    }

    fn tombstone_gc_batch(&self, candidates: &[ChunkId]) -> Result<GcBatch> {
        let mut c = self.client()?;
        let chunks_json = encode_chunk_ids(candidates)?;
        let response = self
            .block_on(async {
                c.tombstone_gc_batch(TombstoneGcBatchRequest {
                    chunks_json,
                    request_id: RequestOpId::random().to_hex(),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_gc_batch(&response.batch_json)
    }

    fn list_gc_tombstones(&self) -> Result<Vec<GcTombstone>> {
        let mut c = self.client()?;
        let response = self
            .block_on(async { c.list_gc_tombstones(ListGcTombstonesRequest {}).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_gc_tombstones(&response.tombstones_json)
    }

    fn initialize_gc_delete_targets(
        &self,
        chunks: &[ChunkId],
        targets: &[WorkerTargetId],
    ) -> Result<()> {
        let mut c = self.client()?;
        let chunks_json = encode_chunk_ids(chunks)?;
        let targets_json = encode_worker_targets(targets)?;
        self.block_on(async {
            c.initialize_gc_delete_targets(InitializeGcDeleteTargetsRequest {
                chunks_json,
                targets_json,
            })
            .await
        })
        .map_err(flux_from_status)?;
        Ok(())
    }

    fn acknowledge_gc_deletes(&self, deleted: &[(ChunkId, WorkerTargetId)]) -> Result<()> {
        let mut c = self.client()?;
        let deleted_json = encode_gc_delete_acks(deleted)?;
        self.block_on(async {
            c.acknowledge_gc_deletes(AcknowledgeGcDeletesRequest { deleted_json })
                .await
        })
        .map_err(flux_from_status)?;
        Ok(())
    }

    fn finalize_gc_tombstones(&self, chunks: &[ChunkId]) -> Result<()> {
        let mut c = self.client()?;
        let chunks_json = encode_chunk_ids(chunks)?;
        self.block_on(async {
            c.finalize_gc_tombstones(FinalizeGcTombstonesRequest {
                chunks_json,
                request_id: RequestOpId::random().to_hex(),
            })
            .await
        })
        .map_err(flux_from_status)?;
        Ok(())
    }

    fn get_manifest(&self, id: ManifestId) -> Result<Manifest> {
        let mut c = self.client()?;
        let resp = self
            .block_on(async { c.get_manifest(GetManifestRequest { id: id.0 }).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_manifest(&resp.manifest_json)
    }

    fn begin_flush_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: InodeId,
        intent: &FlushIntent,
    ) -> Result<Inode> {
        let mut c = self.client()?;
        let request = BeginFlushRequest {
            inode,
            expected_generation,
            intent_json: encode_flush_intent(intent)?,
            request_id: op_id.to_hex(),
        };
        let response = self
            .block_on(async { c.begin_flush(request).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&response.inode_json)
    }

    fn commit_flush_with_id(
        &self,
        op_id: RequestOpId,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        published_ufs: &UfsObject,
    ) -> Result<Inode> {
        let mut c = self.client()?;
        let request = CommitFlushRequest {
            inode,
            expected_generation,
            flush_id: flush_id.0,
            published_ufs_json: encode_ufs_object(published_ufs)?,
            request_id: op_id.to_hex(),
        };
        let response = self
            .block_on(async { c.commit_flush(request).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&response.inode_json)
    }

    fn fail_flush_conflict(
        &self,
        expected_generation: u64,
        inode: InodeId,
        flush_id: FlushId,
        error: &str,
    ) -> Result<Inode> {
        let mut c = self.client()?;
        let request = FailFlushConflictRequest {
            inode,
            expected_generation,
            flush_id: flush_id.0,
            error: error.to_string(),
            request_id: RequestOpId::random().to_hex(),
        };
        let response = self
            .block_on(async { c.fail_flush_conflict(request).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&response.inode_json)
    }

    fn list_flush_intents(&self) -> Result<Vec<(InodeId, FlushIntent)>> {
        let mut c = self.client()?;
        let response = self
            .block_on(async { c.list_flush_intents(ListFlushIntentsRequest {}).await })
            .map_err(flux_from_status)?
            .into_inner();
        decode_flush_intents(&response.intents_json)
    }

    fn begin_gc(&self, lease_id: GcLeaseId) -> Result<GcPlan> {
        let mut c = self.client()?;
        let response = self
            .block_on(async {
                c.begin_gc(BeginGcRequest {
                    lease_id: lease_id.0,
                    request_id: RequestOpId::random().to_hex(),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_gc_plan(&response.plan_json)
    }

    fn current_gc_plan(&self) -> Result<Option<GcPlan>> {
        let mut c = self.client()?;
        let response = self
            .block_on(async { c.current_gc_plan(CurrentGcPlanRequest {}).await })
            .map_err(flux_from_status)?
            .into_inner();
        if response.present {
            decode_gc_plan(&response.plan_json).map(Some)
        } else {
            Ok(None)
        }
    }

    fn finish_gc(&self, lease_id: GcLeaseId) -> Result<()> {
        let mut c = self.client()?;
        self.block_on(async {
            c.finish_gc(FinishGcRequest {
                lease_id: lease_id.0,
                request_id: RequestOpId::random().to_hex(),
            })
            .await
        })
        .map_err(flux_from_status)?;
        Ok(())
    }

    fn import_external_with_id(
        &self,
        op_id: RequestOpId,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
        inode: &Inode,
        manifest: Option<&Manifest>,
    ) -> Result<Inode> {
        let inode_json = encode_inode(inode)?;
        let manifest_json = match manifest {
            Some(m) => encode_manifest(m)?,
            None => Vec::new(),
        };
        let mut c = self.client()?;
        let response = self
            .block_on(async {
                c.import_external(ImportExternalRequest {
                    parent,
                    name: name.to_string(),
                    inode_json,
                    manifest_json,
                    request_id: op_id.to_hex(),
                    expected_parent_generation: expected_parent_generation.unwrap_or(0),
                })
                .await
            })
            .map_err(flux_from_status)?
            .into_inner();
        decode_inode(&response.inode_json)
    }

    fn unlink_cas(
        &self,
        expected_parent_generation: Option<u64>,
        parent: InodeId,
        name: &str,
    ) -> Result<()> {
        let mut c = self.client()?;
        self.block_on(async {
            c.unlink(UnlinkRequest {
                parent,
                name: name.to_string(),
                expected_parent_generation: expected_parent_generation.unwrap_or(0),
            })
            .await
        })
        .map_err(flux_from_status)?;
        Ok(())
    }
}
