//! Process-local counters rendered as Prometheus text exposition.
//!
//! Intentionally dependency-light so Meta/Worker binaries can expose
//! `/metrics` without pulling a full metrics stack into the VFS path.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Shared process metrics registry.
#[derive(Debug, Default)]
pub struct FluxMetrics {
    pub meta_rpc_total: AtomicU64,
    pub meta_rpc_error_total: AtomicU64,
    pub meta_busy_total: AtomicU64,
    pub meta_cas_failed_total: AtomicU64,
    pub chunk_rpc_total: AtomicU64,
    pub chunk_rpc_error_total: AtomicU64,
    pub chunk_put_bytes_total: AtomicU64,
    pub gc_pass_total: AtomicU64,
    pub gc_tombstone_total: AtomicU64,
    pub flush_complete_total: AtomicU64,
    pub flush_conflict_total: AtomicU64,
}

impl FluxMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(counter: &AtomicU64, delta: u64) {
        counter.fetch_add(delta, Ordering::Relaxed);
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(1024);
        fn line(out: &mut String, name: &str, help: &str, value: u64) {
            out.push_str("# HELP ");
            out.push_str(name);
            out.push(' ');
            out.push_str(help);
            out.push('\n');
            out.push_str("# TYPE ");
            out.push_str(name);
            out.push_str(" counter\n");
            out.push_str(name);
            out.push(' ');
            out.push_str(&value.to_string());
            out.push('\n');
        }
        line(
            &mut out,
            "fluxfs_meta_rpc_total",
            "MetaService RPC requests handled",
            self.meta_rpc_total.load(Ordering::Relaxed),
        );
        line(
            &mut out,
            "fluxfs_meta_rpc_error_total",
            "MetaService RPC requests that returned an error Status",
            self.meta_rpc_error_total.load(Ordering::Relaxed),
        );
        line(
            &mut out,
            "fluxfs_meta_busy_total",
            "Meta mutations rejected with Busy",
            self.meta_busy_total.load(Ordering::Relaxed),
        );
        line(
            &mut out,
            "fluxfs_meta_cas_failed_total",
            "Meta CAS failures",
            self.meta_cas_failed_total.load(Ordering::Relaxed),
        );
        line(
            &mut out,
            "fluxfs_chunk_rpc_total",
            "ChunkWorker RPC requests handled",
            self.chunk_rpc_total.load(Ordering::Relaxed),
        );
        line(
            &mut out,
            "fluxfs_chunk_rpc_error_total",
            "ChunkWorker RPC errors",
            self.chunk_rpc_error_total.load(Ordering::Relaxed),
        );
        line(
            &mut out,
            "fluxfs_chunk_put_bytes_total",
            "Bytes accepted by ChunkWorker PutChunk",
            self.chunk_put_bytes_total.load(Ordering::Relaxed),
        );
        line(
            &mut out,
            "fluxfs_gc_pass_total",
            "Concurrent GC passes started by clients",
            self.gc_pass_total.load(Ordering::Relaxed),
        );
        line(
            &mut out,
            "fluxfs_gc_tombstone_total",
            "Chunks tombstoned during GC passes",
            self.gc_tombstone_total.load(Ordering::Relaxed),
        );
        line(
            &mut out,
            "fluxfs_flush_complete_total",
            "Flush intents completed successfully",
            self.flush_complete_total.load(Ordering::Relaxed),
        );
        line(
            &mut out,
            "fluxfs_flush_conflict_total",
            "Flush intents marked DirtyConflict",
            self.flush_conflict_total.load(Ordering::Relaxed),
        );
        out
    }
}

/// Serve Prometheus text on `GET /metrics` (best-effort HTTP/1.x).
pub async fn serve_prometheus(addr: SocketAddr, metrics: Arc<FluxMetrics>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (mut sock, _) = listener.accept().await?;
        let body = metrics.render_prometheus();
        let mut req = [0u8; 1024];
        let _ = sock.read(&mut req).await;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = sock.write_all(response.as_bytes()).await;
        let _ = sock.shutdown().await;
    }
}

/// Spawn [`serve_prometheus`] on a background Tokio task.
pub fn spawn_prometheus(addr: SocketAddr, metrics: Arc<FluxMetrics>) {
    tokio::spawn(async move {
        if let Err(err) = serve_prometheus(addr, metrics).await {
            eprintln!("fluxfs metrics server error: {err}");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_counters() {
        let m = FluxMetrics::new();
        FluxMetrics::inc(&m.meta_rpc_total);
        FluxMetrics::add(&m.chunk_put_bytes_total, 64);
        let text = m.render_prometheus();
        assert!(text.contains("fluxfs_meta_rpc_total 1"));
        assert!(text.contains("fluxfs_chunk_put_bytes_total 64"));
    }
}
