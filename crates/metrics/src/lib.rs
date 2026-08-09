//! Process-local counters / gauges / latency histograms as Prometheus text.
//!
//! Intentionally dependency-light so Meta/Worker/client binaries can expose
//! `/metrics` without pulling a full metrics stack into the VFS path.

use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Fixed latency buckets (milliseconds): 1, 5, 25, 100, 500, 2500, +Inf.
pub const LATENCY_BUCKETS_MS: &[u64] = &[1, 5, 25, 100, 500, 2500];

/// Cumulative latency histogram (Prometheus classic buckets).
#[derive(Debug, Default)]
pub struct LatencyHistogram {
    buckets: [AtomicU64; 7],
    sum_ms: AtomicU64,
    count: AtomicU64,
}

impl LatencyHistogram {
    pub fn observe_ms(&self, ms: u64) {
        for (i, &bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if ms <= bound {
                self.buckets[i].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.buckets[6].fetch_add(1, Ordering::Relaxed); // +Inf
        self.sum_ms.fetch_add(ms, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn start_timer(&self) -> LatencyTimer<'_> {
        LatencyTimer {
            hist: self,
            started: Instant::now(),
        }
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        out.push_str("# HELP ");
        out.push_str(name);
        out.push(' ');
        out.push_str(help);
        out.push('\n');
        out.push_str("# TYPE ");
        out.push_str(name);
        out.push_str(" histogram\n");
        for (i, &bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            out.push_str(name);
            out.push_str("_bucket{le=\"");
            out.push_str(&bound.to_string());
            out.push_str("\"} ");
            out.push_str(&self.buckets[i].load(Ordering::Relaxed).to_string());
            out.push('\n');
        }
        out.push_str(name);
        out.push_str("_bucket{le=\"+Inf\"} ");
        out.push_str(&self.buckets[6].load(Ordering::Relaxed).to_string());
        out.push('\n');
        out.push_str(name);
        out.push_str("_sum ");
        out.push_str(&self.sum_ms.load(Ordering::Relaxed).to_string());
        out.push('\n');
        out.push_str(name);
        out.push_str("_count ");
        out.push_str(&self.count.load(Ordering::Relaxed).to_string());
        out.push('\n');
    }
}

/// Observes latency on Drop so early-return / error paths are included.
pub struct LatencyTimer<'a> {
    hist: &'a LatencyHistogram,
    started: Instant,
}

impl Drop for LatencyTimer<'_> {
    fn drop(&mut self) {
        let ms = self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        self.hist.observe_ms(ms);
    }
}

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
    /// Repair scrub pages executed (client / remote chunk store).
    pub repair_pass_total: AtomicU64,
    /// Replicas rewritten during repair.
    pub repair_replica_total: AtomicU64,

    /// Pending GC tombstones observed at last sample (lag signal).
    pub gc_tombstone_pending: AtomicU64,
    /// Pending flush intents observed at last sample.
    pub flush_intent_pending: AtomicU64,
    /// `1` when the last repair page reported `more` work remaining.
    pub repair_lag_more: AtomicU64,
    /// Minimum `available_bytes` across registered workers (capacity watermark).
    pub worker_available_bytes_min: AtomicU64,
    /// Sum of worker `available_bytes` (capacity headroom).
    pub worker_available_bytes_sum: AtomicU64,
    /// Workers whose advertised `available_bytes` is below placement minimum
    /// ([`fluxfs_types::PLACEMENT_MIN_AVAILABLE_BYTES`] = 4 MiB). Alert signal.
    pub worker_capacity_low: AtomicU64,
    /// ChunkWorker foreground semaphore permits currently available.
    pub chunk_inflight_available: AtomicU64,
    /// ChunkWorker GC-pool semaphore permits currently available.
    pub chunk_gc_inflight_available: AtomicU64,

    pub meta_rpc_latency_ms: LatencyHistogram,
    pub chunk_rpc_latency_ms: LatencyHistogram,
    pub flush_latency_ms: LatencyHistogram,
    pub gc_pass_latency_ms: LatencyHistogram,
    pub repair_pass_latency_ms: LatencyHistogram,
}

static PROCESS_METRICS: OnceLock<Arc<FluxMetrics>> = OnceLock::new();

/// Install the process-wide registry (first call wins). Binaries should call this
/// before serving so client/chunk libraries can increment without plumbing.
pub fn install_process_metrics(metrics: Arc<FluxMetrics>) {
    let _ = PROCESS_METRICS.set(metrics);
}

/// Borrow the process-wide registry when installed.
pub fn process_metrics() -> Option<&'static Arc<FluxMetrics>> {
    PROCESS_METRICS.get()
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

    pub fn set(gauge: &AtomicU64, value: u64) {
        gauge.store(value, Ordering::Relaxed);
    }

    pub fn observe_ms(hist: &LatencyHistogram, started: Instant) {
        let ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        hist.observe_ms(ms);
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::with_capacity(4096);
        fn counter(out: &mut String, name: &str, help: &str, value: u64) {
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
        fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
            out.push_str("# HELP ");
            out.push_str(name);
            out.push(' ');
            out.push_str(help);
            out.push('\n');
            out.push_str("# TYPE ");
            out.push_str(name);
            out.push_str(" gauge\n");
            out.push_str(name);
            out.push(' ');
            out.push_str(&value.to_string());
            out.push('\n');
        }
        counter(
            &mut out,
            "fluxfs_meta_rpc_total",
            "MetaService RPC requests handled",
            self.meta_rpc_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_meta_rpc_error_total",
            "MetaService RPC requests that returned an error Status",
            self.meta_rpc_error_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_meta_busy_total",
            "Meta mutations rejected with Busy",
            self.meta_busy_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_meta_cas_failed_total",
            "Meta CAS failures",
            self.meta_cas_failed_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_chunk_rpc_total",
            "ChunkWorker RPC requests handled",
            self.chunk_rpc_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_chunk_rpc_error_total",
            "ChunkWorker RPC errors",
            self.chunk_rpc_error_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_chunk_put_bytes_total",
            "Bytes accepted by ChunkWorker PutChunk",
            self.chunk_put_bytes_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_gc_pass_total",
            "Concurrent GC passes started by clients",
            self.gc_pass_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_gc_tombstone_total",
            "Chunks tombstoned during GC passes",
            self.gc_tombstone_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_flush_complete_total",
            "Flush intents completed successfully",
            self.flush_complete_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_flush_conflict_total",
            "Flush intents marked DirtyConflict",
            self.flush_conflict_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_repair_pass_total",
            "Background repair/scrub pages executed",
            self.repair_pass_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "fluxfs_repair_replica_total",
            "Chunk replicas rewritten by repair",
            self.repair_replica_total.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "fluxfs_gc_tombstone_pending",
            "GC tombstones waiting for reclaim (lag)",
            self.gc_tombstone_pending.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "fluxfs_flush_intent_pending",
            "Durable flush intents not yet completed (lag)",
            self.flush_intent_pending.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "fluxfs_repair_lag_more",
            "1 when last repair page reported more work remaining",
            self.repair_lag_more.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "fluxfs_worker_available_bytes_min",
            "Minimum available_bytes across registered workers",
            self.worker_available_bytes_min.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "fluxfs_worker_available_bytes_sum",
            "Sum of available_bytes across registered workers",
            self.worker_available_bytes_sum.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "fluxfs_worker_capacity_low",
            "Count of workers with available_bytes below placement minimum (4MiB); alert when > 0",
            self.worker_capacity_low.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "fluxfs_chunk_inflight_available",
            "Foreground ChunkWorker in-flight permits available",
            self.chunk_inflight_available.load(Ordering::Relaxed),
        );
        gauge(
            &mut out,
            "fluxfs_chunk_gc_inflight_available",
            "GC-pool ChunkWorker in-flight permits available",
            self.chunk_gc_inflight_available.load(Ordering::Relaxed),
        );
        self.meta_rpc_latency_ms.render(
            &mut out,
            "fluxfs_meta_rpc_latency_ms",
            "Meta Raft write RPC latency in milliseconds",
        );
        self.chunk_rpc_latency_ms.render(
            &mut out,
            "fluxfs_chunk_rpc_latency_ms",
            "ChunkWorker Put/Get RPC latency in milliseconds",
        );
        self.flush_latency_ms.render(
            &mut out,
            "fluxfs_flush_latency_ms",
            "Client flush_inode latency in milliseconds",
        );
        self.gc_pass_latency_ms.render(
            &mut out,
            "fluxfs_gc_pass_latency_ms",
            "Client concurrent GC pass latency in milliseconds",
        );
        self.repair_pass_latency_ms.render(
            &mut out,
            "fluxfs_repair_pass_latency_ms",
            "Repair scrub page latency in milliseconds",
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
    fn render_contains_counters_histograms_and_gauges() {
        let m = FluxMetrics::new();
        FluxMetrics::inc(&m.meta_rpc_total);
        FluxMetrics::add(&m.chunk_put_bytes_total, 64);
        m.meta_rpc_latency_ms.observe_ms(3);
        FluxMetrics::set(&m.gc_tombstone_pending, 7);
        FluxMetrics::set(&m.worker_capacity_low, 2);
        let text = m.render_prometheus();
        assert!(text.contains("fluxfs_meta_rpc_total 1"));
        assert!(text.contains("fluxfs_chunk_put_bytes_total 64"));
        assert!(text.contains("fluxfs_gc_tombstone_pending 7"));
        assert!(text.contains("fluxfs_worker_capacity_low 2"));
        assert!(text.contains("fluxfs_meta_rpc_latency_ms_bucket{le=\"5\"} 1"));
        assert!(text.contains("fluxfs_meta_rpc_latency_ms_count 1"));
        assert!(text.contains("fluxfs_repair_pass_total"));
    }

    #[test]
    fn latency_timer_observes_on_drop_including_early_return() {
        let m = FluxMetrics::new();
        {
            let _timer = m.meta_rpc_latency_ms.start_timer();
            // simulate error path without explicit observe
        }
        assert_eq!(m.meta_rpc_latency_ms.count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn metrics_http_smoke() {
        let metrics = FluxMetrics::new();
        FluxMetrics::inc(&metrics.meta_rpc_total);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        spawn_prometheus(addr, Arc::clone(&metrics));
        // Give the server a moment to bind.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
        sock.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 8192];
        let n = sock.read(&mut buf).await.unwrap();
        let body = String::from_utf8_lossy(&buf[..n]);
        assert!(body.contains("HTTP/1.1 200 OK"));
        assert!(body.contains("fluxfs_meta_rpc_total 1"));
    }
}
