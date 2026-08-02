use fluxfs_types::{ChunkId, FluxError, Result, WorkerMembership, WorkerRegistration, CHUNK_SIZE};
use std::collections::BTreeSet;

/// Deterministic capacity-weighted rendezvous placement with failure-domain
/// spreading. Expired or too-full Workers are excluded before selection.
pub fn select_worker_targets(
    membership: &WorkerMembership,
    chunk: &ChunkId,
    required: usize,
    now_ms: u64,
) -> Result<Vec<WorkerRegistration>> {
    if required == 0 {
        return Err(FluxError::InvalidArg(
            "replication factor must be non-zero".into(),
        ));
    }
    let mut candidates: Vec<_> = membership
        .active_at(now_ms)
        .filter(|worker| worker.validate().is_ok() && worker.available_bytes >= CHUNK_SIZE)
        .cloned()
        .collect();
    candidates.sort_by(|left, right| {
        placement_score(chunk, right)
            .cmp(&placement_score(chunk, left))
            .then_with(|| left.id.cmp(&right.id))
    });
    if candidates.len() < required {
        return Err(FluxError::Busy);
    }
    let mut selected = Vec::with_capacity(required);
    let mut domains = BTreeSet::new();
    for worker in &candidates {
        if domains.insert(worker.failure_domain.clone()) {
            selected.push(worker.clone());
            if selected.len() == required {
                return Ok(selected);
            }
        }
    }
    for worker in candidates {
        if !selected.iter().any(|chosen| chosen.id == worker.id) {
            selected.push(worker);
            if selected.len() == required {
                return Ok(selected);
            }
        }
    }
    Err(FluxError::Busy)
}

fn placement_score(chunk: &ChunkId, worker: &WorkerRegistration) -> u128 {
    let mut input = Vec::with_capacity(40);
    input.extend_from_slice(chunk.as_bytes());
    input.extend_from_slice(&worker.id.0.to_be_bytes());
    let digest = blake3::hash(&input);
    let raw = u64::from_be_bytes(digest.as_bytes()[..8].try_into().expect("eight bytes"));
    u128::from(raw) * u128::from(worker.available_bytes.saturating_add(1))
        / u128::from(worker.capacity_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxfs_types::WorkerTargetId;

    fn worker(id: u64, domain: &str, available: u64, deadline: u64) -> WorkerRegistration {
        WorkerRegistration {
            id: WorkerTargetId(id),
            endpoint: format!("http://worker-{id}:50052"),
            failure_domain: domain.into(),
            capacity_bytes: 100 * CHUNK_SIZE,
            available_bytes: available,
            lease_deadline_ms: deadline,
        }
    }

    #[test]
    fn placement_is_stable_and_spreads_failure_domains() {
        let membership = WorkerMembership {
            epoch: 7,
            workers: vec![
                worker(1, "rack-a", 90 * CHUNK_SIZE, 100),
                worker(2, "rack-a", 80 * CHUNK_SIZE, 100),
                worker(3, "rack-b", 70 * CHUNK_SIZE, 100),
            ],
        };
        let chunk = ChunkId::from_bytes(b"placement-key");
        let first = select_worker_targets(&membership, &chunk, 2, 10).unwrap();
        assert_eq!(
            first,
            select_worker_targets(&membership, &chunk, 2, 10).unwrap()
        );
        assert_ne!(first[0].failure_domain, first[1].failure_domain);
    }

    #[test]
    fn placement_excludes_expired_and_full_workers() {
        let membership = WorkerMembership {
            epoch: 1,
            workers: vec![
                worker(1, "a", 50 * CHUNK_SIZE, 9),
                worker(2, "b", CHUNK_SIZE - 1, 100),
                worker(3, "c", 50 * CHUNK_SIZE, 100),
            ],
        };
        let chunk = ChunkId::from_bytes(b"x");
        assert_eq!(
            select_worker_targets(&membership, &chunk, 1, 10).unwrap()[0].id,
            WorkerTargetId(3)
        );
        assert_eq!(
            select_worker_targets(&membership, &chunk, 2, 10),
            Err(FluxError::Busy)
        );
    }
}
