use fluxfs_types::{FluxError, Result};

/// On-disk application schema understood by this FluxFS build.
///
/// Version zero is the legacy, unmarked alpha layout. Version one adds the
/// durable marker; version two writes versioned ordered extent trees while
/// retaining a reader for legacy manifest arrays.
pub const CURRENT_META_SCHEMA_VERSION: u32 = 2;
pub const LEGACY_UNMARKED_SCHEMA_VERSION: u32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaMigration {
    pub from: u32,
    pub to: u32,
    pub name: &'static str,
}

const MIGRATIONS: [MetaMigration; 2] = [
    MetaMigration {
        from: LEGACY_UNMARKED_SCHEMA_VERSION,
        to: 1,
        name: "mark legacy heed layout as schema v1",
    },
    MetaMigration {
        from: 1,
        to: 2,
        name: "enable versioned ordered extent-tree manifests",
    },
];

/// Return the ordered, explicit migration path for an older store.
///
/// Engines must reject newer schemas instead of attempting a downgrade. This
/// function is intentionally engine-neutral so a future LSM implementation
/// follows the same compatibility policy.
pub fn migration_path(from: u32, to: u32) -> Result<Vec<MetaMigration>> {
    if from > to {
        return Err(FluxError::Meta(format!(
            "metadata schema {from} is newer than supported schema {to}; refusing downgrade"
        )));
    }

    let mut current = from;
    let mut path = Vec::new();
    while current < to {
        let step = MIGRATIONS
            .iter()
            .find(|migration| migration.from == current)
            .copied()
            .ok_or_else(|| {
                FluxError::Meta(format!(
                    "no metadata migration path from schema {current} to {to}"
                ))
            })?;
        current = step.to;
        path.push(step);
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_path_is_explicit_and_rejects_downgrade() {
        assert_eq!(migration_path(0, 2).unwrap(), MIGRATIONS);
        assert_eq!(migration_path(1, 2).unwrap(), vec![MIGRATIONS[1]]);
        assert!(migration_path(2, 2).unwrap().is_empty());
        assert!(migration_path(3, 2)
            .unwrap_err()
            .to_string()
            .contains("newer"));
    }
}
