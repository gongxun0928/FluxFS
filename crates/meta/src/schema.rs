use fluxfs_types::{FluxError, Result};

/// On-disk application schema understood by this FluxFS build.
///
/// Version zero is the legacy, unmarked alpha layout. Version one adds the
/// durable marker but deliberately leaves data keys unchanged.
pub const CURRENT_META_SCHEMA_VERSION: u32 = 1;
pub const LEGACY_UNMARKED_SCHEMA_VERSION: u32 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaMigration {
    pub from: u32,
    pub to: u32,
    pub name: &'static str,
}

const MIGRATIONS: [MetaMigration; 1] = [MetaMigration {
    from: LEGACY_UNMARKED_SCHEMA_VERSION,
    to: CURRENT_META_SCHEMA_VERSION,
    name: "mark legacy heed layout as schema v1",
}];

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
        assert_eq!(migration_path(0, 1).unwrap(), MIGRATIONS);
        assert!(migration_path(1, 1).unwrap().is_empty());
        assert!(migration_path(2, 1)
            .unwrap_err()
            .to_string()
            .contains("newer"));
    }
}
