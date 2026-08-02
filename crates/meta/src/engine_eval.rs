use serde::{Deserialize, Serialize};

/// Reproducible Meta engine workload dimensions. The benchmark runner records
/// these alongside results so numbers from different datasets are not mixed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaWorkloadConfig {
    pub files: u64,
    pub operations: u64,
    pub lookup_percent: u8,
    pub inode_read_percent: u8,
    pub mutation_percent: u8,
}

impl Default for MetaWorkloadConfig {
    fn default() -> Self {
        Self {
            files: 100_000,
            operations: 1_000_000,
            lookup_percent: 70,
            inode_read_percent: 20,
            mutation_percent: 10,
        }
    }
}

impl MetaWorkloadConfig {
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.files == 0 || self.operations == 0 {
            return Err("files and operations must be non-zero".into());
        }
        let total = u16::from(self.lookup_percent)
            + u16::from(self.inode_read_percent)
            + u16::from(self.mutation_percent);
        if total != 100 {
            return Err(format!(
                "operation percentages must sum to 100, got {total}"
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetaWorkloadReport {
    pub engine: String,
    pub schema_version: u32,
    pub config: MetaWorkloadConfig,
    pub load_ops_per_second: f64,
    pub mixed_ops_per_second: f64,
    pub lookup_p99_micros: u64,
    pub inode_read_p99_micros: u64,
    pub mutation_p99_micros: u64,
    pub reopen_millis: u64,
    pub database_bytes: u64,
}

/// Provisional production gate. Projects can tighten this in CI without
/// changing the benchmark or coupling VFS callers to an engine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct MetaEngineGate {
    pub min_mixed_ops_per_second: f64,
    pub max_lookup_p99_micros: u64,
    pub max_mutation_p99_micros: u64,
    pub max_reopen_millis: u64,
}

impl Default for MetaEngineGate {
    fn default() -> Self {
        Self {
            min_mixed_ops_per_second: 20_000.0,
            max_lookup_p99_micros: 2_000,
            max_mutation_p99_micros: 10_000,
            max_reopen_millis: 30_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaEngineDecision {
    pub passes: bool,
    pub failures: Vec<String>,
}

pub fn evaluate_meta_engine(
    report: &MetaWorkloadReport,
    gate: MetaEngineGate,
) -> MetaEngineDecision {
    let mut failures = Vec::new();
    if report.mixed_ops_per_second < gate.min_mixed_ops_per_second {
        failures.push("mixed throughput below gate".into());
    }
    if report.lookup_p99_micros > gate.max_lookup_p99_micros {
        failures.push("lookup p99 above gate".into());
    }
    if report.mutation_p99_micros > gate.max_mutation_p99_micros {
        failures.push("mutation p99 above gate".into());
    }
    if report.reopen_millis > gate.max_reopen_millis {
        failures.push("reopen time above gate".into());
    }
    MetaEngineDecision {
        passes: failures.is_empty(),
        failures,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> MetaWorkloadReport {
        MetaWorkloadReport {
            engine: "test".into(),
            schema_version: 1,
            config: MetaWorkloadConfig::default(),
            load_ops_per_second: 1.0,
            mixed_ops_per_second: 20_000.0,
            lookup_p99_micros: 2_000,
            inode_read_p99_micros: 2_000,
            mutation_p99_micros: 10_000,
            reopen_millis: 30_000,
            database_bytes: 1,
        }
    }

    #[test]
    fn gate_is_inclusive_and_reports_each_regression() {
        assert!(evaluate_meta_engine(&report(), MetaEngineGate::default()).passes);
        let mut slow = report();
        slow.mixed_ops_per_second = 19_999.0;
        slow.lookup_p99_micros += 1;
        slow.mutation_p99_micros += 1;
        slow.reopen_millis += 1;
        let decision = evaluate_meta_engine(&slow, MetaEngineGate::default());
        assert!(!decision.passes);
        assert_eq!(decision.failures.len(), 4);
    }

    #[test]
    fn workload_mix_must_sum_to_one_hundred() {
        let config = MetaWorkloadConfig {
            mutation_percent: 9,
            ..MetaWorkloadConfig::default()
        };
        assert!(config.validate().is_err());
    }
}
