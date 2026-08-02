use fluxfs_meta::{
    evaluate_meta_engine, HeedMetaStore, HeedMetaStoreOptions, MetaEngineGate, MetaStore,
    MetaWorkloadConfig, MetaWorkloadReport,
};
use fluxfs_types::{FileType, ROOT_INODE};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Debug)]
struct Args {
    path: Option<PathBuf>,
    files: u64,
    operations: u64,
    map_size_bytes: usize,
}

#[derive(Serialize)]
struct Output {
    report: MetaWorkloadReport,
    gate: MetaEngineGate,
    decision: fluxfs_meta::MetaEngineDecision,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args()?;
    let temp = match &args.path {
        Some(_) => None,
        None => Some(tempfile::tempdir()?),
    };
    let path = args
        .path
        .as_deref()
        .or_else(|| temp.as_ref().map(|dir| dir.path()))
        .expect("benchmark path");
    if path.exists() && path.read_dir()?.next().is_some() {
        return Err(format!("benchmark path {} must be empty", path.display()).into());
    }

    let config = MetaWorkloadConfig {
        files: args.files,
        operations: args.operations,
        ..MetaWorkloadConfig::default()
    };
    config.validate()?;
    let options = HeedMetaStoreOptions {
        map_size_bytes: args.map_size_bytes,
    };
    let store = HeedMetaStore::open_with_options(path, options)?;

    let load_started = Instant::now();
    let mut inode_ids = Vec::with_capacity(args.files.try_into()?);
    for index in 0..args.files {
        let inode = store.create(
            ROOT_INODE,
            &file_name(index),
            FileType::Regular,
            0o644,
            0,
            0,
        )?;
        inode_ids.push(inode.id);
    }
    let load_elapsed = load_started.elapsed();

    let mut lookup_latency = Vec::new();
    let mut inode_latency = Vec::new();
    let mut mutation_latency = Vec::new();
    let mixed_started = Instant::now();
    let mut random = 0x4d59_5df4_d0f3_3173_u64;
    for _ in 0..args.operations {
        random = random
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let index = random % args.files;
        let selector = (random >> 32) % 100;
        let started = Instant::now();
        if selector < u64::from(config.lookup_percent) {
            store.lookup(ROOT_INODE, &file_name(index))?;
            lookup_latency.push(started.elapsed().as_nanos());
        } else if selector < u64::from(config.lookup_percent + config.inode_read_percent) {
            store.get_inode(inode_ids[index as usize])?;
            inode_latency.push(started.elapsed().as_nanos());
        } else {
            let mut inode = store.get_inode(inode_ids[index as usize])?;
            inode.generation = inode.generation.saturating_add(1);
            store.put_inode(&inode)?;
            mutation_latency.push(started.elapsed().as_nanos());
        }
    }
    let mixed_elapsed = mixed_started.elapsed();
    let schema_version = store.schema_version()?;
    drop(store);

    let reopen_started = Instant::now();
    let reopened = HeedMetaStore::open_with_options(path, options)?;
    let reopen_millis = millis(reopen_started.elapsed().as_nanos());
    reopened.lookup(ROOT_INODE, &file_name(args.files - 1))?;

    let report = MetaWorkloadReport {
        engine: "heed-lmdb".into(),
        schema_version,
        config,
        load_ops_per_second: rate(args.files, load_elapsed.as_nanos()),
        mixed_ops_per_second: rate(args.operations, mixed_elapsed.as_nanos()),
        lookup_p99_micros: percentile_micros(&mut lookup_latency, 99),
        inode_read_p99_micros: percentile_micros(&mut inode_latency, 99),
        mutation_p99_micros: percentile_micros(&mut mutation_latency, 99),
        reopen_millis,
        database_bytes: directory_bytes(path)?,
    };
    let gate = MetaEngineGate::default();
    let output = Output {
        decision: evaluate_meta_engine(&report, gate),
        report,
        gate,
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn parse_args() -> Result<Args, Box<dyn std::error::Error>> {
    let defaults = MetaWorkloadConfig::default();
    let mut args = Args {
        path: None,
        files: defaults.files,
        operations: defaults.operations,
        map_size_bytes: 4 * 1024 * 1024 * 1024,
    };
    let mut values = std::env::args().skip(1);
    while let Some(flag) = values.next() {
        let value = values
            .next()
            .ok_or_else(|| format!("missing value for {flag}"))?;
        match flag.as_str() {
            "--path" => args.path = Some(value.into()),
            "--files" => args.files = value.parse()?,
            "--operations" => args.operations = value.parse()?,
            "--map-size-gib" => {
                let gib: usize = value.parse()?;
                args.map_size_bytes = gib
                    .checked_mul(1024 * 1024 * 1024)
                    .ok_or("map size overflow")?;
            }
            _ => return Err(format!("unknown argument {flag}").into()),
        }
    }
    Ok(args)
}

fn file_name(index: u64) -> String {
    format!("file-{index:016x}")
}

fn rate(operations: u64, elapsed_nanos: u128) -> f64 {
    operations as f64 * 1_000_000_000.0 / elapsed_nanos.max(1) as f64
}

fn percentile_micros(values: &mut [u128], percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let rank = values.len().saturating_mul(percentile).div_ceil(100);
    let index = rank.saturating_sub(1).min(values.len() - 1);
    micros(values[index])
}

fn micros(nanos: u128) -> u64 {
    (nanos / 1_000).try_into().unwrap_or(u64::MAX)
}

fn millis(nanos: u128) -> u64 {
    (nanos / 1_000_000).try_into().unwrap_or(u64::MAX)
}

fn directory_bytes(path: &Path) -> std::io::Result<u64> {
    let mut bytes = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            bytes = bytes.saturating_add(directory_bytes(&entry.path())?);
        } else {
            bytes = bytes.saturating_add(metadata.len());
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_and_rate_are_deterministic() {
        let mut values = vec![5_000, 1_000, 3_000, 2_000, 4_000];
        assert_eq!(percentile_micros(&mut values, 99), 5);
        assert_eq!(rate(10, 1_000_000_000), 10.0);
    }
}
