//! Deterministic, cgroup-aware CPU kernels with bounded spill.

#![allow(missing_docs)]

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const PARTITION_DOMAIN: &[u8] = b"ngkg-hpc-partition-v1\0";
const LINESET_DOMAIN: &[u8] = b"ngkg-hpc-canonical-lineset-v1\0";

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceBudget {
    pub threads: usize,
    pub memory_bytes: u64,
    pub spill_bytes: u64,
}

impl ResourceBudget {
    pub fn from_cgroup(
        memory_fraction_percent: u64,
        maximum_spill_bytes: u64,
    ) -> Result<Self, HpcError> {
        if !(1..=90).contains(&memory_fraction_percent) || maximum_spill_bytes == 0 {
            return Err(HpcError::InvalidBudget);
        }
        let memory_limit = cgroup_memory_limit_bytes().unwrap_or(512 * 1024 * 1024);
        Ok(Self {
            threads: cgroup_cpu_count(),
            memory_bytes: memory_limit.saturating_mul(memory_fraction_percent) / 100,
            spill_bytes: maximum_spill_bytes,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KernelReceipt {
    pub result_sha256: String,
    pub input_bytes: u64,
    pub records: u64,
    pub threads: usize,
    pub spilled_bytes: u64,
    pub run_count: usize,
}

pub fn deterministic_partition_root(
    partition_hashes: &[(u32, String)],
) -> Result<String, HpcError> {
    let mut ordered = partition_hashes.to_vec();
    ordered.sort_by_key(|(ordinal, _)| *ordinal);
    if ordered.iter().enumerate().any(|(index, (ordinal, hash))| {
        usize::try_from(*ordinal).ok() != Some(index) || decode_hash(hash).is_err()
    }) {
        return Err(HpcError::NonContiguousPartitions);
    }
    let mut digest = Sha256::new();
    digest.update(PARTITION_DOMAIN);
    for (ordinal, hash) in ordered {
        digest.update(ordinal.to_be_bytes());
        digest.update(decode_hash(&hash)?);
    }
    Ok(hex::encode(digest.finalize()))
}

pub fn canonical_lineset(
    input: &[u8],
    spill_directory: &Path,
    budget: ResourceBudget,
) -> Result<KernelReceipt, HpcError> {
    if budget.threads == 0 || budget.memory_bytes < 4096 || budget.spill_bytes == 0 {
        return Err(HpcError::InvalidBudget);
    }
    let text = std::str::from_utf8(input).map_err(|_| HpcError::Utf8)?;
    let maximum_run = usize::try_from(budget.memory_bytes.min(64 * 1024 * 1024))
        .map_err(|_| HpcError::InvalidBudget)?;
    fs::create_dir_all(spill_directory)?;
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(budget.threads)
        .thread_name(|index| format!("ngkg-hpc-{index}"))
        .build()?;
    let mut runs = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0_usize;
    let mut records = 0_u64;
    let mut spilled = 0_u64;
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        records = records.checked_add(1).ok_or(HpcError::Limit)?;
        current_bytes = current_bytes
            .checked_add(line.len() + 1)
            .ok_or(HpcError::Limit)?;
        current.push(line.to_owned());
        if current_bytes >= maximum_run {
            spilled = spill_run(
                &pool,
                spill_directory,
                &mut runs,
                &mut current,
                spilled,
                budget.spill_bytes,
            )?;
            current_bytes = 0;
        }
    }
    if !current.is_empty() {
        spilled = spill_run(
            &pool,
            spill_directory,
            &mut runs,
            &mut current,
            spilled,
            budget.spill_bytes,
        )?;
    }
    let result = merge_runs(&runs)?;
    for path in &runs {
        fs::remove_file(path)?;
    }
    Ok(KernelReceipt {
        result_sha256: result,
        input_bytes: u64::try_from(input.len()).map_err(|_| HpcError::Limit)?,
        records,
        threads: budget.threads,
        spilled_bytes: spilled,
        run_count: runs.len(),
    })
}

fn spill_run(
    pool: &rayon::ThreadPool,
    directory: &Path,
    runs: &mut Vec<PathBuf>,
    lines: &mut Vec<String>,
    spilled: u64,
    maximum_spill: u64,
) -> Result<u64, HpcError> {
    pool.install(|| lines.par_sort_unstable());
    lines.dedup();
    let written = lines.iter().try_fold(0_u64, |total, line| {
        total
            .checked_add(u64::try_from(line.len() + 1).map_err(|_| HpcError::Limit)?)
            .ok_or(HpcError::Limit)
    })?;
    let total = spilled.checked_add(written).ok_or(HpcError::Limit)?;
    if total > maximum_spill {
        return Err(HpcError::SpillLimit);
    }
    let path = directory.join(format!("run-{:08}.txt.partial", runs.len()));
    let final_path = directory.join(format!("run-{:08}.txt", runs.len()));
    let mut writer = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?,
    );
    for line in lines.iter() {
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(path, &final_path)?;
    runs.push(final_path);
    lines.clear();
    Ok(total)
}

fn merge_runs(runs: &[PathBuf]) -> Result<String, HpcError> {
    let mut readers = runs
        .iter()
        .map(File::open)
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(BufReader::new)
        .collect::<Vec<_>>();
    let mut heap = BinaryHeap::<Reverse<(String, usize)>>::new();
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(line) = read_line(reader)? {
            heap.push(Reverse((line, index)));
        }
    }
    let mut digest = Sha256::new();
    digest.update(LINESET_DOMAIN);
    let mut previous: Option<String> = None;
    while let Some(Reverse((line, index))) = heap.pop() {
        if previous.as_deref() != Some(line.as_str()) {
            digest.update(
                u64::try_from(line.len())
                    .map_err(|_| HpcError::Limit)?
                    .to_be_bytes(),
            );
            digest.update(line.as_bytes());
            previous = Some(line);
        }
        if let Some(next) = read_line(&mut readers[index])? {
            heap.push(Reverse((next, index)));
        }
    }
    Ok(hex::encode(digest.finalize()))
}

fn read_line(reader: &mut BufReader<File>) -> Result<Option<String>, HpcError> {
    let mut value = String::new();
    if reader.read_line(&mut value)? == 0 {
        return Ok(None);
    }
    while value.ends_with('\n') || value.ends_with('\r') {
        value.pop();
    }
    Ok(Some(value))
}

pub fn cgroup_cpu_count() -> usize {
    let available = std::thread::available_parallelism().map_or(1, usize::from);
    let v2 = fs::read_to_string("/sys/fs/cgroup/cpu.max")
        .ok()
        .and_then(|value| parse_v2_cpu_quota(&value));
    let v1 = fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .zip(
            fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok()),
        )
        .and_then(|(quota, period)| {
            if quota <= 0 || period == 0 {
                None
            } else {
                usize::try_from((quota as u64).saturating_add(period - 1) / period).ok()
            }
        });
    available.min(v2.or(v1).unwrap_or(available)).max(1)
}

fn parse_v2_cpu_quota(value: &str) -> Option<usize> {
    let mut parts = value.split_whitespace();
    let quota = parts.next()?;
    let period = parts.next()?.parse::<u64>().ok()?;
    if quota == "max" || period == 0 {
        None
    } else {
        let quota = quota.parse::<u64>().ok()?;
        usize::try_from(quota.saturating_add(period - 1) / period).ok()
    }
}

fn finite_memory_limit(value: &str) -> Option<u64> {
    let value = value.trim();
    if value == "max" {
        return None;
    }
    let parsed = value.parse::<u64>().ok()?;
    // cgroup v1 commonly reports a page-aligned value close to i64::MAX for
    // unlimited. Treat it as absent instead of budgeting against host RAM.
    (parsed > 0 && parsed < (1_u64 << 60)).then_some(parsed)
}

pub fn cgroup_memory_limit_bytes() -> Option<u64> {
    [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .find_map(|path| {
        fs::read_to_string(path)
            .ok()
            .and_then(|value| finite_memory_limit(&value))
    })
}

fn decode_hash(value: &str) -> Result<[u8; 32], HpcError> {
    let bytes = hex::decode(value).map_err(|_| HpcError::Hash)?;
    bytes.try_into().map_err(|_| HpcError::Hash)
}

#[derive(Debug, Error)]
pub enum HpcError {
    #[error("resource budget is invalid")]
    InvalidBudget,
    #[error("partition ordinals or hashes are invalid")]
    NonContiguousPartitions,
    #[error("hash is invalid")]
    Hash,
    #[error("input is not UTF-8")]
    Utf8,
    #[error("configured limit exceeded")]
    Limit,
    #[error("bounded spill limit exceeded")]
    SpillLimit,
    #[error("I/O failed")]
    Io(#[from] std::io::Error),
    #[error("parallel pool failed")]
    Rayon(#[from] rayon::ThreadPoolBuildError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn partition_root_is_order_independent_but_ordinal_bound() {
        let a = "11".repeat(32);
        let b = "22".repeat(32);
        let left = deterministic_partition_root(&[(1, b.clone()), (0, a.clone())]);
        let right = deterministic_partition_root(&[(0, a), (1, b)]);
        assert_eq!(left.ok(), right.ok());
    }

    #[test]
    fn kernel_is_thread_count_equivalent() -> Result<(), Box<dyn std::error::Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root =
            std::env::temp_dir().join(format!("ngkg-hpc-test-{}-{nonce}", std::process::id()));
        let input = b"zeta\nalpha\nzeta\nbeta\n";
        let one = canonical_lineset(
            input,
            &root.join("one"),
            ResourceBudget {
                threads: 1,
                memory_bytes: 4096,
                spill_bytes: 4096,
            },
        )?;
        let many = canonical_lineset(
            input,
            &root.join("many"),
            ResourceBudget {
                threads: 4,
                memory_bytes: 4096,
                spill_bytes: 4096,
            },
        )?;
        assert_eq!(one.result_sha256, many.result_sha256);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn spill_limit_fails_before_creating_a_run() -> Result<(), Box<dyn std::error::Error>> {
        let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!(
            "ngkg-hpc-limit-test-{}-{nonce}",
            std::process::id()
        ));
        let result = canonical_lineset(
            b"line-too-large\n",
            &root,
            ResourceBudget {
                threads: 1,
                memory_bytes: 4096,
                spill_bytes: 1,
            },
        );
        assert!(matches!(result, Err(HpcError::SpillLimit)));
        assert_eq!(fs::read_dir(&root)?.count(), 0);
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
