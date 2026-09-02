//! Finite multi-rank Parquet execution with deterministic receipts.
//!
//! Kubernetes/Kueue owns gang admission and the pinned MPI launcher owns rank
//! lifetime. This module owns semantic identity: ranks receive an immutable
//! plan, process a stable modulo partition assignment, and emit content-bound
//! receipts. Rank zero may publish a run certificate only after the exact rank
//! and partition sets verify.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
};

use ngkg_hpc_runtime::LocalComputeBackend;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    LeafPredicate, LeafScanLimits, NativeRuntimeError, scan_verified_parquet_leaf,
};

/// Version of the MPI/Parquet run-plan and receipt contract.
pub const HPC_RUN_FORMAT_VERSION: u32 = 1;

static RECEIPT_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// One immutable Parquet partition assigned by ordinal, never by discovery order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HpcPartitionPlan {
    pub ordinal: u32,
    pub semantic_partition: u32,
    pub facts_path: PathBuf,
    pub facts_sha256: String,
    pub facts_bytes: u64,
    pub predicate: LeafPredicate,
    pub limits: LeafScanLimits,
}

/// Snapshot- and authorization-bound finite MPI execution plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HpcRunPlan {
    pub format_version: u32,
    pub run_id: String,
    pub snapshot_manifest_sha256: String,
    pub semantic_root_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub rank_count: u32,
    pub local_backend: LocalComputeBackend,
    pub local_compute_threads: usize,
    pub blocking_io_threads: usize,
    pub estimated_row_bytes: usize,
    pub maximum_rows: u64,
    pub maximum_decoded_bytes: u64,
    pub partitions: Vec<HpcPartitionPlan>,
}

/// Exact result identity for one immutable partition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HpcPartitionReceipt {
    pub ordinal: u32,
    pub semantic_partition: u32,
    pub input_sha256: String,
    pub output_sha256: String,
    pub scanned_rows: u64,
    pub physically_scanned_rows: u64,
    pub pruned_row_groups: u64,
    pub pruned_rows: u64,
    pub matched_rows: u64,
    pub decoded_bytes: u64,
}

/// One rank's terminal, content-addressed receipt.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HpcRankReceipt {
    pub format_version: u32,
    pub run_id: String,
    pub plan_sha256: String,
    pub snapshot_manifest_sha256: String,
    pub semantic_root_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub rank: u32,
    pub rank_count: u32,
    pub partitions: Vec<HpcPartitionReceipt>,
    pub partition_set_sha256: String,
    pub complete: bool,
}

/// Rank-zero certificate for the exact immutable gang.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct HpcRunCertificate {
    pub format_version: u32,
    pub run_id: String,
    pub plan_sha256: String,
    pub snapshot_manifest_sha256: String,
    pub semantic_root_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub rank_count: u32,
    pub partition_count: u32,
    pub scanned_rows: u64,
    pub physically_scanned_rows: u64,
    pub pruned_row_groups: u64,
    pub pruned_rows: u64,
    pub matched_rows: u64,
    pub decoded_bytes: u64,
    pub rank_receipt_set_sha256: String,
    pub result_set_sha256: String,
    pub complete: bool,
}

/// Validate a plan and return the SHA-256 used by every rank receipt.
pub fn validate_hpc_run_plan(plan: &HpcRunPlan) -> Result<String, NativeRuntimeError> {
    if plan.format_version != HPC_RUN_FORMAT_VERSION
        || plan.run_id.is_empty()
        || plan.run_id.len() > 128
        || plan.rank_count < 2
        || plan.local_compute_threads == 0
        || plan.blocking_io_threads == 0
        || plan.estimated_row_bytes == 0
        || plan.maximum_rows == 0
        || plan.maximum_decoded_bytes == 0
        || !digest(&plan.snapshot_manifest_sha256)
        || !digest(&plan.semantic_root_sha256)
        || !digest(&plan.authorized_graph_set_sha256)
        || plan.partitions.is_empty()
    {
        return Err(NativeRuntimeError::InvalidHpcRunPlan);
    }
    let mut ordinals = BTreeSet::new();
    for partition in &plan.partitions {
        let metadata = fs::symlink_metadata(&partition.facts_path)?;
        if !partition.facts_path.is_absolute()
            || metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() != partition.facts_bytes
            || partition.facts_bytes == 0
            || !digest(&partition.facts_sha256)
            || partition.predicate.allowed_graph_ids.is_empty()
            || !ordinals.insert(partition.ordinal)
        {
            return Err(NativeRuntimeError::InvalidHpcRunPlan);
        }
        let expected_mode = match plan.local_backend {
            LocalComputeBackend::Rust => crate::LeafExecutionMode::Rust,
            LocalComputeBackend::OpenMp => crate::LeafExecutionMode::OpenMp,
        };
        if partition.limits.execution_mode != expected_mode {
            return Err(NativeRuntimeError::InvalidHpcRunPlan);
        }
    }
    let count = u32::try_from(plan.partitions.len()).map_err(|_| NativeRuntimeError::LimitExceeded)?;
    if count < plan.rank_count || ordinals.iter().copied().ne(0..count) {
        return Err(NativeRuntimeError::InvalidHpcRunPlan);
    }
    Ok(sha256_json(plan)?)
}

/// Execute the stable subset `ordinal % rank_count == rank`.
pub fn execute_hpc_rank(
    plan: &HpcRunPlan,
    rank: u32,
    rank_count: u32,
    cancelled: &AtomicBool,
) -> Result<HpcRankReceipt, NativeRuntimeError> {
    let plan_sha256 = validate_hpc_run_plan(plan)?;
    if rank_count != plan.rank_count || rank >= rank_count {
        return Err(NativeRuntimeError::InvalidHpcRank);
    }
    let assigned = plan
        .partitions
        .iter()
        .filter(|value| value.ordinal % rank_count == rank)
        .collect::<Vec<_>>();
    let parallelism = match plan.local_backend {
        LocalComputeBackend::Rust => plan.local_compute_threads.min(assigned.len().max(1)),
        LocalComputeBackend::OpenMp => 1,
    };
    let receipts = std::thread::scope(|scope| -> Result<Vec<HpcPartitionReceipt>, NativeRuntimeError> {
        let mut handles = Vec::new();
        for lane in 0..parallelism {
            let lane_partitions = assigned
                .iter()
                .skip(lane)
                .step_by(parallelism)
                .copied()
                .collect::<Vec<_>>();
            handles.push(scope.spawn(move || -> Result<Vec<HpcPartitionReceipt>, NativeRuntimeError> {
                let mut lane_receipts = Vec::new();
                for partition in lane_partitions {
                    let result = scan_verified_parquet_leaf(
                        &partition.facts_path,
                        &partition.facts_sha256,
                        partition.facts_bytes,
                        partition.semantic_partition,
                        partition.predicate.clone(),
                        partition.limits,
                        cancelled,
                    )?;
                    lane_receipts.push(HpcPartitionReceipt {
                        ordinal: partition.ordinal,
                        semantic_partition: partition.semantic_partition,
                        input_sha256: result.input_sha256,
                        output_sha256: result.output_sha256,
                        scanned_rows: result.scanned_rows,
                        physically_scanned_rows: result.physically_scanned_rows,
                        pruned_row_groups: result.pruned_row_groups,
                        pruned_rows: result.pruned_rows,
                        matched_rows: result.matched_rows,
                        decoded_bytes: result.decoded_bytes,
                    });
                }
                Ok(lane_receipts)
            }));
        }
        let mut combined = Vec::new();
        for handle in handles {
            combined.extend(handle.join().map_err(|_| NativeRuntimeError::HpcWorkerPanicked)??);
        }
        Ok(combined)
    })?;
    let mut receipts = receipts;
    let mut total_rows = 0_u64;
    let mut total_decoded = 0_u64;
    for result in &receipts {
        total_rows = total_rows
            .checked_add(result.matched_rows)
            .filter(|value| *value <= plan.maximum_rows)
            .ok_or(NativeRuntimeError::LimitExceeded)?;
        total_decoded = total_decoded
            .checked_add(result.decoded_bytes)
            .filter(|value| *value <= plan.maximum_decoded_bytes)
            .ok_or(NativeRuntimeError::LimitExceeded)?;
    }
    receipts.sort_by_key(|value| value.ordinal);
    let partition_set_sha256 = sha256_json(&receipts)?;
    Ok(HpcRankReceipt {
        format_version: HPC_RUN_FORMAT_VERSION,
        run_id: plan.run_id.clone(),
        plan_sha256,
        snapshot_manifest_sha256: plan.snapshot_manifest_sha256.clone(),
        semantic_root_sha256: plan.semantic_root_sha256.clone(),
        authorized_graph_set_sha256: plan.authorized_graph_set_sha256.clone(),
        rank,
        rank_count,
        partitions: receipts,
        partition_set_sha256,
        complete: true,
    })
}

/// Verify every rank and partition before constructing the canonical run root.
pub fn finalize_hpc_run(
    plan: &HpcRunPlan,
    receipts: &[HpcRankReceipt],
) -> Result<HpcRunCertificate, NativeRuntimeError> {
    let plan_sha256 = validate_hpc_run_plan(plan)?;
    if receipts.len() != usize::try_from(plan.rank_count).map_err(|_| NativeRuntimeError::LimitExceeded)? {
        return Err(NativeRuntimeError::IncompleteHpcRun);
    }
    let mut by_rank = BTreeMap::new();
    let mut partitions = BTreeMap::new();
    let mut scanned_rows = 0_u64;
    let mut physically_scanned_rows = 0_u64;
    let mut pruned_row_groups = 0_u64;
    let mut pruned_rows = 0_u64;
    let mut matched_rows = 0_u64;
    let mut decoded_bytes = 0_u64;
    for receipt in receipts {
        if receipt.format_version != HPC_RUN_FORMAT_VERSION
            || !receipt.complete
            || receipt.run_id != plan.run_id
            || receipt.plan_sha256 != plan_sha256
            || receipt.snapshot_manifest_sha256 != plan.snapshot_manifest_sha256
            || receipt.semantic_root_sha256 != plan.semantic_root_sha256
            || receipt.authorized_graph_set_sha256 != plan.authorized_graph_set_sha256
            || receipt.rank_count != plan.rank_count
            || receipt.rank >= plan.rank_count
            || receipt.partition_set_sha256 != sha256_json(&receipt.partitions)?
            || by_rank.insert(receipt.rank, receipt.clone()).is_some()
        {
            return Err(NativeRuntimeError::InvalidHpcReceipt);
        }
        for partition in &receipt.partitions {
            if partition.ordinal % plan.rank_count != receipt.rank
                || !digest(&partition.input_sha256)
                || !digest(&partition.output_sha256)
                || partition
                    .physically_scanned_rows
                    .checked_add(partition.pruned_rows)
                    != Some(partition.scanned_rows)
                || partition.matched_rows > partition.physically_scanned_rows
                || partitions.insert(partition.ordinal, partition.clone()).is_some()
            {
                return Err(NativeRuntimeError::InvalidHpcReceipt);
            }
            scanned_rows = scanned_rows.checked_add(partition.scanned_rows).ok_or(NativeRuntimeError::LimitExceeded)?;
            physically_scanned_rows = physically_scanned_rows.checked_add(partition.physically_scanned_rows).ok_or(NativeRuntimeError::LimitExceeded)?;
            pruned_row_groups = pruned_row_groups.checked_add(partition.pruned_row_groups).ok_or(NativeRuntimeError::LimitExceeded)?;
            pruned_rows = pruned_rows.checked_add(partition.pruned_rows).ok_or(NativeRuntimeError::LimitExceeded)?;
            matched_rows = matched_rows
                .checked_add(partition.matched_rows)
                .filter(|value| *value <= plan.maximum_rows)
                .ok_or(NativeRuntimeError::LimitExceeded)?;
            decoded_bytes = decoded_bytes
                .checked_add(partition.decoded_bytes)
                .filter(|value| *value <= plan.maximum_decoded_bytes)
                .ok_or(NativeRuntimeError::LimitExceeded)?;
        }
    }
    let partition_count = u32::try_from(plan.partitions.len()).map_err(|_| NativeRuntimeError::LimitExceeded)?;
    if by_rank.keys().copied().ne(0..plan.rank_count)
        || partitions.keys().copied().ne(0..partition_count)
    {
        return Err(NativeRuntimeError::IncompleteHpcRun);
    }
    for expected in &plan.partitions {
        let actual = partitions.get(&expected.ordinal).ok_or(NativeRuntimeError::IncompleteHpcRun)?;
        if actual.semantic_partition != expected.semantic_partition
            || actual.input_sha256 != expected.facts_sha256
        {
            return Err(NativeRuntimeError::InvalidHpcReceipt);
        }
    }
    Ok(HpcRunCertificate {
        format_version: HPC_RUN_FORMAT_VERSION,
        run_id: plan.run_id.clone(),
        plan_sha256,
        snapshot_manifest_sha256: plan.snapshot_manifest_sha256.clone(),
        semantic_root_sha256: plan.semantic_root_sha256.clone(),
        authorized_graph_set_sha256: plan.authorized_graph_set_sha256.clone(),
        rank_count: plan.rank_count,
        partition_count,
        scanned_rows,
        physically_scanned_rows,
        pruned_row_groups,
        pruned_rows,
        matched_rows,
        decoded_bytes,
        rank_receipt_set_sha256: sha256_json(&by_rank)?,
        result_set_sha256: sha256_json(&partitions)?,
        complete: true,
    })
}

/// Publish bytes without overwriting a different retry result.
pub fn write_content_bound_json<T: Serialize>(path: &Path, value: &T) -> Result<String, NativeRuntimeError> {
    let bytes = serde_json::to_vec(value)?;
    let checksum = hex::encode(Sha256::digest(&bytes));
    if let Ok(existing) = fs::read(path) {
        return if existing == bytes { Ok(checksum) } else { Err(NativeRuntimeError::ConflictingCompletion) };
    }
    let parent = path.parent().ok_or(NativeRuntimeError::InvalidHpcRunPlan)?;
    fs::create_dir_all(parent)?;
    let name = path.file_name().and_then(|value| value.to_str()).ok_or(NativeRuntimeError::InvalidHpcRunPlan)?;
    let temporary = parent.join(format!(
        ".{name}.{}.{}.partial",
        std::process::id(),
        RECEIPT_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed),
    ));
    let mut file = OpenOptions::new().create_new(true).write(true).open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    match fs::hard_link(&temporary, path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            if fs::read(path)? != bytes {
                let _ = fs::remove_file(&temporary);
                return Err(NativeRuntimeError::ConflictingCompletion);
            }
        }
        Err(error) => {
            let _ = fs::remove_file(&temporary);
            return Err(error.into());
        }
    }
    fs::remove_file(temporary)?;
    File::open(parent)?.sync_all()?;
    Ok(checksum)
}

fn sha256_json<T: Serialize>(value: &T) -> Result<String, NativeRuntimeError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(value)?)))
}

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, fs};

    use sha2::{Digest, Sha256};
    use ngkg_hpc_runtime::LocalComputeBackend;

    use super::{
        HpcPartitionPlan, HpcPartitionReceipt, HpcRankReceipt, HpcRunPlan,
        finalize_hpc_run, validate_hpc_run_plan, write_content_bound_json,
    };
    use crate::{LeafExecutionMode, LeafPredicate, LeafScanLimits};

    fn hash(value: char) -> String {
        std::iter::repeat_n(value, 64).collect()
    }

    fn plan(path: &std::path::Path, bytes: &[u8]) -> HpcRunPlan {
        let partition = |ordinal| HpcPartitionPlan {
            ordinal,
            semantic_partition: ordinal,
            facts_path: path.to_path_buf(),
            facts_sha256: hex::encode(Sha256::digest(bytes)),
            facts_bytes: u64::try_from(bytes.len()).unwrap_or(0),
            predicate: LeafPredicate {
                allowed_graph_ids: BTreeSet::from([7]),
                require_queryable: true,
                ..LeafPredicate::default()
            },
            limits: LeafScanLimits {
                max_rows: 10,
                max_decoded_bytes: 1_024,
                batch_rows: 2,
                execution_mode: LeafExecutionMode::Rust,
            },
        };
        HpcRunPlan {
            format_version: 1,
            run_id: "run-1".to_owned(),
            snapshot_manifest_sha256: hash('a'),
            semantic_root_sha256: hash('b'),
            authorized_graph_set_sha256: hash('c'),
            rank_count: 2,
            local_backend: LocalComputeBackend::Rust,
            local_compute_threads: 2,
            blocking_io_threads: 1,
            estimated_row_bytes: 128,
            maximum_rows: 10,
            maximum_decoded_bytes: 2_048,
            partitions: vec![partition(0), partition(1)],
        }
    }

    #[test]
    fn rank_zero_cannot_finalize_an_incomplete_or_conflicting_gang() -> Result<(), Box<dyn std::error::Error>> {
        let directory = std::env::temp_dir().join(format!("ngkg-hpc-plan-{}", std::process::id()));
        fs::create_dir_all(&directory)?;
        let path = directory.join("partition.parquet");
        let bytes = b"immutable-test-artifact";
        fs::write(&path, bytes)?;
        let plan = plan(&path, bytes);
        let plan_sha256 = validate_hpc_run_plan(&plan)?;
        let receipt = |rank, ordinal| {
            let partitions = vec![HpcPartitionReceipt {
                ordinal,
                semantic_partition: ordinal,
                input_sha256: hex::encode(Sha256::digest(bytes)),
                output_sha256: hash(if rank == 0 { 'd' } else { 'e' }),
                scanned_rows: 1,
                physically_scanned_rows: 1,
                pruned_row_groups: 0,
                pruned_rows: 0,
                matched_rows: 1,
                decoded_bytes: 32,
            }];
            HpcRankReceipt {
                format_version: 1,
                run_id: plan.run_id.clone(),
                plan_sha256: plan_sha256.clone(),
                snapshot_manifest_sha256: plan.snapshot_manifest_sha256.clone(),
                semantic_root_sha256: plan.semantic_root_sha256.clone(),
                authorized_graph_set_sha256: plan.authorized_graph_set_sha256.clone(),
                rank,
                rank_count: 2,
                partition_set_sha256: super::sha256_json(&partitions).unwrap_or_default(),
                partitions,
                complete: true,
            }
        };
        assert!(finalize_hpc_run(&plan, &[receipt(0, 0)]).is_err());
        let certificate = finalize_hpc_run(&plan, &[receipt(1, 1), receipt(0, 0)])?;
        assert!(certificate.complete);
        assert_eq!(certificate.partition_count, 2);
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn receipt_publication_is_idempotent_but_never_overwrites() -> Result<(), Box<dyn std::error::Error>> {
        let directory = std::env::temp_dir().join(format!("ngkg-hpc-receipt-{}", std::process::id()));
        fs::create_dir_all(&directory)?;
        let path = directory.join("rank-0.json");
        let first = serde_json::json!({"complete": true, "rank": 0});
        let conflicting = serde_json::json!({"complete": true, "rank": 1});
        assert_eq!(write_content_bound_json(&path, &first)?, write_content_bound_json(&path, &first)?);
        assert!(write_content_bound_json(&path, &conflicting).is_err());
        fs::remove_dir_all(directory)?;
        Ok(())
    }

    #[test]
    fn every_admitted_rank_has_at_least_one_partition() -> Result<(), Box<dyn std::error::Error>> {
        let directory = std::env::temp_dir().join(format!("ngkg-hpc-ranks-{}", std::process::id()));
        fs::create_dir_all(&directory)?;
        let path = directory.join("partition.parquet");
        let bytes = b"immutable-test-artifact";
        fs::write(&path, bytes)?;
        let mut invalid = plan(&path, bytes);
        invalid.rank_count = 3;
        assert!(validate_hpc_run_plan(&invalid).is_err());
        fs::remove_dir_all(directory)?;
        Ok(())
    }
}
