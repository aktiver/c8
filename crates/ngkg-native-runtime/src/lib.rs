//! Fail-closed native distributed query admission, Parquet leaf scans, and completion barriers.
//!
//! This crate deliberately does not depend on Oxigraph, the reference runtime, HTTP, Kubernetes,
//! or a database.  It is the storage/execution boundary shared by query coordinators and workers.
//! The scalar evaluator remains a qualification oracle outside this crate and cannot be selected
//! by a production cutover decision.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufReader, Read},
    path::Path,
    str::FromStr,
    sync::atomic::{AtomicBool, Ordering},
};

use arrow_array::{Array, BooleanArray, RecordBatch, StringArray, UInt32Array, UInt64Array, UInt8Array};
use ngkg_query_planner::{AlgebraExecutionLane, DistributedAlgebraPlan};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// Version of the native public-query cutover contract.
pub const NATIVE_CUTOVER_FORMAT_VERSION: u32 = 1;

/// Runtime policy for the public query surface.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeCutoverMode {
    /// Legacy evaluator is permitted. Development only.
    Disabled,
    /// Native execution is compared with the oracle but its result is not served.
    Shadow,
    /// Only a complete native plan or an explicitly bounded exact-reasoner stage may execute.
    Required,
}

impl NativeCutoverMode {
    /// True when a missing native certificate must reject the query.
    #[must_use]
    pub const fn requires_native(self) -> bool {
        matches!(self, Self::Required)
    }

    /// Stable wire value used by qualification and operational evidence.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Shadow => "shadow",
            Self::Required => "required",
        }
    }
}

impl FromStr for NativeCutoverMode {
    type Err = NativeRuntimeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "disabled" => Ok(Self::Disabled),
            "shadow" => Ok(Self::Shadow),
            "required" => Ok(Self::Required),
            _ => Err(NativeRuntimeError::InvalidCutoverMode(value.to_owned())),
        }
    }
}

/// Snapshot-bound evidence authorizing exact reasoning for only uncovered BGP stages.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReasoningCoverage {
    /// Versioned finite-closure index identity.
    pub closure_index_sha256: String,
    /// Algebra stage IDs proven complete in the finite closure.
    pub covered_stage_ids: BTreeSet<String>,
    /// Algebra stage IDs proven outside finite coverage and authorized for exact HermiT.
    pub exact_stage_ids: BTreeSet<String>,
    /// Proof manifest binding the two disjoint sets to the published snapshot.
    pub coverage_proof_sha256: String,
}

/// Immutable native cutover certificate emitted during offline compilation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NativeQueryCertificate {
    pub format_version: u32,
    pub query_sha256: String,
    pub algebra_sha256: String,
    pub plan_sha256: String,
    pub snapshot_manifest_sha256: String,
    pub authorized_graph_set_sha256: String,
    pub active_dataset_sha256: String,
    pub partition_count: u32,
    pub maximum_input_rows: u64,
    pub maximum_output_rows: u64,
    pub maximum_exchange_bytes: u64,
    pub maximum_spill_bytes: u64,
    pub reasoning: ReasoningCoverage,
}

/// Result of admitting a plan to the public native runtime.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeAdmission {
    pub plan_sha256: String,
    pub native_stage_count: u32,
    pub exact_reasoner_stage_count: u32,
    pub partition_count: u32,
}

/// Verify all immutable identities and reject every scalar-oracle stage in required mode.
pub fn admit_native_plan(
    mode: NativeCutoverMode,
    certificate: &NativeQueryCertificate,
    plan: &DistributedAlgebraPlan,
    query_sha256: &str,
    snapshot_manifest_sha256: &str,
    authorized_graph_set_sha256: &str,
    active_dataset_sha256: &str,
) -> Result<NativeAdmission, NativeRuntimeError> {
    if certificate.format_version != NATIVE_CUTOVER_FORMAT_VERSION
        || plan.format_version != 1
        || !sha256(query_sha256)
        || !sha256(snapshot_manifest_sha256)
        || !sha256(authorized_graph_set_sha256)
        || !sha256(active_dataset_sha256)
        || certificate.query_sha256 != query_sha256
        || certificate.algebra_sha256 != plan.query_algebra_sha256
        || certificate.snapshot_manifest_sha256 != snapshot_manifest_sha256
        || certificate.authorized_graph_set_sha256 != authorized_graph_set_sha256
        || certificate.active_dataset_sha256 != active_dataset_sha256
        || certificate.partition_count < 2
        || certificate.maximum_input_rows == 0
        || certificate.maximum_output_rows == 0
        || certificate.maximum_exchange_bytes == 0
        || certificate.maximum_spill_bytes == 0
        || !sha256(&certificate.reasoning.closure_index_sha256)
        || !sha256(&certificate.reasoning.coverage_proof_sha256)
    {
        return Err(NativeRuntimeError::CertificateMismatch);
    }
    let encoded = serde_json::to_vec(plan)?;
    let actual_plan_sha256 = hex::encode(Sha256::digest(encoded));
    if certificate.plan_sha256 != actual_plan_sha256 {
        return Err(NativeRuntimeError::PlanChecksumMismatch);
    }
    if !plan.require_complete_partition_set {
        return Err(NativeRuntimeError::IncompletePlan);
    }

    let all_stage_ids = plan
        .stages
        .iter()
        .map(|stage| stage.stage_id.as_str())
        .collect::<BTreeSet<_>>();
    if certificate
        .reasoning
        .covered_stage_ids
        .intersection(&certificate.reasoning.exact_stage_ids)
        .next()
        .is_some()
        || certificate
            .reasoning
            .covered_stage_ids
            .iter()
            .chain(certificate.reasoning.exact_stage_ids.iter())
            .any(|stage| !all_stage_ids.contains(stage.as_str()))
    {
        return Err(NativeRuntimeError::InvalidReasoningCoverage);
    }

    let mut native = 0_u32;
    let mut exact = 0_u32;
    for stage in &plan.stages {
        if stage.partition_count != certificate.partition_count
            || stage.max_input_rows > certificate.maximum_input_rows
            || stage.max_output_rows > certificate.maximum_output_rows
            || stage.max_exchange_bytes > certificate.maximum_exchange_bytes
            || stage.max_spill_bytes > certificate.maximum_spill_bytes
        {
            return Err(NativeRuntimeError::StageBudgetMismatch(stage.stage_id.clone()));
        }
        match stage.lane {
            AlgebraExecutionLane::NativePartitioned => native = native.saturating_add(1),
            AlgebraExecutionLane::ExactReasonerPartitioned => {
                if !certificate.reasoning.covered_stage_ids.contains(&stage.stage_id)
                    && !certificate.reasoning.exact_stage_ids.contains(&stage.stage_id)
                {
                    return Err(NativeRuntimeError::InvalidReasoningCoverage);
                }
                exact = exact.saturating_add(1);
            }
            AlgebraExecutionLane::ScalarOraclePartitioned if mode.requires_native() => {
                return Err(NativeRuntimeError::ScalarOracleForbidden(stage.stage_id.clone()));
            }
            AlgebraExecutionLane::ScalarOraclePartitioned => {}
        }
    }
    Ok(NativeAdmission {
        plan_sha256: actual_plan_sha256,
        native_stage_count: native,
        exact_reasoner_stage_count: exact,
        partition_count: certificate.partition_count,
    })
}

/// Dictionary-ID predicate pushed into one immutable Parquet leaf scan.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LeafPredicate {
    pub subject_id: Option<u64>,
    pub predicate_id: Option<u64>,
    pub object_id: Option<u64>,
    pub graph_id: Option<u64>,
    /// Worker-derived graph IDs authorized by the authenticated active dataset.
    #[serde(default)]
    pub allowed_graph_ids: BTreeSet<u64>,
    /// If set, skip rows that are not exposed as RDF.
    pub require_queryable: bool,
}

/// Hard memory and work bounds for one file scan.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LeafScanLimits {
    pub max_rows: usize,
    pub max_decoded_bytes: u64,
    pub batch_rows: usize,
}

/// Compact encoded RDF fact returned by a native leaf scan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EncodedFact {
    pub subject_id: u64,
    pub predicate_id: u64,
    pub object_kind: u8,
    pub object_id: u64,
    pub graph_id: u64,
    pub partition_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_nquad: Option<String>,
}

/// Complete result and evidence for a checksum-verified partition scan.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeafScanResult {
    pub input_sha256: String,
    pub output_sha256: String,
    pub scanned_rows: u64,
    pub matched_rows: u64,
    pub decoded_bytes: u64,
    pub rows: Vec<EncodedFact>,
    pub complete: bool,
}

/// Verify and scan one immutable semantic `facts.parquet` partition.
///
/// Parquet record batches bound peak RAM. Predicate checks happen before row materialization and
/// cancellation is observed between every batch. A checksum or limit failure returns no partial
/// result.
pub fn scan_verified_parquet_leaf(
    path: &Path,
    expected_sha256: &str,
    expected_bytes: u64,
    expected_partition: u32,
    predicate: LeafPredicate,
    limits: LeafScanLimits,
    cancelled: &AtomicBool,
) -> Result<LeafScanResult, NativeRuntimeError> {
    if !sha256(expected_sha256) || limits.max_rows == 0 || limits.max_decoded_bytes == 0 || limits.batch_rows == 0 {
        return Err(NativeRuntimeError::InvalidScanContract);
    }
    let metadata = path.metadata()?;
    if metadata.len() != expected_bytes || file_sha256(path)? != expected_sha256 {
        return Err(NativeRuntimeError::ArtifactMismatch);
    }
    let file = File::open(path)?;
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(limits.batch_rows)
        .build()?;
    let mut scanned = 0_u64;
    let mut decoded = 0_u64;
    let mut rows = Vec::new();
    for batch in &mut reader {
        if cancelled.load(Ordering::Acquire) {
            return Err(NativeRuntimeError::Cancelled);
        }
        let batch = batch?;
        let columns = ScanColumns::new(&batch)?;
        for row in 0..batch.num_rows() {
            scanned = scanned.checked_add(1).ok_or(NativeRuntimeError::LimitExceeded)?;
            if columns.partition(row)? != u64::from(expected_partition) {
                return Err(NativeRuntimeError::PartitionMismatch);
            }
            if predicate.require_queryable && !columns.queryable(row)? {
                continue;
            }
            let subject = columns.subject(row)?;
            let property = columns.predicate(row)?;
            let object = columns.object(row)?;
            let graph = columns.graph(row)?;
            if predicate.allowed_graph_ids.is_empty()
                || !predicate.allowed_graph_ids.contains(&graph)
            {
                continue;
            }
            if predicate.subject_id.is_some_and(|value| value != subject)
                || predicate.predicate_id.is_some_and(|value| value != property)
                || predicate.object_id.is_some_and(|value| value != object)
                || predicate.graph_id.is_some_and(|value| value != graph)
            {
                continue;
            }
            if rows.len() == limits.max_rows {
                return Err(NativeRuntimeError::LimitExceeded);
            }
            let canonical_nquad = columns.canonical(row)?;
            decoded = decoded
                .checked_add(49_u64.saturating_add(canonical_nquad.as_ref().map_or(0, |value| value.len() as u64)))
                .filter(|value| *value <= limits.max_decoded_bytes)
                .ok_or(NativeRuntimeError::LimitExceeded)?;
            rows.push(EncodedFact {
                subject_id: subject,
                predicate_id: property,
                object_kind: columns.object_kind(row)?,
                object_id: object,
                graph_id: graph,
                partition_id: u64::from(expected_partition),
                canonical_nquad,
            });
        }
    }
    let output_sha256 = logical_rows_sha256(&rows)?;
    Ok(LeafScanResult {
        input_sha256: expected_sha256.to_owned(),
        output_sha256,
        scanned_rows: scanned,
        matched_rows: u64::try_from(rows.len()).map_err(|_| NativeRuntimeError::LimitExceeded)?,
        decoded_bytes: decoded,
        rows,
        complete: true,
    })
}

struct ScanColumns<'a> {
    batch: &'a RecordBatch,
    subject: usize,
    predicate: usize,
    object_kind: usize,
    object: usize,
    graph: usize,
    partition: Option<usize>,
    queryable: Option<usize>,
    canonical: Option<usize>,
}

impl<'a> ScanColumns<'a> {
    fn new(batch: &'a RecordBatch) -> Result<Self, NativeRuntimeError> {
        let schema = batch.schema();
        Ok(Self {
            batch,
            subject: schema.index_of("subject_id64")?,
            predicate: schema.index_of("predicate_id64")?,
            object_kind: schema.index_of("object_kind")?,
            object: schema.index_of("object_id64").or_else(|_| schema.index_of("object_term_id64"))?,
            graph: schema.index_of("graph_id64")?,
            partition: schema.index_of("partition_id").ok(),
            queryable: schema.index_of("queryable_as_rdf").ok(),
            canonical: schema.index_of("canonical_nquad").ok(),
        })
    }

    fn integer(&self, column: usize, row: usize) -> Result<u64, NativeRuntimeError> {
        let array = self.batch.column(column);
        if array.is_null(row) {
            return Err(NativeRuntimeError::InvalidParquetSchema);
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt64Array>() {
            return Ok(values.value(row));
        }
        if let Some(values) = array.as_any().downcast_ref::<UInt32Array>() {
            return Ok(u64::from(values.value(row)));
        }
        Err(NativeRuntimeError::InvalidParquetSchema)
    }

    fn subject(&self, row: usize) -> Result<u64, NativeRuntimeError> { self.integer(self.subject, row) }
    fn predicate(&self, row: usize) -> Result<u64, NativeRuntimeError> { self.integer(self.predicate, row) }
    fn object(&self, row: usize) -> Result<u64, NativeRuntimeError> { self.integer(self.object, row) }
    fn graph(&self, row: usize) -> Result<u64, NativeRuntimeError> { self.integer(self.graph, row) }
    fn partition(&self, row: usize) -> Result<u64, NativeRuntimeError> {
        self.partition.map_or(Ok(0), |column| self.integer(column, row))
    }
    fn object_kind(&self, row: usize) -> Result<u8, NativeRuntimeError> {
        let values = self.batch.column(self.object_kind).as_any().downcast_ref::<UInt8Array>()
            .ok_or(NativeRuntimeError::InvalidParquetSchema)?;
        if values.is_null(row) { return Err(NativeRuntimeError::InvalidParquetSchema); }
        Ok(values.value(row))
    }
    fn queryable(&self, row: usize) -> Result<bool, NativeRuntimeError> {
        let Some(column) = self.queryable else { return Ok(true); };
        let values = self.batch.column(column).as_any().downcast_ref::<BooleanArray>()
            .ok_or(NativeRuntimeError::InvalidParquetSchema)?;
        Ok(!values.is_null(row) && values.value(row))
    }
    fn canonical(&self, row: usize) -> Result<Option<String>, NativeRuntimeError> {
        let Some(column) = self.canonical else { return Ok(None); };
        let values = self.batch.column(column).as_any().downcast_ref::<StringArray>()
            .ok_or(NativeRuntimeError::InvalidParquetSchema)?;
        Ok((!values.is_null(row)).then(|| values.value(row).to_owned()))
    }
}

/// One terminal worker result submitted to a stage barrier.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PartitionCompletion {
    pub partition: u32,
    pub partition_count: u32,
    pub worker_id: String,
    pub pod_uid: String,
    pub node_uid: String,
    pub input_sha256: String,
    pub output_sha256: String,
    pub row_count: u64,
    pub exchange_bytes: u64,
    pub spill_bytes: u64,
    pub complete: bool,
}

/// Completion certificate generated only after the exact partition set passes its barrier.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StageCompletionCertificate {
    pub stage_id: String,
    pub partition_count: u32,
    pub row_count: u64,
    pub exchange_bytes: u64,
    pub spill_bytes: u64,
    pub worker_ids: BTreeSet<String>,
    pub pod_uids: BTreeSet<String>,
    pub node_uids: BTreeSet<String>,
    pub partition_set_sha256: String,
    pub complete: bool,
}

/// Bounded exact-once terminal barrier. Duplicate delivery is accepted only when byte-identical.
pub struct StageBarrier {
    stage_id: String,
    partition_count: u32,
    max_rows: u64,
    max_exchange_bytes: u64,
    max_spill_bytes: u64,
    completions: BTreeMap<u32, PartitionCompletion>,
}

impl StageBarrier {
    pub fn new(stage_id: String, partition_count: u32, max_rows: u64, max_exchange_bytes: u64, max_spill_bytes: u64) -> Result<Self, NativeRuntimeError> {
        if stage_id.is_empty() || partition_count < 2 || max_rows == 0 || max_exchange_bytes == 0 || max_spill_bytes == 0 {
            return Err(NativeRuntimeError::InvalidBarrier);
        }
        Ok(Self { stage_id, partition_count, max_rows, max_exchange_bytes, max_spill_bytes, completions: BTreeMap::new() })
    }

    pub fn record(&mut self, completion: PartitionCompletion) -> Result<(), NativeRuntimeError> {
        if !completion.complete
            || completion.partition_count != self.partition_count
            || completion.partition >= self.partition_count
            || completion.worker_id.is_empty()
            || completion.pod_uid.is_empty()
            || completion.node_uid.is_empty()
            || !sha256(&completion.input_sha256)
            || !sha256(&completion.output_sha256)
        {
            return Err(NativeRuntimeError::InvalidCompletion);
        }
        if let Some(existing) = self.completions.get(&completion.partition) {
            return if existing == &completion { Ok(()) } else { Err(NativeRuntimeError::ConflictingCompletion) };
        }
        let (rows, exchange, spill) = totals(self.completions.values())?;
        if rows.checked_add(completion.row_count).is_none_or(|value| value > self.max_rows)
            || exchange.checked_add(completion.exchange_bytes).is_none_or(|value| value > self.max_exchange_bytes)
            || spill.checked_add(completion.spill_bytes).is_none_or(|value| value > self.max_spill_bytes)
        {
            return Err(NativeRuntimeError::LimitExceeded);
        }
        self.completions.insert(completion.partition, completion);
        Ok(())
    }

    pub fn finish(self) -> Result<StageCompletionCertificate, NativeRuntimeError> {
        if self.completions.len() != self.partition_count as usize
            || self.completions.keys().copied().ne(0..self.partition_count)
        {
            return Err(NativeRuntimeError::IncompletePartitionSet);
        }
        let (rows, exchange, spill) = totals(self.completions.values())?;
        if rows > self.max_rows || exchange > self.max_exchange_bytes || spill > self.max_spill_bytes {
            return Err(NativeRuntimeError::LimitExceeded);
        }
        let digest = hex::encode(Sha256::digest(serde_json::to_vec(&self.completions)?));
        Ok(StageCompletionCertificate {
            stage_id: self.stage_id,
            partition_count: self.partition_count,
            row_count: rows,
            exchange_bytes: exchange,
            spill_bytes: spill,
            worker_ids: self.completions.values().map(|value| value.worker_id.clone()).collect(),
            pod_uids: self.completions.values().map(|value| value.pod_uid.clone()).collect(),
            node_uids: self.completions.values().map(|value| value.node_uid.clone()).collect(),
            partition_set_sha256: digest,
            complete: true,
        })
    }

}

fn totals<'a>(values: impl Iterator<Item = &'a PartitionCompletion>) -> Result<(u64, u64, u64), NativeRuntimeError> {
    values.try_fold((0_u64, 0_u64, 0_u64), |(rows, exchange, spill), value| {
        Ok((
            rows.checked_add(value.row_count).ok_or(NativeRuntimeError::LimitExceeded)?,
            exchange.checked_add(value.exchange_bytes).ok_or(NativeRuntimeError::LimitExceeded)?,
            spill.checked_add(value.spill_bytes).ok_or(NativeRuntimeError::LimitExceeded)?,
        ))
    })
}

fn logical_rows_sha256(rows: &[EncodedFact]) -> Result<String, NativeRuntimeError> {
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(rows)?)))
}

fn file_sha256(path: &Path) -> Result<String, NativeRuntimeError> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 1024 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 { break; }
        hash.update(&buffer[..count]);
    }
    Ok(hex::encode(hash.finalize()))
}

fn sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[derive(Debug, Error)]
pub enum NativeRuntimeError {
    #[error("invalid native cutover mode: {0}")]
    InvalidCutoverMode(String),
    #[error("native query certificate does not match the request identity")]
    CertificateMismatch,
    #[error("native algebra plan checksum mismatch")]
    PlanChecksumMismatch,
    #[error("native algebra plan does not require a complete partition set")]
    IncompletePlan,
    #[error("invalid or overlapping reasoning coverage")]
    InvalidReasoningCoverage,
    #[error("scalar-oracle stage is forbidden after native cutover: {0}")]
    ScalarOracleForbidden(String),
    #[error("stage budget exceeds its native certificate: {0}")]
    StageBudgetMismatch(String),
    #[error("invalid native Parquet scan contract")]
    InvalidScanContract,
    #[error("native Parquet artifact checksum or size mismatch")]
    ArtifactMismatch,
    #[error("native Parquet schema is incompatible")]
    InvalidParquetSchema,
    #[error("native Parquet row belongs to a different partition")]
    PartitionMismatch,
    #[error("native execution resource ceiling exceeded")]
    LimitExceeded,
    #[error("native execution cancelled")]
    Cancelled,
    #[error("invalid stage barrier")]
    InvalidBarrier,
    #[error("invalid partition completion")]
    InvalidCompletion,
    #[error("conflicting duplicate partition completion")]
    ConflictingCompletion,
    #[error("stage partition set is incomplete")]
    IncompletePartitionSet,
    #[error("native runtime I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("native runtime Parquet failed: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("native runtime Arrow schema failed: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("native runtime JSON failed: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::{Arc, atomic::AtomicBool}};

    use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt64Array, UInt8Array};
    use arrow_schema::{DataType, Field, Schema};
    use ngkg_query_planner::{
        AlgebraExecutionLane, DistributedAlgebraOperator, DistributedAlgebraPlan,
        DistributedAlgebraStage,
    };
    use parquet::arrow::ArrowWriter;
    use sha2::{Digest, Sha256};

    use super::{
        LeafPredicate, LeafScanLimits, NativeCutoverMode, NativeQueryCertificate,
        PartitionCompletion, ReasoningCoverage, StageBarrier, admit_native_plan, file_sha256,
        scan_verified_parquet_leaf,
    };

    fn hash(byte: char) -> String { std::iter::repeat_n(byte, 64).collect() }

    #[test]
    fn required_mode_parses_and_fails_closed() {
        assert!("required".parse::<NativeCutoverMode>().is_ok());
        assert!("legacy".parse::<NativeCutoverMode>().is_err());
    }

    #[test]
    fn required_admission_rejects_scalar_oracle_plan() -> Result<(), Box<dyn std::error::Error>> {
        let mut plan = DistributedAlgebraPlan {
            format_version: 1,
            query_algebra_sha256: hash('a'),
            root_stage_id: "stage-1".to_owned(),
            stages: vec![DistributedAlgebraStage {
                stage_id: "stage-1".to_owned(),
                operator: DistributedAlgebraOperator::Join,
                inputs: Vec::new(),
                lane: AlgebraExecutionLane::NativePartitioned,
                algebra_sha256: hash('b'),
                partition_count: 2,
                max_input_rows: 10,
                max_output_rows: 10,
                max_exchange_bytes: 10,
                max_spill_bytes: 10,
            }],
            require_complete_partition_set: true,
            require_scalar_equivalence: true,
        };
        let plan_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&plan)?));
        let certificate = NativeQueryCertificate {
            format_version: 1,
            query_sha256: hash('c'),
            algebra_sha256: hash('a'),
            plan_sha256,
            snapshot_manifest_sha256: hash('d'),
            authorized_graph_set_sha256: hash('e'),
            active_dataset_sha256: hash('f'),
            partition_count: 2,
            maximum_input_rows: 10,
            maximum_output_rows: 10,
            maximum_exchange_bytes: 10,
            maximum_spill_bytes: 10,
            reasoning: ReasoningCoverage {
                closure_index_sha256: hash('1'),
                covered_stage_ids: Default::default(),
                exact_stage_ids: Default::default(),
                coverage_proof_sha256: hash('2'),
            },
        };
        assert!(admit_native_plan(NativeCutoverMode::Required, &certificate, &plan, &hash('c'), &hash('d'), &hash('e'), &hash('f')).is_ok());
        plan.stages[0].lane = AlgebraExecutionLane::ScalarOraclePartitioned;
        let mut rejected = certificate;
        rejected.plan_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&plan)?));
        assert!(admit_native_plan(NativeCutoverMode::Required, &rejected, &plan, &hash('c'), &hash('d'), &hash('e'), &hash('f')).is_err());
        Ok(())
    }

    #[test]
    fn barrier_accepts_identical_retry_and_rejects_conflict() -> Result<(), Box<dyn std::error::Error>> {
        let mut barrier = StageBarrier::new("stage-1".to_owned(), 2, 10, 10, 10)?;
        let first = PartitionCompletion { partition: 0, partition_count: 2, worker_id: "worker-a".to_owned(), pod_uid: "pod-a".to_owned(), node_uid: "node-a".to_owned(), input_sha256: hash('a'), output_sha256: hash('b'), row_count: 2, exchange_bytes: 2, spill_bytes: 0, complete: true };
        assert!(barrier.record(first.clone()).is_ok());
        assert!(barrier.record(first.clone()).is_ok());
        let mut conflict = first;
        conflict.row_count = 3;
        assert!(barrier.record(conflict).is_err());
        Ok(())
    }

    #[test]
    fn barrier_requires_every_partition() -> Result<(), Box<dyn std::error::Error>> {
        let mut barrier = StageBarrier::new("stage-1".to_owned(), 2, 10, 10, 10)?;
        let completion = |partition, worker| PartitionCompletion { partition, partition_count: 2, worker_id: worker.to_owned(), pod_uid: format!("pod-{worker}"), node_uid: format!("node-{worker}"), input_sha256: hash('a'), output_sha256: hash(if partition == 0 { 'b' } else { 'c' }), row_count: 2, exchange_bytes: 2, spill_bytes: 0, complete: true };
        assert!(barrier.record(completion(0, "a")).is_ok());
        assert!(barrier.record(completion(1, "b")).is_ok());
        let certificate = barrier.finish()?;
        assert!(certificate.complete);
        assert_eq!(certificate.partition_count, 2);
        assert_eq!(certificate.node_uids.len(), 2);
        Ok(())
    }

    #[test]
    fn parquet_leaf_scan_is_checksum_bounded_and_graph_filtered() -> Result<(), Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!("ngkg-native-leaf-{}.parquet", std::process::id()));
        let schema = Arc::new(Schema::new(vec![
            Field::new("subject_id64", DataType::UInt64, false),
            Field::new("predicate_id64", DataType::UInt64, false),
            Field::new("object_kind", DataType::UInt8, false),
            Field::new("object_id64", DataType::UInt64, false),
            Field::new("graph_id64", DataType::UInt64, false),
            Field::new("partition_id", DataType::UInt64, false),
            Field::new("canonical_nquad", DataType::Utf8, false),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(vec![1, 2])),
            Arc::new(UInt64Array::from(vec![10, 10])),
            Arc::new(UInt8Array::from(vec![1, 1])),
            Arc::new(UInt64Array::from(vec![3, 4])),
            Arc::new(UInt64Array::from(vec![7, 8])),
            Arc::new(UInt64Array::from(vec![0, 0])),
            Arc::new(StringArray::from(vec!["<a> <p> <b> <g> .", "<c> <p> <d> <h> ."])),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
        let mut writer = ArrowWriter::try_new(File::create(&path)?, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        let checksum = file_sha256(&path)?;
        let bytes = path.metadata()?.len();
        let result = scan_verified_parquet_leaf(
            &path,
            &checksum,
            bytes,
            0,
            LeafPredicate {
                predicate_id: Some(10),
                allowed_graph_ids: [7].into_iter().collect(),
                require_queryable: true,
                ..LeafPredicate::default()
            },
            LeafScanLimits { max_rows: 10, max_decoded_bytes: 4096, batch_rows: 1 },
            &AtomicBool::new(false),
        )?;
        assert!(result.complete);
        assert_eq!(result.scanned_rows, 2);
        assert_eq!(result.matched_rows, 1);
        std::fs::remove_file(path)?;
        Ok(())
    }

    #[test]
    fn cancellation_token_is_constructible_without_async_runtime() {
        assert!(!AtomicBool::new(false).load(std::sync::atomic::Ordering::Relaxed));
    }
}
