//! Bounded worker-local Grace hash join for fully bound SPARQL shuffle partitions.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use ngkg_query_executor::{ExecutionError, grace_partition_for_binding, inner_join_sparql_json};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const ROOT_MARKER: &str = ".ngkg-grace-join-v1";
const ROOT_MARKER_BYTES: &[u8] = b"ngkg-worker-grace-join-v1\n";
const FILE_MAGIC: &[u8; 8] = b"NGKGGR30";
const FILE_HEADER_BYTES: u64 = 49;
const FILE_TRAILER_BYTES: u64 = 32;

/// Immutable semantic identity of one worker-local join execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraceJoinIdentity {
    /// Authenticated tenant.
    pub tenant_id: Uuid,
    /// Dataset UUID.
    pub dataset_id: Uuid,
    /// Published immutable snapshot UUID.
    pub snapshot_id: Uuid,
    /// Exact certified query SHA-256.
    pub query_sha256: String,
    /// Exact distributed plan SHA-256.
    pub plan_sha256: String,
    /// Join stage.
    pub stage: u32,
    /// Cross-node owner partition.
    pub partition: u32,
    /// Total cross-node partition count.
    pub partition_count: u32,
    /// Deterministic checksum of the complete left input relation.
    pub left_input_sha256: String,
    /// Deterministic checksum of the complete right input relation.
    pub right_input_sha256: String,
}

impl GraceJoinIdentity {
    /// Compute the domain-separated binary identity used in every spill header.
    ///
    /// # Errors
    ///
    /// Returns [`GraceJoinError::InvalidIdentity`] for a malformed digest or partition.
    pub fn digest(&self) -> Result<[u8; 32], GraceJoinError> {
        if self.partition_count < 2
            || self.partition >= self.partition_count
            || !is_sha256(&self.query_sha256)
            || !is_sha256(&self.plan_sha256)
            || !is_sha256(&self.left_input_sha256)
            || !is_sha256(&self.right_input_sha256)
        {
            return Err(GraceJoinError::InvalidIdentity);
        }
        let mut hash = Sha256::new();
        hash.update(b"ngkg-worker-grace-identity-v2\0");
        hash.update(self.tenant_id.as_bytes());
        hash.update(self.dataset_id.as_bytes());
        hash.update(self.snapshot_id.as_bytes());
        hash.update(decode_sha256(&self.query_sha256)?);
        hash.update(decode_sha256(&self.plan_sha256)?);
        hash.update(self.stage.to_be_bytes());
        hash.update(self.partition.to_be_bytes());
        hash.update(self.partition_count.to_be_bytes());
        hash.update(decode_sha256(&self.left_input_sha256)?);
        hash.update(decode_sha256(&self.right_input_sha256)?);
        Ok(hash.finalize().into())
    }
}

/// Execution evidence returned with the exact joined bag.
#[derive(Debug, Eq, PartialEq)]
pub struct GraceJoinOutcome {
    /// Exact SPARQL bag rows.
    pub bindings: Vec<Value>,
    /// `in_memory_hash_v1` or `grace_hash_nvme_v1`.
    pub mode: &'static str,
    /// Peak disk bytes reserved by this request.
    pub spill_bytes: u64,
    /// Non-empty local buckets processed.
    pub buckets_processed: u32,
    /// Largest right-side build chunk loaded at once.
    pub max_build_rows: u64,
}

/// Relation side carried by an incrementally decoded worker shuffle row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraceJoinSide {
    /// Probe-side relation.
    Left,
    /// Build-side relation.
    Right,
}

impl GraceJoinSide {
    const fn code(self) -> u8 {
        match self {
            Self::Left => 0,
            Self::Right => 1,
        }
    }
}

/// Fail-closed worker join errors.
#[derive(Debug, Error)]
pub enum GraceJoinError {
    /// Snapshot, plan, input bag, or partition identity is invalid.
    #[error("Grace join identity is invalid")]
    InvalidIdentity,
    /// Engine configuration cannot enforce its resource contract.
    #[error("Grace join configuration is invalid: {0}")]
    InvalidConfiguration(String),
    /// The operator-owned spill root is unsafe or unmanaged.
    #[error("Grace join root is unsafe: {0}")]
    UnsafeRoot(String),
    /// One request exceeded its bounded local-NVMe allocation.
    #[error("Grace join request exceeded its spill-byte ceiling")]
    RequestSpillLimit,
    /// Concurrent requests exhausted the process-local spill allocation.
    #[error("Grace join process spill capacity is exhausted")]
    ProcessSpillLimit,
    /// A spill record exceeded its configured row-byte ceiling.
    #[error("Grace join row exceeded its byte ceiling")]
    RowTooLarge,
    /// Spill accounting overflowed.
    #[error("Grace join spill accounting overflow")]
    AccountingOverflow,
    /// A spill file was truncated, modified, or addressed by another join.
    #[error("Grace join spill file failed integrity validation")]
    CorruptSpill,
    /// Shared spill accounting was poisoned.
    #[error("Grace join spill accounting lock is poisoned")]
    Poisoned,
    /// Exact SPARQL join execution failed.
    #[error("Grace join execution failed: {0}")]
    Execution(#[from] ExecutionError),
    /// Local storage failed.
    #[error("Grace join I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Spill JSON encoding or decoding failed.
    #[error("Grace join JSON failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Shared bounded engine for one fragment-worker process.
pub struct GraceJoinEngine {
    root: PathBuf,
    max_total_spill_bytes: u64,
    max_request_spill_bytes: u64,
    bucket_count: u32,
    max_open_files: usize,
    max_build_rows: usize,
    max_probe_rows: usize,
    max_row_bytes: usize,
    in_memory_build_rows: usize,
    active_spill_bytes: Mutex<u64>,
}

impl GraceJoinEngine {
    /// Validate configuration, take ownership of an empty/marked root, and recover crash debris.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe storage, invalid bounds, or failed crash cleanup.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        root: &Path,
        max_total_spill_bytes: u64,
        max_request_spill_bytes: u64,
        bucket_count: u32,
        max_open_files: usize,
        max_build_rows: usize,
        max_probe_rows: usize,
        max_row_bytes: usize,
        in_memory_build_rows: usize,
    ) -> Result<Self, GraceJoinError> {
        let file_count = usize::try_from(bucket_count)
            .ok()
            .and_then(|count| count.checked_mul(2))
            .ok_or_else(|| {
                GraceJoinError::InvalidConfiguration("bucket file count overflow".to_owned())
            })?;
        if max_total_spill_bytes == 0
            || max_request_spill_bytes == 0
            || max_request_spill_bytes > max_total_spill_bytes
            || bucket_count < 2
            || max_open_files < file_count
            || max_build_rows == 0
            || max_probe_rows == 0
            || max_row_bytes == 0
            || in_memory_build_rows == 0
            || in_memory_build_rows > max_build_rows
        {
            return Err(GraceJoinError::InvalidConfiguration(
                "positive byte/row limits, at least two buckets, two files per bucket, and an in-memory threshold no larger than the build chunk are required"
                    .to_owned(),
            ));
        }
        prepare_root(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            max_total_spill_bytes,
            max_request_spill_bytes,
            bucket_count,
            max_open_files,
            max_build_rows,
            max_probe_rows,
            max_row_bytes,
            in_memory_build_rows,
            active_spill_bytes: Mutex::new(0),
        })
    }

    /// Join one fully bound cross-node partition using RAM or bounded local-NVMe buckets.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities/bindings, output growth beyond `max_rows`,
    /// spill corruption, or exhaustion of the request/process byte budgets.
    pub fn join(
        &self,
        identity: &GraceJoinIdentity,
        left: Vec<Value>,
        right: Vec<Value>,
        join_keys: &[String],
        max_rows: usize,
    ) -> Result<GraceJoinOutcome, GraceJoinError> {
        let identity_digest = identity.digest()?;
        if join_keys.is_empty() || max_rows == 0 {
            return Err(GraceJoinError::InvalidConfiguration(
                "join keys and output row ceiling must be non-empty".to_owned(),
            ));
        }
        if right.len() <= self.in_memory_build_rows {
            let max_build_rows =
                u64::try_from(right.len()).map_err(|_| GraceJoinError::AccountingOverflow)?;
            return Ok(GraceJoinOutcome {
                bindings: inner_join_sparql_json(&left, &right, max_rows)?,
                mode: "in_memory_hash_v1",
                spill_bytes: 0,
                buckets_processed: 0,
                max_build_rows,
            });
        }
        self.join_out_of_core(identity_digest, left, right, join_keys, max_rows)
    }

    /// Join an incrementally decoded partition without retaining either complete relation.
    ///
    /// Every accepted row is immediately assigned to a bounded, checksum-protected local
    /// bucket. An input decoder error aborts the operation and the stage guard releases all
    /// files and process accounting before the error is returned.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid identities, malformed rows, a failed input decoder,
    /// spill exhaustion/corruption, or output growth beyond `max_rows`.
    pub fn join_stream<I>(
        &self,
        identity: &GraceJoinIdentity,
        rows: I,
        join_keys: &[String],
        max_rows: usize,
    ) -> Result<GraceJoinOutcome, GraceJoinError>
    where
        I: IntoIterator<Item = Result<(GraceJoinSide, Value), GraceJoinError>>,
    {
        let identity_digest = identity.digest()?;
        if join_keys.is_empty() || max_rows == 0 {
            return Err(GraceJoinError::InvalidConfiguration(
                "join keys and output row ceiling must be non-empty".to_owned(),
            ));
        }
        let mut stage = SpillStage::create(self, identity_digest)?;
        let mut writers = self.empty_writers()?;
        for decoded in rows {
            let (side, row) = decoded?;
            stage.write_row(
                &mut writers,
                side.code(),
                row,
                join_keys,
                self.max_row_bytes,
            )?;
        }
        let files = stage.finish_writers(writers)?;
        self.execute_spilled(stage, files, identity_digest, max_rows)
    }

    /// Current bytes reserved by live worker requests.
    ///
    /// # Errors
    ///
    /// Returns [`GraceJoinError::Poisoned`] if accounting state was poisoned.
    pub fn active_spill_bytes(&self) -> Result<u64, GraceJoinError> {
        self.active_spill_bytes
            .lock()
            .map(|value| *value)
            .map_err(|_| GraceJoinError::Poisoned)
    }

    fn join_out_of_core(
        &self,
        identity_digest: [u8; 32],
        left: Vec<Value>,
        right: Vec<Value>,
        join_keys: &[String],
        max_rows: usize,
    ) -> Result<GraceJoinOutcome, GraceJoinError> {
        let mut stage = SpillStage::create(self, identity_digest)?;
        let mut writers = self.empty_writers()?;
        for row in left {
            stage.write_row(&mut writers, 0, row, join_keys, self.max_row_bytes)?;
        }
        for row in right {
            stage.write_row(&mut writers, 1, row, join_keys, self.max_row_bytes)?;
        }
        let files = stage.finish_writers(writers)?;
        self.execute_spilled(stage, files, identity_digest, max_rows)
    }

    fn empty_writers(&self) -> Result<Vec<Option<BucketWriter>>, GraceJoinError> {
        let file_slots = usize::try_from(self.bucket_count)
            .map_err(|_| GraceJoinError::AccountingOverflow)?
            .checked_mul(2)
            .ok_or(GraceJoinError::AccountingOverflow)?;
        if file_slots > self.max_open_files {
            return Err(GraceJoinError::InvalidConfiguration(
                "open-file ceiling cannot cover configured buckets".to_owned(),
            ));
        }
        Ok((0..file_slots).map(|_| None).collect())
    }

    fn execute_spilled(
        &self,
        mut stage: SpillStage<'_>,
        files: Vec<SpillFile>,
        identity_digest: [u8; 32],
        max_rows: usize,
    ) -> Result<GraceJoinOutcome, GraceJoinError> {
        let spill_bytes = stage.reserved_bytes;
        let mut output = Vec::new();
        let mut buckets_processed = 0_u32;
        let mut max_build_rows = 0_u64;
        for bucket in 0..self.bucket_count {
            let left_file = file_for(&files, 0, bucket);
            let right_file = file_for(&files, 1, bucket);
            let (Some(left_file), Some(right_file)) = (left_file, right_file) else {
                continue;
            };
            buckets_processed = buckets_processed
                .checked_add(1)
                .ok_or(GraceJoinError::AccountingOverflow)?;
            let mut right_reader = VerifiedRecordReader::open(
                right_file,
                identity_digest,
                self.bucket_count,
                self.max_row_bytes,
            )?;
            while let Some(right_chunk) = right_reader.next_chunk(self.max_build_rows)? {
                max_build_rows = max_build_rows.max(
                    u64::try_from(right_chunk.len())
                        .map_err(|_| GraceJoinError::AccountingOverflow)?,
                );
                let mut left_reader = VerifiedRecordReader::open(
                    left_file,
                    identity_digest,
                    self.bucket_count,
                    self.max_row_bytes,
                )?;
                while let Some(left_chunk) = left_reader.next_chunk(self.max_probe_rows)? {
                    let remaining = max_rows.saturating_sub(output.len());
                    let joined = inner_join_sparql_json(&left_chunk, &right_chunk, remaining)?;
                    output.extend(joined);
                }
                left_reader.finish()?;
            }
            right_reader.finish()?;
        }
        stage.cleanup()?;
        Ok(GraceJoinOutcome {
            bindings: output,
            mode: "grace_hash_nvme_v1",
            spill_bytes,
            buckets_processed,
            max_build_rows,
        })
    }

    fn reserve(&self, current_request: u64, bytes: u64) -> Result<u64, GraceJoinError> {
        let next_request = current_request
            .checked_add(bytes)
            .ok_or(GraceJoinError::AccountingOverflow)?;
        if next_request > self.max_request_spill_bytes {
            return Err(GraceJoinError::RequestSpillLimit);
        }
        let mut active = self
            .active_spill_bytes
            .lock()
            .map_err(|_| GraceJoinError::Poisoned)?;
        let next_active = active
            .checked_add(bytes)
            .ok_or(GraceJoinError::AccountingOverflow)?;
        if next_active > self.max_total_spill_bytes {
            return Err(GraceJoinError::ProcessSpillLimit);
        }
        *active = next_active;
        Ok(next_request)
    }

    fn release(&self, bytes: u64) -> Result<(), GraceJoinError> {
        let mut active = self
            .active_spill_bytes
            .lock()
            .map_err(|_| GraceJoinError::Poisoned)?;
        *active = active
            .checked_sub(bytes)
            .ok_or(GraceJoinError::AccountingOverflow)?;
        Ok(())
    }
}

struct SpillStage<'a> {
    engine: &'a GraceJoinEngine,
    root: PathBuf,
    identity_digest: [u8; 32],
    reserved_bytes: u64,
    cleaned: bool,
}

struct BucketWriter {
    writer: BufWriter<File>,
    path: PathBuf,
    side: u8,
    bucket: u32,
    rows: u64,
    bytes: u64,
    hasher: Sha256,
}

#[derive(Clone)]
struct SpillFile {
    path: PathBuf,
    side: u8,
    bucket: u32,
    rows: u64,
    bytes: u64,
}

impl<'a> SpillStage<'a> {
    fn create(
        engine: &'a GraceJoinEngine,
        identity_digest: [u8; 32],
    ) -> Result<Self, GraceJoinError> {
        let root = engine.root.join(format!("stage-{}", Uuid::new_v4()));
        fs::create_dir(&root)?;
        Ok(Self {
            engine,
            root,
            identity_digest,
            reserved_bytes: 0,
            cleaned: false,
        })
    }

    fn write_row(
        &mut self,
        writers: &mut [Option<BucketWriter>],
        side: u8,
        row: Value,
        join_keys: &[String],
        max_row_bytes: usize,
    ) -> Result<(), GraceJoinError> {
        let bucket = grace_partition_for_binding(&row, join_keys, self.engine.bucket_count)?;
        let bucket_index =
            usize::try_from(bucket).map_err(|_| GraceJoinError::AccountingOverflow)?;
        let side_offset = usize::from(side)
            .checked_mul(
                usize::try_from(self.engine.bucket_count)
                    .map_err(|_| GraceJoinError::AccountingOverflow)?,
            )
            .ok_or(GraceJoinError::AccountingOverflow)?;
        let index = side_offset
            .checked_add(bucket_index)
            .ok_or(GraceJoinError::AccountingOverflow)?;
        if writers.get(index).is_none() {
            return Err(GraceJoinError::AccountingOverflow);
        }
        if writers[index].is_none() {
            writers[index] = Some(self.create_writer(side, bucket)?);
        }
        let bytes = serde_json::to_vec(&row)?;
        if bytes.is_empty() || bytes.len() > max_row_bytes {
            return Err(GraceJoinError::RowTooLarge);
        }
        let length = u32::try_from(bytes.len()).map_err(|_| GraceJoinError::RowTooLarge)?;
        let record_bytes = u64::from(length)
            .checked_add(4)
            .ok_or(GraceJoinError::AccountingOverflow)?;
        self.reserved_bytes = self.engine.reserve(self.reserved_bytes, record_bytes)?;
        let writer = writers[index]
            .as_mut()
            .ok_or(GraceJoinError::AccountingOverflow)?;
        writer.writer.write_all(&length.to_be_bytes())?;
        writer.writer.write_all(&bytes)?;
        writer.hasher.update(length.to_be_bytes());
        writer.hasher.update(&bytes);
        writer.rows = writer
            .rows
            .checked_add(1)
            .ok_or(GraceJoinError::AccountingOverflow)?;
        writer.bytes = writer
            .bytes
            .checked_add(record_bytes)
            .ok_or(GraceJoinError::AccountingOverflow)?;
        Ok(())
    }

    fn create_writer(&mut self, side: u8, bucket: u32) -> Result<BucketWriter, GraceJoinError> {
        let path = self
            .root
            .join(format!("side-{side}-bucket-{bucket:08}.spill"));
        let header = spill_header(self.identity_digest, side, bucket, self.engine.bucket_count);
        let initial_bytes = FILE_HEADER_BYTES
            .checked_add(FILE_TRAILER_BYTES)
            .ok_or(GraceJoinError::AccountingOverflow)?;
        self.reserved_bytes = self.engine.reserve(self.reserved_bytes, initial_bytes)?;
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(&header)?;
        let mut hasher = Sha256::new();
        hasher.update(&header);
        Ok(BucketWriter {
            writer,
            path,
            side,
            bucket,
            rows: 0,
            bytes: initial_bytes,
            hasher,
        })
    }

    fn finish_writers(
        &mut self,
        writers: Vec<Option<BucketWriter>>,
    ) -> Result<Vec<SpillFile>, GraceJoinError> {
        let mut files = Vec::new();
        for writer in writers.into_iter().flatten() {
            let BucketWriter {
                mut writer,
                path,
                side,
                bucket,
                rows,
                bytes,
                hasher,
            } = writer;
            let digest: [u8; 32] = hasher.finalize().into();
            writer.write_all(&digest)?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
            drop(writer);
            if fs::metadata(&path)?.len() != bytes {
                return Err(GraceJoinError::CorruptSpill);
            }
            files.push(SpillFile {
                path,
                side,
                bucket,
                rows,
                bytes,
            });
        }
        Ok(files)
    }

    fn cleanup(&mut self) -> Result<(), GraceJoinError> {
        fs::remove_dir_all(&self.root)?;
        self.engine.release(self.reserved_bytes)?;
        self.reserved_bytes = 0;
        self.cleaned = true;
        Ok(())
    }
}

impl Drop for SpillStage<'_> {
    fn drop(&mut self) {
        if self.cleaned {
            return;
        }
        if fs::remove_dir_all(&self.root).is_ok() {
            let _released = self.engine.release(self.reserved_bytes);
            self.reserved_bytes = 0;
            self.cleaned = true;
        }
    }
}

struct VerifiedRecordReader {
    reader: BufReader<File>,
    rows_remaining: u64,
    expected_bytes: u64,
    bytes_read: u64,
    max_row_bytes: usize,
    hasher: Sha256,
    finished: bool,
}

impl VerifiedRecordReader {
    fn open(
        file: &SpillFile,
        identity_digest: [u8; 32],
        bucket_count: u32,
        max_row_bytes: usize,
    ) -> Result<Self, GraceJoinError> {
        if fs::symlink_metadata(&file.path)?.file_type().is_symlink()
            || fs::metadata(&file.path)?.len() != file.bytes
        {
            return Err(GraceJoinError::CorruptSpill);
        }
        let expected_header = spill_header(identity_digest, file.side, file.bucket, bucket_count);
        let mut reader = BufReader::new(File::open(&file.path)?);
        let mut actual_header = vec![
            0_u8;
            usize::try_from(FILE_HEADER_BYTES)
                .map_err(|_| GraceJoinError::AccountingOverflow)?
        ];
        reader.read_exact(&mut actual_header)?;
        if actual_header != expected_header {
            return Err(GraceJoinError::CorruptSpill);
        }
        let mut hasher = Sha256::new();
        hasher.update(&actual_header);
        Ok(Self {
            reader,
            rows_remaining: file.rows,
            expected_bytes: file.bytes,
            bytes_read: FILE_HEADER_BYTES,
            max_row_bytes,
            hasher,
            finished: false,
        })
    }

    fn next_chunk(&mut self, maximum: usize) -> Result<Option<Vec<Value>>, GraceJoinError> {
        if maximum == 0 {
            return Err(GraceJoinError::InvalidConfiguration(
                "reader chunk bound must be positive".to_owned(),
            ));
        }
        if self.rows_remaining == 0 {
            self.verify_trailer()?;
            return Ok(None);
        }
        let mut rows = Vec::with_capacity(
            usize::try_from(self.rows_remaining)
                .unwrap_or(usize::MAX)
                .min(maximum),
        );
        while rows.len() < maximum && self.rows_remaining > 0 {
            let mut length_bytes = [0_u8; 4];
            self.reader.read_exact(&mut length_bytes)?;
            let length = usize::try_from(u32::from_be_bytes(length_bytes))
                .map_err(|_| GraceJoinError::RowTooLarge)?;
            if length == 0 || length > self.max_row_bytes {
                return Err(GraceJoinError::RowTooLarge);
            }
            let mut payload = vec![0_u8; length];
            self.reader.read_exact(&mut payload)?;
            self.hasher.update(length_bytes);
            self.hasher.update(&payload);
            let record_bytes = u64::try_from(length)
                .map_err(|_| GraceJoinError::AccountingOverflow)?
                .checked_add(4)
                .ok_or(GraceJoinError::AccountingOverflow)?;
            self.bytes_read = self
                .bytes_read
                .checked_add(record_bytes)
                .ok_or(GraceJoinError::AccountingOverflow)?;
            let row: Value = serde_json::from_slice(&payload)?;
            if !row.is_object() {
                return Err(GraceJoinError::CorruptSpill);
            }
            rows.push(row);
            self.rows_remaining -= 1;
        }
        Ok(Some(rows))
    }

    fn finish(&mut self) -> Result<(), GraceJoinError> {
        if self.rows_remaining != 0 {
            return Err(GraceJoinError::CorruptSpill);
        }
        self.verify_trailer()
    }

    fn verify_trailer(&mut self) -> Result<(), GraceJoinError> {
        if self.finished {
            return Ok(());
        }
        let mut expected_digest = [0_u8; 32];
        self.reader.read_exact(&mut expected_digest)?;
        self.bytes_read = self
            .bytes_read
            .checked_add(FILE_TRAILER_BYTES)
            .ok_or(GraceJoinError::AccountingOverflow)?;
        let mut trailing = [0_u8; 1];
        if self.bytes_read != self.expected_bytes
            || self.reader.read(&mut trailing)? != 0
            || <[u8; 32]>::from(self.hasher.clone().finalize()) != expected_digest
        {
            return Err(GraceJoinError::CorruptSpill);
        }
        self.finished = true;
        Ok(())
    }
}

fn file_for(files: &[SpillFile], side: u8, bucket: u32) -> Option<&SpillFile> {
    files
        .iter()
        .find(|file| file.side == side && file.bucket == bucket)
}

fn spill_header(identity_digest: [u8; 32], side: u8, bucket: u32, bucket_count: u32) -> Vec<u8> {
    let mut header = Vec::with_capacity(usize::try_from(FILE_HEADER_BYTES).unwrap_or(49));
    header.extend_from_slice(FILE_MAGIC);
    header.extend_from_slice(&identity_digest);
    header.push(side);
    header.extend_from_slice(&bucket.to_be_bytes());
    header.extend_from_slice(&bucket_count.to_be_bytes());
    header
}

fn prepare_root(root: &Path) -> Result<(), GraceJoinError> {
    if root.exists() {
        let metadata = fs::symlink_metadata(root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(GraceJoinError::UnsafeRoot(
                "root must be a real directory".to_owned(),
            ));
        }
    } else {
        fs::create_dir_all(root)?;
    }
    let marker = root.join(ROOT_MARKER);
    if marker.exists() {
        if fs::symlink_metadata(&marker)?.file_type().is_symlink()
            || fs::read(&marker)? != ROOT_MARKER_BYTES
        {
            return Err(GraceJoinError::UnsafeRoot(
                "root marker is invalid".to_owned(),
            ));
        }
    } else {
        if fs::read_dir(root)?.next().transpose()?.is_some() {
            return Err(GraceJoinError::UnsafeRoot(
                "uninitialized root must be empty".to_owned(),
            ));
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&marker)?;
        file.write_all(ROOT_MARKER_BYTES)?;
        file.sync_all()?;
        File::open(root)?.sync_all()?;
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| GraceJoinError::UnsafeRoot("non-UTF-8 entry".to_owned()))?;
        if name == ROOT_MARKER {
            continue;
        }
        let Some(identifier) = name.strip_prefix("stage-") else {
            return Err(GraceJoinError::UnsafeRoot(format!(
                "unmanaged entry {name}"
            )));
        };
        if identifier.parse::<Uuid>().is_err()
            || entry.file_type()?.is_symlink()
            || !entry.file_type()?.is_dir()
        {
            return Err(GraceJoinError::UnsafeRoot(format!("invalid stage {name}")));
        }
        // A root can be shared by concurrent query executions. Never treat a
        // UUID-shaped directory as abandoned merely because this process did
        // not create it. Each execution guard removes only its own stage; stale
        // recovery is a separate lease/checkpoint-aware maintenance operation.
    }
    Ok(())
}

fn decode_sha256(value: &str) -> Result<[u8; 32], GraceJoinError> {
    hex::decode(value)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(GraceJoinError::InvalidIdentity)
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        fs::{self, OpenOptions},
        io::Write,
        path::PathBuf,
    };

    use ngkg_query_executor::inner_join_sparql_json;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::{
        GraceJoinEngine, GraceJoinError, GraceJoinIdentity, GraceJoinSide, SpillStage,
        VerifiedRecordReader,
    };

    fn root() -> PathBuf {
        std::env::temp_dir().join(format!("ngkg-grace-join-test-{}", Uuid::new_v4()))
    }

    fn identity(left: &[serde_json::Value], right: &[serde_json::Value]) -> GraceJoinIdentity {
        GraceJoinIdentity {
            tenant_id: Uuid::from_u128(1),
            dataset_id: Uuid::from_u128(2),
            snapshot_id: Uuid::from_u128(3),
            query_sha256: "1".repeat(64),
            plan_sha256: "2".repeat(64),
            stage: 0,
            partition: 0,
            partition_count: 2,
            left_input_sha256: hex::encode(Sha256::digest(
                serde_json::to_vec(left).unwrap_or_default(),
            )),
            right_input_sha256: hex::encode(Sha256::digest(
                serde_json::to_vec(right).unwrap_or_default(),
            )),
        }
    }

    fn engine(root: &std::path::Path) -> Result<GraceJoinEngine, Box<dyn std::error::Error>> {
        Ok(GraceJoinEngine::open(
            root,
            1 << 24,
            1 << 23,
            8,
            16,
            2,
            2,
            4096,
            1,
        )?)
    }

    fn bag(rows: &[serde_json::Value]) -> Result<BTreeMap<Vec<u8>, u64>, serde_json::Error> {
        let mut counts = BTreeMap::new();
        for row in rows {
            let key = serde_json::to_vec(row)?;
            *counts.entry(key).or_default() += 1;
        }
        Ok(counts)
    }

    #[test]
    fn out_of_core_join_matches_exact_hot_key_bag_and_cleans_up()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let engine = engine(&root)?;
        let left = vec![
            json!({"k": {"type": "uri", "value": "urn:k"}, "l": {"type": "literal", "value": "1"}}),
            json!({"k": {"type": "uri", "value": "urn:k"}, "l": {"type": "literal", "value": "2"}}),
        ];
        let right = vec![
            json!({"k": {"type": "uri", "value": "urn:k"}, "r": {"type": "literal", "value": "a"}}),
            json!({"k": {"type": "uri", "value": "urn:k"}, "r": {"type": "literal", "value": "b"}}),
            json!({"k": {"type": "uri", "value": "urn:k"}, "r": {"type": "literal", "value": "c"}}),
        ];
        let expected = inner_join_sparql_json(&left, &right, 16)?;
        let outcome = engine.join(&identity(&left, &right), left, right, &["k".to_owned()], 16)?;
        assert_eq!(outcome.mode, "grace_hash_nvme_v1");
        assert_eq!(outcome.bindings.len(), 6);
        assert_eq!(bag(&outcome.bindings)?, bag(&expected)?);
        assert!(outcome.spill_bytes > 0);
        assert!(outcome.max_build_rows <= 2);
        assert_eq!(engine.active_spill_bytes()?, 0);
        assert_eq!(fs::read_dir(&root)?.count(), 1);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn output_limit_fails_closed_and_releases_spill_budget()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let engine = engine(&root)?;
        let left = vec![json!({"k": {"type": "uri", "value": "urn:k"}}); 2];
        let right = vec![json!({"k": {"type": "uri", "value": "urn:k"}}); 3];
        assert!(
            engine
                .join(&identity(&left, &right), left, right, &["k".to_owned()], 5)
                .is_err()
        );
        assert_eq!(engine.active_spill_bytes()?, 0);
        assert_eq!(fs::read_dir(&root)?.count(), 1);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn small_build_side_uses_in_memory_fast_path() -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let engine = engine(&root)?;
        let left = vec![json!({"k": {"type": "uri", "value": "urn:k"}})];
        let right = vec![json!({"k": {"type": "uri", "value": "urn:k"}})];
        let outcome = engine.join(&identity(&left, &right), left, right, &["k".to_owned()], 2)?;
        assert_eq!(outcome.mode, "in_memory_hash_v1");
        assert_eq!(outcome.bindings.len(), 1);
        assert_eq!(outcome.spill_bytes, 0);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn streamed_join_matches_independent_bag_and_releases_on_decoder_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let engine = engine(&root)?;
        let left = vec![json!({"k": {"type": "uri", "value": "urn:k"}}); 2];
        let right = vec![json!({"k": {"type": "uri", "value": "urn:k"}}); 3];
        let expected = inner_join_sparql_json(&left, &right, 8)?;
        let rows = left
            .iter()
            .cloned()
            .map(|row| Ok((GraceJoinSide::Left, row)))
            .chain(
                right
                    .iter()
                    .cloned()
                    .map(|row| Ok((GraceJoinSide::Right, row))),
            );
        let outcome = engine.join_stream(&identity(&left, &right), rows, &["k".to_owned()], 8)?;
        assert_eq!(outcome.mode, "grace_hash_nvme_v1");
        assert_eq!(bag(&outcome.bindings)?, bag(&expected)?);
        let failure = vec![
            Ok((GraceJoinSide::Left, left[0].clone())),
            Err(GraceJoinError::CorruptSpill),
        ];
        assert!(
            engine
                .join_stream(&identity(&left, &right), failure, &["k".to_owned()], 8)
                .is_err()
        );
        assert_eq!(engine.active_spill_bytes()?, 0);
        assert_eq!(fs::read_dir(&root)?.count(), 1);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn request_and_process_spill_budgets_are_independent() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = root();
        let engine = GraceJoinEngine::open(&root, 300, 200, 2, 4, 2, 2, 4096, 1)?;
        let first_request = engine.reserve(0, 150)?;
        assert!(matches!(
            engine.reserve(first_request, 51),
            Err(GraceJoinError::RequestSpillLimit)
        ));
        assert!(matches!(
            engine.reserve(0, 151),
            Err(GraceJoinError::ProcessSpillLimit)
        ));
        engine.release(first_request)?;
        assert_eq!(engine.active_spill_bytes()?, 0);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn appended_spill_corruption_is_rejected_and_cleaned() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = root();
        let engine = engine(&root)?;
        let row = json!({"k": {"type": "uri", "value": "urn:k"}});
        let identity = identity(std::slice::from_ref(&row), std::slice::from_ref(&row));
        let digest = identity.digest()?;
        let mut stage = SpillStage::create(&engine, digest)?;
        let mut writers = (0..16).map(|_| None).collect::<Vec<_>>();
        stage.write_row(&mut writers, 0, row, &["k".to_owned()], 4096)?;
        let files = stage.finish_writers(writers)?;
        let file = files
            .first()
            .ok_or_else(|| std::io::Error::other("spill file is absent"))?;
        OpenOptions::new()
            .append(true)
            .open(&file.path)?
            .write_all(b"corrupt")?;
        assert!(VerifiedRecordReader::open(file, digest, 8, 4096).is_err());
        stage.cleanup()?;
        assert_eq!(engine.active_spill_bytes()?, 0);
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn opening_a_parallel_executor_preserves_live_spill_namespace()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let first_engine = engine(&root)?;
        let first = SpillStage::create(&first_engine, [7_u8; 32])?;
        let first_path = first.root.clone();
        assert!(first_path.is_dir());

        let second_engine = engine(&root)?;
        let second = SpillStage::create(&second_engine, [8_u8; 32])?;
        assert!(first_path.is_dir(), "parallel executor erased a live spill directory");
        assert_ne!(first.root, second.root);

        second.cleanup()?;
        assert!(first_path.is_dir());
        first.cleanup()?;
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
