//! Direct GUID/FactID routing and physical range coalescing.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use async_trait::async_trait;
use memmap2::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const SHARDED_MAGIC: &[u8; 8] = b"NGKGLI01";
const SHARDED_HEADER_BYTES: usize = 64;
const SHARDED_RECORD_BYTES: usize = 44;

/// Direct lookup key. One key can resolve to multiple rows and objects.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum LocatorKey {
    Entity(Uuid),
    Event(Uuid),
    Record(Uuid),
    Source(Uuid),
    Fact([u8; 16]),
}

/// Exact immutable physical range.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PhysicalRange {
    pub object_uri: String,
    pub row_group: u32,
    pub first_row: u64,
    pub row_count: u64,
    pub column_mask_id: u32,
    pub graph_id: u32,
    pub object_sha256: [u8; 32],
}

/// Snapshot-bound lookup result.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocatorEntry {
    pub snapshot_id: Uuid,
    pub schema_hash: [u8; 32],
    pub ranges: Vec<PhysicalRange>,
}

/// One non-overlapping hash interval in the global directory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocatorOwner {
    pub start_inclusive: u64,
    pub end_inclusive: u64,
    pub shard_id: String,
    pub endpoints: Vec<String>,
    pub root_sha256: [u8; 32],
}

/// Global directory used instead of broadcasting a key to every shard.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LocatorDirectory {
    pub snapshot_id: Uuid,
    pub owners: Vec<LocatorOwner>,
}

/// Locator errors make hydration fail rather than returning incomplete payload.
#[derive(Debug, Error)]
pub enum LocatorError {
    #[error("locator directory has overlapping or discontinuous ranges")]
    InvalidDirectory,
    #[error("no locator owner for routed hash {0}")]
    OwnerMissing(u64),
    #[error("locator key is absent from a certified snapshot")]
    MissingKey,
    #[error("locator entry snapshot mismatch")]
    SnapshotMismatch,
    #[error("locator dependency failed: {0}")]
    Dependency(String),
    #[error("locator I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("locator checksum is invalid or differs from its immutable root")]
    ChecksumMismatch,
    #[error("sharded locator input or binary encoding is invalid")]
    InvalidShardedFormat,
    #[error("locator output already exists: {0}")]
    ImmutableConflict(PathBuf),
    #[error("locator record count exceeds this platform")]
    RecordCountOverflow,
}

/// Exact physical payload address encoded in the Phase 18 hot locator.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardLocatorRecord {
    /// Entity whose payload is addressed.
    pub entity_guid: Uuid,
    /// Logical artifact partition.
    pub partition_index: u32,
    /// Parquet row group within the partition.
    pub row_group: u32,
    /// Row offset within the Parquet row group.
    pub row_in_group: u32,
    /// Dense named-graph term ID.
    pub graph_id: u64,
    /// Dense predicate term ID.
    pub predicate_id: u64,
}

/// Read-only file-backed memory map containing a verified fixed-width locator.
///
/// The immutable locator file is mapped once and searched directly by every
/// query lane. This avoids an additional locator-sized RAM copy while allowing
/// the kernel page cache to be shared across worker threads and processes.
pub struct MmapLocatorIndex {
    snapshot_id: Uuid,
    source_locator_sha256: [u8; 32],
    record_count: usize,
    bytes: Mmap,
}

impl std::fmt::Debug for MmapLocatorIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MmapLocatorIndex")
            .field("snapshot_id", &self.snapshot_id)
            .field("record_count", &self.record_count)
            .finish_non_exhaustive()
    }
}

/// Compile the globally sorted Phase 17 TSV locator into a fixed-width binary index.
pub fn compile_sharded_locator(
    input: &Path,
    expected_input_sha256: &str,
    snapshot_id: Uuid,
    output: &Path,
) -> Result<u64, LocatorError> {
    let expected = decode_sha256(expected_input_sha256)?;
    if sha256_path(input)? != expected {
        return Err(LocatorError::ChecksumMismatch);
    }
    let file = match OpenOptions::new().create_new(true).write(true).open(output) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(LocatorError::ImmutableConflict(output.to_owned()));
        }
        Err(error) => return Err(LocatorError::Io(error)),
    };
    let result = (move || {
        let mut writer = BufWriter::new(file);
        writer.write_all(SHARDED_MAGIC)?;
        writer.write_all(snapshot_id.as_bytes())?;
        writer.write_all(&expected)?;
        writer.write_all(&0_u64.to_be_bytes())?;
        let mut count = 0_u64;
        let mut previous: Option<ShardLocatorRecord> = None;
        for line in BufReader::new(File::open(input)?).lines() {
            let record = parse_locator_line(&line?)?;
            if previous.is_some_and(|value| value >= record) {
                return Err(LocatorError::InvalidShardedFormat);
            }
            write_sharded_record(&mut writer, record)?;
            previous = Some(record);
            count = count
                .checked_add(1)
                .ok_or(LocatorError::RecordCountOverflow)?;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        drop(writer);
        let mut file = OpenOptions::new().read(true).write(true).open(output)?;
        file.seek(SeekFrom::Start(56))?;
        file.write_all(&count.to_be_bytes())?;
        file.sync_all()?;
        Ok(count)
    })();
    if result.is_err() {
        let _cleanup_result = fs::remove_file(output);
    }
    result
}

impl MmapLocatorIndex {
    /// Open, checksum, map read-only, and validate every fixed-width record.
    #[allow(unsafe_code)]
    pub fn open(
        path: &Path,
        expected_binary_sha256: &str,
        expected_snapshot_id: Uuid,
        expected_source_locator_sha256: &str,
    ) -> Result<Self, LocatorError> {
        let expected_binary = decode_sha256(expected_binary_sha256)?;
        let bytes_len = usize::try_from(fs::metadata(path)?.len())
            .map_err(|_| LocatorError::RecordCountOverflow)?;
        if bytes_len < SHARDED_HEADER_BYTES {
            return Err(LocatorError::InvalidShardedFormat);
        }
        let file = File::open(path)?;
        // SAFETY: the descriptor is opened read-only; NGKG publishes locator files
        // by atomic rename and never mutates an admitted path. The complete mapped
        // bytes are checksum-verified before any record is exposed. Deployment
        // policy mounts the snapshot cache read-only to all consumers.
        let map = unsafe { MmapOptions::new().len(bytes_len).map(&file)? };
        let observed_binary: [u8; 32] = Sha256::digest(&map[..]).into();
        if observed_binary != expected_binary {
            return Err(LocatorError::ChecksumMismatch);
        }
        if map.len() < SHARDED_HEADER_BYTES
            || &map[..8] != SHARDED_MAGIC
            || map[8..24] != *expected_snapshot_id.as_bytes()
        {
            return Err(LocatorError::InvalidShardedFormat);
        }
        let expected_source = decode_sha256(expected_source_locator_sha256)?;
        if map[24..56] != expected_source {
            return Err(LocatorError::ChecksumMismatch);
        }
        let record_count = usize::try_from(read_u64(&map[56..64]))
            .map_err(|_| LocatorError::RecordCountOverflow)?;
        let expected_bytes = SHARDED_HEADER_BYTES
            .checked_add(
                record_count
                    .checked_mul(SHARDED_RECORD_BYTES)
                    .ok_or(LocatorError::RecordCountOverflow)?,
            )
            .ok_or(LocatorError::RecordCountOverflow)?;
        if map.len() != expected_bytes {
            return Err(LocatorError::InvalidShardedFormat);
        }
        let index = Self {
            snapshot_id: expected_snapshot_id,
            source_locator_sha256: expected_source,
            record_count,
            bytes: map,
        };
        index.validate_sorted()?;
        Ok(index)
    }

    /// Snapshot bound into the index header.
    #[must_use]
    pub const fn snapshot_id(&self) -> Uuid {
        self.snapshot_id
    }

    /// Exact Phase 17 locator checksum bound into the index header.
    #[must_use]
    pub const fn source_locator_sha256(&self) -> [u8; 32] {
        self.source_locator_sha256
    }

    /// Number of physical payload records.
    #[must_use]
    pub const fn record_count(&self) -> usize {
        self.record_count
    }

    /// Binary-search one GUID without scanning unrelated locator records.
    pub fn lookup(&self, guid: Uuid) -> Result<Vec<ShardLocatorRecord>, LocatorError> {
        let key = *guid.as_bytes();
        let start = self.partition_point(|record| *record.entity_guid.as_bytes() < key)?;
        let end = self.partition_point(|record| *record.entity_guid.as_bytes() <= key)?;
        (start..end).map(|index| self.record(index)).collect()
    }

    fn partition_point<F>(&self, mut predicate: F) -> Result<usize, LocatorError>
    where
        F: FnMut(ShardLocatorRecord) -> bool,
    {
        let mut left = 0_usize;
        let mut right = self.record_count;
        while left < right {
            let middle = left + (right - left) / 2;
            if predicate(self.record(middle)?) {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        Ok(left)
    }

    fn validate_sorted(&self) -> Result<(), LocatorError> {
        let mut previous = None;
        for index in 0..self.record_count {
            let record = self.record(index)?;
            if previous.is_some_and(|value| value >= record) {
                return Err(LocatorError::InvalidShardedFormat);
            }
            previous = Some(record);
        }
        Ok(())
    }

    fn record(&self, index: usize) -> Result<ShardLocatorRecord, LocatorError> {
        if index >= self.record_count {
            return Err(LocatorError::InvalidShardedFormat);
        }
        let start = SHARDED_HEADER_BYTES
            .checked_add(
                index
                    .checked_mul(SHARDED_RECORD_BYTES)
                    .ok_or(LocatorError::RecordCountOverflow)?,
            )
            .ok_or(LocatorError::RecordCountOverflow)?;
        parse_sharded_record(&self.bytes[start..start + SHARDED_RECORD_BYTES])
    }
}

impl LocatorDirectory {
    /// Validate complete, non-overlapping coverage of the 64-bit key space.
    pub fn validate(&self) -> Result<(), LocatorError> {
        if self.owners.is_empty() || self.owners[0].start_inclusive != 0 {
            return Err(LocatorError::InvalidDirectory);
        }
        for pair in self.owners.windows(2) {
            if pair[0].end_inclusive.checked_add(1) != Some(pair[1].start_inclusive) {
                return Err(LocatorError::InvalidDirectory);
            }
        }
        if self
            .owners
            .last()
            .is_none_or(|owner| owner.end_inclusive != u64::MAX)
        {
            return Err(LocatorError::InvalidDirectory);
        }
        Ok(())
    }

    /// Route one precomputed 64-bit key hash to exactly one owner range.
    pub fn owner_for(&self, key_hash: u64) -> Result<&LocatorOwner, LocatorError> {
        self.owners
            .iter()
            .find(|owner| owner.start_inclusive <= key_hash && key_hash <= owner.end_inclusive)
            .ok_or(LocatorError::OwnerMissing(key_hash))
    }
}

/// Batched binary service boundary implemented by the locator service.
#[async_trait]
pub trait LocatorClient: Send + Sync {
    async fn lookup_batch(
        &self,
        snapshot_id: Uuid,
        keys: &[LocatorKey],
    ) -> Result<BTreeMap<LocatorKey, LocatorEntry>, LocatorError>;
}

/// Sort and merge adjacent rows with identical object, row group and column mask.
#[must_use]
pub fn coalesce_ranges(mut ranges: Vec<PhysicalRange>) -> Vec<PhysicalRange> {
    ranges.sort_by(|left, right| {
        (
            &left.object_uri,
            left.row_group,
            left.column_mask_id,
            left.first_row,
        )
            .cmp(&(
                &right.object_uri,
                right.row_group,
                right.column_mask_id,
                right.first_row,
            ))
    });
    let mut output: Vec<PhysicalRange> = Vec::new();
    for range in ranges {
        if let Some(previous) = output.last_mut() {
            let adjacent =
                previous.first_row.checked_add(previous.row_count) == Some(range.first_row);
            if adjacent
                && previous.object_uri == range.object_uri
                && previous.row_group == range.row_group
                && previous.column_mask_id == range.column_mask_id
                && previous.graph_id == range.graph_id
                && previous.object_sha256 == range.object_sha256
            {
                previous.row_count = previous.row_count.saturating_add(range.row_count);
                continue;
            }
        }
        output.push(range);
    }
    output
}

fn parse_locator_line(line: &str) -> Result<ShardLocatorRecord, LocatorError> {
    let fields = line.split('\t').collect::<Vec<_>>();
    if fields.len() != 6
        || fields[0].len() != 32
        || !fields[0]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(LocatorError::InvalidShardedFormat);
    }
    let guid_bytes = hex::decode(fields[0]).map_err(|_| LocatorError::InvalidShardedFormat)?;
    let entity_guid =
        Uuid::from_slice(&guid_bytes).map_err(|_| LocatorError::InvalidShardedFormat)?;
    Ok(ShardLocatorRecord {
        entity_guid,
        partition_index: parse_decimal(fields[1])?,
        row_group: parse_decimal(fields[2])?,
        row_in_group: parse_decimal(fields[3])?,
        graph_id: parse_decimal(fields[4])?,
        predicate_id: parse_decimal(fields[5])?,
    })
}

fn parse_decimal<T: std::str::FromStr>(value: &str) -> Result<T, LocatorError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(LocatorError::InvalidShardedFormat);
    }
    value
        .parse::<T>()
        .map_err(|_| LocatorError::InvalidShardedFormat)
}

fn write_sharded_record(
    writer: &mut impl Write,
    record: ShardLocatorRecord,
) -> Result<(), LocatorError> {
    writer.write_all(record.entity_guid.as_bytes())?;
    writer.write_all(&record.partition_index.to_be_bytes())?;
    writer.write_all(&record.row_group.to_be_bytes())?;
    writer.write_all(&record.row_in_group.to_be_bytes())?;
    writer.write_all(&record.graph_id.to_be_bytes())?;
    writer.write_all(&record.predicate_id.to_be_bytes())?;
    Ok(())
}

fn parse_sharded_record(bytes: &[u8]) -> Result<ShardLocatorRecord, LocatorError> {
    if bytes.len() != SHARDED_RECORD_BYTES {
        return Err(LocatorError::InvalidShardedFormat);
    }
    Ok(ShardLocatorRecord {
        entity_guid: Uuid::from_slice(&bytes[..16])
            .map_err(|_| LocatorError::InvalidShardedFormat)?,
        partition_index: read_u32(&bytes[16..20]),
        row_group: read_u32(&bytes[20..24]),
        row_in_group: read_u32(&bytes[24..28]),
        graph_id: read_u64(&bytes[28..36]),
        predicate_id: read_u64(&bytes[36..44]),
    })
}

fn read_u32(bytes: &[u8]) -> u32 {
    let mut value = [0_u8; 4];
    value.copy_from_slice(bytes);
    u32::from_be_bytes(value)
}

fn read_u64(bytes: &[u8]) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(bytes);
    u64::from_be_bytes(value)
}

fn decode_sha256(value: &str) -> Result<[u8; 32], LocatorError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(LocatorError::ChecksumMismatch);
    }
    hex::decode(value)
        .map_err(|_| LocatorError::ChecksumMismatch)?
        .try_into()
        .map_err(|_| LocatorError::ChecksumMismatch)
}

fn sha256_path(path: &Path) -> Result<[u8; 32], LocatorError> {
    let mut hasher = Sha256::new();
    let mut reader = BufReader::new(File::open(path)?);
    let mut block = vec![0_u8; 1024 * 1024].into_boxed_slice();
    loop {
        let read = std::io::Read::read(&mut reader, &mut block)?;
        if read == 0 {
            break;
        }
        hasher.update(&block[..read]);
    }
    Ok(hasher.finalize().into())
}

#[cfg(test)]
mod sharded_tests {
    use std::fs;

    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::{MmapLocatorIndex, compile_sharded_locator};

    #[test]
    fn compiled_locator_preserves_multirow_direct_lookup() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = std::env::temp_dir().join(format!("ngkg-sharded-locator-{}", Uuid::new_v4()));
        fs::create_dir(&root)?;
        let input = root.join("locator.tsv");
        let binary = root.join("locator.bin");
        let guid = Uuid::from_u128(2);
        let text = format!(
            "{}\t00000\t0000000000\t0000000000\t00000000000000000001\t00000000000000000002\n{}\t00001\t0000000000\t0000000001\t00000000000000000001\t00000000000000000003\n",
            hex::encode(guid.as_bytes()),
            hex::encode(guid.as_bytes()),
        );
        fs::write(&input, text.as_bytes())?;
        let input_sha = hex::encode(Sha256::digest(text.as_bytes()));
        let count = compile_sharded_locator(&input, &input_sha, Uuid::from_u128(1), &binary)?;
        assert_eq!(count, 2);
        let binary_sha = hex::encode(Sha256::digest(fs::read(&binary)?));
        let index = MmapLocatorIndex::open(&binary, &binary_sha, Uuid::from_u128(1), &input_sha)?;
        assert_eq!(index.lookup(guid)?.len(), 2);
        let _ = fs::remove_dir_all(root);
        Ok(())
    }
}
