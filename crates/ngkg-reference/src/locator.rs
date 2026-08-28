//! Fixed-width, checksum-bound locator used by the reference hydration path.

use std::{fs, path::Path};

use thiserror::Error;
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"NGKGLI01";
const HEADER_BYTES: usize = 64;
const RECORD_BYTES: usize = 32;

/// Physical payload row addressed by an entity GUID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocatorRecord {
    pub entity_guid: Uuid,
    pub row_group: u32,
    pub row_in_group: u32,
    pub graph_id: u32,
    pub predicate_id: u32,
}

/// Fully verified in-memory reference index. Distributed phases shard this exact key space.
pub struct LocatorIndex {
    snapshot_id: Uuid,
    payload_sha256: [u8; 32],
    records: Vec<LocatorRecord>,
}

#[derive(Debug, Error)]
pub enum LocatorFileError {
    #[error("locator file I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("locator header, length, sort order, or version is invalid")]
    InvalidFormat,
    #[error("locator belongs to snapshot {found}, not {expected}")]
    SnapshotMismatch { expected: Uuid, found: Uuid },
    #[error("payload checksum does not match locator header")]
    PayloadMismatch,
    #[error("locator record count does not fit the platform")]
    RecordCountOverflow,
}

/// Write a deterministic locator. Duplicate GUIDs are retained for multi-valued payloads.
pub fn write_locator(
    path: &Path,
    snapshot_id: Uuid,
    payload_sha256: [u8; 32],
    records: &mut [LocatorRecord],
) -> Result<(), LocatorFileError> {
    records.sort_unstable_by_key(|record| {
        (
            *record.entity_guid.as_bytes(),
            record.row_group,
            record.row_in_group,
            record.graph_id,
            record.predicate_id,
        )
    });
    let count = u64::try_from(records.len()).map_err(|_| LocatorFileError::RecordCountOverflow)?;
    let mut bytes =
        Vec::with_capacity(HEADER_BYTES.saturating_add(records.len().saturating_mul(RECORD_BYTES)));
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(snapshot_id.as_bytes());
    bytes.extend_from_slice(&payload_sha256);
    bytes.extend_from_slice(&count.to_be_bytes());
    for record in records {
        bytes.extend_from_slice(record.entity_guid.as_bytes());
        bytes.extend_from_slice(&record.row_group.to_be_bytes());
        bytes.extend_from_slice(&record.row_in_group.to_be_bytes());
        bytes.extend_from_slice(&record.graph_id.to_be_bytes());
        bytes.extend_from_slice(&record.predicate_id.to_be_bytes());
    }
    fs::write(path, bytes)?;
    Ok(())
}

impl LocatorIndex {
    /// Open and verify the complete fixed-width directory before serving lookups.
    pub fn open(
        path: &Path,
        expected_snapshot: Uuid,
        expected_payload_sha256: [u8; 32],
    ) -> Result<Self, LocatorFileError> {
        let bytes = fs::read(path)?;
        if bytes.len() < HEADER_BYTES || &bytes[..8] != MAGIC {
            return Err(LocatorFileError::InvalidFormat);
        }
        let snapshot_id =
            Uuid::from_slice(&bytes[8..24]).map_err(|_| LocatorFileError::InvalidFormat)?;
        if snapshot_id != expected_snapshot {
            return Err(LocatorFileError::SnapshotMismatch {
                expected: expected_snapshot,
                found: snapshot_id,
            });
        }
        let mut payload_sha256 = [0_u8; 32];
        payload_sha256.copy_from_slice(&bytes[24..56]);
        if payload_sha256 != expected_payload_sha256 {
            return Err(LocatorFileError::PayloadMismatch);
        }
        let mut count_bytes = [0_u8; 8];
        count_bytes.copy_from_slice(&bytes[56..64]);
        let count = usize::try_from(u64::from_be_bytes(count_bytes))
            .map_err(|_| LocatorFileError::RecordCountOverflow)?;
        let expected_len = HEADER_BYTES
            .checked_add(
                count
                    .checked_mul(RECORD_BYTES)
                    .ok_or(LocatorFileError::InvalidFormat)?,
            )
            .ok_or(LocatorFileError::InvalidFormat)?;
        if bytes.len() != expected_len {
            return Err(LocatorFileError::InvalidFormat);
        }
        let mut records = Vec::with_capacity(count);
        for chunk in bytes[HEADER_BYTES..].chunks_exact(RECORD_BYTES) {
            let entity_guid =
                Uuid::from_slice(&chunk[..16]).map_err(|_| LocatorFileError::InvalidFormat)?;
            records.push(LocatorRecord {
                entity_guid,
                row_group: read_u32(&chunk[16..20]),
                row_in_group: read_u32(&chunk[20..24]),
                graph_id: read_u32(&chunk[24..28]),
                predicate_id: read_u32(&chunk[28..32]),
            });
        }
        if !records.windows(2).all(|pair| {
            let left = (
                *pair[0].entity_guid.as_bytes(),
                pair[0].row_group,
                pair[0].row_in_group,
                pair[0].graph_id,
                pair[0].predicate_id,
            );
            let right = (
                *pair[1].entity_guid.as_bytes(),
                pair[1].row_group,
                pair[1].row_in_group,
                pair[1].graph_id,
                pair[1].predicate_id,
            );
            left <= right
        }) {
            return Err(LocatorFileError::InvalidFormat);
        }
        Ok(Self {
            snapshot_id,
            payload_sha256,
            records,
        })
    }

    #[must_use]
    pub const fn snapshot_id(&self) -> Uuid {
        self.snapshot_id
    }

    #[must_use]
    pub const fn payload_sha256(&self) -> [u8; 32] {
        self.payload_sha256
    }

    /// Binary-search one GUID, then return every directly addressed payload row.
    #[must_use]
    pub fn lookup(&self, guid: Uuid) -> &[LocatorRecord] {
        let start = self
            .records
            .partition_point(|record| record.entity_guid < guid);
        let end = self
            .records
            .partition_point(|record| record.entity_guid <= guid);
        &self.records[start..end]
    }
}

fn read_u32(bytes: &[u8]) -> u32 {
    let mut value = [0_u8; 4];
    value.copy_from_slice(bytes);
    u32::from_be_bytes(value)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{LocatorIndex, LocatorRecord, write_locator};
    use uuid::Uuid;

    #[test]
    fn direct_lookup_retains_multiple_rows() {
        let root = std::env::temp_dir().join(format!("ngkg-locator-{}", Uuid::new_v4()));
        assert!(fs::create_dir(&root).is_ok());
        let path = root.join("locator.bin");
        let snapshot = Uuid::from_u128(1);
        let guid = Uuid::from_u128(2);
        let mut rows = vec![
            LocatorRecord {
                entity_guid: guid,
                row_group: 0,
                row_in_group: 2,
                graph_id: 1,
                predicate_id: 4,
            },
            LocatorRecord {
                entity_guid: guid,
                row_group: 0,
                row_in_group: 1,
                graph_id: 1,
                predicate_id: 3,
            },
        ];
        let payload_hash = [7_u8; 32];
        assert!(write_locator(&path, snapshot, payload_hash, &mut rows).is_ok());
        let index = LocatorIndex::open(&path, snapshot, payload_hash);
        assert!(index.is_ok());
        assert!(index.is_ok_and(|value| value.lookup(guid).len() == 2));
        let _ = fs::remove_dir_all(root);
    }
}
