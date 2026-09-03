use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

use memmap2::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{SliceError, valid_hash};

const MAGIC: &[u8; 8] = b"NGKGSIDX";
const VERSION: u32 = 1;
const HEADER_BYTES: usize = 96;
const RECORD_BYTES: usize = 56;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkLocator {
    pub chunk_sha256: String,
    pub ordinal: u32,
    pub byte_start: u64,
    pub byte_end_exclusive: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct IndexLimits {
    pub maximum_records: usize,
    pub maximum_mapped_bytes: usize,
    pub expected_owner_uid: u32,
}

pub fn build_index(
    mut records: Vec<ChunkLocator>,
    content_sha256: &str,
    total_bytes: u64,
) -> Result<Vec<u8>, SliceError> {
    if records.is_empty() || !valid_hash(content_sha256) {
        return Err(SliceError::Invalid("index identity"));
    }
    validate_logical_records(&records, total_bytes)?;
    records.sort_by(|left, right| {
        left.chunk_sha256
            .cmp(&right.chunk_sha256)
            .then(left.ordinal.cmp(&right.ordinal))
    });
    let record_bytes = records
        .len()
        .checked_mul(RECORD_BYTES)
        .ok_or(SliceError::Limit)?;
    let capacity = HEADER_BYTES
        .checked_add(record_bytes)
        .ok_or(SliceError::Limit)?;
    let mut encoded = vec![0_u8; capacity];
    encoded[0..8].copy_from_slice(MAGIC);
    encoded[8..12].copy_from_slice(&VERSION.to_le_bytes());
    encoded[12..16].copy_from_slice(
        &u32::try_from(RECORD_BYTES)
            .map_err(|_| SliceError::Limit)?
            .to_le_bytes(),
    );
    encoded[16..24].copy_from_slice(
        &u64::try_from(records.len())
            .map_err(|_| SliceError::Limit)?
            .to_le_bytes(),
    );
    encoded[24..32].copy_from_slice(&total_bytes.to_le_bytes());
    encoded[32..64]
        .copy_from_slice(&hex::decode(content_sha256).map_err(|_| SliceError::Checksum)?);
    for (position, record) in records.iter().enumerate() {
        let offset = HEADER_BYTES + position * RECORD_BYTES;
        encoded[offset..offset + 32]
            .copy_from_slice(&hex::decode(&record.chunk_sha256).map_err(|_| SliceError::Checksum)?);
        encoded[offset + 32..offset + 36].copy_from_slice(&record.ordinal.to_le_bytes());
        // bytes 36..40 are required zero padding for a stable fixed-width ABI.
        encoded[offset + 40..offset + 48].copy_from_slice(&record.byte_start.to_le_bytes());
        encoded[offset + 48..offset + 56].copy_from_slice(&record.byte_end_exclusive.to_le_bytes());
    }
    let records_sha = Sha256::digest(&encoded[HEADER_BYTES..]);
    encoded[64..96].copy_from_slice(&records_sha);
    Ok(encoded)
}

/// A verified, fixed-width index backed by an immutable, read-only file mapping.
pub struct VerifiedLocatorIndex {
    mapping: Mmap,
    count: usize,
    total_bytes: u64,
    content_sha256: String,
}

impl VerifiedLocatorIndex {
    #[allow(unsafe_code)]
    pub fn from_staged_file(
        path: &Path,
        expected_index_sha256: &str,
        expected_length: usize,
        limits: IndexLimits,
    ) -> Result<Self, SliceError> {
        if !valid_hash(expected_index_sha256)
            || expected_length < HEADER_BYTES
            || expected_length > limits.maximum_mapped_bytes
        {
            return Err(SliceError::Limit);
        }
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.len() != u64::try_from(expected_length).map_err(|_| SliceError::Limit)?
        {
            return Err(SliceError::Integrity("staged index file type or length"));
        }
        #[cfg(unix)]
        if metadata.uid() != limits.expected_owner_uid || metadata.permissions().mode() & 0o222 != 0 {
            return Err(SliceError::Integrity("staged index owner"));
        }
        let mut file = File::open(path)?;
        let opened_metadata = file.metadata()?;
        if opened_metadata.len() != metadata.len() {
            return Err(SliceError::Integrity("staged index changed while opening"));
        }
        #[cfg(unix)]
        if opened_metadata.dev() != metadata.dev()
            || opened_metadata.ino() != metadata.ino()
            || opened_metadata.uid() != metadata.uid()
            || opened_metadata.permissions().mode() & 0o222 != 0
        {
            return Err(SliceError::Integrity("staged index replaced while opening"));
        }
        let mut digest = Sha256::new();
        let mut buffer = [0_u8; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        if hex::encode(digest.finalize()) != expected_index_sha256 {
            return Err(SliceError::Checksum);
        }
        // SAFETY: the descriptor was opened read-only, its regular-file identity and
        // exact length were checked, and callers stage content-addressed files that
        // are never modified in place. The resulting Mmap is read-only.
        let mapping = unsafe { MmapOptions::new().len(expected_length).map(&file)? };
        let bytes: &[u8] = &mapping;
        if &bytes[0..8] != MAGIC
            || read_u32(bytes, 8)? != VERSION
            || usize::try_from(read_u32(bytes, 12)?).map_err(|_| SliceError::Limit)? != RECORD_BYTES
        {
            return Err(SliceError::Integrity(
                "index magic, version, or record width",
            ));
        }
        let count = usize::try_from(read_u64(bytes, 16)?).map_err(|_| SliceError::Limit)?;
        if count == 0 || count > limits.maximum_records {
            return Err(SliceError::Limit);
        }
        let expected = HEADER_BYTES
            .checked_add(count.checked_mul(RECORD_BYTES).ok_or(SliceError::Limit)?)
            .ok_or(SliceError::Limit)?;
        if bytes.len() != expected {
            return Err(SliceError::Integrity(
                "index exact length or reserved bytes",
            ));
        }
        let observed_records_sha = Sha256::digest(&bytes[HEADER_BYTES..]);
        if observed_records_sha[..] != bytes[64..96] {
            return Err(SliceError::Checksum);
        }
        let total_bytes = read_u64(bytes, 24)?;
        validate_physical_records(bytes, count, total_bytes)?;
        Ok(Self {
            mapping,
            count,
            total_bytes,
            content_sha256: hex::encode(&bytes[32..64]),
        })
    }

    pub fn mapped_bytes(&self) -> usize {
        self.mapping.len()
    }
    /// Conservative cgroup charge: every mapped byte is treated as resident.
    pub fn resident_estimate_bytes(&self) -> usize {
        self.mapping.len()
    }
    pub const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }
    pub fn content_sha256(&self) -> &str {
        &self.content_sha256
    }

    pub fn locate(&self, digest: &str, ordinal: u32) -> Result<Option<ChunkLocator>, SliceError> {
        if !valid_hash(digest) {
            return Err(SliceError::Invalid("chunk hash"));
        }
        let needle = hex::decode(digest).map_err(|_| SliceError::Invalid("chunk hash"))?;
        let mut low = 0_usize;
        let mut high = self.count;
        while low < high {
            let middle = low + (high - low) / 2;
            let offset = HEADER_BYTES + middle * RECORD_BYTES;
            let observed_ordinal = read_u32(&self.mapping, offset + 32)?;
            match self.mapping[offset..offset + 32]
                .cmp(&needle)
                .then(observed_ordinal.cmp(&ordinal))
            {
                std::cmp::Ordering::Less => low = middle + 1,
                std::cmp::Ordering::Greater => high = middle,
                std::cmp::Ordering::Equal => {
                    return Ok(Some(decode_record(&self.mapping, middle)?));
                }
            }
        }
        Ok(None)
    }
}

fn validate_logical_records(records: &[ChunkLocator], total_bytes: u64) -> Result<(), SliceError> {
    let mut by_ordinal = records.to_vec();
    by_ordinal.sort_by_key(|record| record.ordinal);
    let mut cursor = 0_u64;
    for (expected_ordinal, record) in by_ordinal.iter().enumerate() {
        if usize::try_from(record.ordinal).map_err(|_| SliceError::Limit)? != expected_ordinal
            || !valid_hash(&record.chunk_sha256)
            || record.byte_start != cursor
            || record.byte_end_exclusive <= record.byte_start
            || record.byte_end_exclusive > total_bytes
        {
            return Err(SliceError::Integrity("non-contiguous logical chunk map"));
        }
        cursor = record.byte_end_exclusive;
    }
    if cursor != total_bytes {
        return Err(SliceError::Integrity("chunk map total"));
    }
    Ok(())
}

fn validate_physical_records(
    bytes: &[u8],
    count: usize,
    total_bytes: u64,
) -> Result<(), SliceError> {
    let mut previous: Option<([u8; 32], u32)> = None;
    let mut records = Vec::with_capacity(count);
    for position in 0..count {
        let record = decode_record(bytes, position)?;
        let digest: [u8; 32] = hex::decode(&record.chunk_sha256)
            .map_err(|_| SliceError::Checksum)?
            .try_into()
            .map_err(|_| SliceError::Checksum)?;
        if let Some(prior) = previous
            && (digest, record.ordinal) <= prior
        {
            return Err(SliceError::Integrity("index sort order"));
        }
        previous = Some((digest, record.ordinal));
        records.push(record);
    }
    validate_logical_records(&records, total_bytes)
}

fn decode_record(bytes: &[u8], position: usize) -> Result<ChunkLocator, SliceError> {
    let offset = HEADER_BYTES
        .checked_add(
            position
                .checked_mul(RECORD_BYTES)
                .ok_or(SliceError::Limit)?,
        )
        .ok_or(SliceError::Limit)?;
    if offset + RECORD_BYTES > bytes.len()
        || bytes[offset + 36..offset + 40]
            .iter()
            .any(|value| *value != 0)
    {
        return Err(SliceError::Integrity("record offset"));
    }
    Ok(ChunkLocator {
        chunk_sha256: hex::encode(&bytes[offset..offset + 32]),
        ordinal: read_u32(bytes, offset + 32)?,
        byte_start: read_u64(bytes, offset + 40)?,
        byte_end_exclusive: read_u64(bytes, offset + 48)?,
    })
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, SliceError> {
    let raw: [u8; 4] = bytes
        .get(offset..offset + 4)
        .ok_or(SliceError::Integrity("u32 offset"))?
        .try_into()
        .map_err(|_| SliceError::Integrity("u32 width"))?;
    Ok(u32::from_le_bytes(raw))
}
fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, SliceError> {
    let raw: [u8; 8] = bytes
        .get(offset..offset + 8)
        .ok_or(SliceError::Integrity("u64 offset"))?
        .try_into()
        .map_err(|_| SliceError::Integrity("u64 width"))?;
    Ok(u64::from_le_bytes(raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_mutated_index() -> Result<(), Box<dyn std::error::Error>> {
        let chunks = vec![
            ChunkLocator {
                chunk_sha256: crate::sha256(b"a"),
                ordinal: 0,
                byte_start: 0,
                byte_end_exclusive: 1,
            },
            ChunkLocator {
                chunk_sha256: crate::sha256(b"bc"),
                ordinal: 1,
                byte_start: 1,
                byte_end_exclusive: 3,
            },
        ];
        let mut encoded = build_index(chunks, &crate::sha256(b"abc"), 3).unwrap_or_default();
        encoded[HEADER_BYTES] ^= 1;
        let path = std::env::temp_dir().join(format!(
            "ngkg-mutated-index-{}-{}.idx",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::write(&path, &encoded)?;
        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o400))?;
        let metadata = fs::metadata(&path)?;
        #[cfg(unix)]
        let owner = metadata.uid();
        #[cfg(not(unix))]
        let owner = 0;
        let result = VerifiedLocatorIndex::from_staged_file(
            &path,
            &crate::sha256(&encoded),
            encoded.len(),
            IndexLimits {
                maximum_records: 10,
                maximum_mapped_bytes: 4096,
                expected_owner_uid: owner,
            },
        );
        let _ = fs::remove_file(path);
        assert!(result.is_err());
        Ok(())
    }
}
