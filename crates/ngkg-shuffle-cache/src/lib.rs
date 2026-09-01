//! Bounded, checksum-verified local cache for immutable shuffle results.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const CACHE_MAGIC: &[u8; 8] = b"NGKGSC26";
const CACHE_HEADER_BYTES: usize = 80;
const CACHE_ROOT_MARKER: &str = ".ngkg-shuffle-cache-v1";
const CACHE_ROOT_MARKER_BYTES: &[u8] = b"ngkg-shuffle-result-cache-v1\n";

/// Immutable semantic identity of one shuffle partition result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShuffleCacheKey {
    /// Tenant that owns the dataset.
    pub tenant_id: Uuid,
    /// Dataset UUID.
    pub dataset_id: Uuid,
    /// Published immutable snapshot UUID.
    pub snapshot_id: Uuid,
    /// Exact certified query SHA-256.
    pub query_sha256: String,
    /// Exact distributed-plan artifact SHA-256.
    pub plan_sha256: String,
    /// Zero-based join stage.
    pub stage: u32,
    /// Stable logical partition.
    pub partition: u32,
    /// Total stable partition count.
    pub partition_count: u32,
    /// Deterministic checksum of the complete left input representation.
    pub left_input_sha256: String,
    /// Deterministic checksum of the complete right input representation.
    pub right_input_sha256: String,
}

impl ShuffleCacheKey {
    /// Return the lowercase SHA-256 key used as the only managed filename.
    pub fn digest(&self) -> Result<String, ShuffleCacheError> {
        if self.partition_count < 2
            || self.partition >= self.partition_count
            || !is_sha256(&self.query_sha256)
            || !is_sha256(&self.plan_sha256)
            || !is_sha256(&self.left_input_sha256)
            || !is_sha256(&self.right_input_sha256)
        {
            return Err(ShuffleCacheError::InvalidKey);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"ngkg-shuffle-result-cache-key-v2");
        hasher.update(self.tenant_id.as_bytes());
        hasher.update(self.dataset_id.as_bytes());
        hasher.update(self.snapshot_id.as_bytes());
        update_hex(&mut hasher, &self.query_sha256)?;
        update_hex(&mut hasher, &self.plan_sha256)?;
        hasher.update(self.stage.to_be_bytes());
        hasher.update(self.partition.to_be_bytes());
        hasher.update(self.partition_count.to_be_bytes());
        update_hex(&mut hasher, &self.left_input_sha256)?;
        update_hex(&mut hasher, &self.right_input_sha256)?;
        Ok(hex::encode(hasher.finalize()))
    }
}

/// A verified cache lookup result.
#[derive(Debug, Eq, PartialEq)]
pub enum CacheLookup {
    /// The immutable key was not resident.
    Miss,
    /// Exact payload bytes from a checksum-valid entry.
    Hit(Vec<u8>),
}

/// Local cache failures never authorize use of unverified bytes.
#[derive(Debug, Error)]
pub enum ShuffleCacheError {
    /// Cache key contains an invalid identity or checksum.
    #[error("shuffle cache key is invalid")]
    InvalidKey,
    /// Cache root is unsafe or contains an unmanaged object.
    #[error("shuffle cache root is unsafe: {0}")]
    UnsafeRoot(String),
    /// One cache entry exceeds its configured limit.
    #[error("shuffle cache entry exceeds its byte ceiling")]
    EntryTooLarge,
    /// Cache capacity cannot hold the entry.
    #[error("shuffle cache capacity is too small for the entry")]
    CapacityTooSmall,
    /// Cache accounting overflowed.
    #[error("shuffle cache accounting overflow")]
    AccountingOverflow,
    /// Process-local cache state was poisoned.
    #[error("shuffle cache state lock is poisoned")]
    Poisoned,
    /// Local storage failed.
    #[error("shuffle cache I/O failed: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
struct CacheEntry {
    path: PathBuf,
    bytes: u64,
}

struct CacheState {
    entries: BTreeMap<String, CacheEntry>,
    least_to_most_recent: VecDeque<String>,
    bytes: u64,
}

/// Thread-safe bounded cache backed by one operator-owned local directory.
pub struct ShuffleResultCache {
    root: PathBuf,
    max_bytes: u64,
    max_entries: usize,
    max_entry_bytes: u64,
    state: Mutex<CacheState>,
}

impl ShuffleResultCache {
    /// Open a marked cache root, validate every managed header, and enforce bounds.
    pub fn open(
        root: &Path,
        max_bytes: u64,
        max_entries: usize,
        max_entry_bytes: u64,
    ) -> Result<Self, ShuffleCacheError> {
        if max_bytes == 0 || max_entries == 0 || max_entry_bytes == 0 || max_entry_bytes > max_bytes
        {
            return Err(ShuffleCacheError::CapacityTooSmall);
        }
        prepare_root(root)?;
        let mut entries = BTreeMap::new();
        let mut order = VecDeque::new();
        let mut bytes = 0_u64;
        let mut corrupt = Vec::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| ShuffleCacheError::UnsafeRoot("non-UTF-8 entry".to_owned()))?;
            if name == CACHE_ROOT_MARKER {
                continue;
            }
            if is_temp_name(name) {
                corrupt.push(entry.path());
                continue;
            }
            let Some(digest) = cache_file_digest(name) else {
                return Err(ShuffleCacheError::UnsafeRoot(format!(
                    "unmanaged entry {name}"
                )));
            };
            let file_type = entry.file_type()?;
            if file_type.is_symlink() || !file_type.is_file() {
                return Err(ShuffleCacheError::UnsafeRoot(format!(
                    "entry {name} is not a regular file"
                )));
            }
            let path = entry.path();
            match validate_file_header(&path, &digest, max_entry_bytes) {
                Ok(size) => {
                    bytes = bytes
                        .checked_add(size)
                        .ok_or(ShuffleCacheError::AccountingOverflow)?;
                    entries.insert(digest.clone(), CacheEntry { path, bytes: size });
                    order.push_back(digest);
                }
                Err(ShuffleCacheError::Io(_))
                | Err(ShuffleCacheError::EntryTooLarge)
                | Err(ShuffleCacheError::InvalidKey) => corrupt.push(path),
                Err(error) => return Err(error),
            }
        }
        for path in corrupt {
            fs::remove_file(path)?;
        }
        let cache = Self {
            root: root.to_path_buf(),
            max_bytes,
            max_entries,
            max_entry_bytes,
            state: Mutex::new(CacheState {
                entries,
                least_to_most_recent: order,
                bytes,
            }),
        };
        cache.enforce_limits()?;
        Ok(cache)
    }

    /// Read and verify an exact immutable entry. Corrupt bytes are removed and miss.
    pub fn get(&self, key: &ShuffleCacheKey) -> Result<CacheLookup, ShuffleCacheError> {
        let digest = key.digest()?;
        let (mut file, expected_bytes, path) = {
            let mut state = self.state.lock().map_err(|_| ShuffleCacheError::Poisoned)?;
            let Some(entry) = state.entries.get(&digest).cloned() else {
                return Ok(CacheLookup::Miss);
            };
            let file = match File::open(&entry.path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    state.entries.remove(&digest);
                    state.bytes = state
                        .bytes
                        .checked_sub(entry.bytes)
                        .ok_or(ShuffleCacheError::AccountingOverflow)?;
                    state.least_to_most_recent.retain(|value| value != &digest);
                    return Ok(CacheLookup::Miss);
                }
                Err(error) => return Err(error.into()),
            };
            touch(&mut state.least_to_most_recent, &digest);
            (file, entry.bytes, entry.path)
        };
        match read_verified(&mut file, &digest, expected_bytes, self.max_entry_bytes) {
            Ok(payload) => Ok(CacheLookup::Hit(payload)),
            Err(ShuffleCacheError::Io(_))
            | Err(ShuffleCacheError::EntryTooLarge)
            | Err(ShuffleCacheError::InvalidKey) => {
                drop(file);
                self.remove_if_present(&digest, &path)?;
                Ok(CacheLookup::Miss)
            }
            Err(error) => Err(error),
        }
    }

    /// Atomically publish verified logical result bytes under their immutable key.
    pub fn insert(&self, key: &ShuffleCacheKey, payload: &[u8]) -> Result<(), ShuffleCacheError> {
        let payload_bytes =
            u64::try_from(payload.len()).map_err(|_| ShuffleCacheError::EntryTooLarge)?;
        let total_bytes = payload_bytes
            .checked_add(
                u64::try_from(CACHE_HEADER_BYTES)
                    .map_err(|_| ShuffleCacheError::AccountingOverflow)?,
            )
            .ok_or(ShuffleCacheError::AccountingOverflow)?;
        if total_bytes > self.max_entry_bytes {
            return Err(ShuffleCacheError::EntryTooLarge);
        }
        let digest = key.digest()?;
        let final_path = self.root.join(format!("{digest}.cache"));
        let temp_path = self.root.join(format!(".{digest}.{}.tmp", Uuid::new_v4()));
        let write_result = (|| {
            let payload_sha256: [u8; 32] = Sha256::digest(payload).into();
            let digest_bytes = decode_digest(&digest)?;
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)?;
            file.write_all(CACHE_MAGIC)?;
            file.write_all(&digest_bytes)?;
            file.write_all(&payload_bytes.to_be_bytes())?;
            file.write_all(&payload_sha256)?;
            file.write_all(payload)?;
            file.sync_all()?;
            Ok::<(), ShuffleCacheError>(())
        })();
        if let Err(error) = write_result {
            let _cleanup = fs::remove_file(&temp_path);
            return Err(error);
        }
        let mut state = self.state.lock().map_err(|_| ShuffleCacheError::Poisoned)?;
        if state.entries.contains_key(&digest) {
            fs::remove_file(temp_path)?;
            touch(&mut state.least_to_most_recent, &digest);
            return Ok(());
        }
        if let Err(error) =
            evict_for_insert(&mut state, total_bytes, self.max_bytes, self.max_entries)
        {
            let _cleanup = fs::remove_file(&temp_path);
            return Err(error);
        }
        if let Err(error) = fs::hard_link(&temp_path, &final_path) {
            let _cleanup = fs::remove_file(&temp_path);
            return Err(error.into());
        }
        if let Err(error) = fs::remove_file(&temp_path) {
            let _cleanup = fs::remove_file(&final_path);
            return Err(error.into());
        }
        if let Err(error) = File::open(&self.root).and_then(|directory| directory.sync_all()) {
            let _cleanup = fs::remove_file(&final_path);
            return Err(error.into());
        }
        state.bytes = state
            .bytes
            .checked_add(total_bytes)
            .ok_or(ShuffleCacheError::AccountingOverflow)?;
        state.entries.insert(
            digest.clone(),
            CacheEntry {
                path: final_path,
                bytes: total_bytes,
            },
        );
        touch(&mut state.least_to_most_recent, &digest);
        Ok(())
    }

    /// Remove one exact cache entry, if resident.
    pub fn invalidate(&self, key: &ShuffleCacheKey) -> Result<(), ShuffleCacheError> {
        let digest = key.digest()?;
        let path = self.root.join(format!("{digest}.cache"));
        self.remove_if_present(&digest, &path)
    }

    /// Return current entry and byte counts for operational metrics.
    pub fn usage(&self) -> Result<(usize, u64), ShuffleCacheError> {
        let state = self.state.lock().map_err(|_| ShuffleCacheError::Poisoned)?;
        Ok((state.entries.len(), state.bytes))
    }

    fn enforce_limits(&self) -> Result<(), ShuffleCacheError> {
        let mut state = self.state.lock().map_err(|_| ShuffleCacheError::Poisoned)?;
        while state.entries.len() > self.max_entries || state.bytes > self.max_bytes {
            evict_one(&mut state)?;
        }
        Ok(())
    }

    fn remove_if_present(
        &self,
        digest: &str,
        expected_path: &Path,
    ) -> Result<(), ShuffleCacheError> {
        let mut state = self.state.lock().map_err(|_| ShuffleCacheError::Poisoned)?;
        let Some(entry) = state.entries.get(digest) else {
            return Ok(());
        };
        if entry.path != expected_path {
            return Err(ShuffleCacheError::UnsafeRoot(
                "cache index path changed unexpectedly".to_owned(),
            ));
        }
        let entry = state
            .entries
            .get(digest)
            .cloned()
            .ok_or(ShuffleCacheError::InvalidKey)?;
        match fs::remove_file(&entry.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        state.entries.remove(digest);
        state.bytes = state
            .bytes
            .checked_sub(entry.bytes)
            .ok_or(ShuffleCacheError::AccountingOverflow)?;
        state.least_to_most_recent.retain(|value| value != digest);
        Ok(())
    }
}

fn prepare_root(root: &Path) -> Result<(), ShuffleCacheError> {
    if root.exists() {
        let metadata = fs::symlink_metadata(root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ShuffleCacheError::UnsafeRoot(
                "root must be a real directory".to_owned(),
            ));
        }
    } else {
        fs::create_dir_all(root)?;
    }
    let marker = root.join(CACHE_ROOT_MARKER);
    if marker.exists() {
        if fs::symlink_metadata(&marker)?.file_type().is_symlink()
            || fs::read(&marker)? != CACHE_ROOT_MARKER_BYTES
        {
            return Err(ShuffleCacheError::UnsafeRoot(
                "root marker is invalid".to_owned(),
            ));
        }
    } else {
        if fs::read_dir(root)?.next().transpose()?.is_some() {
            return Err(ShuffleCacheError::UnsafeRoot(
                "uninitialized root must be empty".to_owned(),
            ));
        }
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(marker)?;
        file.write_all(CACHE_ROOT_MARKER_BYTES)?;
        file.sync_all()?;
        File::open(root)?.sync_all()?;
    }
    Ok(())
}

fn cache_file_digest(name: &str) -> Option<String> {
    let digest = name.strip_suffix(".cache")?;
    is_sha256(digest).then(|| digest.to_owned())
}

fn is_temp_name(name: &str) -> bool {
    let Some(value) = name.strip_prefix('.') else {
        return false;
    };
    let Some((digest, suffix)) = value.split_once('.') else {
        return false;
    };
    let Some(uuid) = suffix.strip_suffix(".tmp") else {
        return false;
    };
    is_sha256(digest) && uuid.parse::<Uuid>().is_ok()
}

fn validate_file_header(
    path: &Path,
    digest: &str,
    max_entry_bytes: u64,
) -> Result<u64, ShuffleCacheError> {
    let size = fs::metadata(path)?.len();
    let mut file = File::open(path)?;
    validate_header(&mut file, digest, size, max_entry_bytes)?;
    Ok(size)
}

fn validate_header(
    file: &mut File,
    digest: &str,
    expected_bytes: u64,
    max_entry_bytes: u64,
) -> Result<[u8; CACHE_HEADER_BYTES], ShuffleCacheError> {
    if expected_bytes > max_entry_bytes
        || expected_bytes
            < u64::try_from(CACHE_HEADER_BYTES)
                .map_err(|_| ShuffleCacheError::AccountingOverflow)?
    {
        return Err(ShuffleCacheError::EntryTooLarge);
    }
    let mut header = [0_u8; CACHE_HEADER_BYTES];
    file.read_exact(&mut header)?;
    let digest_bytes = decode_digest(digest)?;
    if &header[..8] != CACHE_MAGIC || header[8..40] != digest_bytes {
        return Err(ShuffleCacheError::InvalidKey);
    }
    let payload_bytes = u64::from_be_bytes(
        header[40..48]
            .try_into()
            .map_err(|_| ShuffleCacheError::InvalidKey)?,
    );
    let total = payload_bytes
        .checked_add(
            u64::try_from(CACHE_HEADER_BYTES).map_err(|_| ShuffleCacheError::AccountingOverflow)?,
        )
        .ok_or(ShuffleCacheError::AccountingOverflow)?;
    if total != expected_bytes {
        return Err(ShuffleCacheError::InvalidKey);
    }
    Ok(header)
}

fn read_verified(
    file: &mut File,
    digest: &str,
    expected_bytes: u64,
    max_entry_bytes: u64,
) -> Result<Vec<u8>, ShuffleCacheError> {
    let header = validate_header(file, digest, expected_bytes, max_entry_bytes)?;
    let payload_bytes = u64::from_be_bytes(
        header[40..48]
            .try_into()
            .map_err(|_| ShuffleCacheError::InvalidKey)?,
    );
    let payload_len =
        usize::try_from(payload_bytes).map_err(|_| ShuffleCacheError::EntryTooLarge)?;
    let mut payload = vec![0_u8; payload_len];
    file.read_exact(&mut payload)?;
    let mut trailing = [0_u8; 1];
    if file.read(&mut trailing)? != 0
        || <[u8; 32]>::from(Sha256::digest(&payload)) != header[48..80]
    {
        return Err(ShuffleCacheError::InvalidKey);
    }
    Ok(payload)
}

fn evict_for_insert(
    state: &mut CacheState,
    incoming_bytes: u64,
    max_bytes: u64,
    max_entries: usize,
) -> Result<(), ShuffleCacheError> {
    if incoming_bytes > max_bytes {
        return Err(ShuffleCacheError::CapacityTooSmall);
    }
    while state
        .entries
        .len()
        .checked_add(1)
        .is_none_or(|count| count > max_entries)
        || state
            .bytes
            .checked_add(incoming_bytes)
            .is_none_or(|bytes| bytes > max_bytes)
    {
        evict_one(state)?;
    }
    Ok(())
}

fn evict_one(state: &mut CacheState) -> Result<(), ShuffleCacheError> {
    let digest = state
        .least_to_most_recent
        .front()
        .cloned()
        .ok_or(ShuffleCacheError::AccountingOverflow)?;
    let entry = state
        .entries
        .get(&digest)
        .cloned()
        .ok_or(ShuffleCacheError::AccountingOverflow)?;
    match fs::remove_file(&entry.path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    state.least_to_most_recent.pop_front();
    state.entries.remove(&digest);
    state.bytes = state
        .bytes
        .checked_sub(entry.bytes)
        .ok_or(ShuffleCacheError::AccountingOverflow)?;
    Ok(())
}

fn touch(order: &mut VecDeque<String>, digest: &str) {
    order.retain(|value| value != digest);
    order.push_back(digest.to_owned());
}

fn update_hex(hasher: &mut Sha256, value: &str) -> Result<(), ShuffleCacheError> {
    hasher.update(decode_digest(value)?);
    Ok(())
}

fn decode_digest(value: &str) -> Result<[u8; 32], ShuffleCacheError> {
    hex::decode(value)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(ShuffleCacheError::InvalidKey)
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
        fs::{self, OpenOptions},
        io::Write,
    };

    use super::{
        CACHE_ROOT_MARKER, CacheLookup, ShuffleCacheError, ShuffleCacheKey, ShuffleResultCache,
    };
    use uuid::Uuid;

    fn root() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("ngkg-shuffle-cache-test-{}", Uuid::new_v4()))
    }

    fn key(partition: u32) -> ShuffleCacheKey {
        ShuffleCacheKey {
            tenant_id: Uuid::from_u128(1),
            dataset_id: Uuid::from_u128(2),
            snapshot_id: Uuid::from_u128(3),
            query_sha256: "1".repeat(64),
            plan_sha256: "2".repeat(64),
            stage: 0,
            partition,
            partition_count: 4,
            left_input_sha256: "3".repeat(64),
            right_input_sha256: "4".repeat(64),
        }
    }

    fn remove_root(root: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
        if root.exists() {
            fs::remove_dir_all(root)?;
        }
        Ok(())
    }

    #[test]
    fn cache_round_trip_and_reopen_preserve_exact_bytes() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = root();
        let cache = ShuffleResultCache::open(&root, 4096, 4, 2048)?;
        assert_eq!(cache.get(&key(0))?, CacheLookup::Miss);
        cache.insert(&key(0), b"exact-result")?;
        assert_eq!(
            cache.get(&key(0))?,
            CacheLookup::Hit(b"exact-result".to_vec())
        );
        drop(cache);
        let reopened = ShuffleResultCache::open(&root, 4096, 4, 2048)?;
        assert_eq!(
            reopened.get(&key(0))?,
            CacheLookup::Hit(b"exact-result".to_vec())
        );
        remove_root(&root)?;
        Ok(())
    }

    #[test]
    fn corruption_is_removed_and_becomes_a_miss() -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let cache = ShuffleResultCache::open(&root, 4096, 4, 2048)?;
        cache.insert(&key(0), b"exact-result")?;
        let path = root.join(format!("{}.cache", key(0).digest()?));
        OpenOptions::new()
            .append(true)
            .open(path)?
            .write_all(b"corrupt")?;
        assert_eq!(cache.get(&key(0))?, CacheLookup::Miss);
        assert_eq!(cache.usage()?, (0, 0));
        remove_root(&root)?;
        Ok(())
    }

    #[test]
    fn lru_eviction_enforces_entry_and_byte_bounds() -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let cache = ShuffleResultCache::open(&root, 200, 2, 100)?;
        cache.insert(&key(0), b"one")?;
        cache.insert(&key(1), b"two")?;
        let _hit = cache.get(&key(0))?;
        cache.insert(&key(2), b"three")?;
        assert!(matches!(cache.get(&key(1))?, CacheLookup::Miss));
        assert!(matches!(cache.get(&key(0))?, CacheLookup::Hit(_)));
        assert!(matches!(cache.get(&key(2))?, CacheLookup::Hit(_)));
        remove_root(&root)?;
        Ok(())
    }

    #[test]
    fn every_semantic_identity_field_changes_the_key() -> Result<(), Box<dyn std::error::Error>> {
        let original = key(0);
        let original_digest = original.digest()?;
        let variants = [
            ShuffleCacheKey {
                tenant_id: Uuid::from_u128(11),
                ..original.clone()
            },
            ShuffleCacheKey {
                dataset_id: Uuid::from_u128(12),
                ..original.clone()
            },
            ShuffleCacheKey {
                snapshot_id: Uuid::from_u128(13),
                ..original.clone()
            },
            ShuffleCacheKey {
                query_sha256: "5".repeat(64),
                ..original.clone()
            },
            ShuffleCacheKey {
                plan_sha256: "6".repeat(64),
                ..original.clone()
            },
            ShuffleCacheKey {
                stage: 1,
                ..original.clone()
            },
            ShuffleCacheKey {
                partition: 1,
                ..original.clone()
            },
            ShuffleCacheKey {
                partition_count: 8,
                ..original.clone()
            },
            ShuffleCacheKey {
                left_input_sha256: "7".repeat(64),
                ..original.clone()
            },
            ShuffleCacheKey {
                right_input_sha256: "8".repeat(64),
                ..original
            },
        ];
        for variant in variants {
            assert_ne!(variant.digest()?, original_digest);
        }
        Ok(())
    }

    #[test]
    fn invalid_or_oversized_entries_are_never_published() -> Result<(), Box<dyn std::error::Error>>
    {
        let root = root();
        let cache = ShuffleResultCache::open(&root, 256, 4, 96)?;
        assert!(matches!(
            cache.insert(&key(0), &[0_u8; 17]),
            Err(ShuffleCacheError::EntryTooLarge)
        ));
        let mut invalid = key(0);
        invalid.query_sha256 = "not-a-checksum".to_owned();
        assert!(matches!(
            cache.insert(&invalid, b"value"),
            Err(ShuffleCacheError::InvalidKey)
        ));
        assert_eq!(cache.usage()?, (0, 0));
        remove_root(&root)?;
        Ok(())
    }

    #[test]
    fn truncation_extension_and_wrong_key_become_misses() -> Result<(), Box<dyn std::error::Error>>
    {
        for mutation in 0..3 {
            let root = root();
            let cache = ShuffleResultCache::open(&root, 4096, 4, 2048)?;
            cache.insert(&key(0), b"exact-result")?;
            let source = root.join(format!("{}.cache", key(0).digest()?));
            match mutation {
                0 => OpenOptions::new().write(true).open(&source)?.set_len(84)?,
                1 => OpenOptions::new()
                    .append(true)
                    .open(&source)?
                    .write_all(b"extension")?,
                _ => {
                    let destination = root.join(format!("{}.cache", key(1).digest()?));
                    fs::rename(&source, &destination)?;
                    drop(cache);
                    let reopened = ShuffleResultCache::open(&root, 4096, 4, 2048)?;
                    assert_eq!(reopened.usage()?, (0, 0));
                    remove_root(&root)?;
                    continue;
                }
            }
            assert_eq!(cache.get(&key(0))?, CacheLookup::Miss);
            assert_eq!(cache.usage()?, (0, 0));
            remove_root(&root)?;
        }
        Ok(())
    }

    #[test]
    fn abandoned_owned_temp_is_removed_on_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let cache = ShuffleResultCache::open(&root, 4096, 4, 2048)?;
        drop(cache);
        let temp = root.join(format!(".{}.{}.tmp", key(0).digest()?, Uuid::new_v4()));
        fs::write(&temp, b"incomplete")?;
        let reopened = ShuffleResultCache::open(&root, 4096, 4, 2048)?;
        assert!(!temp.exists());
        assert_eq!(reopened.usage()?, (0, 0));
        remove_root(&root)?;
        Ok(())
    }

    #[test]
    fn unmanaged_objects_make_the_root_fail_closed() -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let cache = ShuffleResultCache::open(&root, 4096, 4, 2048)?;
        drop(cache);
        fs::write(root.join("operator-data"), b"must-not-delete")?;
        assert!(matches!(
            ShuffleResultCache::open(&root, 4096, 4, 2048),
            Err(ShuffleCacheError::UnsafeRoot(_))
        ));
        assert_eq!(fs::read(root.join("operator-data"))?, b"must-not-delete");
        assert!(root.join(CACHE_ROOT_MARKER).exists());
        remove_root(&root)?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_cache_entry_makes_the_root_fail_closed() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::unix::fs::symlink;

        let root = root();
        let cache = ShuffleResultCache::open(&root, 4096, 4, 2048)?;
        drop(cache);
        let outside = root.with_extension("outside");
        fs::write(&outside, b"external")?;
        symlink(&outside, root.join(format!("{}.cache", key(0).digest()?)))?;
        assert!(matches!(
            ShuffleResultCache::open(&root, 4096, 4, 2048),
            Err(ShuffleCacheError::UnsafeRoot(_))
        ));
        assert_eq!(fs::read(&outside)?, b"external");
        remove_root(&root)?;
        fs::remove_file(outside)?;
        Ok(())
    }
}
