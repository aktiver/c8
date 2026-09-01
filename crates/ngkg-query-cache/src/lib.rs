//! Bounded, checksum-verified local cache for complete certified query responses.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use bytes::Bytes;
use memmap2::MmapMut;
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const CACHE_MAGIC: &[u8; 8] = b"NGKGQC37";
/// Fixed on-disk header length used by application and deployment bounds.
pub const QUERY_CACHE_HEADER_BYTES: usize = 80;
const CACHE_HEADER_BYTES: usize = QUERY_CACHE_HEADER_BYTES;
const CACHE_HEADER_BYTES_U64: u64 = 80;
const CACHE_ROOT_MARKER: &str = ".ngkg-query-cache-v2";
const CACHE_ROOT_MARKER_BYTES: &[u8] = b"ngkg-certified-query-result-cache-v2\n";
const RESPONSE_SCHEMA_VERSION: u32 = 2;

/// Complete immutable identity of one public query response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryCacheKey {
    /// Tenant that owns the dataset and result.
    pub tenant_id: Uuid,
    /// Dataset addressed by the request.
    pub dataset_id: Uuid,
    /// Published immutable snapshot.
    pub snapshot_id: Uuid,
    /// Exact snapshot manifest SHA-256.
    pub manifest_sha256: String,
    /// Exact physical serving-root SHA-256.
    pub serving_root_sha256: String,
    /// Exact certified SPARQL query-byte SHA-256.
    pub query_sha256: String,
    /// Exact set of query-visible graphs authorized for this principal.
    pub authorized_graph_set_sha256: String,
    /// Exact active default/named dataset after protocol-over-query precedence.
    pub active_dataset_sha256: String,
    /// Stable precedence branch: service default, query dataset, or protocol dataset.
    pub dataset_selection_source: u8,
    /// Whether the public response includes Parquet payload hydration.
    pub hydrate: bool,
}

impl QueryCacheKey {
    /// Return the lowercase SHA-256 used as the only managed filename.
    ///
    /// # Errors
    ///
    /// Returns [`QueryCacheError::InvalidKey`] when any digest is not canonical.
    pub fn digest(&self) -> Result<String, QueryCacheError> {
        if !is_sha256(&self.manifest_sha256)
            || !is_sha256(&self.serving_root_sha256)
            || !is_sha256(&self.query_sha256)
            || !is_sha256(&self.authorized_graph_set_sha256)
            || !is_sha256(&self.active_dataset_sha256)
            || self.dataset_selection_source > 2
        {
            return Err(QueryCacheError::InvalidKey);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"ngkg-certified-query-result-key-v2");
        hasher.update(RESPONSE_SCHEMA_VERSION.to_be_bytes());
        hasher.update(self.tenant_id.as_bytes());
        hasher.update(self.dataset_id.as_bytes());
        hasher.update(self.snapshot_id.as_bytes());
        update_hex(&mut hasher, &self.manifest_sha256)?;
        update_hex(&mut hasher, &self.serving_root_sha256)?;
        update_hex(&mut hasher, &self.query_sha256)?;
        update_hex(&mut hasher, &self.authorized_graph_set_sha256)?;
        update_hex(&mut hasher, &self.active_dataset_sha256)?;
        hasher.update([self.dataset_selection_source]);
        hasher.update([u8::from(self.hydrate)]);
        Ok(hex::encode(hasher.finalize()))
    }
}

/// Verified cache lookup result.
#[derive(Debug, Eq, PartialEq)]
pub enum QueryCacheLookup {
    /// No usable exact response is resident.
    Miss,
    /// Read-only anonymous mmap bytes owning the complete verified JSON payload.
    Hit(Bytes),
}

/// Cache failures never authorize unverified response reuse.
#[derive(Debug, Error)]
pub enum QueryCacheError {
    /// Cache key contains an invalid identity or checksum.
    #[error("query cache key is invalid")]
    InvalidKey,
    /// Cache root contains an unmanaged or unsafe entry.
    #[error("query cache root is unsafe: {0}")]
    UnsafeRoot(String),
    /// An entry exceeds its configured bound or this platform.
    #[error("query cache entry exceeds its byte ceiling")]
    EntryTooLarge,
    /// Configured capacity cannot hold one admitted entry.
    #[error("query cache capacity is too small")]
    CapacityTooSmall,
    /// Size accounting overflowed.
    #[error("query cache accounting overflow")]
    AccountingOverflow,
    /// Process-local cache metadata was poisoned.
    #[error("query cache state lock is poisoned")]
    Poisoned,
    /// Local storage failed.
    #[error("query cache I/O failed: {0}")]
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

/// Thread-safe bounded cache backed by an operator-owned local-NVMe directory.
pub struct QueryResultCache {
    root: PathBuf,
    max_bytes: u64,
    max_entries: usize,
    max_entry_bytes: u64,
    state: Mutex<CacheState>,
}

impl QueryResultCache {
    /// Open the marked root, validate managed headers, and enforce hard bounds.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid limits, unsafe roots, corrupt accounting, or I/O failure.
    pub fn open(
        root: &Path,
        max_bytes: u64,
        max_entries: usize,
        max_entry_bytes: u64,
    ) -> Result<Self, QueryCacheError> {
        if max_bytes == 0
            || max_entries == 0
            || max_entry_bytes <= CACHE_HEADER_BYTES_U64
            || max_entry_bytes > max_bytes
        {
            return Err(QueryCacheError::CapacityTooSmall);
        }
        prepare_root(root)?;
        let mut entries = BTreeMap::new();
        let mut order = VecDeque::new();
        let mut bytes = 0_u64;
        let mut invalid = Vec::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| QueryCacheError::UnsafeRoot("non-UTF-8 entry".to_owned()))?;
            if name == CACHE_ROOT_MARKER {
                continue;
            }
            if is_temp_name(name) {
                invalid.push(entry.path());
                continue;
            }
            let Some(digest) = cache_file_digest(name) else {
                return Err(QueryCacheError::UnsafeRoot(format!(
                    "unmanaged entry {name}"
                )));
            };
            let file_type = entry.file_type()?;
            if file_type.is_symlink() || !file_type.is_file() {
                return Err(QueryCacheError::UnsafeRoot(format!(
                    "entry {name} is not a regular file"
                )));
            }
            let path = entry.path();
            match validate_file_header(&path, &digest, max_entry_bytes) {
                Ok(size) => {
                    bytes = bytes
                        .checked_add(size)
                        .ok_or(QueryCacheError::AccountingOverflow)?;
                    entries.insert(digest.clone(), CacheEntry { path, bytes: size });
                    order.push_back(digest);
                }
                Err(
                    QueryCacheError::Io(_)
                    | QueryCacheError::EntryTooLarge
                    | QueryCacheError::InvalidKey,
                ) => invalid.push(path),
                Err(error) => return Err(error),
            }
        }
        for path in invalid {
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

    /// Read, checksum, and expose one response through a read-only anonymous mmap.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is invalid or the managed cache cannot be accessed safely.
    pub fn get(&self, key: &QueryCacheKey) -> Result<QueryCacheLookup, QueryCacheError> {
        let digest = key.digest()?;
        let (mut file, expected_bytes, path) = {
            let mut state = self.state.lock().map_err(|_| QueryCacheError::Poisoned)?;
            let Some(entry) = state.entries.get(&digest).cloned() else {
                return Ok(QueryCacheLookup::Miss);
            };
            let file = match File::open(&entry.path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    remove_state_entry(&mut state, &digest, entry.bytes)?;
                    return Ok(QueryCacheLookup::Miss);
                }
                Err(error) => return Err(error.into()),
            };
            touch(&mut state.least_to_most_recent, &digest);
            (file, entry.bytes, entry.path)
        };
        match read_verified_mmap(&mut file, &digest, expected_bytes, self.max_entry_bytes) {
            Ok(payload) => Ok(QueryCacheLookup::Hit(payload)),
            Err(
                QueryCacheError::Io(_)
                | QueryCacheError::EntryTooLarge
                | QueryCacheError::InvalidKey,
            ) => {
                drop(file);
                self.remove_if_present(&digest, &path)?;
                Ok(QueryCacheLookup::Miss)
            }
            Err(error) => Err(error),
        }
    }

    /// Atomically publish one complete verified JSON response.
    ///
    /// # Errors
    ///
    /// Returns an error when identity, capacity, accounting, or durable publication fails.
    pub fn insert(&self, key: &QueryCacheKey, payload: &[u8]) -> Result<(), QueryCacheError> {
        let payload_bytes =
            u64::try_from(payload.len()).map_err(|_| QueryCacheError::EntryTooLarge)?;
        let total_bytes = payload_bytes
            .checked_add(CACHE_HEADER_BYTES_U64)
            .ok_or(QueryCacheError::AccountingOverflow)?;
        if total_bytes > self.max_entry_bytes {
            return Err(QueryCacheError::EntryTooLarge);
        }
        let digest = key.digest()?;
        let final_path = self.root.join(format!("{digest}.cache"));
        let temp_path = self.root.join(format!(".{digest}.{}.tmp", Uuid::new_v4()));
        let write_result = (|| {
            let payload_sha256: [u8; 32] = Sha256::digest(payload).into();
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)?;
            file.write_all(CACHE_MAGIC)?;
            file.write_all(&decode_digest(&digest)?)?;
            file.write_all(&payload_bytes.to_be_bytes())?;
            file.write_all(&payload_sha256)?;
            file.write_all(payload)?;
            file.sync_all()?;
            Ok::<(), QueryCacheError>(())
        })();
        if let Err(error) = write_result {
            let _cleanup = fs::remove_file(&temp_path);
            return Err(error);
        }
        let mut state = self.state.lock().map_err(|_| QueryCacheError::Poisoned)?;
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
            .ok_or(QueryCacheError::AccountingOverflow)?;
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

    /// Remove one exact entry if it is resident.
    ///
    /// # Errors
    ///
    /// Returns an error when the key is invalid or the managed entry cannot be removed safely.
    pub fn invalidate(&self, key: &QueryCacheKey) -> Result<(), QueryCacheError> {
        let digest = key.digest()?;
        let path = self.root.join(format!("{digest}.cache"));
        self.remove_if_present(&digest, &path)
    }

    /// Return current entry and byte counts.
    ///
    /// # Errors
    ///
    /// Returns [`QueryCacheError::Poisoned`] if another thread panicked while holding state.
    pub fn usage(&self) -> Result<(usize, u64), QueryCacheError> {
        let state = self.state.lock().map_err(|_| QueryCacheError::Poisoned)?;
        Ok((state.entries.len(), state.bytes))
    }

    fn enforce_limits(&self) -> Result<(), QueryCacheError> {
        let mut state = self.state.lock().map_err(|_| QueryCacheError::Poisoned)?;
        while state.entries.len() > self.max_entries || state.bytes > self.max_bytes {
            evict_one(&mut state)?;
        }
        Ok(())
    }

    fn remove_if_present(&self, digest: &str, path: &Path) -> Result<(), QueryCacheError> {
        let mut state = self.state.lock().map_err(|_| QueryCacheError::Poisoned)?;
        let Some(entry) = state.entries.get(digest).cloned() else {
            return Ok(());
        };
        if entry.path != path {
            return Err(QueryCacheError::UnsafeRoot(
                "cache index path changed unexpectedly".to_owned(),
            ));
        }
        match fs::remove_file(&entry.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        remove_state_entry(&mut state, digest, entry.bytes)
    }
}

fn prepare_root(root: &Path) -> Result<(), QueryCacheError> {
    if root.exists() {
        let metadata = fs::symlink_metadata(root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(QueryCacheError::UnsafeRoot(
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
            return Err(QueryCacheError::UnsafeRoot(
                "root marker is invalid".to_owned(),
            ));
        }
    } else {
        if fs::read_dir(root)?.next().transpose()?.is_some() {
            return Err(QueryCacheError::UnsafeRoot(
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

fn validate_file_header(
    path: &Path,
    digest: &str,
    max_entry_bytes: u64,
) -> Result<u64, QueryCacheError> {
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
) -> Result<[u8; CACHE_HEADER_BYTES], QueryCacheError> {
    if expected_bytes > max_entry_bytes || expected_bytes <= CACHE_HEADER_BYTES_U64 {
        return Err(QueryCacheError::EntryTooLarge);
    }
    let mut header = [0_u8; CACHE_HEADER_BYTES];
    file.read_exact(&mut header)?;
    if &header[..8] != CACHE_MAGIC || header[8..40] != decode_digest(digest)? {
        return Err(QueryCacheError::InvalidKey);
    }
    let payload_bytes = u64::from_be_bytes(
        header[40..48]
            .try_into()
            .map_err(|_| QueryCacheError::InvalidKey)?,
    );
    let total = payload_bytes
        .checked_add(CACHE_HEADER_BYTES_U64)
        .ok_or(QueryCacheError::AccountingOverflow)?;
    if total != expected_bytes {
        return Err(QueryCacheError::InvalidKey);
    }
    Ok(header)
}

fn read_verified_mmap(
    file: &mut File,
    digest: &str,
    expected_bytes: u64,
    max_entry_bytes: u64,
) -> Result<Bytes, QueryCacheError> {
    if file.metadata()?.len() != expected_bytes {
        return Err(QueryCacheError::InvalidKey);
    }
    let length = usize::try_from(expected_bytes).map_err(|_| QueryCacheError::EntryTooLarge)?;
    let mut map = MmapMut::map_anon(length)?;
    file.read_exact(&mut map)?;
    let map = map.make_read_only()?;
    let header = validate_header_bytes(&map, digest, expected_bytes, max_entry_bytes)?;
    let payload = &map[CACHE_HEADER_BYTES..];
    if <[u8; 32]>::from(Sha256::digest(payload)) != header[48..80] {
        return Err(QueryCacheError::InvalidKey);
    }
    Ok(Bytes::from_owner(map).slice(CACHE_HEADER_BYTES..))
}

fn validate_header_bytes(
    bytes: &[u8],
    digest: &str,
    expected_bytes: u64,
    max_entry_bytes: u64,
) -> Result<[u8; CACHE_HEADER_BYTES], QueryCacheError> {
    if bytes.len() < CACHE_HEADER_BYTES {
        return Err(QueryCacheError::InvalidKey);
    }
    let mut header = [0_u8; CACHE_HEADER_BYTES];
    header.copy_from_slice(&bytes[..CACHE_HEADER_BYTES]);
    if expected_bytes > max_entry_bytes
        || &header[..8] != CACHE_MAGIC
        || header[8..40] != decode_digest(digest)?
    {
        return Err(QueryCacheError::InvalidKey);
    }
    let payload_bytes = u64::from_be_bytes(
        header[40..48]
            .try_into()
            .map_err(|_| QueryCacheError::InvalidKey)?,
    );
    if payload_bytes.checked_add(CACHE_HEADER_BYTES_U64) != Some(expected_bytes) {
        return Err(QueryCacheError::InvalidKey);
    }
    Ok(header)
}

fn evict_for_insert(
    state: &mut CacheState,
    incoming_bytes: u64,
    max_bytes: u64,
    max_entries: usize,
) -> Result<(), QueryCacheError> {
    if incoming_bytes > max_bytes {
        return Err(QueryCacheError::CapacityTooSmall);
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

fn evict_one(state: &mut CacheState) -> Result<(), QueryCacheError> {
    let digest = state
        .least_to_most_recent
        .front()
        .cloned()
        .ok_or(QueryCacheError::AccountingOverflow)?;
    let entry = state
        .entries
        .get(&digest)
        .cloned()
        .ok_or(QueryCacheError::AccountingOverflow)?;
    match fs::remove_file(&entry.path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    remove_state_entry(state, &digest, entry.bytes)
}

fn remove_state_entry(
    state: &mut CacheState,
    digest: &str,
    bytes: u64,
) -> Result<(), QueryCacheError> {
    state.entries.remove(digest);
    state.least_to_most_recent.retain(|value| value != digest);
    state.bytes = state
        .bytes
        .checked_sub(bytes)
        .ok_or(QueryCacheError::AccountingOverflow)?;
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

fn touch(order: &mut VecDeque<String>, digest: &str) {
    order.retain(|value| value != digest);
    order.push_back(digest.to_owned());
}

fn update_hex(hasher: &mut Sha256, value: &str) -> Result<(), QueryCacheError> {
    hasher.update(decode_digest(value)?);
    Ok(())
}

fn decode_digest(value: &str) -> Result<[u8; 32], QueryCacheError> {
    hex::decode(value)
        .ok()
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(QueryCacheError::InvalidKey)
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
        path::{Path, PathBuf},
    };

    use bytes::Bytes;

    use super::{QueryCacheError, QueryCacheKey, QueryCacheLookup, QueryResultCache};
    use uuid::Uuid;

    fn root() -> PathBuf {
        std::env::temp_dir().join(format!("ngkg-query-cache-test-{}", Uuid::new_v4()))
    }

    fn key(hydrate: bool) -> QueryCacheKey {
        QueryCacheKey {
            tenant_id: Uuid::from_u128(1),
            dataset_id: Uuid::from_u128(2),
            snapshot_id: Uuid::from_u128(3),
            manifest_sha256: "1".repeat(64),
            serving_root_sha256: "2".repeat(64),
            query_sha256: "3".repeat(64),
            authorized_graph_set_sha256: "4".repeat(64),
            active_dataset_sha256: "5".repeat(64),
            dataset_selection_source: 0,
            hydrate,
        }
    }

    fn remove_root(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
        if root.exists() {
            fs::remove_dir_all(root)?;
        }
        Ok(())
    }

    #[test]
    fn verified_mmap_round_trip_and_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let cache = QueryResultCache::open(&root, 4096, 4, 2048)?;
        assert_eq!(cache.get(&key(false))?, QueryCacheLookup::Miss);
        cache.insert(&key(false), br#"{"complete":true}"#)?;
        assert_eq!(
            cache.get(&key(false))?,
            QueryCacheLookup::Hit(Bytes::from_static(br#"{"complete":true}"#))
        );
        drop(cache);
        let reopened = QueryResultCache::open(&root, 4096, 4, 2048)?;
        assert!(matches!(
            reopened.get(&key(false))?,
            QueryCacheLookup::Hit(_)
        ));
        remove_root(&root)?;
        Ok(())
    }

    #[test]
    fn hydrate_identity_and_every_snapshot_root_field_change_key() -> Result<(), QueryCacheError> {
        let original = key(false);
        let digest = original.digest()?;
        let variants = [
            key(true),
            QueryCacheKey {
                tenant_id: Uuid::from_u128(9),
                ..original.clone()
            },
            QueryCacheKey {
                dataset_id: Uuid::from_u128(9),
                ..original.clone()
            },
            QueryCacheKey {
                snapshot_id: Uuid::from_u128(9),
                ..original.clone()
            },
            QueryCacheKey {
                manifest_sha256: "4".repeat(64),
                ..original.clone()
            },
            QueryCacheKey {
                serving_root_sha256: "5".repeat(64),
                ..original.clone()
            },
            QueryCacheKey {
                query_sha256: "6".repeat(64),
                ..original.clone()
            },
            QueryCacheKey {
                authorized_graph_set_sha256: "7".repeat(64),
                ..original.clone()
            },
            QueryCacheKey {
                active_dataset_sha256: "8".repeat(64),
                ..original.clone()
            },
            QueryCacheKey {
                dataset_selection_source: 2,
                ..original
            },
        ];
        for variant in variants {
            assert_ne!(variant.digest()?, digest);
        }
        Ok(())
    }

    #[test]
    fn corruption_is_removed_and_never_served() -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let cache = QueryResultCache::open(&root, 4096, 4, 2048)?;
        cache.insert(&key(false), b"exact")?;
        let path = root.join(format!("{}.cache", key(false).digest()?));
        OpenOptions::new()
            .append(true)
            .open(path)?
            .write_all(b"corrupt")?;
        assert_eq!(cache.get(&key(false))?, QueryCacheLookup::Miss);
        assert_eq!(cache.usage()?, (0, 0));
        remove_root(&root)?;
        Ok(())
    }

    #[test]
    fn lru_bounds_are_enforced() -> Result<(), Box<dyn std::error::Error>> {
        let root = root();
        let cache = QueryResultCache::open(&root, 256, 2, 128)?;
        let mut first = key(false);
        cache.insert(&first, b"one")?;
        first.query_sha256 = "4".repeat(64);
        cache.insert(&first, b"two")?;
        let _hit = cache.get(&key(false))?;
        first.query_sha256 = "5".repeat(64);
        cache.insert(&first, b"three")?;
        assert_eq!(cache.usage()?.0, 2);
        remove_root(&root)?;
        Ok(())
    }
}
