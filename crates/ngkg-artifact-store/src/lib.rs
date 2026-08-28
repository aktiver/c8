//! Checksum-bound local/S3 object access without broad storage discovery.

use std::{
    path::Path as FsPath,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use futures::StreamExt;
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    aws::AmazonS3Builder, buffered::BufWriter, local::LocalFileSystem, path::Path,
};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::io::{AsyncWriteExt, BufWriter as TokioBufWriter};
use url::Url;

/// Storage errors are explicit so integrity failures never become cache misses.
#[derive(Debug, Error)]
pub enum ArtifactStoreError {
    /// The configured base URL is unsupported or unsafe.
    #[error("artifact base URL is invalid: {0}")]
    BaseUrl(String),
    /// An object key is not a normalized relative path.
    #[error("object key is unsafe: {0}")]
    UnsafeKey(String),
    /// SHA-256 text is malformed.
    #[error("expected SHA-256 must be 64 lowercase hexadecimal characters")]
    InvalidSha256,
    /// Object metadata or content exceeded its declared bound.
    #[error("object {key} exceeds the byte limit {limit}")]
    SizeLimit { key: String, limit: u64 },
    /// Concurrent staging would exceed the shared attempt budget.
    #[error("concurrent object materialization exceeds the aggregate byte limit {limit}")]
    AggregateSizeLimit { limit: u64 },
    /// The retrieved or published bytes are not the immutable expected object.
    #[error("object {key} checksum mismatch")]
    ChecksumMismatch { key: String },
    /// An existing content-addressed object differs from the requested bytes.
    #[error("immutable object {0} already exists with different content")]
    ImmutableConflict(String),
    /// Local staging failed.
    #[error("local artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The configured object-store backend failed.
    #[error("object-store operation failed: {0}")]
    Store(#[from] ObjectStoreError),
}

/// One operator-configured bucket/filesystem root plus an optional key prefix.
#[derive(Clone)]
pub struct ArtifactStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    base_url: Url,
}

impl ArtifactStore {
    /// Open `file:///absolute/root[/prefix]` or `s3://bucket[/prefix]`.
    ///
    /// S3 credentials, region, workload identity and an optional compatible endpoint
    /// are read by `AmazonS3Builder::from_env`; uploaded manifests cannot change them.
    pub fn from_base_url(value: &str) -> Result<Self, ArtifactStoreError> {
        let url = Url::parse(value).map_err(|error| ArtifactStoreError::BaseUrl(error.to_string()))?;
        if url.query().is_some() || url.fragment().is_some() || !url.username().is_empty() || url.password().is_some() {
            return Err(ArtifactStoreError::BaseUrl(
                "credentials, query strings and fragments are forbidden".to_owned(),
            ));
        }
        match url.scheme() {
            "file" => {
                let root = url
                    .to_file_path()
                    .map_err(|()| ArtifactStoreError::BaseUrl("file URL must contain an absolute local path".to_owned()))?;
                let store = LocalFileSystem::new_with_prefix(root)
                    .map_err(ArtifactStoreError::Store)?;
                Ok(Self { store: Arc::new(store), prefix: String::new(), base_url: url })
            }
            "s3" => {
                let bucket = url
                    .host_str()
                    .filter(|bucket| !bucket.is_empty())
                    .ok_or_else(|| ArtifactStoreError::BaseUrl("S3 URL requires a bucket".to_owned()))?;
                let prefix = url.path().trim_matches('/').to_owned();
                validate_optional_prefix(&prefix)?;
                let store = AmazonS3Builder::from_env().with_bucket_name(bucket).build()?;
                Ok(Self { store: Arc::new(store), prefix, base_url: url })
            }
            scheme => Err(ArtifactStoreError::BaseUrl(format!(
                "unsupported scheme {scheme}; only file and s3 are accepted"
            ))),
        }
    }

    /// Return the configured root for audit metadata; no credentials are embedded.
    #[must_use]
    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    /// Fetch one exact object to a new local path while hashing its stream.
    pub async fn materialize_verified(
        &self,
        key: &str,
        expected_sha256: &str,
        max_bytes: u64,
        destination: &FsPath,
    ) -> Result<u64, ArtifactStoreError> {
        self.materialize_verified_inner(key, expected_sha256, max_bytes, destination, None)
            .await
    }

    /// Fetch an operator-derived immutable key with a strict byte ceiling.
    ///
    /// This is reserved for completion manifests whose content digest cannot be known
    /// before the worker runs. Callers must validate every embedded identity against a
    /// checksum-bound plan and publish a checksum-bound aggregate before consumption.
    pub async fn materialize_unverified_bounded(
        &self,
        key: &str,
        max_bytes: u64,
        destination: &FsPath,
    ) -> Result<u64, ArtifactStoreError> {
        let location = self.location(key)?;
        let metadata = self.store.head(&location).await?;
        if metadata.size > max_bytes {
            return Err(ArtifactStoreError::SizeLimit {
                key: key.to_owned(),
                limit: max_bytes,
            });
        }
        let parent = destination.parent().ok_or_else(|| {
            ArtifactStoreError::Io(std::io::Error::other("artifact destination has no parent"))
        })?;
        tokio::fs::create_dir_all(parent).await?;
        let mut writer = TokioBufWriter::new(
            tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(destination)
                .await?,
        );
        let mut stream = self.store.get(&location).await?.into_stream();
        let mut observed = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            observed = observed
                .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                    ArtifactStoreError::SizeLimit {
                        key: key.to_owned(),
                        limit: max_bytes,
                    }
                })?)
                .ok_or_else(|| ArtifactStoreError::SizeLimit {
                    key: key.to_owned(),
                    limit: max_bytes,
                })?;
            if observed > max_bytes {
                return Err(ArtifactStoreError::SizeLimit {
                    key: key.to_owned(),
                    limit: max_bytes,
                });
            }
            writer.write_all(&chunk).await?;
        }
        writer.flush().await?;
        writer.get_ref().sync_all().await?;
        if observed != metadata.size {
            return Err(ArtifactStoreError::ChecksumMismatch {
                key: key.to_owned(),
            });
        }
        Ok(observed)
    }

    /// Verify that one immutable remote object exists, is bounded, and has the expected digest.
    pub async fn verify_remote(
        &self,
        key: &str,
        expected_sha256: &str,
        max_bytes: u64,
    ) -> Result<u64, ArtifactStoreError> {
        let expected = decode_sha256(expected_sha256)?;
        let location = self.location(key)?;
        let metadata = self.store.head(&location).await?;
        if metadata.size > max_bytes {
            return Err(ArtifactStoreError::SizeLimit {
                key: key.to_owned(),
                limit: max_bytes,
            });
        }
        if self.hash_remote(&location).await? != expected {
            return Err(ArtifactStoreError::ChecksumMismatch {
                key: key.to_owned(),
            });
        }
        Ok(metadata.size)
    }

    /// Fetch one exact object while reserving its declared size from a shared budget.
    pub async fn materialize_verified_with_budget(
        &self,
        key: &str,
        expected_sha256: &str,
        max_bytes: u64,
        destination: &FsPath,
        aggregate_bytes: Arc<AtomicU64>,
        aggregate_limit: u64,
    ) -> Result<u64, ArtifactStoreError> {
        self.materialize_verified_inner(
            key,
            expected_sha256,
            max_bytes,
            destination,
            Some((aggregate_bytes, aggregate_limit)),
        )
        .await
    }

    async fn materialize_verified_inner(
        &self,
        key: &str,
        expected_sha256: &str,
        max_bytes: u64,
        destination: &FsPath,
        aggregate_budget: Option<(Arc<AtomicU64>, u64)>,
    ) -> Result<u64, ArtifactStoreError> {
        let expected = decode_sha256(expected_sha256)?;
        let location = self.location(key)?;
        let metadata = self.store.head(&location).await?;
        if metadata.size > max_bytes {
            return Err(ArtifactStoreError::SizeLimit { key: key.to_owned(), limit: max_bytes });
        }
        if let Some((observed, limit)) = aggregate_budget {
            observed
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current
                        .checked_add(metadata.size)
                        .filter(|next| *next <= limit)
                })
                .map_err(|_| ArtifactStoreError::AggregateSizeLimit { limit })?;
        }
        let parent = destination.parent().ok_or_else(|| {
            ArtifactStoreError::Io(std::io::Error::other("artifact destination has no parent"))
        })?;
        tokio::fs::create_dir_all(parent).await?;
        let file_name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ArtifactStoreError::Io(std::io::Error::other("artifact destination has no UTF-8 name")))?;
        let temporary = parent.join(format!(".{file_name}.partial"));
        let file = tokio::fs::OpenOptions::new().create_new(true).write(true).open(&temporary).await?;
        let mut writer = TokioBufWriter::new(file);
        let mut stream = self.store.get(&location).await?.into_stream();
        let mut observed_bytes = 0_u64;
        let mut hasher = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            observed_bytes = observed_bytes
                .checked_add(u64::try_from(chunk.len()).map_err(|_| {
                    ArtifactStoreError::SizeLimit { key: key.to_owned(), limit: max_bytes }
                })?)
                .ok_or_else(|| ArtifactStoreError::SizeLimit { key: key.to_owned(), limit: max_bytes })?;
            if observed_bytes > max_bytes {
                return Err(ArtifactStoreError::SizeLimit { key: key.to_owned(), limit: max_bytes });
            }
            hasher.update(&chunk);
            writer.write_all(&chunk).await?;
        }
        writer.flush().await?;
        writer.get_ref().sync_all().await?;
        drop(writer);
        if observed_bytes != metadata.size || hasher.finalize().as_slice() != expected {
            let _ = tokio::fs::remove_file(&temporary).await;
            return Err(ArtifactStoreError::ChecksumMismatch { key: key.to_owned() });
        }
        tokio::fs::rename(&temporary, destination).await?;
        sync_directory(parent)?;
        Ok(observed_bytes)
    }

    /// Publish a checksum-addressed local file and verify the stored bytes.
    ///
    /// Small objects use a conditional create. Large objects use bounded multipart
    /// streaming; production IAM must deny overwrite of the immutable snapshot prefix.
    pub async fn put_file_immutable(
        &self,
        key: &str,
        expected_sha256: &str,
        source: &FsPath,
        single_put_max_bytes: u64,
        multipart_buffer_bytes: usize,
        multipart_concurrency: usize,
    ) -> Result<u64, ArtifactStoreError> {
        let expected = decode_sha256(expected_sha256)?;
        let source_size = tokio::fs::metadata(source).await?.len();
        let location = self.location(key)?;
        match self.store.head(&location).await {
            Ok(metadata) => {
                if metadata.size != source_size || self.hash_remote(&location).await? != expected {
                    return Err(ArtifactStoreError::ImmutableConflict(key.to_owned()));
                }
                return Ok(source_size);
            }
            Err(ObjectStoreError::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }
        if source_size <= single_put_max_bytes {
            let content = tokio::fs::read(source).await?;
            match self
                .store
                .put_opts(
                    &location,
                    Bytes::from(content).into(),
                    PutOptions { mode: PutMode::Create, ..PutOptions::default() },
                )
                .await
            {
                Ok(_) | Err(ObjectStoreError::AlreadyExists { .. }) => {}
                Err(error) => return Err(error.into()),
            }
        } else {
            if multipart_buffer_bytes == 0 || multipart_concurrency == 0 {
                return Err(ArtifactStoreError::BaseUrl(
                    "multipart buffer and concurrency must be positive".to_owned(),
                ));
            }
            let mut source_file = tokio::fs::File::open(source).await?;
            let mut writer = BufWriter::with_capacity(Arc::clone(&self.store), location.clone(), multipart_buffer_bytes)
                .with_max_concurrency(multipart_concurrency);
            tokio::io::copy(&mut source_file, &mut writer).await?;
            writer.shutdown().await?;
        }
        let metadata = self.store.head(&location).await?;
        if metadata.size != source_size || self.hash_remote(&location).await? != expected {
            return Err(ArtifactStoreError::ChecksumMismatch { key: key.to_owned() });
        }
        Ok(source_size)
    }

    fn location(&self, key: &str) -> Result<Path, ArtifactStoreError> {
        validate_key(key)?;
        let joined = if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.prefix)
        };
        if joined.len() > 1024 {
            return Err(ArtifactStoreError::UnsafeKey(
                "prefixed object key exceeds 1024 ASCII bytes".to_owned(),
            ));
        }
        Path::parse(joined).map_err(|error| ArtifactStoreError::UnsafeKey(error.to_string()))
    }

    async fn hash_remote(&self, location: &Path) -> Result<[u8; 32], ArtifactStoreError> {
        let mut hasher = Sha256::new();
        let mut stream = self.store.get(location).await?.into_stream();
        while let Some(chunk) = stream.next().await {
            hasher.update(&chunk?);
        }
        Ok(hasher.finalize().into())
    }
}

fn validate_optional_prefix(value: &str) -> Result<(), ArtifactStoreError> {
    if value.is_empty() {
        Ok(())
    } else {
        validate_key(value)
    }
}

fn validate_key(value: &str) -> Result<(), ArtifactStoreError> {
    if value.is_empty()
        || value.len() > 1024
        || value.starts_with('/')
        || value.ends_with('/')
        || value.contains('\\')
        || value.split('/').any(|segment| {
            if segment.len() > 255 {
                return true;
            }
            let mut bytes = segment.bytes();
            !bytes
                .next()
                .is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                || bytes.any(|byte| {
                    !(byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
                })
        })
    {
        return Err(ArtifactStoreError::UnsafeKey(value.to_owned()));
    }
    Ok(())
}

fn decode_sha256(value: &str) -> Result<[u8; 32], ArtifactStoreError> {
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(ArtifactStoreError::InvalidSha256);
    }
    let bytes = hex::decode(value).map_err(|_| ArtifactStoreError::InvalidSha256)?;
    let mut output = [0_u8; 32];
    output.copy_from_slice(&bytes);
    Ok(output)
}

fn sync_directory(path: &FsPath) -> Result<(), ArtifactStoreError> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, atomic::AtomicU64},
        time::{SystemTime, UNIX_EPOCH},
    };

    use sha2::{Digest, Sha256};

    use super::{ArtifactStore, ArtifactStoreError, validate_key};

    #[test]
    fn rejects_escaping_object_keys() {
        for key in ["", "/absolute", "a/../b", "a//b", "a\\b", "a/./b"] {
            assert!(validate_key(key).is_err(), "accepted unsafe key {key}");
        }
        for key in [".hidden", "a/.hidden", "a space/b", "a/@b"] {
            assert!(validate_key(key).is_err(), "accepted non-normalized key {key}");
        }
        assert!(validate_key("bundles/sha256/example.json").is_ok());
    }

    #[test]
    fn rejects_network_and_credential_bearing_base_urls() {
        assert!(matches!(
            ArtifactStore::from_base_url("https://example.invalid/data"),
            Err(ArtifactStoreError::BaseUrl(_))
        ));
        assert!(ArtifactStore::from_base_url("s3://user:secret@bucket/prefix").is_err());
    }

    #[tokio::test]
    async fn local_store_round_trip_is_checksum_bound_and_budgeted(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            ?
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "ngkg-artifact-store-test-{}-{nonce}",
            std::process::id()
        ));
        let object_root = root.join("objects");
        std::fs::create_dir_all(&object_root)?;
        let source = root.join("source.bin");
        let bytes = b"immutable-ngkg-artifact";
        std::fs::write(&source, bytes)?;
        let checksum = hex::encode(Sha256::digest(bytes));
        let store = ArtifactStore::from_base_url(
            &url::Url::from_directory_path(&object_root)
                .map_err(|()| std::io::Error::other("temporary path is not absolute"))?
                .to_string(),
        )
        ?;
        store
            .put_file_immutable("inputs/source.bin", &checksum, &source, 1024, 1024, 1)
            .await?;
        let destination = root.join("staged/source.bin");
        let observed = store
            .materialize_verified_with_budget(
                "inputs/source.bin",
                &checksum,
                1024,
                &destination,
                Arc::new(AtomicU64::new(0)),
                u64::try_from(bytes.len())?,
            )
            .await?;
        assert_eq!(observed, u64::try_from(bytes.len())?);
        assert_eq!(std::fs::read(destination)?, bytes);

        let result = store
            .materialize_verified_with_budget(
                "inputs/source.bin",
                &checksum,
                1024,
                &root.join("rejected/source.bin"),
                Arc::new(AtomicU64::new(0)),
                1,
            )
            .await;
        assert!(matches!(
            result,
            Err(ArtifactStoreError::AggregateSizeLimit { limit: 1 })
        ));
        std::fs::remove_dir_all(root)?;
        Ok(())
    }
}
