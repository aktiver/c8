use std::{env, path::PathBuf, sync::Arc};

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::{SliceError, sha256};

#[derive(Clone, Debug)]
pub enum ContextStoreConfiguration {
    S3 { bucket: String },
    Azure { container: String },
    Gcs { bucket: String },
    LocalTest { root: PathBuf },
}

impl ContextStoreConfiguration {
    pub fn from_environment() -> Result<Self, SliceError> {
        match required("NGKG_CONTEXT_OBJECT_STORE")?.as_str() {
            "s3" => Ok(Self::S3 {
                bucket: required("NGKG_CONTEXT_S3_BUCKET")?,
            }),
            "azure" => Ok(Self::Azure {
                container: required("NGKG_CONTEXT_AZURE_CONTAINER")?,
            }),
            "gcs" => Ok(Self::Gcs {
                bucket: required("NGKG_CONTEXT_GCS_BUCKET")?,
            }),
            "local-test" => Ok(Self::LocalTest {
                root: PathBuf::from(required("NGKG_CONTEXT_LOCAL_ROOT")?),
            }),
            _ => Err(SliceError::Configuration("NGKG_CONTEXT_OBJECT_STORE")),
        }
    }
}

#[derive(Clone)]
pub struct ContextObjectStore {
    inner: Arc<dyn ObjectStore>,
    hash_lanes: Arc<Semaphore>,
}

impl ContextObjectStore {
    pub fn build(
        configuration: ContextStoreConfiguration,
        hash_tasks: usize,
    ) -> Result<Self, SliceError> {
        let inner: Arc<dyn ObjectStore> = match configuration {
            ContextStoreConfiguration::S3 { bucket } => Arc::new(
                object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()?,
            ),
            ContextStoreConfiguration::Azure { container } => Arc::new(
                object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(container)
                    .build()?,
            ),
            ContextStoreConfiguration::Gcs { bucket } => Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()?,
            ),
            ContextStoreConfiguration::LocalTest { root } => {
                Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root)?)
            }
        };
        if hash_tasks == 0 || hash_tasks > 1024 {
            return Err(SliceError::Configuration("NGKG_CONTEXT_MAX_HASH_TASKS"));
        }
        Ok(Self {
            inner,
            hash_lanes: Arc::new(Semaphore::new(hash_tasks)),
        })
    }

    pub async fn put_chunk(
        &self,
        tenant_id: Uuid,
        slice_id: Uuid,
        ordinal: u32,
        expected_sha256: &str,
        bytes: Bytes,
    ) -> Result<String, SliceError> {
        if self.digest_bytes(bytes.clone()).await? != expected_sha256 {
            return Err(SliceError::Checksum);
        }
        let reference = Self::chunk_reference(tenant_id, slice_id, ordinal, expected_sha256)?;
        self.put_immutable(&reference, expected_sha256, bytes)
            .await?;
        Ok(reference)
    }

    pub fn chunk_reference(
        tenant_id: Uuid,
        slice_id: Uuid,
        ordinal: u32,
        digest: &str,
    ) -> Result<String, SliceError> {
        if !crate::valid_hash(digest) {
            return Err(SliceError::Invalid("chunk hash"));
        }
        Ok(format!(
            "tenants/{tenant_id}/context-slices/{slice_id}/chunks/{ordinal:010}-{digest}"
        ))
    }

    pub async fn put_index(
        &self,
        tenant_id: Uuid,
        slice_id: Uuid,
        bytes: Bytes,
    ) -> Result<(String, String), SliceError> {
        let digest = self.digest_bytes(bytes.clone()).await?;
        let reference = format!("tenants/{tenant_id}/context-slices/{slice_id}/indexes/{digest}");
        self.put_immutable(&reference, &digest, bytes).await?;
        Ok((reference, digest))
    }

    pub async fn put_manifest(
        &self,
        tenant_id: Uuid,
        slice_id: Uuid,
        digest: &str,
        bytes: Bytes,
    ) -> Result<String, SliceError> {
        if self.digest_bytes(bytes.clone()).await? != digest {
            return Err(SliceError::Checksum);
        }
        let reference = format!("tenants/{tenant_id}/context-slices/{slice_id}/manifests/{digest}");
        self.put_immutable(&reference, digest, bytes).await?;
        Ok(reference)
    }

    async fn put_immutable(
        &self,
        reference: &str,
        digest: &str,
        bytes: Bytes,
    ) -> Result<(), SliceError> {
        let path = Path::parse(reference)?;
        match self.inner.get(&path).await {
            Ok(existing) => {
                if existing.meta.size
                    != u64::try_from(bytes.len()).map_err(|_| SliceError::Limit)?
                {
                    return Err(SliceError::Integrity("content-address length collision"));
                }
                let observed = existing.bytes().await?;
                if self.digest_bytes(observed).await? != digest {
                    return Err(SliceError::Integrity("content-address collision"));
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                self.inner.put(&path, bytes.into()).await?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    pub async fn get_verified(
        &self,
        reference: &str,
        expected_sha256: &str,
        maximum_bytes: usize,
    ) -> Result<Bytes, SliceError> {
        let result = self.inner.get(&Path::parse(reference)?).await?;
        if result.meta.size > u64::try_from(maximum_bytes).map_err(|_| SliceError::Limit)? {
            return Err(SliceError::Limit);
        }
        let bytes = result.bytes().await?;
        if self.digest_bytes(bytes.clone()).await? != expected_sha256 {
            return Err(SliceError::Checksum);
        }
        Ok(bytes)
    }

    pub async fn delete(&self, reference: &str) -> Result<(), SliceError> {
        match self.inner.delete(&Path::parse(reference)?).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn digest_bytes(&self, bytes: Bytes) -> Result<String, SliceError> {
        let _permit = Arc::clone(&self.hash_lanes)
            .acquire_owned()
            .await
            .map_err(|_| SliceError::ComputeTask)?;
        tokio::task::spawn_blocking(move || sha256(&bytes))
            .await
            .map_err(|_| SliceError::ComputeTask)
    }
}

fn required(name: &'static str) -> Result<String, SliceError> {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(SliceError::Configuration(name))
}
