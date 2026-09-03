use std::{env, path::PathBuf, sync::Arc};

use bytes::Bytes;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum ObjectStoreConfiguration {
    S3 { bucket: String },
    Azure { container: String },
    Gcs { bucket: String },
    LocalTest { root: PathBuf },
}

impl ObjectStoreConfiguration {
    pub fn from_environment() -> Result<Self, StorageError> {
        match required("NGKG_INPUT_OBJECT_STORE")?.as_str() {
            "s3" => Ok(Self::S3 {
                bucket: required("NGKG_INPUT_S3_BUCKET")?,
            }),
            "azure" => Ok(Self::Azure {
                container: required("NGKG_INPUT_AZURE_CONTAINER")?,
            }),
            "gcs" => Ok(Self::Gcs {
                bucket: required("NGKG_INPUT_GCS_BUCKET")?,
            }),
            "local-test" => Ok(Self::LocalTest {
                root: PathBuf::from(required("NGKG_INPUT_LOCAL_ROOT")?),
            }),
            _ => Err(StorageError::Configuration(
                "NGKG_INPUT_OBJECT_STORE must be s3, azure, gcs, or local-test",
            )),
        }
    }
}

#[derive(Clone)]
pub struct InputObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl InputObjectStore {
    pub fn build(configuration: ObjectStoreConfiguration) -> Result<Self, StorageError> {
        let inner: Arc<dyn ObjectStore> = match configuration {
            ObjectStoreConfiguration::S3 { bucket } => Arc::new(
                object_store::aws::AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()?,
            ),
            ObjectStoreConfiguration::Azure { container } => Arc::new(
                object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(container)
                    .build()?,
            ),
            ObjectStoreConfiguration::Gcs { bucket } => Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()?,
            ),
            ObjectStoreConfiguration::LocalTest { root } => {
                Arc::new(object_store::local::LocalFileSystem::new_with_prefix(root)?)
            }
        };
        Ok(Self { inner })
    }

    /// Create an immutable part object. The caller supplies the verified digest;
    /// the object key is deterministic and never returned to a model.
    pub async fn put_part(
        &self,
        tenant_id: Uuid,
        input_id: Uuid,
        ordinal: u32,
        digest: &str,
        bytes: Bytes,
    ) -> Result<String, StorageError> {
        if sha256(&bytes) != digest {
            return Err(StorageError::Checksum);
        }
        let reference =
            format!("tenants/{tenant_id}/agent-inputs/{input_id}/parts/{ordinal:010}-{digest}");
        let path = Path::parse(&reference)?;
        match self.inner.get(&path).await {
            Ok(existing) => {
                let observed = existing.bytes().await?;
                if sha256(&observed) != digest {
                    return Err(StorageError::Collision);
                }
            }
            Err(object_store::Error::NotFound { .. }) => {
                self.inner.put(&path, bytes.into()).await?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(reference)
    }

    pub async fn get_verified(
        &self,
        reference: &str,
        expected_sha256: &str,
        maximum_bytes: usize,
    ) -> Result<Bytes, StorageError> {
        let path = Path::parse(reference)?;
        let result = self.inner.get(&path).await?;
        let size = result.meta.size;
        if size > u64::try_from(maximum_bytes).unwrap_or(u64::MAX) {
            return Err(StorageError::Limit);
        }
        let bytes = result.bytes().await?;
        if sha256(&bytes) != expected_sha256 {
            return Err(StorageError::Checksum);
        }
        Ok(bytes)
    }
}

fn required(name: &'static str) -> Result<String, StorageError> {
    env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or(StorageError::Configuration(name))
}
fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object-store configuration is invalid: {0}")]
    Configuration(&'static str),
    #[error("object-store operation failed")]
    Store(#[from] object_store::Error),
    #[error("object path is invalid")]
    Path(#[from] object_store::path::Error),
    #[error("object checksum does not match")]
    Checksum,
    #[error("content-addressed object collision")]
    Collision,
    #[error("object exceeds configured limit")]
    Limit,
}
