use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufRead, BufReader},
    path::{Component, Path, PathBuf},
    sync::{Arc, atomic::AtomicU64},
};

use futures::{StreamExt, stream};
use ngkg_artifact_store::{ArtifactStore, ArtifactStoreError};
use ngkg_catalog::{
    CatalogError, DistributedWorkKind, JobState, OperationRepository, ServingCertification,
};
use ngkg_distributed_build::DistributedRootManifest;
use ngkg_distributed_artifacts::DistributedArtifactRootManifest;
use ngkg_hydration::{
    HydratedShardRow, ServingEquivalenceReport, ServingQueryEquivalence, ServingRootManifest,
    ShardedQualifiedGuid, VerifiedPayloadShard, hydrate_sharded_payload,
    verify_payload_shard,
};
use ngkg_identity::guid_for_canonical_iri;
use ngkg_locator::MmapLocatorIndex;
use ngkg_reference::{
    ArtifactRecord, CertifiedQueryInput, CompilationBundle, HydratedPayload, InputArtifact,
    ObjectArtifact, ReferenceCompileManifest, ReferenceSnapshotManifest, TrustedReasonerConfig,
    TrustedResourceCeilings, compile_from_manifest, execute_snapshot_query, sha256_path,
};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub(crate) enum ObjectCompileError {
    #[error("worker configuration is invalid: {0}")]
    Config(String),
    #[error("catalog access failed: {0}")]
    Catalog(#[from] CatalogError),
    #[error("object storage failed: {0}")]
    Store(#[from] ArtifactStoreError),
    #[error("compilation bundle is invalid: {0}")]
    Bundle(String),
    #[error("local staging failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("reference compilation failed: {0}")]
    Reference(String),
    #[error("blocking compiler task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("PostgreSQL connection failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("distributed hydration failed: {0}")]
    Hydration(#[from] ngkg_hydration::HydrationError),
    #[error("distributed locator failed: {0}")]
    Locator(#[from] ngkg_locator::LocatorError),
    #[error("distributed identity failed: {0}")]
    Identity(#[from] ngkg_identity::IdentityError),
}

struct ServingInputs {
    manifest: ServingRootManifest,
    manifest_sha256: String,
    binary_locator_path: PathBuf,
    dictionary_path: PathBuf,
}

struct QueryHydrationCase {
    query_id: String,
    query_sha256: String,
    reference: Vec<HydratedPayload>,
    qualified: Vec<ShardedQualifiedGuid>,
    guid_to_iri: BTreeMap<Uuid, String>,
}

struct LocalServingCertification {
    relative_path: String,
    sha256: String,
    certified_query_count: i32,
    hydrated_row_count: i64,
}

impl ObjectCompileError {
    pub(crate) const fn deterministic(&self) -> bool {
        matches!(
            self,
            Self::Bundle(_)
                | Self::Reference(_)
                | Self::Hydration(_)
                | Self::Locator(_)
                | Self::Identity(_)
        )
            || matches!(
                self,
                Self::Store(
                    ArtifactStoreError::UnsafeKey(_)
                        | ArtifactStoreError::InvalidSha256
                        | ArtifactStoreError::SizeLimit { .. }
                        | ArtifactStoreError::AggregateSizeLimit { .. }
                        | ArtifactStoreError::ChecksumMismatch { .. }
                        | ArtifactStoreError::ImmutableConflict(_)
                )
            )
    }
}

pub(crate) async fn compile_object_store(
    options: &BTreeMap<String, String>,
) -> Result<String, ObjectCompileError> {
    let tenant_id = uuid_option(options, "tenant-id")?;
    let operation_id = uuid_option(options, "operation-id")?;
    let dataset_id = uuid_option(options, "dataset-id")?;
    let target_snapshot_id = uuid_option(options, "target-snapshot-id")?;
    let bundle_object_key = value(options, "bundle-object-key")?;
    let bundle_sha256 = value(options, "bundle-sha256")?;
    let bundle_hash = decode_sha256(&bundle_sha256)?;
    let database_url = required_env("NGKG_DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await?;
    let catalog = OperationRepository::new(pool);
    let durable = catalog.get_compilation(tenant_id, operation_id).await?;
    if durable.operation.dataset_id != dataset_id
        || durable.operation.target_snapshot_id != target_snapshot_id
        || durable.request.bundle_object_key != bundle_object_key
        || durable.request.bundle_sha256 != bundle_hash
    {
        return Err(ObjectCompileError::Config(
            "worker arguments do not match the durable catalog request".to_owned(),
        ));
    }
    if durable.operation.state == JobState::Published {
        return Ok(serde_json::json!({
            "status": "already-published",
            "operationId": operation_id,
            "snapshotId": target_snapshot_id
        })
        .to_string());
    }
    if durable.operation.state == JobState::Certified {
        let snapshot = catalog
            .get_snapshot(tenant_id, dataset_id, target_snapshot_id)
            .await?;
        let manifest_hash = decode_sha256(&snapshot.manifest_sha256)?;
        let actor = format!("worker:{operation_id}");
        let outcome = catalog
            .commit_reference_certification(
                tenant_id,
                operation_id,
                &snapshot.manifest_object_key,
                &manifest_hash,
                &actor,
            )
            .await?;
        return Ok(serde_json::json!({
            "status": if outcome.published { "published" } else { "already-certified" },
            "operationId": operation_id,
            "snapshotId": target_snapshot_id,
            "automaticPublicationConflict": outcome.publication_conflict
        })
        .to_string());
    }
    let distributed_finalize = options.contains_key("distributed-root-object-key")
        || options.contains_key("distributed-root-sha256");
    let distributed_artifacts = options.contains_key("distributed-artifact-root-object-key")
        || options.contains_key("distributed-artifact-root-sha256");
    let distributed_serving = options.contains_key("distributed-serving-root-object-key")
        || options.contains_key("distributed-serving-root-sha256");
    if distributed_finalize
        && !(options.contains_key("distributed-root-object-key")
            && options.contains_key("distributed-root-sha256"))
    {
        return Err(ObjectCompileError::Config(
            "distributed root key and hash must be supplied together".to_owned(),
        ));
    }
    if distributed_artifacts
        && !(options.contains_key("distributed-artifact-root-object-key")
            && options.contains_key("distributed-artifact-root-sha256"))
    {
        return Err(ObjectCompileError::Config(
            "distributed artifact root key and hash must be supplied together".to_owned(),
        ));
    }
    if distributed_serving
        && !(options.contains_key("distributed-serving-root-object-key")
            && options.contains_key("distributed-serving-root-sha256"))
    {
        return Err(ObjectCompileError::Config(
            "distributed serving root key and hash must be supplied together".to_owned(),
        ));
    }
    if distributed_finalize != distributed_artifacts
        || distributed_artifacts != distributed_serving
    {
        return Err(ObjectCompileError::Config(
            "distributed source, artifact and serving roots must be supplied together"
                .to_owned(),
        ));
    }
    let executable_state = if distributed_finalize {
        JobState::Indexed
    } else {
        JobState::Registered
    };
    if durable.operation.state != executable_state {
        return Err(ObjectCompileError::Config(format!(
            "operation is not executable from state {:?}; expected {:?}",
            durable.operation.state, executable_state
        )));
    }

    let attempt = run_attempt(
        options,
        &catalog,
        tenant_id,
        operation_id,
        dataset_id,
        target_snapshot_id,
        &bundle_object_key,
        &bundle_sha256,
        durable.request.parent_snapshot_id,
        durable.identity_namespace,
        &durable.policy_version,
    )
    .await;
    if let Err(error) = &attempt
        && error.deterministic()
    {
        let actor = format!("worker:{operation_id}");
        if let Err(catalog_error) = catalog
            .fail(
                tenant_id,
                operation_id,
                "REFERENCE_COMPILATION_FAILED",
                None,
                &actor,
            )
            .await
        {
            return Err(ObjectCompileError::Bundle(format!(
                "{error}; additionally failed to persist terminal state: {catalog_error}"
            )));
        }
    }
    attempt
}

#[allow(clippy::too_many_arguments)]
async fn run_attempt(
    options: &BTreeMap<String, String>,
    catalog: &OperationRepository,
    tenant_id: Uuid,
    operation_id: Uuid,
    dataset_id: Uuid,
    target_snapshot_id: Uuid,
    bundle_object_key: &str,
    bundle_sha256: &str,
    parent_snapshot_id: Option<Uuid>,
    identity_namespace: Uuid,
    policy_version: &str,
) -> Result<String, ObjectCompileError> {
    let store = ArtifactStore::from_base_url(&required_env("NGKG_ARTIFACT_BASE_URL")?)?;
    let scratch_root = path(options, "scratch-root")?;
    let attempt_root = scratch_root.join(operation_id.to_string());
    if attempt_root.exists() {
        return Err(ObjectCompileError::Config(format!(
            "scratch attempt path already exists: {}",
            attempt_root.display()
        )));
    }
    let input_root = attempt_root.join("input");
    let output_root = attempt_root.join("output");
    tokio::fs::create_dir_all(&input_root).await?;
    tokio::fs::create_dir_all(&output_root).await?;
    let bundle_path = input_root.join("compilation-bundle.json");
    store
        .materialize_verified(
            bundle_object_key,
            bundle_sha256,
            positive_u64(options, "ceiling-bundle-bytes")?,
            &bundle_path,
        )
        .await?;
    let bundle: CompilationBundle = serde_json::from_slice(&tokio::fs::read(&bundle_path).await?)
        .map_err(|error| ObjectCompileError::Bundle(error.to_string()))?;
    validate_bundle(
        &bundle,
        dataset_id,
        target_snapshot_id,
        parent_snapshot_id,
        identity_namespace,
        policy_version,
    )?;
    let distributed_finalize = options.contains_key("distributed-root-object-key");
    let max_staged_total_bytes = positive_u64(options, "ceiling-staged-total-bytes")?;
    let staged = materialize_inputs(
        &store,
        &bundle,
        &input_root,
        positive_u64(options, "ceiling-staged-object-bytes")?,
        max_staged_total_bytes,
        positive_usize(options, "ceiling-staged-artifacts")?,
        positive_usize(options, "download-concurrency")?,
        !distributed_finalize,
    )
    .await?;
    let distributed_source = if let Some(root_key) = options.get("distributed-root-object-key") {
        let root_sha256 = value(options, "distributed-root-sha256")?;
        let catalog_root = catalog
            .get_distributed_root(tenant_id, operation_id)
            .await?;
        let catalog_plan = catalog
            .get_distributed_plan(tenant_id, operation_id)
            .await?;
        if catalog_root.root_manifest_object_key != *root_key
            || catalog_root.root_manifest_sha256 != root_sha256
        {
            return Err(ObjectCompileError::Config(
                "distributed root arguments differ from catalog truth".to_owned(),
            ));
        }
        let root_path = input_root.join("distributed-root.json");
        store
            .materialize_verified(
                root_key,
                &root_sha256,
                positive_u64(options, "ceiling-staged-object-bytes")?,
                &root_path,
            )
            .await?;
        let root: DistributedRootManifest =
            serde_json::from_slice(&tokio::fs::read(&root_path).await?)
                .map_err(|error| ObjectCompileError::Bundle(error.to_string()))?;
        if root.dataset_id != dataset_id
            || root.snapshot_id != target_snapshot_id
            || root.source_plan_sha256 != catalog_plan.source_plan_sha256
            || root.canonical_source_sha256 != catalog_root.canonical_source_sha256
            || root.dictionary_sha256 != catalog_root.dictionary_sha256
            || root.semantic_content_sha256 != catalog_root.semantic_content_sha256
        {
            return Err(ObjectCompileError::Bundle(
                "distributed root identity or content differs from catalog truth".to_owned(),
            ));
        }
        let canonical_source = input_root.join("canonical-source.nq");
        store
            .materialize_verified(
                &catalog_root.canonical_source_object_key,
                &catalog_root.canonical_source_sha256,
                positive_u64(options, "ceiling-staged-object-bytes")?,
                &canonical_source,
            )
            .await?;
        let total = staged_bytes(&staged)?
            .checked_add(tokio::fs::metadata(&canonical_source).await?.len())
            .ok_or_else(|| ObjectCompileError::Bundle("staged byte count overflow".to_owned()))?;
        if total > max_staged_total_bytes {
            return Err(ObjectCompileError::Bundle(format!(
                "staged inputs contain {total} bytes, exceeding operator ceiling {max_staged_total_bytes}"
            )));
        }
        Some(InputArtifact {
            path: canonical_source,
            sha256: catalog_root.canonical_source_sha256,
        })
    } else {
        None
    };
    let distributed_artifact_root = if let Some(root_key) =
        options.get("distributed-artifact-root-object-key")
    {
        let root_sha256 = value(options, "distributed-artifact-root-sha256")?;
        let catalog_root = catalog.get_artifact_root(tenant_id, operation_id).await?;
        let catalog_plan = catalog.get_artifact_plan(tenant_id, operation_id).await?;
        let catalog_partitions = catalog
            .list_distributed_outputs(
                tenant_id,
                operation_id,
                DistributedWorkKind::Artifact,
            )
            .await?;
        if catalog_root.root_manifest_object_key != *root_key
            || catalog_root.root_manifest_sha256 != root_sha256
        {
            return Err(ObjectCompileError::Config(
                "distributed artifact root arguments differ from catalog truth".to_owned(),
            ));
        }
        let root_path = input_root.join("distributed-artifact-root.json");
        store
            .materialize_verified(
                root_key,
                &root_sha256,
                positive_u64(options, "ceiling-staged-object-bytes")?,
                &root_path,
            )
            .await?;
        let root: DistributedArtifactRootManifest =
            serde_json::from_slice(&tokio::fs::read(&root_path).await?)
                .map_err(|error| ObjectCompileError::Bundle(error.to_string()))?;
        if root.dataset_id != dataset_id
            || root.snapshot_id != target_snapshot_id
            || root.source_plan_sha256 != catalog_plan.source_plan_sha256
            || root.dictionary_sha256 != catalog_plan.dictionary_sha256
            || root.semantic_content_sha256 != catalog_root.semantic_content_sha256
            || i64::try_from(root.fact_count).ok() != Some(catalog_root.fact_count)
            || i64::try_from(root.semantic_row_count).ok()
                != Some(catalog_root.semantic_row_count)
            || i64::try_from(root.payload_row_count).ok()
                != Some(catalog_root.payload_row_count)
            || i64::try_from(root.locator_record_count).ok()
                != Some(catalog_root.locator_record_count)
            || root.partitions.len()
                != usize::try_from(catalog_plan.partition_count).map_err(|_| {
                    ObjectCompileError::Bundle("artifact partition count overflow".to_owned())
                })?
            || root.partitions.len() != catalog_partitions.len()
            || root.locator_path != catalog_root.locator_object_key
            || root.locator_sha256 != catalog_root.locator_sha256
        {
            return Err(ObjectCompileError::Bundle(
                "distributed artifact root identity or content differs from catalog truth"
                    .to_owned(),
            ));
        }
        for (reference, catalog_partition) in
            root.partitions.iter().zip(catalog_partitions.iter())
        {
            let (Some(manifest_key), Some(manifest_sha256)) = (
                catalog_partition.output_manifest_object_key.as_deref(),
                catalog_partition.output_manifest_sha256.as_deref(),
            ) else {
                return Err(ObjectCompileError::Bundle(
                    "successful artifact completion omits its manifest identity".to_owned(),
                ));
            };
            if i32::try_from(reference.partition_index).ok()
                != Some(catalog_partition.work_index)
                || reference.manifest_path != manifest_key
                || reference.manifest_sha256 != manifest_sha256
            {
                return Err(ObjectCompileError::Bundle(
                    "artifact partition root differs from catalog completion indexes".to_owned(),
                ));
            }
        }
        let locator_path = input_root.join("distributed-locator.tsv");
        store
            .materialize_verified(
                &catalog_root.locator_object_key,
                &catalog_root.locator_sha256,
                positive_u64(options, "ceiling-staged-object-bytes")?,
                &locator_path,
            )
            .await?;
        let added = tokio::fs::metadata(&root_path).await?.len()
            .checked_add(tokio::fs::metadata(&locator_path).await?.len())
            .ok_or_else(|| ObjectCompileError::Bundle("staged byte count overflow".to_owned()))?;
        let canonical_bytes = match distributed_source.as_ref() {
            Some(source) => std::fs::metadata(&source.path)?.len(),
            None => 0,
        };
        let total = staged_bytes(&staged)?
            .checked_add(canonical_bytes)
            .and_then(|total| total.checked_add(added))
            .ok_or_else(|| ObjectCompileError::Bundle("staged byte count overflow".to_owned()))?;
        if total > max_staged_total_bytes {
            return Err(ObjectCompileError::Bundle(format!(
                "staged inputs contain {total} bytes, exceeding operator ceiling {max_staged_total_bytes}"
            )));
        }
        Some(root)
    } else {
        None
    };
    let serving_inputs = if let Some(root_key) =
        options.get("distributed-serving-root-object-key")
    {
        let root_sha256 = value(options, "distributed-serving-root-sha256")?;
        let catalog_root = catalog.get_serving_root(tenant_id, operation_id).await?;
        let artifact_catalog = catalog.get_artifact_root(tenant_id, operation_id).await?;
        let artifact_plan = catalog.get_artifact_plan(tenant_id, operation_id).await?;
        if catalog_root.serving_root_object_key != *root_key
            || catalog_root.serving_root_sha256 != root_sha256
        {
            return Err(ObjectCompileError::Config(
                "distributed serving root arguments differ from catalog truth".to_owned(),
            ));
        }
        let serving_root_path = input_root.join("serving-root.json");
        store
            .materialize_verified(
                root_key,
                &root_sha256,
                positive_u64(options, "ceiling-staged-object-bytes")?,
                &serving_root_path,
            )
            .await?;
        let manifest: ServingRootManifest =
            serde_json::from_slice(&tokio::fs::read(&serving_root_path).await?)
                .map_err(|error| ObjectCompileError::Bundle(error.to_string()))?;
        manifest.validate()?;
        if manifest.dataset_id != dataset_id
            || manifest.snapshot_id != target_snapshot_id
            || manifest.artifact_root_object_key != artifact_catalog.root_manifest_object_key
            || manifest.artifact_root_sha256 != artifact_catalog.root_manifest_sha256
            || manifest.dictionary_object_key != artifact_plan.dictionary_object_key
            || manifest.dictionary_sha256 != artifact_plan.dictionary_sha256
            || manifest.source_locator_object_key != artifact_catalog.locator_object_key
            || manifest.source_locator_sha256 != artifact_catalog.locator_sha256
            || manifest.binary_locator_object_key != catalog_root.binary_locator_object_key
            || manifest.binary_locator_sha256 != catalog_root.binary_locator_sha256
            || manifest.semantic_content_sha256 != catalog_root.semantic_content_sha256
            || i32::try_from(manifest.row_group_rows).ok() != Some(catalog_root.row_group_rows)
            || i64::try_from(manifest.locator_record_count).ok()
                != Some(catalog_root.locator_record_count)
            || i32::try_from(manifest.partitions.len()).ok()
                != Some(catalog_root.partition_count)
        {
            return Err(ObjectCompileError::Bundle(
                "distributed serving root differs from catalog or artifact truth".to_owned(),
            ));
        }
        let binary_locator_path = input_root.join("distributed-locator.bin");
        store
            .materialize_verified(
                &manifest.binary_locator_object_key,
                &manifest.binary_locator_sha256,
                positive_u64(options, "ceiling-staged-object-bytes")?,
                &binary_locator_path,
            )
            .await?;
        let locator = MmapLocatorIndex::open(
            &binary_locator_path,
            &manifest.binary_locator_sha256,
            target_snapshot_id,
            &manifest.source_locator_sha256,
        )?;
        if u64::try_from(locator.record_count()).ok() != Some(manifest.locator_record_count) {
            return Err(ObjectCompileError::Bundle(
                "binary locator count differs from serving root".to_owned(),
            ));
        }
        let dictionary_path = input_root.join("distributed-dictionary.tsv");
        store
            .materialize_verified(
                &manifest.dictionary_object_key,
                &manifest.dictionary_sha256,
                positive_u64(options, "ceiling-staged-object-bytes")?,
                &dictionary_path,
            )
            .await?;
        let dictionary_bytes = tokio::fs::metadata(&dictionary_path).await?.len();
        let serving_bytes = tokio::fs::metadata(&serving_root_path)
            .await?
            .len()
            .checked_add(tokio::fs::metadata(&binary_locator_path).await?.len())
            .and_then(|value| value.checked_add(dictionary_bytes));
        let serving_bytes = serving_bytes.ok_or_else(|| {
            ObjectCompileError::Bundle("serving input byte count overflow".to_owned())
        })?;
        let canonical_bytes = match distributed_source.as_ref() {
            Some(source) => std::fs::metadata(&source.path)?.len(),
            None => 0,
        };
        let prior_distributed_bytes = tokio::fs::metadata(
            input_root.join("distributed-artifact-root.json"),
        )
        .await?
        .len()
        .checked_add(
            tokio::fs::metadata(input_root.join("distributed-locator.tsv"))
                .await?
                .len(),
        )
        .ok_or_else(|| {
            ObjectCompileError::Bundle("distributed input byte count overflow".to_owned())
        })?;
        let total = staged_bytes(&staged)?
            .checked_add(canonical_bytes)
            .and_then(|value| value.checked_add(prior_distributed_bytes))
            .and_then(|value| value.checked_add(serving_bytes))
            .ok_or_else(|| {
                ObjectCompileError::Bundle("staged input byte count overflow".to_owned())
            })?;
        if total > max_staged_total_bytes {
            return Err(ObjectCompileError::Bundle(
                "serving inputs exceed staged byte ceiling".to_owned(),
            ));
        }
        Some(ServingInputs {
            manifest,
            manifest_sha256: root_sha256,
            binary_locator_path,
            dictionary_path,
        })
    } else {
        None
    };
    let manifest = local_manifest(
        &bundle,
        &staged,
        output_root.clone(),
        distributed_source,
    )?;
    let manifest_path = input_root.join("reference-compile.json");
    write_json_new(&manifest_path, &manifest).await?;

    let trusted_reasoner = TrustedReasonerConfig {
        java_executable: path(options, "java-executable")?,
        adapter_jar: InputArtifact {
            path: path(options, "reasoner-adapter-jar")?,
            sha256: value(options, "reasoner-adapter-sha256")?,
        },
        expected_name: value(options, "reasoner-name")?,
        expected_version: value(options, "reasoner-version")?,
    };
    let ceilings = TrustedResourceCeilings {
        max_input_bytes: positive_u64(options, "ceiling-input-bytes")?,
        max_quads: positive_u64(options, "ceiling-quads")?,
        max_dictionary_terms: positive_u64(options, "ceiling-dictionary-terms")?,
        max_reasoner_seconds: positive_u64(options, "ceiling-reasoner-seconds")?,
        max_parquet_row_group_rows: positive_usize(options, "ceiling-parquet-row-group-rows")?,
        max_named_individuals: positive_u64(options, "ceiling-named-individuals")?,
        max_properties: positive_u64(options, "ceiling-properties")?,
    };
    let compile_manifest = manifest_path.clone();
    let allowed_input = input_root.clone();
    let allowed_output = output_root.clone();
    let snapshot_manifest_path = tokio::task::spawn_blocking(move || {
        compile_from_manifest(
            &compile_manifest,
            &allowed_input,
            &allowed_output,
            &trusted_reasoner,
            ceilings,
        )
        .map_err(|error| error.to_string())
    })
    .await?
    .map_err(ObjectCompileError::Reference)?;
    let mut snapshot: ReferenceSnapshotManifest =
        serde_json::from_slice(&tokio::fs::read(&snapshot_manifest_path).await?)
            .map_err(|error| ObjectCompileError::Reference(error.to_string()))?;
    if snapshot.dataset_id != dataset_id || snapshot.snapshot_id != target_snapshot_id {
        return Err(ObjectCompileError::Reference(
            "compiler output identity differs from the durable request".to_owned(),
        ));
    }
    if let Some(root) = &distributed_artifact_root {
        validate_artifact_equivalence(&snapshot_manifest_path, root)?;
    }
    let serving_certification = if let Some(serving) = &serving_inputs {
        let report = certify_sharded_hydration(
            &store,
            serving,
            &bundle,
            &staged,
            &snapshot_manifest_path,
            &input_root,
            positive_usize(options, "hydration-worker-threads")?,
            positive_u64(options, "ceiling-hydration-rows")?,
            positive_u64(options, "ceiling-staged-object-bytes")?,
            max_staged_total_bytes,
        )
        .await?;
        let relative_path = "certification/distributed-serving-equivalence.json".to_owned();
        let report_path = output_root.join(&relative_path);
        let report_bytes = serde_json::to_vec_pretty(&report)
            .map_err(|error| ObjectCompileError::Reference(error.to_string()))?;
        write_new(&report_path, &report_bytes).await?;
        let report_sha256 = sha256_path(&report_path)?;
        snapshot.artifacts.push(ArtifactRecord {
            relative_path: relative_path.clone(),
            sha256: report_sha256.clone(),
            bytes: u64::try_from(report_bytes.len()).map_err(|_| {
                ObjectCompileError::Reference("serving report size overflow".to_owned())
            })?,
        });
        snapshot
            .artifacts
            .sort_unstable_by(|left, right| left.relative_path.cmp(&right.relative_path));
        tokio::fs::write(
            &snapshot_manifest_path,
            serde_json::to_vec_pretty(&snapshot)
                .map_err(|error| ObjectCompileError::Reference(error.to_string()))?,
        )
        .await?;
        let hydrated_row_count = report.queries.iter().try_fold(0_i64, |total, query| {
            i64::try_from(query.reference_row_count)
                .ok()
                .and_then(|rows| total.checked_add(rows))
                .ok_or_else(|| {
                    ObjectCompileError::Reference(
                        "serving certification row count overflow".to_owned(),
                    )
                })
        })?;
        Some(LocalServingCertification {
            relative_path,
            sha256: report_sha256,
            certified_query_count: i32::try_from(report.queries.len()).map_err(|_| {
                ObjectCompileError::Reference("serving query count overflow".to_owned())
            })?,
            hydrated_row_count,
        })
    } else {
        None
    };
    validate_snapshot_budget(
        &snapshot,
        positive_u64(options, "ceiling-output-bytes")?,
        positive_usize(options, "ceiling-output-artifacts")?,
    )?;
    let manifest_sha256 = sha256_path(&snapshot_manifest_path)?;
    let manifest_hash = decode_sha256(&manifest_sha256)?;
    let snapshot_prefix = format!("snapshots/{tenant_id}/{dataset_id}/{target_snapshot_id}");
    upload_snapshot(
        &store,
        &snapshot_manifest_path,
        &snapshot,
        &snapshot_prefix,
        positive_usize(options, "upload-concurrency")?,
        positive_u64(options, "single-put-max-bytes")?,
        positive_usize(options, "multipart-buffer-bytes")?,
        positive_usize(options, "multipart-concurrency")?,
    )
    .await?;
    let manifest_object_key = format!("{snapshot_prefix}/snapshot-manifest.json");
    store
        .put_file_immutable(
            &manifest_object_key,
            &manifest_sha256,
            &snapshot_manifest_path,
            positive_u64(options, "single-put-max-bytes")?,
            positive_usize(options, "multipart-buffer-bytes")?,
            positive_usize(options, "multipart-concurrency")?,
        )
        .await?;
    if let Some(certification) = serving_certification {
        catalog
            .commit_serving_certification(
                tenant_id,
                operation_id,
                &ServingCertification {
                    report_object_key: format!(
                        "{snapshot_prefix}/{}",
                        certification.relative_path
                    ),
                    report_sha256: certification.sha256,
                    serving_root_sha256: serving_inputs
                        .as_ref()
                        .ok_or_else(|| {
                            ObjectCompileError::Reference(
                                "serving certificate lost its serving root".to_owned(),
                            )
                        })?
                        .manifest_sha256
                        .clone(),
                    binary_locator_sha256: serving_inputs
                        .as_ref()
                        .ok_or_else(|| {
                            ObjectCompileError::Reference(
                                "serving certificate lost its binary locator".to_owned(),
                            )
                        })?
                        .manifest
                        .binary_locator_sha256
                        .clone(),
                    reference_manifest_object_key: manifest_object_key.clone(),
                    reference_manifest_sha256: manifest_sha256.clone(),
                    certified_query_count: certification.certified_query_count,
                    hydrated_row_count: certification.hydrated_row_count,
                },
            )
            .await?;
    }
    let actor = format!("worker:{operation_id}");
    let outcome = catalog
        .commit_reference_certification(
            tenant_id,
            operation_id,
            &manifest_object_key,
            &manifest_hash,
            &actor,
        )
        .await?;
    Ok(serde_json::json!({
        "status": if outcome.published { "published" } else { "certified" },
        "operationId": operation_id,
        "snapshotId": target_snapshot_id,
        "snapshotManifestObjectKey": manifest_object_key,
        "snapshotManifestSha256": manifest_sha256,
        "automaticPublicationConflict": outcome.publication_conflict
    })
    .to_string())
}

#[allow(clippy::too_many_arguments)]
async fn certify_sharded_hydration(
    store: &ArtifactStore,
    serving: &ServingInputs,
    bundle: &CompilationBundle,
    staged: &BTreeMap<String, PathBuf>,
    snapshot_manifest_path: &Path,
    input_root: &Path,
    worker_threads: usize,
    max_rows: u64,
    max_object_bytes: u64,
    max_total_payload_bytes: u64,
) -> Result<ServingEquivalenceReport, ObjectCompileError> {
    let snapshot_sha256 = sha256_path(snapshot_manifest_path)?;
    let locator = MmapLocatorIndex::open(
        &serving.binary_locator_path,
        &serving.manifest.binary_locator_sha256,
        serving.manifest.snapshot_id,
        &serving.manifest.source_locator_sha256,
    )?;
    let dictionary = read_iri_dictionary(&serving.dictionary_path)?;
    let mut cases = Vec::with_capacity(bundle.certified_queries.len());
    let mut needed_partitions = BTreeSet::new();
    for query in &bundle.certified_queries {
        let query_path = staged.get(&query.query.file_name).ok_or_else(|| {
            ObjectCompileError::Reference(format!(
                "certified query was not staged: {}",
                query.query.file_name
            ))
        })?;
        let result = execute_snapshot_query(
            snapshot_manifest_path,
            &snapshot_sha256,
            query_path,
            input_root,
            true,
        )
        .map_err(|error| ObjectCompileError::Reference(error.to_string()))?;
        let mut guid_to_iri = BTreeMap::new();
        for entity_iri in &result.qualified_entity_iris {
            let guid = guid_for_canonical_iri(bundle.dataset_namespace, entity_iri)?;
            if guid_to_iri
                .insert(guid, entity_iri.clone())
                .is_some_and(|existing| existing.as_str() != entity_iri.as_str())
            {
                return Err(ObjectCompileError::Reference(
                    "two qualified IRIs resolved to one GUID".to_owned(),
                ));
            }
        }
        let qualified = guid_to_iri
            .keys()
            .enumerate()
            .map(|(ordinal, guid)| {
                Ok(ShardedQualifiedGuid {
                    query_ordinal: u64::try_from(ordinal).map_err(|_| {
                        ObjectCompileError::Reference(
                            "hydration query ordinal overflow".to_owned(),
                        )
                    })?,
                    entity_guid: *guid,
                    multiplicity: 1,
                })
            })
            .collect::<Result<Vec<_>, ObjectCompileError>>()?;
        for qualified_guid in &qualified {
            for record in locator.lookup(qualified_guid.entity_guid)? {
                needed_partitions.insert(record.partition_index);
            }
        }
        cases.push(QueryHydrationCase {
            query_id: query.query_id.clone(),
            query_sha256: result.query_sha256,
            reference: result.hydrated_payload,
            qualified,
            guid_to_iri,
        });
    }
    let payload_root = input_root.join("serving-payloads");
    tokio::fs::create_dir_all(&payload_root).await?;
    let mut shards = BTreeMap::<u32, VerifiedPayloadShard>::new();
    let mut downloaded_payload_bytes = 0_u64;
    for partition_index in needed_partitions {
        let partition = serving
            .manifest
            .partitions
            .get(usize::try_from(partition_index).map_err(|_| {
                ObjectCompileError::Reference("payload partition index overflow".to_owned())
            })?)
            .filter(|value| value.partition_index == partition_index)
            .ok_or_else(|| {
                ObjectCompileError::Reference(
                    "binary locator references an absent payload partition".to_owned(),
                )
            })?;
        if partition.payload_bytes > max_object_bytes {
            return Err(ObjectCompileError::Reference(format!(
                "payload partition {partition_index} exceeds the object ceiling"
            )));
        }
        downloaded_payload_bytes = downloaded_payload_bytes
            .checked_add(partition.payload_bytes)
            .ok_or_else(|| {
                ObjectCompileError::Reference("payload certification bytes overflow".to_owned())
            })?;
        if downloaded_payload_bytes > max_total_payload_bytes {
            return Err(ObjectCompileError::Reference(
                "payload certification exceeds the aggregate staging ceiling".to_owned(),
            ));
        }
        let local = payload_root.join(format!("payload-{partition_index:05}.parquet"));
        store
            .materialize_verified(
                &partition.payload_object_key,
                &partition.payload_sha256,
                max_object_bytes,
                &local,
            )
            .await?;
        if tokio::fs::metadata(&local).await?.len() != partition.payload_bytes {
            return Err(ObjectCompileError::Reference(format!(
                "payload partition {partition_index} byte count differs from serving root"
            )));
        }
        let verified = verify_payload_shard(
            partition_index,
            &local,
            &partition.payload_sha256,
        )?;
        shards.insert(partition_index, verified);
    }
    let mut query_reports = Vec::with_capacity(cases.len());
    for case in cases {
        let sharded = if case.qualified.is_empty() {
            Vec::new()
        } else {
            hydrate_sharded_payload(
                &locator,
                serving.manifest.snapshot_id,
                &case.qualified,
                &shards,
                worker_threads,
                max_rows,
            )?
        };
        let reference_rows = canonical_reference_rows(&case.reference)?;
        let sharded_rows = canonical_sharded_rows(
            &sharded,
            &case.guid_to_iri,
            &dictionary,
        )?;
        if reference_rows != sharded_rows {
            return Err(ObjectCompileError::Reference(format!(
                "sharded hydration differs from reference query {}",
                case.query_id
            )));
        }
        query_reports.push(ServingQueryEquivalence {
            query_id: case.query_id,
            query_sha256: case.query_sha256,
            reference_row_count: u64::try_from(reference_rows.len()).map_err(|_| {
                ObjectCompileError::Reference("reference row count overflow".to_owned())
            })?,
            sharded_row_count: u64::try_from(sharded_rows.len()).map_err(|_| {
                ObjectCompileError::Reference("sharded row count overflow".to_owned())
            })?,
            canonical_rows_sha256: canonical_rows_sha256(&reference_rows)?,
        });
    }
    query_reports.sort_unstable_by(|left, right| left.query_id.cmp(&right.query_id));
    Ok(ServingEquivalenceReport {
        format_version: 1,
        dataset_id: serving.manifest.dataset_id,
        snapshot_id: serving.manifest.snapshot_id,
        serving_root_sha256: serving.manifest_sha256.clone(),
        binary_locator_sha256: serving.manifest.binary_locator_sha256.clone(),
        queries: query_reports,
        equivalent: true,
    })
}

fn canonical_reference_rows(
    rows: &[HydratedPayload],
) -> Result<Vec<String>, ObjectCompileError> {
    let mut canonical = rows
        .iter()
        .map(|row| {
            serde_json::to_string(&(
                row.subject_resource_kind,
                &row.subject_term,
                &row.predicate_iri,
                &row.lexical_value,
                row.datatype_iri.as_deref().unwrap_or(""),
                row.language.as_deref().unwrap_or(""),
                row.graph_scope,
                row.graph_iri.as_deref().unwrap_or(""),
            ))
            .map_err(|error| ObjectCompileError::Reference(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    canonical.sort_unstable();
    Ok(canonical)
}

fn canonical_sharded_rows(
    rows: &[HydratedShardRow],
    guid_to_iri: &BTreeMap<Uuid, String>,
    dictionary: &BTreeMap<u64, String>,
) -> Result<Vec<String>, ObjectCompileError> {
    let mut canonical = Vec::with_capacity(rows.len());
    for row in rows {
        let subject = guid_to_iri.get(&row.entity_guid).ok_or_else(|| {
            ObjectCompileError::Reference("hydrated GUID has no reference IRI".to_owned())
        })?;
        if row.subject_resource_kind != ngkg_hydration::RdfResourceKind::NamedNode
            || &row.subject_term != subject
        {
            return Err(ObjectCompileError::Reference(
                "sharded hydration subject differs from its qualified named-node IRI".to_owned(),
            ));
        }
        let predicate = dictionary.get(&row.predicate_id).ok_or_else(|| {
            ObjectCompileError::Reference("payload predicate ID is absent from dictionary".to_owned())
        })?;
        let graph = dictionary.get(&row.graph_id).ok_or_else(|| {
            ObjectCompileError::Reference("payload graph ID is absent from dictionary".to_owned())
        })?;
        canonical.push(
            serde_json::to_string(&(
                ngkg_reference::ResourceTermKind::NamedNode,
                &row.subject_term,
                predicate,
                &row.lexical_value,
                &row.datatype_iri,
                row.language.as_deref().unwrap_or(""),
                match row.graph_scope {
                    ngkg_hydration::RdfGraphScope::Default => ngkg_reference::GraphScope::Default,
                    ngkg_hydration::RdfGraphScope::Named => ngkg_reference::GraphScope::Named,
                },
                if row.graph_scope == ngkg_hydration::RdfGraphScope::Named {
                    graph.as_str()
                } else {
                    ""
                },
            ))
            .map_err(|error| ObjectCompileError::Reference(error.to_string()))?,
        );
    }
    canonical.sort_unstable();
    Ok(canonical)
}

fn read_iri_dictionary(path: &Path) -> Result<BTreeMap<u64, String>, ObjectCompileError> {
    let mut dictionary = BTreeMap::new();
    let mut expected = 0_u64;
    for line in BufReader::new(std::fs::File::open(path)?).lines() {
        let line = line?;
        let mut fields = line.splitn(3, '\t');
        let id = fields.next().and_then(|value| value.parse::<u64>().ok());
        let kind = fields.next();
        let term = fields.next();
        if id != Some(expected) || kind.is_none() || term.is_none() {
            return Err(ObjectCompileError::Reference(
                "distributed dictionary is not dense or canonical".to_owned(),
            ));
        }
        if kind == Some("I") {
            let term = term.ok_or_else(|| {
                ObjectCompileError::Reference("dictionary IRI is absent".to_owned())
            })?;
            if term.is_empty() || dictionary.insert(expected, term.to_owned()).is_some() {
                return Err(ObjectCompileError::Reference(
                    "distributed IRI dictionary is invalid".to_owned(),
                ));
            }
        } else if !matches!(kind, Some("B") | Some("L")) {
            return Err(ObjectCompileError::Reference(
                "distributed dictionary term kind is invalid".to_owned(),
            ));
        }
        expected = expected.checked_add(1).ok_or_else(|| {
            ObjectCompileError::Reference("distributed dictionary ID overflow".to_owned())
        })?;
    }
    Ok(dictionary)
}

fn canonical_rows_sha256(rows: &[String]) -> Result<String, ObjectCompileError> {
    let mut hasher = Sha256::new();
    hasher.update(b"ngkg-serving-hydration-equivalence-v1\0");
    for row in rows {
        let row_bytes = u64::try_from(row.len()).map_err(|_| {
            ObjectCompileError::Reference("canonical hydration row length overflow".to_owned())
        })?;
        hasher.update(row_bytes.to_be_bytes());
        hasher.update(row.as_bytes());
    }
    Ok(hex::encode(hasher.finalize()))
}

fn validate_artifact_equivalence(
    snapshot_manifest_path: &Path,
    root: &DistributedArtifactRootManifest,
) -> Result<(), ObjectCompileError> {
    let snapshot_root = snapshot_manifest_path.parent().ok_or_else(|| {
        ObjectCompileError::Reference("snapshot manifest has no parent".to_owned())
    })?;
    let verification_path = snapshot_root.join("certification/verification.json");
    let verification: serde_json::Value = serde_json::from_slice(&std::fs::read(verification_path)?)
        .map_err(|error| ObjectCompileError::Reference(error.to_string()))?;
    let count = |name: &str| {
        verification
            .get(name)
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                ObjectCompileError::Reference(format!(
                    "reference verification omits unsigned count {name}"
                ))
            })
    };
    if count("sourceQuadCount")? != root.fact_count
        || count("semanticSpineRowCount")? != root.semantic_row_count
        || count("payloadRowCount")? != root.payload_row_count
        || count("locatorRecordCount")? != root.locator_record_count
    {
        return Err(ObjectCompileError::Reference(
            "distributed artifacts are not count-equivalent to the reference compiler output"
                .to_owned(),
        ));
    }
    Ok(())
}

async fn materialize_inputs(
    store: &ArtifactStore,
    bundle: &CompilationBundle,
    input_root: &Path,
    max_object_bytes: u64,
    max_total_bytes: u64,
    max_artifacts: usize,
    concurrency: usize,
    include_source: bool,
) -> Result<BTreeMap<String, PathBuf>, ObjectCompileError> {
    let artifacts = bundle_artifacts(bundle, include_source)?;
    if artifacts.len() > max_artifacts {
        return Err(ObjectCompileError::Bundle(format!(
            "bundle contains {} staged artifacts, exceeding operator ceiling {max_artifacts}",
            artifacts.len()
        )));
    }
    let destinations = artifacts
        .into_iter()
        .map(|artifact| {
            let destination = input_root.join(&artifact.file_name);
            (artifact, destination)
        })
        .collect::<Vec<_>>();
    let store = Arc::new(store.clone());
    let aggregate_bytes = Arc::new(AtomicU64::new(0));
    let results = stream::iter(destinations.into_iter().map(|(artifact, destination)| {
        let store = Arc::clone(&store);
        let aggregate_bytes = Arc::clone(&aggregate_bytes);
        async move {
            let bytes = store
                .materialize_verified_with_budget(
                    &artifact.object_key,
                    &artifact.sha256,
                    max_object_bytes,
                    &destination,
                    aggregate_bytes,
                    max_total_bytes,
                )
                .await?;
            Ok::<_, ArtifactStoreError>((artifact.file_name, destination, bytes))
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;
    let mut staged = BTreeMap::new();
    let mut total_bytes = 0_u64;
    for result in results {
        let (name, destination, bytes) = result?;
        total_bytes = total_bytes
            .checked_add(bytes)
            .ok_or_else(|| ObjectCompileError::Bundle("staged input byte count overflow".to_owned()))?;
        if total_bytes > max_total_bytes {
            return Err(ObjectCompileError::Bundle(format!(
                "staged inputs exceed operator ceiling {max_total_bytes}"
            )));
        }
        if staged.insert(name.clone(), destination).is_some() {
            return Err(ObjectCompileError::Bundle(format!("duplicate staged file name {name}")));
        }
    }
    Ok(staged)
}

fn staged_bytes(staged: &BTreeMap<String, PathBuf>) -> Result<u64, ObjectCompileError> {
    staged.values().try_fold(0_u64, |total, path| {
        let bytes = std::fs::metadata(path)?.len();
        total
            .checked_add(bytes)
            .ok_or_else(|| ObjectCompileError::Bundle("staged byte count overflow".to_owned()))
    })
}

fn bundle_artifacts(
    bundle: &CompilationBundle,
    include_source: bool,
) -> Result<Vec<ObjectArtifact>, ObjectCompileError> {
    let mut artifacts = Vec::new();
    if include_source {
        artifacts.push(bundle.source.clone());
    }
    artifacts.extend(bundle.ontology_bundle.iter().cloned());
    for query in &bundle.certified_queries {
        artifacts.push(query.query.clone());
        artifacts.push(query.expected.clone());
    }
    let mut names = BTreeSet::new();
    for artifact in &artifacts {
        if artifact.file_name.len() > 255
            || !normalized_segment(&artifact.file_name)
            || matches!(
                artifact.file_name.as_str(),
                "compilation-bundle.json" | "reference-compile.json"
            )
        {
            return Err(ObjectCompileError::Bundle(format!(
                "unsafe staged file name {}",
                artifact.file_name
            )));
        }
        if !names.insert(artifact.file_name.clone()) {
            return Err(ObjectCompileError::Bundle(format!(
                "staged file name is not unique: {}",
                artifact.file_name
            )));
        }
        decode_sha256(&artifact.sha256)?;
    }
    Ok(artifacts)
}

fn normalized_segment(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && bytes.all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn local_manifest(
    bundle: &CompilationBundle,
    staged: &BTreeMap<String, PathBuf>,
    output_directory: PathBuf,
    source_override: Option<InputArtifact>,
) -> Result<ReferenceCompileManifest, ObjectCompileError> {
    let artifact = |input: &ObjectArtifact| -> Result<InputArtifact, ObjectCompileError> {
        Ok(InputArtifact {
            path: staged
                .get(&input.file_name)
                .cloned()
                .ok_or_else(|| ObjectCompileError::Bundle(format!("missing staged file {}", input.file_name)))?,
            sha256: input.sha256.clone(),
        })
    };
    let ontology_bundle = bundle
        .ontology_bundle
        .iter()
        .map(artifact)
        .collect::<Result<Vec<_>, _>>()?;
    let certified_queries = bundle
        .certified_queries
        .iter()
        .map(|query| {
            Ok(CertifiedQueryInput {
                query_id: query.query_id.clone(),
                ordered: query.ordered,
                query: artifact(&query.query)?,
                expected: artifact(&query.expected)?,
                required_source_iris: query.required_source_iris.clone(),
            })
        })
        .collect::<Result<Vec<_>, ObjectCompileError>>()?;
    let used_source_override = source_override.is_some();
    Ok(ReferenceCompileManifest {
        format_version: bundle.format_version,
        dataset_id: bundle.dataset_id,
        snapshot_id: bundle.snapshot_id,
        parent_snapshot_id: bundle.parent_snapshot_id,
        dataset_namespace: bundle.dataset_namespace,
        source_guid: bundle.source_guid,
        source_snapshot: bundle.source_snapshot.clone(),
        source: match source_override {
            Some(source) => source,
            None => artifact(&bundle.source)?,
        },
        source_identity_sha256: distributed_finalize_source_hash(bundle, used_source_override),
        ontology_bundle,
        output_directory,
        projection: bundle.projection.clone(),
        reasoning: bundle.reasoning.clone(),
        graph_catalog: bundle.graph_catalog.clone(),
        certified_queries,
        limits: bundle.limits.clone(),
    })
}

fn distributed_finalize_source_hash(
    bundle: &CompilationBundle,
    used_source_override: bool,
) -> Option<String> {
    used_source_override.then(|| bundle.source.sha256.clone())
}

#[allow(clippy::too_many_arguments)]
async fn upload_snapshot(
    store: &ArtifactStore,
    snapshot_manifest_path: &Path,
    snapshot: &ReferenceSnapshotManifest,
    snapshot_prefix: &str,
    concurrency: usize,
    single_put_max_bytes: u64,
    multipart_buffer_bytes: usize,
    multipart_concurrency: usize,
) -> Result<(), ObjectCompileError> {
    let snapshot_root = snapshot_manifest_path
        .parent()
        .ok_or_else(|| ObjectCompileError::Reference("snapshot manifest has no parent".to_owned()))?;
    let mut paths = BTreeSet::new();
    let uploads = snapshot
        .artifacts
        .iter()
        .map(|artifact| {
            let relative = Path::new(&artifact.relative_path);
            if relative.is_absolute()
                || relative
                    .components()
                    .any(|component| !matches!(component, Component::Normal(_)))
                || !paths.insert(artifact.relative_path.clone())
            {
                return Err(ObjectCompileError::Reference(format!(
                    "unsafe or duplicate snapshot artifact {}",
                    artifact.relative_path
                )));
            }
            let source = snapshot_root.join(relative);
            let observed_bytes = std::fs::metadata(&source)
                .map_err(ObjectCompileError::Io)?
                .len();
            if observed_bytes != artifact.bytes {
                return Err(ObjectCompileError::Reference(format!(
                    "snapshot artifact size differs from manifest: {}",
                    artifact.relative_path
                )));
            }
            Ok((
                format!("{snapshot_prefix}/{}", artifact.relative_path),
                artifact.sha256.clone(),
                source,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let store = Arc::new(store.clone());
    let results = stream::iter(uploads.into_iter().map(|(key, sha, source)| {
        let store = Arc::clone(&store);
        async move {
            store
                .put_file_immutable(
                    &key,
                    &sha,
                    &source,
                    single_put_max_bytes,
                    multipart_buffer_bytes,
                    multipart_concurrency,
                )
                .await
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;
    for result in results {
        result?;
    }
    Ok(())
}

fn validate_snapshot_budget(
    snapshot: &ReferenceSnapshotManifest,
    max_bytes: u64,
    max_artifacts: usize,
) -> Result<(), ObjectCompileError> {
    if snapshot.artifacts.len() > max_artifacts {
        return Err(ObjectCompileError::Reference(format!(
            "snapshot contains {} artifacts, exceeding operator ceiling {max_artifacts}",
            snapshot.artifacts.len()
        )));
    }
    let total = snapshot.artifacts.iter().try_fold(0_u64, |total, artifact| {
        total.checked_add(artifact.bytes).ok_or_else(|| {
            ObjectCompileError::Reference("snapshot artifact byte count overflow".to_owned())
        })
    })?;
    if total > max_bytes {
        return Err(ObjectCompileError::Reference(format!(
            "snapshot contains {total} bytes, exceeding operator ceiling {max_bytes}"
        )));
    }
    Ok(())
}

fn validate_bundle(
    bundle: &CompilationBundle,
    dataset_id: Uuid,
    snapshot_id: Uuid,
    parent_snapshot_id: Option<Uuid>,
    identity_namespace: Uuid,
    policy_version: &str,
) -> Result<(), ObjectCompileError> {
    if bundle.format_version != 1 {
        return Err(ObjectCompileError::Bundle("unsupported formatVersion".to_owned()));
    }
    if bundle.dataset_id != dataset_id || bundle.snapshot_id != snapshot_id {
        return Err(ObjectCompileError::Bundle(
            "bundle dataset or snapshot differs from catalog request".to_owned(),
        ));
    }
    if bundle.parent_snapshot_id != parent_snapshot_id {
        return Err(ObjectCompileError::Bundle(
            "bundle parent snapshot differs from catalog request".to_owned(),
        ));
    }
    if bundle.dataset_namespace != identity_namespace {
        return Err(ObjectCompileError::Bundle(
            "bundle identity namespace differs from the durable dataset".to_owned(),
        ));
    }
    if bundle.projection.policy_id != policy_version {
        return Err(ObjectCompileError::Bundle(
            "bundle projection policy differs from the durable dataset policy".to_owned(),
        ));
    }
    if bundle.ontology_bundle.is_empty() || bundle.certified_queries.is_empty() {
        return Err(ObjectCompileError::Bundle(
            "ontologyBundle and certifiedQueries must be non-empty".to_owned(),
        ));
    }
    bundle_artifacts(bundle, true)?;
    Ok(())
}

async fn write_json_new<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), ObjectCompileError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| ObjectCompileError::Bundle(error.to_string()))?;
    let mut file = tokio::fs::OpenOptions::new().create_new(true).write(true).open(path).await?;
    tokio::io::AsyncWriteExt::write_all(&mut file, &bytes).await?;
    file.sync_all().await?;
    Ok(())
}

async fn write_new(path: &Path, bytes: &[u8]) -> Result<(), ObjectCompileError> {
    let mut file = tokio::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .await?;
    tokio::io::AsyncWriteExt::write_all(&mut file, bytes).await?;
    file.sync_all().await?;
    Ok(())
}

fn value(options: &BTreeMap<String, String>, name: &str) -> Result<String, ObjectCompileError> {
    options
        .get(name)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| ObjectCompileError::Config(format!("--{name} is required")))
}

fn required_env(name: &str) -> Result<String, ObjectCompileError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ObjectCompileError::Config(format!("{name} is required")))
}

fn path(options: &BTreeMap<String, String>, name: &str) -> Result<PathBuf, ObjectCompileError> {
    value(options, name).map(PathBuf::from)
}

fn uuid_option(options: &BTreeMap<String, String>, name: &str) -> Result<Uuid, ObjectCompileError> {
    value(options, name)?
        .parse()
        .map_err(|error| ObjectCompileError::Config(format!("--{name} must be a UUID: {error}")))
}

fn positive_u64(options: &BTreeMap<String, String>, name: &str) -> Result<u64, ObjectCompileError> {
    value(options, name)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ObjectCompileError::Config(format!("--{name} must be a positive 64-bit integer")))
}

fn positive_usize(options: &BTreeMap<String, String>, name: &str) -> Result<usize, ObjectCompileError> {
    value(options, name)?
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ObjectCompileError::Config(format!("--{name} must be a positive platform-sized integer")))
}

fn decode_sha256(value: &str) -> Result<[u8; 32], ObjectCompileError> {
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(ObjectCompileError::Bundle("invalid lowercase SHA-256".to_owned()));
    }
    let bytes = hex::decode(value).map_err(|error| ObjectCompileError::Bundle(error.to_string()))?;
    let mut output = [0_u8; 32];
    output.copy_from_slice(&bytes);
    Ok(output)
}
