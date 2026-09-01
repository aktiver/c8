//! CLI boundary for deterministic safe-scan, projection, reduction, and equality.
//!
//! Kubernetes completion indexes select immutable logical partitions. Paths are
//! local materializations of exact object keys; object transfer and catalog CAS
//! remain control-plane responsibilities, never implicit bucket listings.

use std::{
    collections::BTreeMap,
    env,
    path::{Path, PathBuf},
    process::ExitCode,
};

use ngkg_distributed_artifacts::{
    ArtifactPartitionRequest, compare_artifact_roots, finalize_artifact_partitions,
    materialize_artifact_partition,
};
use ngkg_distributed_build::{
    SafeScanRequest, compare_roots, finalize_reducers, project_partition, reduce_projection_runs,
    safe_scan_trig,
};
use ngkg_locator::compile_sharded_locator;
use ngkg_reference::{ProjectionPolicy, sha256_path};
use ngkg_semantic_compiler::{
    CompilerHandoffManifest, MapLimits, ReduceLimits, finalize_dictionary, finalize_semantic_root,
    map_fragment, reduce_partition,
};
use uuid::Uuid;

mod object_stage;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(value) => {
            println!("{value}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("ngkg-distributed-worker: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<String, String> {
    let mut arguments = env::args().skip(1);
    let command = arguments.next().ok_or_else(usage)?;
    let options = parse_options(arguments.collect())?;
    match command.as_str() {
        "safe-scan" => safe_scan(&options),
        "project-partition" => project(&options),
        "reduce-range" => reduce(&options),
        "finalize-reducers" => finalize(&options),
        "compare-builds" => compare(&options),
        "materialize-artifact-partition" => materialize_artifact(&options),
        "finalize-artifact-partitions" => finalize_artifacts(&options),
        "compare-artifact-roots" => compare_artifacts(&options),
        "compile-mmap-locator" => compile_mmap_locator(&options),
        "semantic-map" => semantic_map(&options),
        "semantic-dictionary" => semantic_dictionary(&options),
        "semantic-partition" => semantic_partition(&options),
        "semantic-finalize" => semantic_finalize(&options),
        "plan-object-store" => object_stage::plan(&options)
            .await
            .map_err(|error| error.to_string()),
        "project-object-store" => object_stage::project(&options)
            .await
            .map_err(|error| error.to_string()),
        "reduce-object-store" => object_stage::reduce(&options)
            .await
            .map_err(|error| error.to_string()),
        "finalize-object-store" => object_stage::finalize(&options)
            .await
            .map_err(|error| error.to_string()),
        "prepare-artifacts-object-store" => object_stage::prepare_artifacts(&options)
            .await
            .map_err(|error| error.to_string()),
        "materialize-artifact-object-store" => {
            object_stage::materialize_artifact_object_store(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "finalize-artifacts-object-store" => {
            object_stage::finalize_artifacts_object_store(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "prepare-serving-root-object-store" => {
            object_stage::prepare_serving_root_object_store(&options)
                .await
                .map_err(|error| error.to_string())
        }
        _ => Err(usage()),
    }
}

fn semantic_map(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &[
            "handoff",
            "handoff-sha256",
            "fragment",
            "fragment-ordinal",
            "output-root",
            "max-fragment-bytes",
            "max-fragment-quads",
            "max-rows-in-memory",
            "max-run-bytes",
            "worker-threads",
        ],
    )?;
    let handoff_path = path(options, "handoff")?;
    let handoff_sha256 = value(options, "handoff-sha256")?;
    verify_hash(&handoff_path, &handoff_sha256)?;
    let handoff: CompilerHandoffManifest =
        serde_json::from_slice(&std::fs::read(&handoff_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let manifest = map_fragment(
        &handoff,
        &handoff_sha256,
        nonnegative_u32(options, "fragment-ordinal")?,
        &path(options, "fragment")?,
        &path(options, "output-root")?,
        MapLimits {
            max_fragment_bytes: positive_u64(options, "max-fragment-bytes")?,
            max_fragment_quads: positive_u64(options, "max-fragment-quads")?,
            max_rows_in_memory: positive_usize(options, "max-rows-in-memory")?,
            max_run_bytes: positive_u64(options, "max-run-bytes")?,
            worker_threads: positive_usize(options, "worker-threads")?,
        },
    )
    .map_err(|error| error.to_string())?;
    Ok(serde_json::json!({
        "status": "semantic-map-complete",
        "fragmentMapManifest": manifest,
        "fragmentMapManifestSha256": sha256_path(&manifest).map_err(|error| error.to_string())?
    })
    .to_string())
}

fn semantic_dictionary(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &[
            "handoff",
            "handoff-sha256",
            "map-manifest-list",
            "output-root",
        ],
    )?;
    let handoff_path = path(options, "handoff")?;
    let handoff_sha256 = value(options, "handoff-sha256")?;
    verify_hash(&handoff_path, &handoff_sha256)?;
    let handoff: CompilerHandoffManifest =
        serde_json::from_slice(&std::fs::read(&handoff_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let manifest = finalize_dictionary(
        &handoff,
        &handoff_sha256,
        &manifest_list(&path(options, "map-manifest-list")?)?,
        &path(options, "output-root")?,
    )
    .map_err(|error| error.to_string())?;
    Ok(serde_json::json!({
        "status": "semantic-dictionary-complete",
        "dictionaryManifest": manifest,
        "dictionaryManifestSha256": sha256_path(&manifest).map_err(|error| error.to_string())?
    })
    .to_string())
}

fn semantic_partition(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &[
            "handoff",
            "handoff-sha256",
            "map-manifest-list",
            "dictionary-manifest",
            "dictionary-manifest-sha256",
            "partition-index",
            "output-root",
            "max-input-runs",
            "max-partition-quads",
            "parquet-row-group-rows",
        ],
    )?;
    let handoff_path = path(options, "handoff")?;
    let handoff_sha256 = value(options, "handoff-sha256")?;
    verify_hash(&handoff_path, &handoff_sha256)?;
    let handoff: CompilerHandoffManifest =
        serde_json::from_slice(&std::fs::read(&handoff_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let manifest = reduce_partition(
        &handoff,
        &handoff_sha256,
        &manifest_list(&path(options, "map-manifest-list")?)?,
        &path(options, "dictionary-manifest")?,
        &value(options, "dictionary-manifest-sha256")?,
        nonnegative_u32(options, "partition-index")?,
        &path(options, "output-root")?,
        ReduceLimits {
            max_input_runs: positive_usize(options, "max-input-runs")?,
            max_partition_quads: positive_u64(options, "max-partition-quads")?,
            parquet_row_group_rows: positive_usize(options, "parquet-row-group-rows")?,
        },
    )
    .map_err(|error| error.to_string())?;
    Ok(serde_json::json!({
        "status": "semantic-partition-complete",
        "semanticPartitionManifest": manifest,
        "semanticPartitionManifestSha256": sha256_path(&manifest).map_err(|error| error.to_string())?
    })
    .to_string())
}

fn semantic_finalize(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &[
            "handoff",
            "handoff-sha256",
            "dictionary-manifest",
            "dictionary-manifest-sha256",
            "partition-manifest-list",
            "output-root",
        ],
    )?;
    let handoff_path = path(options, "handoff")?;
    let handoff_sha256 = value(options, "handoff-sha256")?;
    verify_hash(&handoff_path, &handoff_sha256)?;
    let handoff: CompilerHandoffManifest =
        serde_json::from_slice(&std::fs::read(&handoff_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let manifest = finalize_semantic_root(
        &handoff,
        &handoff_sha256,
        &path(options, "dictionary-manifest")?,
        &value(options, "dictionary-manifest-sha256")?,
        &manifest_list(&path(options, "partition-manifest-list")?)?,
        &path(options, "output-root")?,
    )
    .map_err(|error| error.to_string())?;
    Ok(serde_json::json!({
        "status": "semantic-compilation-complete-inactive",
        "semanticCompilationRoot": manifest,
        "semanticCompilationRootSha256": sha256_path(&manifest).map_err(|error| error.to_string())?
    })
    .to_string())
}

fn compile_mmap_locator(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &["locator", "locator-sha256", "snapshot-id", "output"],
    )?;
    let locator = path(options, "locator")?;
    let output = path(options, "output")?;
    let count = compile_sharded_locator(
        &locator,
        &value(options, "locator-sha256")?,
        uuid(options, "snapshot-id")?,
        &output,
    )
    .map_err(|error| error.to_string())?;
    let output_sha256 = sha256_path(&output).map_err(|error| error.to_string())?;
    Ok(serde_json::json!({
        "status":"mmap-locator-compiled",
        "output":output,
        "outputSha256":output_sha256,
        "recordCount":count
    })
    .to_string())
}

fn materialize_artifact(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &[
            "source-plan",
            "source-plan-sha256",
            "dictionary",
            "dictionary-sha256",
            "partition-index",
            "dataset-namespace",
            "source-guid",
            "source-snapshot",
            "source-sha256",
            "projection-policy",
            "projection-policy-sha256",
            "output-root",
            "max-quads",
            "row-group-rows",
        ],
    )?;
    let policy_path = path(options, "projection-policy")?;
    verify_hash(&policy_path, &value(options, "projection-policy-sha256")?)?;
    let policy: ProjectionPolicy =
        serde_json::from_slice(&std::fs::read(&policy_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let source_sha256 = value(options, "source-sha256")?;
    let source_snapshot = value(options, "source-snapshot")?;
    let request = ArtifactPartitionRequest {
        source_sha256: &source_sha256,
        dataset_namespace: uuid(options, "dataset-namespace")?,
        source_guid: uuid(options, "source-guid")?,
        source_snapshot: &source_snapshot,
        projection_policy: &policy,
        max_quads: positive_u64(options, "max-quads")?,
        row_group_rows: positive_usize(options, "row-group-rows")?,
    };
    let manifest = materialize_artifact_partition(
        &path(options, "source-plan")?,
        &value(options, "source-plan-sha256")?,
        &path(options, "dictionary")?,
        &value(options, "dictionary-sha256")?,
        nonnegative_u32(options, "partition-index")?,
        &path(options, "output-root")?,
        &request,
    )
    .map_err(|error| error.to_string())?;
    let sha = sha256_path(&manifest).map_err(|error| error.to_string())?;
    Ok(serde_json::json!({
        "status":"artifact-partition-materialized",
        "artifactPartitionManifest":manifest,
        "artifactPartitionManifestSha256":sha
    })
    .to_string())
}

fn finalize_artifacts(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &[
            "source-plan",
            "source-plan-sha256",
            "dictionary",
            "dictionary-sha256",
            "artifact-manifest-list",
            "output-root",
        ],
    )?;
    let manifests = manifest_list(&path(options, "artifact-manifest-list")?)?;
    let root = finalize_artifact_partitions(
        &path(options, "source-plan")?,
        &value(options, "source-plan-sha256")?,
        &path(options, "dictionary")?,
        &value(options, "dictionary-sha256")?,
        &manifests,
        &path(options, "output-root")?,
    )
    .map_err(|error| error.to_string())?;
    let sha = sha256_path(&root).map_err(|error| error.to_string())?;
    Ok(serde_json::json!({
        "status":"artifact-root-finalized",
        "distributedArtifactRoot":root,
        "distributedArtifactRootSha256":sha
    })
    .to_string())
}

fn compare_artifacts(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(options, &["baseline-root", "candidate-root", "report"])?;
    let report = compare_artifact_roots(
        &path(options, "baseline-root")?,
        &path(options, "candidate-root")?,
        &path(options, "report")?,
    )
    .map_err(|error| error.to_string())?;
    if !report.equivalent {
        return Err(format!(
            "artifact roots are not equivalent: {}",
            report.mismatches.join("; ")
        ));
    }
    serde_json::to_string(&report).map_err(|error| error.to_string())
}

fn safe_scan(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &[
            "source",
            "source-sha256",
            "projection-policy",
            "projection-policy-sha256",
            "output-root",
            "dataset-id",
            "snapshot-id",
            "dataset-namespace",
            "source-guid",
            "source-snapshot",
            "logical-partitions",
            "max-quads",
        ],
    )?;
    let policy_path = path(options, "projection-policy")?;
    verify_hash(&policy_path, &value(options, "projection-policy-sha256")?)?;
    let policy: ProjectionPolicy =
        serde_json::from_slice(&std::fs::read(&policy_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let source = path(options, "source")?;
    verify_hash(&source, &value(options, "source-sha256")?)?;
    let source_sha256 = value(options, "source-sha256")?;
    let policy_sha256 = value(options, "projection-policy-sha256")?;
    let source_snapshot = value(options, "source-snapshot")?;
    let request = SafeScanRequest {
        dataset_id: uuid(options, "dataset-id")?,
        snapshot_id: uuid(options, "snapshot-id")?,
        dataset_namespace: uuid(options, "dataset-namespace")?,
        source_guid: uuid(options, "source-guid")?,
        source_snapshot: &source_snapshot,
        source_sha256: &source_sha256,
        projection_policy_sha256: &policy_sha256,
        projection_policy: &policy,
        logical_partition_count: positive_u32(options, "logical-partitions")?,
        max_quads: positive_u64(options, "max-quads")?,
    };
    let plan = safe_scan_trig(&source, &path(options, "output-root")?, &request)
        .map_err(|error| error.to_string())?;
    let sha = sha256_path(&plan).map_err(|error| error.to_string())?;
    Ok(
        serde_json::json!({"status":"source-planned","sourcePlan":plan,"sourcePlanSha256":sha})
            .to_string(),
    )
}

fn project(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &[
            "source-plan",
            "source-plan-sha256",
            "partition-index",
            "dataset-namespace",
            "source-guid",
            "source-snapshot",
            "projection-policy",
            "projection-policy-sha256",
            "output-root",
            "max-quads",
        ],
    )?;
    let policy_path = path(options, "projection-policy")?;
    verify_hash(&policy_path, &value(options, "projection-policy-sha256")?)?;
    let policy: ProjectionPolicy =
        serde_json::from_slice(&std::fs::read(&policy_path).map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
    let manifest = project_partition(
        &path(options, "source-plan")?,
        &value(options, "source-plan-sha256")?,
        nonnegative_u32(options, "partition-index")?,
        uuid(options, "dataset-namespace")?,
        uuid(options, "source-guid")?,
        &value(options, "source-snapshot")?,
        &policy,
        &path(options, "output-root")?,
        positive_u64(options, "max-quads")?,
    )
    .map_err(|error| error.to_string())?;
    let sha = sha256_path(&manifest).map_err(|error| error.to_string())?;
    Ok(serde_json::json!({"status":"projected","projectionManifest":manifest,"projectionManifestSha256":sha}).to_string())
}

fn reduce(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &[
            "source-plan",
            "source-plan-sha256",
            "projection-manifest-list",
            "reducer-index",
            "reducer-count",
            "output-root",
        ],
    )?;
    let manifests = manifest_list(&path(options, "projection-manifest-list")?)?;
    let manifest = reduce_projection_runs(
        &path(options, "source-plan")?,
        &value(options, "source-plan-sha256")?,
        &manifests,
        nonnegative_u32(options, "reducer-index")?,
        positive_u32(options, "reducer-count")?,
        &path(options, "output-root")?,
    )
    .map_err(|error| error.to_string())?;
    let sha = sha256_path(&manifest).map_err(|error| error.to_string())?;
    Ok(serde_json::json!({"status":"reduced","reducerManifest":manifest,"reducerManifestSha256":sha}).to_string())
}

fn finalize(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(
        options,
        &[
            "source-plan",
            "source-plan-sha256",
            "reducer-manifest-list",
            "output-root",
        ],
    )?;
    let manifests = manifest_list(&path(options, "reducer-manifest-list")?)?;
    let manifest = finalize_reducers(
        &path(options, "source-plan")?,
        &value(options, "source-plan-sha256")?,
        &manifests,
        &path(options, "output-root")?,
    )
    .map_err(|error| error.to_string())?;
    let sha = sha256_path(&manifest).map_err(|error| error.to_string())?;
    Ok(serde_json::json!({"status":"finalized","distributedRoot":manifest,"distributedRootSha256":sha}).to_string())
}

fn compare(options: &BTreeMap<String, String>) -> Result<String, String> {
    reject_unknown(options, &["baseline-root", "candidate-root", "report"])?;
    let report = compare_roots(
        &path(options, "baseline-root")?,
        &path(options, "candidate-root")?,
        &path(options, "report")?,
    )
    .map_err(|error| error.to_string())?;
    if !report.equivalent {
        return Err(format!(
            "builds are not equivalent: {}",
            report.mismatches.join("; ")
        ));
    }
    serde_json::to_string(&report).map_err(|error| error.to_string())
}

fn manifest_list(path: &PathBuf) -> Result<Vec<PathBuf>, String> {
    let bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    let values: Vec<String> = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if values.is_empty() {
        return Err(format!("manifest list is empty: {}", path.display()));
    }
    values
        .into_iter()
        .map(|value| {
            if value.is_empty() {
                Err("manifest list contains an empty path".to_owned())
            } else {
                Ok(PathBuf::from(value))
            }
        })
        .collect()
}

fn parse_options(arguments: Vec<String>) -> Result<BTreeMap<String, String>, String> {
    if arguments.len() % 2 != 0 {
        return Err("every --option requires one value".to_owned());
    }
    let mut output = BTreeMap::new();
    for pair in arguments.chunks_exact(2) {
        let name = pair[0]
            .strip_prefix("--")
            .filter(|value| !value.is_empty())
            .ok_or_else(|| format!("invalid option {}", pair[0]))?;
        if output.insert(name.to_owned(), pair[1].clone()).is_some() {
            return Err(format!("duplicate option --{name}"));
        }
    }
    Ok(output)
}

fn reject_unknown(options: &BTreeMap<String, String>, allowed: &[&str]) -> Result<(), String> {
    if let Some(name) = options
        .keys()
        .find(|name| !allowed.contains(&name.as_str()))
    {
        return Err(format!("unknown option --{name}"));
    }
    Ok(())
}

fn value(options: &BTreeMap<String, String>, name: &str) -> Result<String, String> {
    options
        .get(name)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| format!("--{name} is required"))
}

fn path(options: &BTreeMap<String, String>, name: &str) -> Result<PathBuf, String> {
    value(options, name).map(PathBuf::from)
}

fn uuid(options: &BTreeMap<String, String>, name: &str) -> Result<Uuid, String> {
    value(options, name)?
        .parse::<Uuid>()
        .map_err(|_| format!("--{name} must be a UUID"))
}

fn positive_u64(options: &BTreeMap<String, String>, name: &str) -> Result<u64, String> {
    value(options, name)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("--{name} must be a positive integer"))
}

fn positive_u32(options: &BTreeMap<String, String>, name: &str) -> Result<u32, String> {
    value(options, name)?
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("--{name} must be a positive 32-bit integer"))
}

fn positive_usize(options: &BTreeMap<String, String>, name: &str) -> Result<usize, String> {
    value(options, name)?
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("--{name} must be a positive platform-sized integer"))
}

fn nonnegative_u32(options: &BTreeMap<String, String>, name: &str) -> Result<u32, String> {
    value(options, name)?
        .parse::<u32>()
        .map_err(|_| format!("--{name} must be a non-negative 32-bit integer"))
}

fn verify_hash(path: &Path, expected: &str) -> Result<(), String> {
    let observed = sha256_path(path).map_err(|error| error.to_string())?;
    if observed != expected {
        return Err(format!("checksum mismatch for {}", path.display()));
    }
    Ok(())
}

fn usage() -> String {
    "usage: ngkg-distributed-worker safe-scan|project-partition|reduce-range|finalize-reducers|compare-builds|materialize-artifact-partition|finalize-artifact-partitions|compare-artifact-roots|compile-mmap-locator|semantic-map|semantic-dictionary|semantic-partition|semantic-finalize|plan-object-store|project-object-store|reduce-object-store|finalize-object-store|prepare-serving-root-object-store with command-specific immutable options documented in docs/phases".to_owned()
}
