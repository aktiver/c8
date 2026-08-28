//! Kubernetes Job/CLI entry point for the first real reference vertical slice.

use std::{collections::BTreeMap, env, path::PathBuf, process::ExitCode};

use ngkg_reference::{
    InputArtifact, TrustedReasonerConfig, TrustedResourceCeilings, compile_from_manifest,
    execute_snapshot_query, sha256_path, write_query_result,
};

mod object_compile;
mod direct_job;
mod cloud_import;
mod cloud_decode;
mod cloud_semantic;
mod cloud_ontology;
mod cloud_offline;
mod cloud_activate;
mod phase40_limits;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(message) => {
            println!("{message}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("ngkg-reference-worker: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<String, String> {
    let mut arguments = env::args().skip(1);
    let command = arguments.next().ok_or_else(usage)?;
    let options = parse_options(arguments.collect())?;
    match command.as_str() {
        "cloud-import" => {
            reject_unknown(
                &options,
                &[
                    "namespace",
                    "import-name",
                    "source-root",
                    "artifact-base-url",
                    "scratch-root",
                    "single-put-max-bytes",
                    "multipart-buffer-bytes",
                    "multipart-concurrency",
                    "scan-concurrency",
                    "decode-target-work-bytes",
                    "decode-max-work-items",
                    "decode-max-plan-bytes",
                ],
            )?;
            cloud_import::execute(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "cloud-decode" => {
            reject_unknown(
                &options,
                &[
                    "source-root",
                    "artifact-base-url",
                    "scratch-root",
                    "decode-plan-object-key",
                    "decode-plan-sha256",
                    "decode-max-plan-bytes",
                    "decode-max-fragment-bytes",
                    "decode-object-concurrency",
                    "completion-index",
                    "single-put-max-bytes",
                    "multipart-buffer-bytes",
                    "multipart-concurrency",
                ],
            )?;
            cloud_decode::execute_decode(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "cloud-decode-finalize" => {
            reject_unknown(
                &options,
                &[
                    "namespace",
                    "import-name",
                    "artifact-base-url",
                    "scratch-root",
                    "decode-plan-object-key",
                    "decode-plan-sha256",
                    "decode-max-plan-bytes",
                    "decode-max-completion-manifest-bytes",
                    "decode-max-fragment-bytes",
                    "decode-finalize-concurrency",
                    "single-put-max-bytes",
                    "multipart-buffer-bytes",
                    "multipart-concurrency",
                ],
            )?;
            cloud_decode::execute_finalize(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "cloud-semantic-map" => {
            cloud_semantic::execute_map(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "cloud-semantic-dictionary" => {
            cloud_semantic::execute_dictionary(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "cloud-semantic-partition" => {
            cloud_semantic::execute_partition(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "cloud-semantic-finalize" => {
            cloud_semantic::execute_finalize(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "cloud-ontology-project" => {
            cloud_ontology::execute_project(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "cloud-ontology-assemble" => {
            cloud_ontology::execute_assemble(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "cloud-ontology-qualify" => {
            cloud_ontology::execute_qualify(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "cloud-offline-plan" => cloud_offline::execute_plan(&options)
            .await.map_err(|error| error.to_string()),
        "cloud-offline-partition" => cloud_offline::execute_partition(&options)
            .await.map_err(|error| error.to_string()),
        "cloud-offline-finalize" => cloud_offline::execute_finalize(&options)
            .await.map_err(|error| error.to_string()),
        "cloud-snapshot-activate" => cloud_activate::execute(&options)
            .await.map_err(|error| error.to_string()),
        "compile-object-store" => {
            reject_unknown(
                &options,
                &[
                    "tenant-id",
                    "operation-id",
                    "dataset-id",
                    "target-snapshot-id",
                    "bundle-object-key",
                    "bundle-sha256",
                    "scratch-root",
                    "java-executable",
                    "reasoner-adapter-jar",
                    "reasoner-adapter-sha256",
                    "reasoner-name",
                    "reasoner-version",
                    "ceiling-bundle-bytes",
                    "ceiling-staged-object-bytes",
                    "ceiling-staged-total-bytes",
                    "ceiling-staged-artifacts",
                    "ceiling-output-bytes",
                    "ceiling-output-artifacts",
                    "ceiling-input-bytes",
                    "ceiling-quads",
                    "ceiling-dictionary-terms",
                    "ceiling-reasoner-seconds",
                    "ceiling-parquet-row-group-rows",
                    "ceiling-named-individuals",
                    "ceiling-properties",
                    "download-concurrency",
                    "upload-concurrency",
                    "single-put-max-bytes",
                    "multipart-buffer-bytes",
                    "multipart-concurrency",
                    "distributed-root-object-key",
                    "distributed-root-sha256",
                    "distributed-artifact-root-object-key",
                    "distributed-artifact-root-sha256",
                    "distributed-serving-root-object-key",
                    "distributed-serving-root-sha256",
                    "hydration-worker-threads",
                    "ceiling-hydration-rows",
                ],
            )?;
            object_compile::compile_object_store(&options)
                .await
                .map_err(|error| error.to_string())
        }
        "compile" => {
            reject_unknown(
                &options,
                &[
                    "manifest",
                    "allowed-input-root",
                    "allowed-output-root",
                    "java-executable",
                    "reasoner-adapter-jar",
                    "reasoner-adapter-sha256",
                    "reasoner-name",
                    "reasoner-version",
                    "ceiling-input-bytes",
                    "ceiling-quads",
                    "ceiling-dictionary-terms",
                    "ceiling-reasoner-seconds",
                    "ceiling-parquet-row-group-rows",
                    "ceiling-named-individuals",
                    "ceiling-properties",
                ],
            )?;
            let manifest = required_path(&options, "manifest")?;
            let input_root = required_path(&options, "allowed-input-root")?;
            let output_root = required_path(&options, "allowed-output-root")?;
            let trusted_reasoner = TrustedReasonerConfig {
                java_executable: required_path(&options, "java-executable")?,
                adapter_jar: InputArtifact {
                    path: required_path(&options, "reasoner-adapter-jar")?,
                    sha256: required_value(&options, "reasoner-adapter-sha256")?,
                },
                expected_name: required_value(&options, "reasoner-name")?,
                expected_version: required_value(&options, "reasoner-version")?,
            };
            let ceilings = TrustedResourceCeilings {
                max_input_bytes: required_u64(&options, "ceiling-input-bytes")?,
                max_quads: required_u64(&options, "ceiling-quads")?,
                max_dictionary_terms: required_u64(&options, "ceiling-dictionary-terms")?,
                max_reasoner_seconds: required_u64(&options, "ceiling-reasoner-seconds")?,
                max_parquet_row_group_rows: required_usize(&options, "ceiling-parquet-row-group-rows")?,
                max_named_individuals: required_u64(&options, "ceiling-named-individuals")?,
                max_properties: required_u64(&options, "ceiling-properties")?,
            };
            let snapshot = compile_from_manifest(
                &manifest,
                &input_root,
                &output_root,
                &trusted_reasoner,
                ceilings,
            )
                .map_err(|error| error.to_string())?;
            let snapshot_sha256 = sha256_path(&snapshot).map_err(|error| error.to_string())?;
            Ok(serde_json::json!({
                "status": "compiled",
                "snapshotManifest": snapshot,
                "snapshotManifestSha256": snapshot_sha256
            })
            .to_string())
        }
        "direct-bgp" => {
            reject_unknown(&options, &["job"])?;
            let job = required_path(&options, "job")?;
            direct_job::execute(&job)
        }
        "query" => {
            reject_unknown(
                &options,
                &[
                    "snapshot",
                    "snapshot-sha256",
                    "query",
                    "allowed-query-root",
                    "output",
                    "hydrate-payload",
                ],
            )?;
            let snapshot = required_path(&options, "snapshot")?;
            let query = required_path(&options, "query")?;
            let query_root = required_path(&options, "allowed-query-root")?;
            let output = required_path(&options, "output")?;
            let hydrate = match options.get("hydrate-payload").map(String::as_str) {
                None | Some("true") => true,
                Some("false") => false,
                Some(_) => return Err("--hydrate-payload must be true or false".to_owned()),
            };
            let snapshot_sha256 = required_value(&options, "snapshot-sha256")?;
            let result = execute_snapshot_query(&snapshot, &snapshot_sha256, &query, &query_root, hydrate)
                .map_err(|error| error.to_string())?;
            write_query_result(&output, &result).map_err(|error| error.to_string())?;
            Ok(serde_json::json!({"status": "queried", "output": output}).to_string())
        }
        _ => Err(usage()),
    }
}

fn parse_options(arguments: Vec<String>) -> Result<BTreeMap<String, String>, String> {
    if arguments.len() % 2 != 0 {
        return Err("every --option requires exactly one value".to_owned());
    }
    let mut options = BTreeMap::new();
    for pair in arguments.chunks_exact(2) {
        let Some(name) = pair[0].strip_prefix("--") else {
            return Err(format!("option must begin with --: {}", pair[0]));
        };
        if name.is_empty() || options.insert(name.to_owned(), pair[1].clone()).is_some() {
            return Err(format!("empty or duplicate option: {}", pair[0]));
        }
    }
    Ok(options)
}

fn required_path(options: &BTreeMap<String, String>, name: &str) -> Result<PathBuf, String> {
    options
        .get(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| format!("--{name} is required"))
}

fn required_value(options: &BTreeMap<String, String>, name: &str) -> Result<String, String> {
    options
        .get(name)
        .filter(|value| !value.is_empty())
        .cloned()
        .ok_or_else(|| format!("--{name} is required"))
}

fn required_u64(options: &BTreeMap<String, String>, name: &str) -> Result<u64, String> {
    let value = required_value(options, name)?;
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("--{name} must be a positive 64-bit integer"))
}

fn required_usize(options: &BTreeMap<String, String>, name: &str) -> Result<usize, String> {
    let value = required_value(options, name)?;
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("--{name} must be a positive platform-sized integer"))
}

fn reject_unknown(options: &BTreeMap<String, String>, allowed: &[&str]) -> Result<(), String> {
    if let Some(name) = options.keys().find(|name| !allowed.contains(&name.as_str())) {
        return Err(format!("unknown option --{name}"));
    }
    Ok(())
}

fn usage() -> String {
    "usage: ngkg-reference-worker cloud-import <bounded discovery/planning options> | cloud-decode <indexed whole-TriG decode options> | cloud-decode-finalize <all-completions barrier options> | direct-bgp --job PATH | compile-object-store <all catalog/storage/trust/bound options> | compile <local compiler options> | query <snapshot query options>".to_owned()
}
