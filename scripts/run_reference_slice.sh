#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${NGKG_REFERENCE_OUTPUT_ROOT:-}" ]]; then
  echo "NGKG_REFERENCE_OUTPUT_ROOT is required" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output_root="$(realpath "${NGKG_REFERENCE_OUTPUT_ROOT}")"
java_executable="$(command -v java)"

mvn --batch-mode --no-transfer-progress -f "${repo_root}/adapters/hermit-reasoner/pom.xml" clean package
adapter_jar="${repo_root}/adapters/hermit-reasoner/target/ngkg-hermit-adapter.jar"
adapter_sha256="$(sha256sum "${adapter_jar}" | cut -d ' ' -f 1)"
manifest="${output_root}/reference-request.json"

python3 "${repo_root}/scripts/create_reference_manifest.py" \
  --source "${repo_root}/test-corpus/datasets/cross-domain.trig" \
  --ontology "${repo_root}/test-corpus/ontologies/core.ttl" \
  --projection-policy "${repo_root}/test-corpus/reference/projection-policy.json" \
  --query "${repo_root}/test-corpus/queries/q01-cross-domain.rq" \
  --expected "${repo_root}/test-corpus/expected/q01-cross-domain.srj" \
  --query-id q01-cross-domain \
  --ordered false \
  --required-source-iri https://ngkg.io/id/source-1 \
  --required-source-iri https://ngkg.io/id/source-2 \
  --closure-graph-iri urn:ngkg:graph:reference-closure \
  --dataset-id 4d2e1a82-c2bc-536a-a809-fda7643ef1f7 \
  --snapshot-id 91054ecb-2f68-5a63-b31a-137333c64a7c \
  --dataset-namespace 7b8e1c18-9c22-5b58-a2b4-7cbf21cc9b2b \
  --source-guid d2cfd1d5-5f83-5bb3-9888-f3bc3229a760 \
  --source-snapshot reference-corpus-v1 \
  --output-directory "${output_root}" \
  --manifest-output "${manifest}" \
  --max-input-bytes 10485760 \
  --max-quads 100000 \
  --max-dictionary-terms 100000 \
  --max-reasoner-seconds 300 \
  --parquet-row-group-rows 65536 \
  --max-named-individuals 100000 \
  --max-properties 10000

cargo run --locked --release --package ngkg-reference-worker -- \
  compile \
  --manifest "${manifest}" \
  --allowed-input-root "${repo_root}" \
  --allowed-output-root "${output_root}" \
  --java-executable "${java_executable}" \
  --reasoner-adapter-jar "${adapter_jar}" \
  --reasoner-adapter-sha256 "${adapter_sha256}" \
  --reasoner-name HermiT \
  --reasoner-version 1.4.5.519 \
  --ceiling-input-bytes 10485760 \
  --ceiling-quads 100000 \
  --ceiling-dictionary-terms 100000 \
  --ceiling-reasoner-seconds 300 \
  --ceiling-parquet-row-group-rows 65536 \
  --ceiling-named-individuals 100000 \
  --ceiling-properties 10000

snapshot_manifest="${output_root}/91054ecb-2f68-5a63-b31a-137333c64a7c/snapshot-manifest.json"
snapshot_sha256="$(sha256sum "${snapshot_manifest}" | cut -d ' ' -f 1)"
cargo run --locked --release --package ngkg-reference-worker -- \
  query \
  --snapshot "${snapshot_manifest}" \
  --snapshot-sha256 "${snapshot_sha256}" \
  --query "${repo_root}/test-corpus/queries/q01-cross-domain.rq" \
  --allowed-query-root "${repo_root}/test-corpus/queries" \
  --output "${output_root}/q01-result.json" \
  --hydrate-payload true

python3 "${repo_root}/scripts/verify_reference_output.py" \
  --snapshot "${snapshot_manifest}" \
  --result "${output_root}/q01-result.json"
