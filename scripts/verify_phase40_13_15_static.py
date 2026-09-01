#!/usr/bin/env python3
"""Static source contract for Phase 40.13.15 (not a native qualification)."""

from pathlib import Path
import json
import yaml

ROOT = Path(__file__).resolve().parents[1]


def need(path: str, tokens: list[str]) -> None:
    text = (ROOT / path).read_text()
    missing = [token for token in tokens if token not in text]
    if missing:
        raise SystemExit(f"{path}: missing {missing}")


need("crates/ngkg-snapshot-activation/src/lib.rs", [
    "SnapshotActivationManifest", "validate_inputs", "build_serving_artifacts",
    "unknown_routes_to_exact_hermit", "all_partitions_verified",
    "https://c8-next-generation.io/", "/alignment", "/closure", "/provenance",
])
need("migrations/0007_cloud_snapshot_activation.sql", [
    "cloud_snapshot_activation", "ENABLE ROW LEVEL SECURITY",
    "cloud_snapshot_activation_immutable", "reference_manifest_sha256",
])
need("crates/ngkg-catalog/src/lib.rs", [
    "commit_cloud_snapshot_activation", "AutomaticAfterCertification",
    "active_snapshot_id IS NOT DISTINCT FROM", "activation_ready", "legacy_ready",
])
need("services/reference-worker/src/cloud_activate.rs", [
    "buffer_unordered", "verify_offline_partitions", "commit_cloud_snapshot_activation",
    "SnapshotPublishedAtomically", "NGKG_DATABASE_URL",
])
need("services/operator/src/main.rs", [
    "cloud-snapshot-activate", "snapshot-activation", "activation-verify-concurrency",
    "NGKG_DATABASE_URL", "Kueue" if "Kueue" in (ROOT / "services/operator/src/main.rs").read_text() else "kueue.x-k8s.io/queue-name",
])
need("services/online-serving/src/main.rs", [
    "cloud_activation", "required_serving_root", "supports semantic querying",
])
need("services/api/src/main.rs", [
    "create_or_get_compilation", "create_cloud_import", "durable.operation.operation_id",
])
need("api/openapi.yaml", ["snapshotActivationManifestSha256", "snapshotPublicationState"])
need("api/online-openapi.yaml", [
    "/v1/datasets/{datasetId}/sparql", "/v1/datasets/{datasetId}/query",
])

json.loads((ROOT / "charts/ngkg-platform/values.schema.json").read_text())
for path in [
    "charts/ngkg-platform/values.yaml",
    "charts/ngkg-crds/crds/ngkg.io_ngkgsourceimports.yaml",
]:
    list(yaml.safe_load_all((ROOT / path).read_text()))

for forbidden in ["ontology alignment job", "schema matching job", "raw data mapping job"]:
    if forbidden in (ROOT / "crates/ngkg-snapshot-activation/src/lib.rs").read_text().lower():
        raise SystemExit(f"forbidden scope introduced: {forbidden}")

print(json.dumps({
    "phase": "40.13.15",
    "status": "passed",
    "checks": 10,
    "publication": "transactional compare-and-swap",
    "queryActivation": "scalar semantic path; hydration fail-closed",
}))
