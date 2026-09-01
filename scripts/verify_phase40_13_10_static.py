#!/usr/bin/env python3
"""Static contracts for Phase 40.13.10 named datasets and existing-cloud TriG import."""

from __future__ import annotations

import json
import pathlib
import re
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(relative: str, *needles: str) -> str:
    text = (ROOT / relative).read_text(encoding="utf-8")
    for needle in needles:
        if needle not in text:
            raise RuntimeError(f"{relative} is missing {needle!r}")
    return text


def main() -> int:
    migration = require(
        "migrations/0006_named_datasets.sql",
        "dataset_name TEXT",
        "UNIQUE (tenant_id, dataset_name)",
        "^[a-z][a-z0-9_]{0,62}$",
    )
    catalog = require(
        "crates/ngkg-catalog/src/lib.rs",
        "DatasetRecord",
        "create_or_get_named_dataset",
        "resolve_dataset_name",
        "Uuid::new_v4()",
    )
    versions = [int(value) for value in re.findall(
        r"catalog migrations through version ([0-9]+) are required", catalog
    )]
    if not versions or max(versions) < 6:
        raise RuntimeError("catalog readiness no longer requires the named-dataset migration")
    api = require(
        "services/api/src/main.rs",
        '"/v1/datasets", post(create_named_dataset)',
        '"/v1/datasets/{dataset_name}/imports"',
        '"/v1/datasets/{dataset_name}/imports/{operation_id}"',
        "CloudObjectVersionPolicy::RequireImmutableChecksum",
        "Idempotency-Key is already bound to a different cloud import",
        "alignment, closure, and provenance",
        "Uuid::new_v5(&operation_id, b\"ngkg-cloud-import-target-snapshot-v1\")",
    )
    kube_contract = require(
        "crates/ngkg-kube/src/lib.rs",
        "NgkgSourceImport",
        "identity_ref",
        "logical_partitions",
        "source_manifest_sha256",
    )
    worker = require(
        "services/reference-worker/src/cloud_import.rs",
        'const SOURCE_MOUNT: &str = "/source"',
        "buffer_unordered(concurrency)",
        "spawn_blocking",
        "RdfFormat::TriG",
        "source object changed while it was being frozen",
        "blank-node graph names are forbidden",
        "put_file_immutable",
        "SourceManifestPublished",
    )
    operator = require(
        "services/operator/src/main.rs",
        '"s3.csi.aws.com"',
        '"blob.csi.azure.com"',
        '"gcsfuse.csi.storage.gke.io"',
        "read_only: Some(true)",
        '"kueue.x-k8s.io/queue-name"',
        '"source-ingestion"',
        "ensure_import_worker_rbac",
        "SourceManifestMissing",
    )
    crd = yaml.safe_load(
        (ROOT / "charts/ngkg-crds/crds/ngkg.io_ngkgsourceimports.yaml").read_text()
    )
    if crd["spec"]["scope"] != "Namespaced":
        raise RuntimeError("NgkgSourceImport must remain tenant-workload namespaced")
    source_schema = json.loads(
        (ROOT / "contracts/cloud-source-manifest.schema.json").read_text()
    )
    if source_schema["properties"]["versionPolicy"].get("const") != "require-immutable-checksum":
        raise RuntimeError("source manifest version policy is not fail closed")
    values = yaml.safe_load((ROOT / "charts/ngkg-workloads/values.yaml").read_text())
    if values["hpcNodeGroups"].get("source_ingestion_num_of_nodes") != 0:
        raise RuntimeError("source-ingestion nodes must be allowed to begin at zero")
    if values["autoscaling"]["sourceIngestion"]["maxNodes"] < 2:
        raise RuntimeError("source-ingestion node pool cannot scale out")
    kueue = require(
        "charts/ngkg-workloads/templates/kueue.yaml",
        "ngkg-source-ingestion",
        "quotas.sourceIngestion.cpu",
        "quotas.sourceIngestion.memory",
    )
    openapi = require(
        "api/openapi.yaml",
        "createNamedDataset",
        "createCloudImportByName",
        "getCloudImportByName",
        "clinical_nutrition",
        "loyalty_graph",
    )
    rbac = require(
        "charts/ngkg-platform/templates/serviceaccounts-rbac.yaml",
        "ngkgsourceimports/status",
        "persistentvolumes",
        "rolebindings",
    )
    require(
        "acceptance/phase-gates.yaml",
        "phase: '40.13.10'",
        "scripts/qualify_phase40_13_10.sh",
    )
    forbidden_request_fields = ("accessKey", "secretKey", "clientSecret", "sasToken")
    request_definition = api[api.index("struct CreateCloudImportRequest"):api.index("struct CloudImportAccepted")]
    if any(field in request_definition for field in forbidden_request_fields):
        raise RuntimeError("inline cloud credentials entered the REST request contract")
    if "align_ontology" in worker or "raw_data_mapping" in operator:
        raise RuntimeError("ontology alignment or raw-data mapping entered cloud import")
    if not all((migration, catalog, api, kube_contract, worker, operator, kueue, openapi, rbac)):
        raise RuntimeError("empty Phase 40.13.10 source")
    print("phase 40.13.10 static qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.10 static qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
