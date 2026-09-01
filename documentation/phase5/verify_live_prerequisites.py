#!/usr/bin/env python3
"""Verify signed Phase 3, affected Phase 4, and Phase 5 live evidence before qualification."""
from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys

REQUIRED = {
    "phase3-certificate.json": {
        "oci_supply_chain", "postgres_ha", "rke2", "eks", "aks", "gke",
        "api", "mcp", "hermit", "autoscaling", "recovery", "gpu", "tenant_isolation",
    },
    "phase4-live-certificate.json": {
        "concurrent_status_writers", "operator_restart", "expired_jobs",
        "idempotent_object_retry", "multipart_conflict", "parallel_spill",
        "invalid_accept_no_execution", "large_hash_query_responsiveness",
        "datatype_differential", "blank_node_differential", "federation_ssrf",
        "azure_artifacts", "gcs_artifacts",
    },
    "phase5-live-certificate.json": {
        "native_parquet_leaf_scan", "multinode_partition_barrier",
        "partition_loss_no_partial_result", "duplicate_delivery_idempotency",
        "conflicting_completion_rejected", "cross_tenant_graph_filter",
        "closure_covered_bgp", "exact_uncovered_bgp", "scalar_public_fallback_absent",
        "sparql_multiset_differential", "property_path_differential",
        "bounded_spill_checkpoint_recovery", "hpa_cpu_80", "hpa_memory_80",
    },
}


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify(path: pathlib.Path, required: set[str]) -> None:
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("status") != "QUALIFIED" or document.get("signed") is not True:
        raise RuntimeError(f"{path.name} is not signed and QUALIFIED")
    observed = {
        scenario["id"] for scenario in document.get("scenarios", [])
        if scenario.get("status") == "PASS" and scenario.get("evidenceSha256")
    }
    missing = sorted(required - observed)
    if missing:
        raise RuntimeError(f"{path.name} lacks passing evidence: {', '.join(missing)}")
    signature = document.get("signature", {})
    if not signature.get("keylessIdentity") or not signature.get("bundleSha256"):
        raise RuntimeError(f"{path.name} lacks identity-bound signature evidence")
    print(f"verified {path.name} sha256={sha256(path)}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-root", required=True, type=pathlib.Path)
    args = parser.parse_args()
    for name, scenarios in REQUIRED.items():
        path = args.evidence_root / name
        if not path.is_file():
            raise RuntimeError(f"missing mandatory live certificate: {path}")
        verify(path, scenarios)
    print("Phase 5 production prerequisites: PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        print(f"Phase 5 production prerequisites: BLOCKED: {error}", file=sys.stderr)
        raise SystemExit(1)
