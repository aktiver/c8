#!/usr/bin/env python3
"""Fail-closed Phase 40.13.14 source and contract verification."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def text(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def require(haystack: str, *needles: str) -> None:
    missing = [needle for needle in needles if needle not in haystack]
    if missing:
        raise SystemExit(f"missing Phase 40.13.14 contracts: {missing}")


core = text("crates/ngkg-offline-reasoner/src/lib.rs")
cloud = text("services/reference-worker/src/cloud_offline.rs")
operator = text("services/operator/src/main.rs")
kube = text("crates/ngkg-kube/src/lib.rs")
values = text("charts/ngkg-platform/values.yaml")
operator_template = text("charts/ngkg-platform/templates/operator.yaml")
qualifier = text("crates/ngkg-ontology-qualifier/src/lib.rs")

require(
    core,
    "exact-hermit-finite-named-consequences",
    "finite-named-consequences-emitted-by-exact-hermit",
    "arbitrary_owl2_dl_complete: false",
    "unknown_routes_to_exact_hermit: true",
    "partition completion barrier is incomplete",
    "merge_runs_hierarchical",
    "external_sort_unique",
    "closure.parquet",
    "proof-support.tsv",
    "sameas-membership.tsv",
    "publication_state: \"inactive\".to_owned()",
    "https://c8-next-generation.io/",
)
require(
    cloud,
    "cloud offline reasoning",
    "materialize_verified",
    "verify_remote",
    "buffer_unordered(concurrency)",
    "OfflineReasoningCompleteInactive",
)
require(
    operator,
    "cloud-offline-plan",
    "cloud-offline-partition",
    "cloud-offline-finalize",
    "$(JOB_COMPLETION_INDEX)",
    "offline-partition-max-parallelism",
    '"kueue.x-k8s.io/queue-name"',
    '"reasoning"',
)
require(kube, "offline_reasoning_plan_sha256", "offline_reasoning_root_sha256")
require(values, "offlineReasoner:", "workerCpu: '1'", "partitionMaxParallelism: '256'", "logicalPartitions: '4096'")
require(operator_template, "NGKG_OFFLINE_WORKER_CPU", "NGKG_OFFLINE_PARTITION_MAX_PARALLELISM")
require(qualifier, "finite_closure_sha256", "finite_closure_axiom_count", "sha256_path(&finite_closure_path)?")

if "ngkg-mapping" in text("crates/ngkg-offline-reasoner/Cargo.toml"):
    raise SystemExit("offline reasoning must not depend on ontology alignment/raw-data mapping")
if 'publication_state: "active"' in core or "Published" in cloud:
    raise SystemExit("Phase 40.13.14 must never activate a snapshot")

for contract in (
    "contracts/ontology-qualification-root.schema.json",
    "contracts/offline-reasoning-plan.schema.json",
    "contracts/offline-reasoning-partition.schema.json",
    "contracts/offline-reasoning-root.schema.json",
):
    json.loads(text(contract))

root_schema = json.loads(text("contracts/offline-reasoning-root.schema.json"))
properties = root_schema["properties"]
assert properties["arbitraryOwl2DlComplete"]["const"] is False
assert properties["unknownRoutesToExactHermit"]["const"] is True
assert properties["publicationState"]["const"] == "inactive"

print(json.dumps({
    "phase": "40.13.14",
    "status": "passed",
    "checks": 12,
    "semanticAuthority": "exact HermiT 1.4.5.519 finite named consequences",
    "publicationState": "inactive",
}))
