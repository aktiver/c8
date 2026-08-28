#!/usr/bin/env python3
"""Fail-closed static qualification for the Phase 40.13.6 online reasoner slice."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
SHA = "a" * 64


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def validate_schema(relative: str, instance: dict) -> None:
    schema = json.loads(text(relative))
    require(schema.get("$schema") == "https://json-schema.org/draft/2020-12/schema", f"{relative} draft is wrong")
    require(schema.get("additionalProperties") is False, f"{relative} is not closed")
    required = set(schema.get("required", []))
    require(required <= set(instance), f"{relative} sample omits required fields")
    require(set(instance) <= set(schema.get("properties", {})), f"{relative} sample has unknown fields")


workspace = text("Cargo.toml")
require('"crates/ngkg-online-reasoning"' in workspace, "online reasoning crate is not in workspace")
require('"services/direct-reasoner-worker"' in workspace, "reasoner worker is not in workspace")

pom = text("adapters/hermit-reasoner/pom.xml")
require("<version>1.4.5.519</version>" in pom, "HermiT 1.4.5.519 is not pinned")

routing = text("crates/ngkg-online-reasoning/src/lib.rs")
for token in (
    "select_authorized_asserted_modules",
    "OntologyGraphRole::AssertedOntology",
    "OntologyGraphRole::FiniteClosure",
    "OntologyGraphRole::Provenance",
    "EntailmentRoute::CertifiedSemanticIndex",
    "EntailmentRoute::CertifiedFiniteClosure",
    "EntailmentRoute::ExactHermit",
    "EntailmentRoute::IllegalOwlDirect",
    "Incomplete and unknown are not false",
    "require_complete_partition_set",
    "complete_distributed_exact_bgp",
    "dispatch_exact_partitions",
    "desired_reasoner_replicas",
):
    require(token in routing, f"online reasoning contract omits {token}")
for forbidden in ("embedding", "lexical similarity", "candidate correspondence"):
    require(
        routing.lower().count(forbidden) <= 1,
        f"new reasoning core appears to implement forbidden alignment concept: {forbidden}",
    )

worker = text("services/direct-reasoner-worker/src/main.rs")
for token in (
    "execute_exact_direct_partition",
    "NGKG_REASONER_MAX_IN_FLIGHT",
    "NGKG_REASONER_MAX_PENDING",
    "validate_heap_budget",
    "NGKG_REASONER_SHARED_TOKEN_SHA256",
    "ngkg_reasoner_queued_candidate_partitions",
    "ngkg_reasoner_estimated_axioms",
    "ngkg_reasoner_oldest_queue_age_milliseconds",
    "ngkg_reasoner_mean_service_latency_milliseconds",
):
    require(token in worker, f"reasoner worker omits {token}")
require("1.4.5.519" in worker, "worker does not enforce the HermiT version pin")
require("-XX:ActiveProcessorCount=1" in text("crates/ngkg-direct-reasoner/src/lib.rs"), "HermiT child is not one-CPU bounded")
online = text("services/online-serving/src/main.rs")
require("/v1/datasets/{dataset_id}/sparql/direct/route" in online, "online route endpoint is missing")
require("CoverageState::Unknown" in online and "route_entailment" in online, "online unknown-to-HermiT fallback is missing")
openapi = yaml.safe_load(text("api/online-openapi.yaml"))
require("/v1/datasets/{datasetId}/sparql/direct/route" in openapi["paths"], "OpenAPI route is missing")

values = yaml.safe_load(text("charts/ngkg-workloads/values.yaml"))
reasoner = values["onlineReasoning"]
require(reasoner["hermitVersion"] == "1.4.5.519", "Helm HermiT pin differs")
require(reasoner["autoscaling"]["minReplicas"] >= 2, "unqualified scale-to-zero is enabled")
require(values["hpcNodeGroups"]["online_reasoning_num_of_nodes"] >= 2, "reasoner pool is not multinode")
require(values["resources"]["reasoner"]["requests"] == values["resources"]["reasoner"]["limits"], "reasoner does not have Guaranteed QoS resources")
require(int(reasoner["maxInFlightPerPod"]) * int(reasoner["heapMiBPerLane"]) <= 64 * 1024 * 80 // 100, "configured JVM heaps exceed 80% of pod memory")
reasoner_nodes = values["autoscaling"]["onlineReasoning"]
require(reasoner_nodes["owner"] == "hpa", "online reasoner node demand is not HPA-owned")
require(reasoner_nodes["minNodes"] >= 2, "online reasoner node floor is below the qualified pool")
require(reasoner_nodes["maxNodes"] >= reasoner["autoscaling"]["maxReplicas"], "node autoscaling ceiling cannot place the HPA maximum")

chart = text("charts/ngkg-workloads/templates/online-reasoner.yaml")
for token in (
    "kind: StatefulSet",
    "podAntiAffinity",
    "kubernetes.io/hostname",
    "kind: HorizontalPodAutoscaler",
    "ngkg_reasoner_queued_candidate_partitions",
    "ngkg_reasoner_estimated_axioms",
    "ngkg_reasoner_oldest_queue_age_milliseconds",
    "ngkg_reasoner_mean_service_latency_milliseconds",
    "averageUtilization",
    "readOnlyRootFilesystem: true",
):
    require(token in chart, f"online reasoner chart omits {token}")
require("minReplicas: {{ .Values.onlineReasoning.autoscaling.minReplicas }}" in chart, "HPA minimum is not bound to the qualified value")

validate_schema(
    "contracts/online-ontology-snapshot-binding.schema.json",
    {
        "formatVersion": 1,
        "datasetId": "8b72d90a-34cb-4443-85bb-42a74a68f8d0",
        "snapshotId": "65ed192f-075b-4ad2-b845-6c4eca4b4247",
        "authorizedGraphSetSha256": SHA,
        "activeDatasetSha256": SHA,
        "datatypePolicySha256": SHA,
        "owlSignatureSha256": SHA,
        "owlProfileQualificationSha256": SHA,
        "owlConsistencyQualificationSha256": SHA,
        "assertedModules": [
            {
                "graphIri": "https://c8-next-generation.io/acme/orders/semkg",
                "contentSha256": SHA,
            }
        ],
        "pinnedImports": [],
        "syntheticOntologySha256": SHA,
    },
)
validate_schema(
    "contracts/distributed-online-reasoner-plan.schema.json",
    {
        "formatVersion": 1,
        "datasetId": "8b72d90a-34cb-4443-85bb-42a74a68f8d0",
        "snapshotId": "65ed192f-075b-4ad2-b845-6c4eca4b4247",
        "querySha256": SHA,
        "bgpSha256": SHA,
        "ontologySnapshotSha256": SHA,
        "candidateBindingCeiling": 1000,
        "maxCandidatesPerPartition": 500,
        "partitions": [{"index": 0, "count": 2}, {"index": 1, "count": 2}],
        "planSha256": SHA,
    },
)

overview = ROOT.parent.parent / "upload" / "Pasted markdown(20260827-141054).md"
if overview.exists():
    observed = hashlib.sha256(overview.read_bytes()).hexdigest()
    require(observed == "a95df65062f24479cc0ae12c5dded1803a36eb6440026c95a0da75a4e415fc3b", "supplied SPARQL overview input changed")

acceptance = yaml.safe_load(text("acceptance/phase-gates.yaml"))["phases"]
require(any(str(item.get("phase")) == "40.13.6" for item in acceptance), "acceptance gate is missing")
print("phase 40.13.6 static qualification passed")
