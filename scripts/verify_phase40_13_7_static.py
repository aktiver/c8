#!/usr/bin/env python3
"""Static contract checks for the Phase 40.13.7 exact online entailment slice."""

from __future__ import annotations

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
    serving = require(
        "services/online-serving/src/main.rs",
        "execute_online_direct_query",
        "restrict_resolved_dataset_to_roles",
        'BTreeSet::from(["semkg".to_owned()])',
        '!iri.ends_with("/semkg")',
        "prepare_exact_direct_bgp_requests",
        "dispatch_exact_partitions_with_retry",
        "complete_distributed_exact_bgp",
        "substitute_exact_bgp_results",
        "execute_exact_entailment_rewritten_with_dataset_bounded_cancellable",
        "ExactEntailmentEvidence",
        "proof_manifests",
        "certificates",
    )
    direct = require(
        "crates/ngkg-direct-reasoner/src/lib.rs",
        "PreparedDirectExactBgp",
        "prepare_exact_direct_bgp_requests",
        '.join("result.json")',
    )
    online = require(
        "crates/ngkg-online-reasoning/src/lib.rs",
        "dispatch_exact_partitions_with_retry",
        "result.request_sha256 != expected_request_sha256",
        "require_complete_partition_set",
        "Incomplete and unknown are not false",
    )
    algebra = require(
        "crates/ngkg-online-reasoning/src/algebra.rs",
        "GraphPattern::Bgp { .. } => self.values()?",
        "Expression::Exists",
        "GraphPattern::LeftJoin",
        "GraphPattern::Union",
        "GraphPattern::Minus",
        "GraphPattern::Group",
        "GraphPattern::Distinct",
        "GraphPattern::OrderBy",
        "Query::Construct",
        "Query::Describe",
        "RowCeiling",
    )
    reference = require(
        "crates/ngkg-reference/src/query.rs",
        "execute_entailment_rewritten_query_with_dataset_cancellable",
        "set_available_named_graphs",
    )
    dataset = require(
        "crates/ngkg-dataset/src/lib.rs",
        "restrict_resolved_dataset_to_roles",
        "Authorization is never broadened",
    )
    chart = require(
        "charts/ngkg-workloads/templates/online-reasoner.yaml",
        "ngkg-direct-reasoners-headless",
        "metadata: {name: ngkg-direct-reasoners}",
        "HorizontalPodAutoscaler",
        "ngkg_reasoner_queued_candidate_partitions",
        "ngkg_reasoner_estimated_axioms",
        "podAntiAffinity",
    )
    data_plane = require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        "NGKG_ONLINE_DIRECT_ENABLED",
        "NGKG_REASONER_WORKER_URLS",
        "NGKG_REASONER_SHARED_TOKEN",
        "direct-workspace",
    )
    harness = require(
        "scripts/run_w3c_conformance.py",
        "--entailment-driver",
        "OWL_DIRECT",
        "owl-direct-query-evaluation",
    )
    values = yaml.safe_load((ROOT / "charts/ngkg-workloads/values.yaml").read_text())
    reasoning = values["onlineReasoning"]
    if reasoning["autoscaling"]["minReplicas"] < 2:
        raise RuntimeError("reasoner autoscaling must retain at least two replicas")
    if int(reasoning["dispatchConcurrency"]) < 2 or int(reasoning["dispatchAttempts"]) < 2:
        raise RuntimeError("distributed dispatch/retry values are not enabled")
    if "clusterIP: None" not in chart or "serviceName: ngkg-direct-reasoners-headless" not in chart:
        raise RuntimeError("StatefulSet governing service is not headless")
    cargo = require("services/online-serving/Cargo.toml")
    if "ngkg-mapping" in cargo or re.search(r"\bfn\s+(align|map_raw_data)\b", serving):
        raise RuntimeError("forbidden ontology-alignment implementation appeared in query path")
    if "finite-closure graph is deliberately excluded" not in require(
        "crates/ngkg-reference/src/lib.rs"
    ):
        raise RuntimeError("exact scalar execution does not state closure exclusion")
    if not all((serving, direct, online, algebra, reference, dataset, data_plane, harness)):
        raise RuntimeError("empty Phase 40.13.7 source")
    print("phase 40.13.7 static qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.7 static qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
