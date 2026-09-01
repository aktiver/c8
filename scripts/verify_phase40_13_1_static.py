#!/usr/bin/env python3
"""Static recovery checks for Phase 40.13.1."""

from __future__ import annotations

import json
import pathlib

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, tokens: tuple[str, ...] = ()) -> str:
    target = ROOT / path
    if not target.is_file():
        raise RuntimeError(f"missing required file: {path}")
    text = target.read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{path} is missing required token: {token}")
    return text


def main() -> int:
    direct = require(
        "crates/ngkg-direct-reasoner/src/lib.rs",
        (
            "&request_set_sha256,\n        &limits,",
            "limits: &DirectExactLimits",
            "enforce_proof_support_limit(required_support_ids, limits)?",
            "enforce_certificate_size_limit(certificate_bytes.len(), limits)?",
            "merge_proof_support_ceiling_is_enforced_at_the_exact_boundary",
            "merge_certificate_ceiling_is_enforced_at_the_exact_boundary",
        ),
    )
    if "fn merge_partition_results(" not in direct:
        raise RuntimeError("exact partition merge function is absent")

    compiler = require(
        "crates/ngkg-sparql-compiler/src/lib.rs",
        (
            "pub struct ExecutionAnalysis",
            "pub has_remote_service: bool",
            "pub volatile_functions: BTreeSet<String>",
            "pub fn require_certifiable(&self)",
            "SparqlCertificationError::RemoteService",
            "SparqlCertificationError::NondeterministicFunction",
            "execution.has_remote_service = true",
            "Function::BNode => Some(\"BNODE\")",
            "Function::Rand => Some(\"RAND\")",
            "Function::Now => Some(\"NOW\")",
            "Function::Uuid => Some(\"UUID\")",
            "Function::StrUuid => Some(\"STRUUID\")",
            "volatile_and_remote_features_parse_then_receive_execution_policy",
        ),
    )
    for forbidden in (
        "return Err(SparqlCompileError::RemoteService)",
        "return Err(SparqlCompileError::NondeterministicFunction",
    ):
        if forbidden in compiler:
            raise RuntimeError(f"standards parser still performs policy rejection: {forbidden}")

    require(
        "crates/ngkg-reference/src/compiler.rs",
        (".require_certifiable()", ".map_err(ReferenceQueryError::from)?"),
    )
    require(
        "services/online-serving/src/main.rs",
        (
            "compiled.execution_analysis().has_remote_service",
            "SPARQL SERVICE parsed successfully",
            "capability_report(ThreadBudget",
            "validated shared Rust/OpenMP/BLAS Kubernetes CPU budget",
        ),
    )
    require(
        "services/online-serving/Cargo.toml",
        ('ngkg-hpc-runtime = { path = "../../crates/ngkg-hpc-runtime" }',),
    )

    data_plane = require("charts/ngkg-workloads/templates/online-data-plane.yaml")
    if data_plane.count("NGKG_RUST_COMPUTE_THREADS, value: '12'") != 2:
        raise RuntimeError("query and fragment roles must each reserve twelve Rust compute lanes")
    if data_plane.count("NGKG_RUST_COMPUTE_THREADS, value: '4'") != 2:
        raise RuntimeError("locator and hydration roles must each reserve four Rust compute lanes")

    autoscaling = require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        (
            "workloadAwareAutoscalingEnabled",
            "name: ngkg_admission_pending",
            "class: query",
            "class: fragment",
            "class: hydration",
            "queryPendingAverageTarget",
            "fragmentPendingAverageTarget",
            "hydrationPendingAverageTarget",
        ),
    )
    if autoscaling.count("name: cpu") != 3 or autoscaling.count("name: memory") != 3:
        raise RuntimeError("workload metrics must augment rather than replace CPU and memory HPA signals")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    metrics = values["metrics"]
    if metrics["workloadAwareAutoscalingEnabled"] is not False:
        raise RuntimeError("custom workload metrics must remain opt-in until their API is required")
    for key in (
        "queryPendingAverageTarget",
        "fragmentPendingAverageTarget",
        "hydrationPendingAverageTarget",
    ):
        if int(metrics[key]) <= 0:
            raise RuntimeError(f"metrics.{key} must be positive")

    production = yaml.safe_load(
        require("charts/ngkg-workloads/profiles/production-workload-autoscaling.yaml")
    )
    if production["metrics"]["workloadAwareAutoscalingEnabled"] is not True:
        raise RuntimeError("production profile does not enable workload-aware scaling")
    if production["rke2"]["customMetricsApi"]["requireAvailable"] is not True:
        raise RuntimeError("production profile does not require the custom-metrics API")

    status = json.loads(require("verification/phase-40.13.1.json"))
    if status["status"] != "repaired-native-partially-qualified-candidate":
        raise RuntimeError("Phase 40.13.1 overclaims qualification")
    if status["federatedServiceHandlerImplemented"] is not False:
        raise RuntimeError("federated SERVICE must remain explicitly incomplete")
    if status["ontologyAlignmentImplemented"] is not False:
        raise RuntimeError("ontology alignment is outside the database")
    if status["productionQualified"] is not False:
        raise RuntimeError("Phase 40.13.1 must not claim production qualification")

    print(
        "Phase 40.13.1 static verification passed: exact merge repaired, legal SPARQL parsing is policy-separated, online CPU budgets are enforced, and workload-aware HPA is production-profile gated"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
