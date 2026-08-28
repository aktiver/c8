#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 22 certified distributed fragments."""

from __future__ import annotations

import json
import pathlib
import sys

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
    compiler = require(
        "crates/ngkg-reference/src/compiler.rs",
        (
            "fn certify_distributed_query",
            "compiled.distributed_graph_fragments()",
            "write_routed_dataset(",
            "let fragment_query = block.query_text.clone()",
            "execute_select(&store, &fragment_query)",
            "inner_join_sparql_json",
            "verify_binding_values(",
            "full_multiset_sha256",
            "plans/distributed/",
        ),
    )
    typed = require(
        "crates/ngkg-sparql-compiler/src/lib.rs",
        (
            "pub fn distributed_graph_fragments(&self)",
            "collect_constant_graph_join_leaves",
            "only_pure_constant_graph_inner_join_is_distributable",
        ),
    )
    if "query_lexemes" in typed or "distributed_graph_blocks" in typed:
        raise RuntimeError("distributed semantic decomposition regressed to query-text scanning")
    fragment_execution = compiler.index("execute_select(&store, &fragment_query)")
    plan_write = compiler.index("serde_json::to_vec_pretty(&plan)", fragment_execution)
    if fragment_execution > plan_write:
        raise RuntimeError("distributed plan is emitted before real fragment execution")
    equality = compiler.index("verify_binding_values(", fragment_execution)
    if equality > plan_write:
        raise RuntimeError("distributed plan is emitted before independent equality verification")

    executor = require(
        "crates/ngkg-query-executor/src/lib.rs",
        (
            "pub fn inner_join_sparql_json",
            "compatible_nested_join",
            "IntermediateRowLimit",
            "pub fn project_sparql_json",
            "json_join_preserves_bag_rows_and_projects_exactly",
        ),
    )
    if executor.index("output.len() >= max_rows") > executor.index("output.push", executor.index("output.len() >= max_rows")):
        raise RuntimeError("distributed join grows output before checking its hard row ceiling")

    runtime = require(
        "services/online-serving/src/main.rs",
        (
            "async fn execute_distributed_query",
            "lookup_host(service)",
            "buffer_unordered",
            "read_bounded_response",
            "AtomicUsize",
            "NGKG_MAX_DISTRIBUTED_EXCHANGE_BYTES",
            "inner_join_sparql_json",
            "canonical_sparql_multiset_sha256",
            "worker_ids.len() < 2",
            "async fn execute_fragment",
            "CertifiedFragmentRuntime",
            "fn validate_distributed_plan",
        ),
    )
    dispatch = runtime.index("lookup_host(service)")
    final_hash = runtime.index("canonical_sparql_multiset_sha256", dispatch)
    response = runtime.index("CertifiedSemanticResult", final_hash)
    if not dispatch < final_hash < response:
        raise RuntimeError("distributed result can become visible before final certificate validation")

    model = require(
        "crates/ngkg-reference/src/model.rs",
        (
            "pub struct DistributedQueryCertificate",
            "pub struct DistributedQueryPlanFile",
            "pub struct DistributedQueryFragment",
            "pub struct CertifiedFragmentResult",
            "deny_unknown_fields",
        ),
    )
    if "skip_serializing_if = \"Option::is_none\"" not in model:
        raise RuntimeError("Phase 21-compatible local fallback is absent")

    for contract in (
        "contracts/query-routing-certificate.schema.json",
        "contracts/distributed-query-plan.schema.json",
    ):
        schema = json.loads(require(contract))
        if schema.get("additionalProperties") is not False:
            raise RuntimeError(f"{contract} is not strict")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    execution = openapi["components"]["schemas"]["Execution"]
    if execution.get("additionalProperties") is not False:
        raise RuntimeError("OpenAPI distributed execution evidence is not strict")
    if "/v1/datasets/{datasetId}/fragments/{querySha256}/{fragmentId}/execute" not in openapi["paths"]:
        raise RuntimeError("OpenAPI omits the real fragment worker endpoint")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    if values["networking"]["internalExchange"] not in {"certified-rest-json", "certified-arrow-ipc-rest"}:
        raise RuntimeError("chart claims an exchange transport that no cumulative phase implements")
    if values["networking"]["tlsMode"] != "external-service-mesh-required":
        raise RuntimeError("chart claims application-native mTLS that Phase 22 does not implement")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("fragment scaling target exceeds 80 percent")
    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        (
            "ngkg-fragment-worker",
            "sparql-fragment-processing",
            "NGKG_MAX_DISTRIBUTED_EXCHANGE_BYTES",
            "requiredDuringSchedulingIgnoredDuringExecution",
            "OMP_NUM_THREADS",
            "OPENBLAS_NUM_THREADS",
            "MKL_NUM_THREADS",
        ),
    )
    require("charts/ngkg-workloads/templates/autoscaling.yaml", ("ngkg-fragment-worker", "averageUtilization"))
    require("scripts/qualify_phase22.sh", ("certified_distributed_fragments", "workerCount >= 2", "cmp"))
    require("docs/phases/PHASE_22.md", ("Acceptance criteria", "Intentional boundary", "fail closed"))
    require("verification/phase-22.json")
    print("Phase 22 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 22 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
