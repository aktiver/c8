#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 21 certified named-graph routing."""

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
    typed = require(
        "crates/ngkg-sparql-compiler/src/lib.rs",
        (
            "pub struct RouteAnalysis",
            "pub semantic_iris: BTreeSet<String>",
            "pub declared_graph_iris: BTreeSet<String>",
            "pub has_graph_variable: bool",
            "pub has_default_graph_pattern: bool",
            "RDF_TYPE_IRI",
            "inspect_graph_pattern(",
        ),
    )
    _ = typed
    compiler = require(
        "crates/ngkg-reference/src/compiler.rs",
        (
            "fn write_graph_capabilities",
            "predicate_to_graphs",
            "class_to_graphs",
            "fn expand_dependencies",
            "compiled.route_analysis()",
            "ROUTE_MODE_TYPED_ACTIVE_DATASET_FALLBACK",
            "fn write_routed_dataset",
            "fn validate_routed_query",
            "build_store(route_path, closure_path",
            "execute_compiled_query_with_dataset(",
            "routed_result_sha256",
            "verify_expected(",
            "verify_source_links(",
            "selection_mode = ROUTE_MODE_TYPED_ACTIVE_DATASET_FALLBACK.to_owned()",
            'format!("data/routes/{}.nq", query.query.sha256)',
        ),
    )
    route_write = compiler.index("write_routed_dataset(&route_path")
    route_validation = compiler.index("validate_routed_query(", route_write)
    fallback = compiler.index(
        "selection_mode = ROUTE_MODE_TYPED_ACTIVE_DATASET_FALLBACK.to_owned()", route_validation
    )
    if not route_write < route_validation < fallback:
        raise RuntimeError("selective typed route is not executed before the full-active-dataset fallback")
    if compiler.index("let artifacts = collect_artifacts(stage)?") < compiler.index(
        "let certified_queries = certify_queries("
    ):
        raise RuntimeError("routed artifacts are collected before their certificates exist")

    model = require(
        "crates/ngkg-reference/src/model.rs",
        (
            "pub struct QueryRoutingCertificate",
            "pub struct GraphCapabilityIndexFile",
            "pub predicate_to_graphs",
            "pub class_to_graphs",
            "pub dependencies",
            "pub route_artifact_sha256",
            "pub routed_multiset_sha256",
        ),
    )
    if "deny_unknown_fields" not in model:
        raise RuntimeError("routing contracts are not strict serde models")

    runtime = require(
        "crates/ngkg-reference/src/lib.rs",
        (
            "pub fn open_routed",
            "only_query_sha256",
            "routing.routed_multiset_sha256 != certificate.observed_multiset_sha256",
            "verify_selected_artifact(",
            "build_store(",
        ),
    )
    if runtime.index("verify_selected_artifact(", runtime.index("pub fn open_routed")) > runtime.index(
        "build_store(", runtime.index("pub fn open_routed")
    ):
        raise RuntimeError("routed runtime builds a store before artifact verification")

    service = require(
        "services/online-serving/src/main.rs",
        (
            "validate_capability_index(",
            "materialize_snapshot_artifact(",
            "CertifiedSemanticRuntime::open_routed",
            "struct BoundedLruCache",
            "NGKG_MAX_RESIDENT_QUERY_ROUTES",
            "routed_dataset_sha256",
        ),
    )
    semantic_start = service.index("async fn semantic_state")
    routed_start = service.index("async fn routed_runtime")
    # Phase 39.4 strictly supersedes Phase 21's no-full-dataset residency rule:
    # supported ad-hoc queries need a bounded exact scalar fallback. The selective
    # certified route remains mandatory for hashes that carry a routing certificate.
    uncertified_start = service.index("async fn execute_uncertified_exact_query")
    full_runtime_start = service.index("async fn full_runtime")
    semantic_slice = service[semantic_start:full_runtime_start]
    if 'materialize_snapshot_artifact(\n            &active,\n            &manifest,\n            "data/query-dataset.nq"' in semantic_slice:
        raise RuntimeError("Phase 39.4 eagerly materializes the full query dataset for certified routes")
    full_runtime_end = service.index("async fn routed_runtime", full_runtime_start)
    if '"data/query-dataset.nq"' not in service[full_runtime_start:full_runtime_end]:
        raise RuntimeError("Phase 39.4 full runtime does not lazily materialize the query dataset")
    if ".full_runtime(Arc::clone(&semantic))" not in service[uncertified_start:]:
        raise RuntimeError("Phase 39.4 ad-hoc exact path does not use the full runtime")
    query_start = service.index("async fn query(")
    distributed_start = service.index("async fn execute_distributed_query")
    query_body = service[query_start:distributed_start]
    if "let Some(certificate) = certificate else" not in query_body:
        raise RuntimeError("query admission no longer distinguishes certified and ad-hoc paths")
    if ".routed_runtime(Arc::clone(&semantic), query_sha256.clone())" not in query_body:
        raise RuntimeError("certified local queries no longer use selective routed runtimes")
    if ".list(" in service or "list_with_delimiter" in service:
        raise RuntimeError("online route loading contains forbidden object-store discovery")
    if service.index("validate_capability_index(", semantic_start) > service.index(
        "Arc::new(SemanticState", semantic_start
    ):
        raise RuntimeError("semantic state becomes visible before capability validation")
    if service.index("materialize_snapshot_artifact(", routed_start) > service.index(
        "CertifiedSemanticRuntime::open_routed", routed_start
    ):
        raise RuntimeError("routed runtime opens before selective artifact materialization")

    for contract in (
        "contracts/graph-capability-index.schema.json",
        "contracts/query-routing-certificate.schema.json",
    ):
        schema = json.loads(require(contract))
        if schema.get("additionalProperties") is not False:
            raise RuntimeError(f"{contract} is not strict")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    routing = openapi["components"]["schemas"]["Routing"]
    if routing.get("additionalProperties") is not False or routing["properties"]["selectedGraphIris"].get(
        "uniqueItems"
    ) is not True:
        raise RuntimeError("OpenAPI routing evidence is not strict and unique")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    if int(values["onlineServing"]["maxResidentQueryRoutes"]) < 1:
        raise RuntimeError("resident routed runtime cache is not positively bounded")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"][
        "memoryUtilizationTargetPercent"
    ] > 80:
        raise RuntimeError("online resource scaling target exceeds 80 percent")
    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        ("NGKG_MAX_RESIDENT_QUERY_ROUTES", "OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS"),
    )
    require(
        "scripts/qualify_phase21.sh",
        (
            "NGKG_EXPECTED_RESULTS_FILE",
            "NGKG_EXPECTED_ROUTING_FILE",
            "cmp",
            ".routing.selectedGraphCount < .routing.totalGraphCount",
            "averageUtilization",
        ),
    )
    require("test-corpus/routing/q01-cross-domain.json", ('"selectedGraphCount": 2', '"totalGraphCount": 3'))
    require("docs/phases/PHASE_21.md", ("Acceptance criteria", "Intentional boundary", "fail closed"))
    require("verification/phase-21.json")
    print("Phase 21 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 21 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
