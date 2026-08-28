#!/usr/bin/env python3
"""Fail-closed static checks for the Phase 37 lossless RDF dataset contract."""

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


def strict_schema(path: str) -> dict:
    value = json.loads(require(path))
    if value.get("additionalProperties") is not False:
        raise RuntimeError(f"{path} must reject unknown top-level fields")
    return value


def main() -> int:
    dataset = require(
        "crates/ngkg-dataset/src/lib.rs",
        (
            "pub enum LogicalGraphName",
            "Default,",
            "Named {",
            "pub struct GraphDeclaration",
            "pub struct GraphCatalog",
            "pub fn compile_catalog(",
            "Zero retains an explicitly declared empty graph",
            "pub fn resolve_dataset(",
            "DatasetSelectionSource::ProtocolDataset",
            "DatasetSelectionSource::QueryDataset",
            "DatasetSelectionSource::ServiceDefault",
            "hash_graph_set(",
            "hash_active_dataset(",
            "ForbiddenRequestedGraph",
            "protocol_dataset_replaces_query_dataset",
            "catalog_retains_default_and_empty_graphs_in_canonical_order",
        ),
    )
    if "unwrap()" in dataset or "expect(" in dataset:
        raise RuntimeError("dataset semantic core contains panic-oriented convenience calls")

    rdf = require(
        "crates/ngkg-reference/src/rdf.rs",
        (
            "pub const DEFAULT_GRAPH_STORAGE_KEY",
            "pub enum GraphScope",
            "pub enum ResourceTermKind",
            "GraphName::BlankNode(_) => return Err(RdfCompileError::BlankGraphRejected)",
            "GraphName::DefaultGraph => (GraphScope::Default",
            "term_kind: ResourceTermKind::BlankNode",
            "fn canonical_blank_key(",
            "repeated_named_graph_blocks_are_set_union",
            "blank_nodes_round_trip_as_blank_terms_with_stable_identity",
        ),
    )
    if 'format!("<{canonical_key}>")' not in rdf:
        raise RuntimeError("named-node serialization contract is absent")

    query = require(
        "crates/ngkg-reference/src/query.rs",
        (
            "pub enum DefaultDatasetPolicy",
            "UnionDefault",
            "pub fn query_dataset_specification(",
            "CompiledSparqlQuery::parse(query_text)",
            "pub fn execute_select_with_dataset(",
            "pub fn execute_compiled_select_with_dataset(",
            "set_default_graph(default_graphs)",
            "set_available_named_graphs(named_graphs)",
            "service_default_is_named_graph_union_and_named_graphs_remain_isolated",
            "explicit_dataset_clause_replaces_union_default",
            "multiple_from_rdf_merge_standardizes_blank_nodes_apart",
            "union_default_uses_rdf_set_union_not_bag_concatenation",
        ),
    )

    compiler = require(
        "crates/ngkg-reference/src/compiler.rs",
        (
            "write_dataset_graph_catalog(stage, manifest, &facts)",
            "semantic_exports_enforce_graph_catalog_visibility",
            'stage.join("indexes/rdf-dataset-catalog.json")',
            "compile_catalog(",
            "graph_catalog_sha256",
            "authorization_labels",
            "reasoning_visible",
            "active_dataset_sha256",
        ),
    )

    parquet = require(
        "crates/ngkg-reference/src/parquet_io.rs",
        (
            'Field::new("subject_resource_kind", DataType::UInt8, false)',
            'Field::new("graph_scope", DataType::UInt8, false)',
            "fact.subject_term_kind.code()",
            "fact.graph_scope.code()",
        ),
    )
    hydration = require(
        "crates/ngkg-hydration/src/lib.rs",
        (
            "pub enum RdfResourceKind",
            "pub enum RdfGraphScope",
            "pub subject_resource_kind: RdfResourceKind",
            "pub graph_scope: RdfGraphScope",
        ),
    )
    distributed = require(
        "crates/ngkg-distributed-artifacts/src/lib.rs",
        (
            'Field::new("subject_resource_kind", DataType::UInt8, false)',
            'Field::new("graph_scope", DataType::UInt8, false)',
        ),
    )
    _ = (parquet, hydration, distributed)

    serving = require(
        "services/online-serving/src/main.rs",
        (
            "authorization_state(identity.tenant_id, dataset_id)",
            "require_reasoning_graph_authorization",
            "compiled_query.dataset_specification().clone()",
            "ProtocolDatasetSpecification",
            "resolve_request_dataset(",
            "active_dataset.active_dataset_sha256 != routing.active_dataset_sha256",
            "authorized_graph_set_sha256",
            "active_dataset_sha256",
            "dataset_selection_source",
            "execute_compiled_with_dataset_bounded_cancellable(",
        ),
    )
    auth_pos = serving.index("authorization_state(identity.tenant_id, dataset_id)")
    semantic_pos = serving.index(".semantic_state(identity.tenant_id, dataset_id)", auth_pos)
    if auth_pos >= semantic_pos:
        raise RuntimeError("semantic state can load before graph authorization")

    cache = require(
        "crates/ngkg-query-cache/src/lib.rs",
        (
            "pub authorized_graph_set_sha256: String",
            "pub active_dataset_sha256: String",
            "pub dataset_selection_source: u8",
        ),
    )
    _ = cache

    capability = strict_schema("contracts/graph-capability-index.schema.json")
    if capability["properties"]["formatVersion"].get("const") != 2:
        raise RuntimeError("graph capability schema does not match formatVersion 2 runtime")
    routing = strict_schema("contracts/query-routing-certificate.schema.json")
    required = set(routing["required"])
    for field in (
        "datasetSelectionSource",
        "defaultGraphIris",
        "namedGraphIris",
        "activeDatasetSha256",
        "includeInternalClosure",
    ):
        if field not in required:
            raise RuntimeError(f"routing certificate omits {field}")
    strict_schema("contracts/rdf-dataset-catalog.schema.json")

    online_openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    response_required = set(online_openapi["components"]["schemas"]["QueryResponse"]["required"])
    if not {"authorizedGraphSetSha256", "activeDatasetSha256"}.issubset(response_required):
        raise RuntimeError("public query response omits active authorization/dataset identity")
    routing_required = set(online_openapi["components"]["schemas"]["Routing"]["required"])
    if not {"datasetSelectionSource", "defaultGraphIris", "namedGraphIris", "activeDatasetSha256"}.issubset(routing_required):
        raise RuntimeError("public routing response omits dataset precedence evidence")

    workloads = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    if workloads["metrics"]["cpuUtilizationTargetPercent"] > 80 or workloads["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("query-plane autoscaling target exceeds 80 percent")
    require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        (
            "name: ngkg-query-shard",
            "name: ngkg-fragment-worker",
            "name: ngkg-hydration",
            "autoscaling/v2",
        ),
    )
    rke2 = yaml.safe_load(require("charts/ngkg-workloads/profiles/rke2.yaml"))
    if rke2["hpcNodeGroups"]["provisioner"] != "cluster-autoscaler":
        raise RuntimeError("RKE2 profile does not delegate node-pool growth to cluster autoscaler")

    print("Phase 37 static contract verification passed; runtime conformance gates remain mandatory")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 37 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
