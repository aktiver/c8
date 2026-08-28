#!/usr/bin/env python3
"""Fail-closed static checks for Phase 39 exact SPARQL 1.1 algebra and result forms."""

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
    typed = require(
        "crates/ngkg-sparql-compiler/src/lib.rs",
        (
            "pub enum QueryForm",
            'Select',
            'Ask',
            'Construct',
            'Describe',
            "pub fn solution_order_is_significant",
            "fn top_level_has_order_by",
            "Query::Select",
            "Query::Ask",
            "Query::Construct",
            "Query::Describe",
        ),
    )
    if "sparql-12" in require("Cargo.toml"):
        raise RuntimeError("Phase 39 production parser must remain SPARQL 1.1")

    query = require(
        "crates/ngkg-reference/src/query.rs",
        (
            "pub const QUERY_RESULT_HASH_VERSION: u32 = 2",
            "pub struct QueryExecutionLimits",
            "pub enum ExecutedQueryResult",
            "QueryResults::Solutions",
            "QueryResults::Boolean",
            "QueryResults::Graph",
            "with_cancellation_token",
            "CanonicalizationAlgorithm::Rdfc10",
            "CanonicalizationHashAlgorithm::Sha256",
            "pub fn canonical_query_result_sha256",
            "pub fn canonical_query_payload_sha256",
            "max_solution_rows",
            "max_graph_triples",
            "max_graph_blank_nodes",
            "phase39_executes_ask_and_full_scalar_algebra",
            "phase39_scalar_algebra_preserves_minus_subquery_paths_and_bag_semantics",
            "phase39_optional_keeps_unbound_variables_and_filter_errors_do_not_become_false_data",
            "phase39_construct_graph_hash_is_blank_node_isomorphism_stable",
        ),
    )
    if "unimplemented!" in query or "todo!" in query:
        raise RuntimeError("Phase 39 exact query engine contains placeholder execution")

    model = require(
        "crates/ngkg-reference/src/model.rs",
        (
            "pub query_form: QueryForm",
            "pub max_solution_rows: u64",
            "pub max_graph_triples: u64",
            "pub max_graph_blank_nodes: u64",
            "pub observed_result_sha256: String",
            "pub observed_multiset_sha256: Option<String>",
            "pub routed_result_sha256: String",
            "pub routed_multiset_sha256: Option<String>",
        ),
    )
    _ = model

    runtime = require(
        "crates/ngkg-reference/src/lib.rs",
        (
            "execute_compiled_with_dataset_bounded_cancellable",
            "certificate.query_form != compiled.form()",
            "certificate.ordered != compiled.solution_order_is_significant()",
            "canonical_query_result_sha256(&executed",
            "certificate.observed_result_sha256",
            "fresh semantic result differs from its offline form-aware certificate",
        ),
    )

    compiler = require(
        "crates/ngkg-reference/src/compiler.rs",
        (
            "query_execution_limits(manifest)",
            "compiled.solution_order_is_significant()",
            "let expected = parse_expected(",
            "observed_result_sha256",
            "routed_result_sha256",
            "observed_multiset_sha256,",
            "match (&observed, &expected, observed_multiset_sha256.as_deref())",
            "ExecutedQueryResult::Solutions",
            "ExpectedQueryResult::Solutions",
        ),
    )
    if any(token in compiler for token in ("QueryLexeme", "query_lexemes(", "query_route_hints(")):
        raise RuntimeError("Phase 39 compiler regressed to lexical semantic scanning")

    serving = require(
        "services/online-serving/src/main.rs",
        (
            'positive_usize("NGKG_MAX_QUERY_RESULT_ROWS")',
            'positive_usize("NGKG_MAX_QUERY_GRAPH_TRIPLES")',
            'positive_usize("NGKG_MAX_QUERY_GRAPH_BLANK_NODES")',
            'positive_u64("NGKG_QUERY_TIMEOUT_SECONDS")',
            "runtime.execute_compiled_with_dataset_bounded_cancellable",
            "CancellationToken::new()",
            "tokio::time::timeout(state.query_timeout",
            "cancellation.cancel()",
            'compiled_query.form() != QueryForm::Select',
            "canonical_query_payload_sha256",
            "serialize_sparql_boolean",
            "serialize_sparql_graph",
            "RdfSerializer::from_format",
            "QueryResultsSerializer::from_format",
            "QueryForm::Select",
            "QueryForm::Ask",
            "QueryForm::Construct",
            "QueryForm::Describe",
        ),
    )
    for stale in ("require_select_form", "QueryFormNotCertified", "DatasetOverrideNotCertified"):
        if stale in serving:
            raise RuntimeError(f"Phase 39 online path contains stale Phase 38 gate: {stale}")

    certified = strict_schema("contracts/certified-query-record.schema.json")
    required = set(certified["required"])
    expected = {
        "queryForm", "resultHashVersion", "maxSolutionRows", "maxGraphTriples",
        "maxGraphBlankNodes", "observedResultSha256", "routing",
    }
    if not expected.issubset(required):
        raise RuntimeError("certified-query v2 schema omits Phase 39 result identity")
    if certified["properties"]["resultHashVersion"].get("const") != 2:
        raise RuntimeError("certified-query schema is not result hash version 2")
    if set(certified["properties"]["queryForm"]["enum"]) != {"SELECT", "ASK", "CONSTRUCT", "DESCRIBE"}:
        raise RuntimeError("certified-query schema does not cover all four SPARQL query forms")

    routing = strict_schema("contracts/query-routing-certificate.schema.json")
    if "routedResultSha256" not in routing["required"]:
        raise RuntimeError("routing certificate omits form-aware routed result hash")
    if "routedMultisetSha256" in routing["required"]:
        raise RuntimeError("routing incorrectly requires SELECT-only multiset evidence for all forms")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    response = openapi["components"]["responses"]["SparqlQuery"]["content"]
    expected_formats = {
        "application/sparql-results+json",
        "application/sparql-results+xml",
        "text/tab-separated-values",
        "text/csv",
        "text/turtle",
        "application/n-triples",
        "application/rdf+xml",
    }
    if set(response) != expected_formats:
        raise RuntimeError("OpenAPI does not expose the complete Phase 39 result-format set")
    qr = openapi["components"]["schemas"]["QueryResponse"]
    if "queryForm" not in qr["required"] or not {"booleanResult", "graphNtriples"}.issubset(qr["properties"]):
        raise RuntimeError("enriched query response does not expose form-aware result fields")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    online = values["onlineServing"]
    for name in ("maxQueryResultRows", "maxQueryGraphTriples", "maxQueryGraphBlankNodes", "queryTimeoutSeconds"):
        if int(online[name]) < 1:
            raise RuntimeError(f"{name} must be a positive deployment-controlled bound")
    if int(online["maxQueryResultRows"]) > int(online["maxDistributedIntermediateRows"]):
        raise RuntimeError("query result rows exceed distributed intermediate row ceiling")
    template = require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        (
            "NGKG_MAX_QUERY_RESULT_ROWS",
            "NGKG_MAX_QUERY_GRAPH_TRIPLES",
            "NGKG_MAX_QUERY_GRAPH_BLANK_NODES",
            "NGKG_QUERY_TIMEOUT_SECONDS",
        ),
    )
    if template.count("NGKG_MAX_QUERY_RESULT_ROWS") != 4:
        raise RuntimeError("all online roles must receive the same explicit query execution contract")

    autoscaling = require(
        "charts/ngkg-workloads/templates/autoscaling.yaml",
        ("ngkg-query-shard", "ngkg-fragment-worker", "ngkg-hydration", "autoscaling/v2"),
    )
    _ = autoscaling

    inheritance = require(
        "scripts/verify_phase_inheritance.py",
        (
            "archive-manifest-chain",
            "archive-parent.json",
            "parentArchiveSha256",
            "cumulative archive inheritance forbids deleted parent files",
        ),
    )
    _ = inheritance
    # archive-parent.json is the moving current-phase pointer. Once later
    # phases exist, Phase 39's immutable parent result lives in its retained
    # phase record instead of that pointer.
    phase39 = json.loads(require("verification/phase-39.json"))
    parent = phase39.get("archiveInheritance", {})
    if parent.get("verified") is not True or parent.get("parentPhase") != 38:
        raise RuntimeError("Phase 39 retained evidence does not bind the immediate Phase 38 parent")
    if parent.get("deletedParentFiles") != 0:
        raise RuntimeError("Phase 39 retained evidence allows parent-file deletion")
    rke2 = yaml.safe_load(require("charts/ngkg-workloads/profiles/rke2.yaml"))
    if rke2["hpcNodeGroups"]["provisioner"] != "cluster-autoscaler":
        raise RuntimeError("RKE2 node growth is not delegated to Cluster Autoscaler")

    print("Phase 39 static contract verification passed; compiler/W3C/live RKE2 execution gates remain mandatory")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 39 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
