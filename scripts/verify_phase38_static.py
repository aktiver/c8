#!/usr/bin/env python3
"""Fail-closed static checks for the Phase 38 typed SPARQL compiler and protocol contract."""

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
    cargo = require(
        "Cargo.toml",
        (
            '"crates/ngkg-sparql-compiler"',
            'spargebra = { version = "=0.4.6", features = ["standard-unicode-escaping"] }',
            'utoipa-swagger-ui = { version = "=9.0.2", default-features = false, features = ["vendored", "debug-embed"] }',
        ),
    )
    if 'features = ["sparql-12"]' in cargo or 'features = ["sparql-12",' in cargo:
        raise RuntimeError("production parser enables experimental SPARQL 1.2 syntax")

    typed = require(
        "crates/ngkg-sparql-compiler/src/lib.rs",
        (
            "pub const SPARQL_ALGEBRA_FORMAT_VERSION: u32 = 1",
            "pub struct CompiledSparqlQuery",
            "SparqlParser::new()",
            ".parse_query(query_text)",
            "query.dataset()",
            "query.to_sse()",
            "canonical_sse_sha256",
            "inspect_graph_pattern(",
            "GraphPattern::Bgp",
            "GraphPattern::Path",
            "GraphPattern::Join",
            "GraphPattern::LeftJoin",
            "GraphPattern::Filter",
            "GraphPattern::Union",
            "GraphPattern::Graph",
            "GraphPattern::Extend",
            "GraphPattern::Minus",
            "GraphPattern::Values",
            "GraphPattern::OrderBy",
            "GraphPattern::Project",
            "GraphPattern::Distinct",
            "GraphPattern::Reduced",
            "GraphPattern::Slice",
            "GraphPattern::Group",
            "GraphPattern::Service",
            "pub struct ExecutionAnalysis",
            "pub enum SparqlCertificationError",
            "SparqlCertificationError::RemoteService",
            "SparqlCertificationError::NondeterministicFunction",
            "pub fn require_certifiable(&self)",
            "volatile_and_remote_features_parse_then_receive_execution_policy",
            "pub fn distributed_graph_fragments(&self)",
            "collect_constant_graph_join_leaves",
            "only_pure_constant_graph_inner_join_is_distributable",
            "canonical_algebra_hash_is_stable_across_surface_whitespace",
        ),
    )
    for forbidden in ("query_lexemes", "QueryLexeme", "keyword_start", "parse_graph_term", "distributed_graph_blocks"):
        if forbidden in typed:
            raise RuntimeError(f"typed compiler still contains lexical semantic scanner {forbidden}")
    if "unwrap()" in typed or "expect(" in typed:
        raise RuntimeError("typed SPARQL compiler contains panic-oriented convenience calls")

    compiler = require(
        "crates/ngkg-reference/src/compiler.rs",
        (
            "CompiledSparqlQuery::parse(&query_text)",
            "compiled.route_analysis()",
            "compiled.dataset_specification()",
            "compiled.distributed_graph_fragments()",
            "SPARQL_ALGEBRA_FORMAT_VERSION",
            "sparql_algebra_format_version",
            "sparql_algebra_sha256",
            '.algebra.sse',
            "ROUTE_MODE_TYPED_ACTIVE_DATASET",
            "ROUTE_MODE_TYPED_DECLARED_GRAPH",
            "ROUTE_MODE_TYPED_PROPERTY_PATH_FULL_ACTIVE_DEFAULT",
            "ROUTE_MODE_TYPED_ACTIVE_DATASET_FALLBACK",
        ),
    )
    for forbidden in (
        "QueryLexeme",
        "query_lexemes(",
        "query_route_hints(",
        "distributed_graph_blocks(",
        "keyword_start(",
        "parse_graph_term(",
    ):
        if forbidden in compiler:
            raise RuntimeError(f"reference compiler still makes semantic decisions via {forbidden}")

    model = require(
        "crates/ngkg-reference/src/model.rs",
        (
            "pub sparql_algebra_format_version: u32",
            "pub sparql_algebra_sha256: String",
        ),
    )
    _ = model
    runtime = require(
        "crates/ngkg-reference/src/lib.rs",
        (
            "CompiledSparqlQuery::parse(query)",
            "pub fn execute_compiled_with_dataset(",
            "certificate.sparql_algebra_format_version",
            "compiled.canonical_sse_sha256()",
            "execute_compiled_query_with_dataset_cancellable(",
        ),
    )
    _ = runtime

    query = require(
        "crates/ngkg-reference/src/query.rs",
        (
            "CompiledSparqlQuery::parse(query_text)",
            "pub fn execute_compiled_query(",
            "pub fn execute_compiled_query_with_dataset_cancellable(",
            ".for_query(compiled.query_clone())",
            "set_default_graph(default_graphs)",
            "set_available_named_graphs(named_graphs)",
        ),
    )
    if ".parse_query(query_text)" in query:
        raise RuntimeError("reference evaluator reparses text instead of consuming shared typed algebra")

    serving = require(
        "services/online-serving/src/main.rs",
        (
            "CompiledSparqlQuery::parse(query)",
            "compiled_query.form()",
            "compiled_query.dataset_specification().clone()",
            "certificate.sparql_algebra_format_version",
            "certificate.sparql_algebra_sha256",
            "runtime.execute_compiled_with_dataset_bounded_cancellable(",
            "ActiveDatasetNotCertified",
            "QueryResultsSerializer::from_format",
            "QueryResultsFormat::Json",
            "QueryResultsFormat::Xml",
            "QueryResultsFormat::Tsv",
            "QueryResultsFormat::Csv",
            "select_sparql_solution_format",
            "serve_swagger_ui",
            'SwaggerConfig::from("/openapi.json")',
            "SWAGGER_CONFIG",
            "content-security-policy",
            "x-content-type-options",
            'QueryForm::Select',
            'QueryForm::Ask',
            'QueryForm::Construct',
            'QueryForm::Describe',
            "GatewayTimeout(String)",
            "error.is_timeout()",
            "StatusCode::GATEWAY_TIMEOUT",
            "upstream_status_error",
            'positive_u32("NGKG_DATABASE_MAX_CONNECTIONS")',
            "tokio::task::spawn_blocking(move ||",
        ),
    )
    if "cdn.jsdelivr.net" in serving or "unpkg.com" in serving:
        raise RuntimeError("Swagger UI has a runtime public-CDN dependency")
    for stale in ("QueryLexeme", "query_lexemes(", "query_route_hints("):
        if stale in serving:
            raise RuntimeError(f"online serving contains stale lexical semantic scanner {stale}")

    routing = strict_schema("contracts/query-routing-certificate.schema.json")
    allowed_modes = set(routing["properties"]["selectionMode"]["enum"])
    expected_modes = {
        "typed_active_dataset",
        "typed_declared_graph",
        "typed_property_path_full_active_default",
        "typed_active_default_no_capability",
        "typed_capability_dependency",
        "typed_active_dataset_fallback",
    }
    if allowed_modes != expected_modes:
        raise RuntimeError("routing schema does not exactly bind Phase 38 typed selection modes")
    if routing["properties"]["selectedGraphIris"].get("minItems", 0) != 0:
        raise RuntimeError("routing schema incorrectly rejects valid zero-graph/zero-row routes")

    certified = strict_schema("contracts/certified-query-record.schema.json")
    required = set(certified["required"])
    if not {
        "sparqlAlgebraFormatVersion",
        "sparqlAlgebraSha256",
        "queryForm",
        "observedResultSha256",
        "routing",
    }.issubset(required):
        raise RuntimeError("certified-query schema omits typed algebra or exact result identity")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    paths = openapi.get("paths", {})
    for path in (
        "/v1/datasets/{datasetId}/sparql",
        "/v1/datasets/{datasetId}/sparql/service-description",
        "/openapi.yaml",
        "/openapi.json",
        "/docs",
    ):
        if path not in paths:
            raise RuntimeError(f"online OpenAPI omits {path}")
    for method in ("get", "post"):
        responses = paths["/v1/datasets/{datasetId}/sparql"][method]["responses"]
        if "504" not in responses:
            raise RuntimeError(f"SPARQL {method.upper()} contract omits timeout status 504")

    formats = set(openapi["components"]["responses"]["SparqlQuery"]["content"])
    expected_formats = {
        "application/sparql-results+json",
        "application/sparql-results+xml",
        "text/tab-separated-values",
        "text/csv",
        "text/turtle",
        "application/n-triples",
        "application/rdf+xml",
    }
    if formats != expected_formats:
        raise RuntimeError("SPARQL response formats do not cover the qualified Phase 39 superset")
    routing_api = openapi["components"]["schemas"]["Routing"]
    if set(routing_api["properties"]["selectionMode"]["enum"]) != expected_modes:
        raise RuntimeError("OpenAPI routing modes drift from the offline routing certificate")
    query_request_properties = openapi["components"]["schemas"]["QueryRequest"]["properties"]
    if not {"defaultGraphUris", "namedGraphUris"}.issubset(query_request_properties):
        raise RuntimeError("enriched query OpenAPI omits typed dataset override fields")

    workloads = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    if int(workloads["onlineServing"]["databaseMaxConnections"]) < 1:
        raise RuntimeError("online database pool must be positive and deployment-configurable")
    workload_schema = json.loads(require("charts/ngkg-workloads/values.schema.json"))
    required_online = set(workload_schema["properties"]["onlineServing"]["required"] )
    if not {"databaseMaxConnections", "maxQueryResultRows", "maxQueryGraphTriples", "maxQueryGraphBlankNodes", "queryTimeoutSeconds"}.issubset(required_online):
        raise RuntimeError("workload schema omits required bounded query execution settings")
    require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        ("NGKG_DATABASE_MAX_CONNECTIONS", "NGKG_MAX_QUERY_RESULT_ROWS", "NGKG_QUERY_TIMEOUT_SECONDS"),
    )

    status = require(
        "BUILD_STATUS_PHASE_38.md",
        (
            "implementation-candidate-not-production-qualified",
            "standards claims remain disabled",
            "`ASK`, `CONSTRUCT`, and `DESCRIBE` remain fail-closed",
            "`Cargo.lock` is intentionally not fabricated",
        ),
    )
    _ = status

    print("Phase 38 static contract verification passed; Cargo/W3C/live RKE2 gates remain mandatory")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 38 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
