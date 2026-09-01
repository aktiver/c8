#!/usr/bin/env python3
"""Compare the native serving lane with an isolated qualification oracle."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import ssl
import sys
import urllib.error
import urllib.request
from typing import Any

from phase6_common import (
    EvidenceRecorder,
    atomic_json,
    canonical,
    epoch_ms,
    load_json,
    monotonic_ms,
    require,
    require_private_file,
    resolve,
    run,
    sha256_bytes,
    sha256_file,
    valid_sha256,
)


def canonical_term(term: dict[str, Any]) -> tuple[str, str, str, str]:
    kind = term.get("type")
    value = term.get("value")
    require(kind in {"uri", "literal", "typed-literal", "bnode"}, "invalid SPARQL term type")
    require(isinstance(value, str), "SPARQL term value is not a string")
    datatype = term.get("datatype", "")
    language = term.get("xml:lang", term.get("lang", ""))
    require(isinstance(datatype, str) and isinstance(language, str), "invalid literal metadata")
    require(not (datatype and language), "literal cannot have datatype and language together")
    # Phase 4 made blank-node labels snapshot/source scoped. Exact label equality is
    # therefore part of differential correctness, not an implementation accident.
    return kind, value, datatype, language.lower()


def canonical_sparql_json(payload: bytes) -> tuple[str, dict[str, Any]]:
    try:
        document = json.loads(payload)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"invalid SPARQL JSON: {error}") from error
    require(isinstance(document, dict), "SPARQL JSON root must be an object")
    if "boolean" in document:
        require(isinstance(document["boolean"], bool), "ASK result is not boolean")
        normalized = {"form": "ASK", "boolean": document["boolean"]}
        return sha256_bytes(canonical(normalized)), normalized
    variables = document.get("head", {}).get("vars")
    bindings = document.get("results", {}).get("bindings")
    require(isinstance(variables, list) and all(isinstance(v, str) for v in variables), "SELECT variables are invalid")
    require(len(variables) == len(set(variables)), "SELECT variables are duplicated")
    require(isinstance(bindings, list), "SELECT bindings are absent")
    ordered_variables = sorted(variables)
    rows: Counter[bytes] = Counter()
    for binding in bindings:
        require(isinstance(binding, dict), "SELECT binding is not an object")
        require(set(binding) <= set(variables), "SELECT binding contains an undeclared variable")
        row = []
        for variable in ordered_variables:
            row.append(None if variable not in binding else canonical_term(binding[variable]))
        rows[canonical(row)] += 1
    normalized_rows = [
        {"binding": json.loads(key), "multiplicity": count}
        for key, count in sorted(rows.items(), key=lambda item: item[0])
    ]
    normalized = {"form": "SELECT", "variables": ordered_variables, "rows": normalized_rows}
    return sha256_bytes(canonical(normalized)), normalized


def canonical_graph(payload: bytes, canonicalizer: Path | None, canonicalizer_sha256: str | None) -> tuple[str, dict[str, Any]]:
    if b"_:" in payload:
        require(canonicalizer is not None, "RDF graph with blank nodes requires a pinned RDFC-1.0 canonicalizer")
        require(canonicalizer.is_absolute() and canonicalizer.is_file(), "RDF canonicalizer is unavailable")
        require(valid_sha256(canonicalizer_sha256) and sha256_file(canonicalizer) == canonicalizer_sha256, "RDF canonicalizer checksum mismatch")
        normalized_bytes = run([str(canonicalizer)], stdin=payload, timeout=600)
    else:
        lines = {line.strip() for line in payload.splitlines() if line.strip() and not line.lstrip().startswith(b"#")}
        normalized_bytes = b"\n".join(sorted(lines)) + (b"\n" if lines else b"")
    return sha256_bytes(normalized_bytes), {"form": "RDF_GRAPH", "canonicalBytes": len(normalized_bytes)}


def endpoint_result(
    endpoint: str,
    token: str,
    query: bytes,
    accept: str,
    maximum_bytes: int,
    timeout_seconds: int,
    ssl_context: ssl.SSLContext,
) -> dict[str, Any]:
    require(endpoint.startswith("https://"), "differential endpoint must use HTTPS")
    request = urllib.request.Request(
        endpoint,
        data=query,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/sparql-query",
            "Accept": accept,
            "X-NGKG-Qualification-Run": "phase6-native-oracle",
        },
    )
    started = monotonic_ms()
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=ssl_context) as response:
            status = response.status
            headers = {key.lower(): value for key, value in response.headers.items()}
            payload = response.read(maximum_bytes + 1)
    except urllib.error.HTTPError as error:
        body = error.read(min(maximum_bytes, 4096)).decode("utf-8", errors="replace")
        raise RuntimeError(f"differential endpoint returned HTTP {error.code}: {body}") from error
    elapsed = monotonic_ms() - started
    require(status == 200, f"differential endpoint returned HTTP {status}")
    require(len(payload) <= maximum_bytes, "differential response exceeded byte ceiling")
    require(headers.get("x-ngkg-complete", "").lower() == "true", "endpoint did not certify a complete result")
    return {"headers": headers, "payload": payload, "elapsedMs": elapsed, "bytes": len(payload)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    config_path = args.config.resolve()
    config = load_json(config_path)
    base = config_path.parent
    require(config.get("formatVersion") == 1, "unsupported differential configuration")
    subject = config.get("subjectSha256")
    require(valid_sha256(subject), "invalid differential subject")
    require(config.get("oracleIsolation") == "QUALIFICATION_ONLY", "oracle must be isolated and qualification-only")
    require(config.get("nativeRequiredMode") is True, "native endpoint must use required cutover mode")
    require(config.get("oracleProductionTraffic") is False, "oracle must not receive production traffic")
    cases = config.get("cases")
    require(isinstance(cases, list) and cases, "differential corpus is empty")
    case_ids = [case.get("id") for case in cases]
    require(all(isinstance(item, str) and item for item in case_ids) and len(case_ids) == len(set(case_ids)), "differential case IDs are invalid")
    minimum_repetitions = int(config.get("measuredRepetitions", 3))
    require(minimum_repetitions >= 3, "at least three measured repetitions are required")

    native_token_path = resolve(base, config["nativeTokenFile"])
    oracle_token_path = resolve(base, config["oracleTokenFile"])
    require_private_file(native_token_path, "native token")
    require_private_file(oracle_token_path, "oracle token")
    native_token = native_token_path.read_text(encoding="utf-8").strip()
    oracle_token = oracle_token_path.read_text(encoding="utf-8").strip()
    require(bool(native_token) and bool(oracle_token), "empty endpoint token")
    ca_path = resolve(base, config["caBundleFile"])
    require(ca_path.is_file(), "CA bundle is missing")
    ssl_context = ssl.create_default_context(cafile=str(ca_path))
    canonicalizer = resolve(base, config["rdfCanonicalizer"]) if config.get("rdfCanonicalizer") else None
    canonicalizer_sha = config.get("rdfCanonicalizerSha256")
    recorder = EvidenceRecorder(args.output.resolve() / "cases", subject)
    semantic_roots: list[dict[str, str]] = []

    for case in cases:
        query_path = resolve(base, case["queryFile"])
        require(query_path.is_file(), f"query fixture is missing: {query_path}")
        query = query_path.read_bytes()
        require(sha256_bytes(query) == case.get("querySha256"), f"query checksum mismatch: {case['id']}")
        accept = case["accept"]
        form = case["form"]
        require(form in {"SELECT", "ASK", "CONSTRUCT", "DESCRIBE"}, "unsupported query form")
        maximum = int(case.get("maximumResponseBytes", 64 * 1024 * 1024))
        require(0 < maximum <= 1024 * 1024 * 1024, "invalid response byte ceiling")
        timeout_seconds = int(case.get("timeoutSeconds", 600))
        started = epoch_ms()
        repetitions = []
        expected_hash: str | None = None
        snapshot_id: str | None = None
        for repetition in range(minimum_repetitions):
            native = endpoint_result(config["nativeEndpoint"], native_token, query, accept, maximum, timeout_seconds, ssl_context)
            oracle = endpoint_result(config["oracleEndpoint"], oracle_token, query, accept, maximum, timeout_seconds, ssl_context)
            if form in {"SELECT", "ASK"}:
                native_hash, native_summary = canonical_sparql_json(native["payload"])
                oracle_hash, oracle_summary = canonical_sparql_json(oracle["payload"])
            else:
                native_hash, native_summary = canonical_graph(native["payload"], canonicalizer, canonicalizer_sha)
                oracle_hash, oracle_summary = canonical_graph(oracle["payload"], canonicalizer, canonicalizer_sha)
            require(native_hash == oracle_hash, f"semantic mismatch in {case['id']} repetition {repetition}")
            require(native_summary == oracle_summary, f"semantic summary mismatch in {case['id']}")
            require(expected_hash in {None, native_hash}, f"nondeterministic native result in {case['id']}")
            expected_hash = native_hash
            reported_hash = native["headers"].get("x-ngkg-semantic-result-sha256")
            require(valid_sha256(reported_hash), "native response omitted its canonical semantic result hash")
            require(
                not repetitions or reported_hash == repetitions[0]["nativeSemanticResultSha256"],
                f"native certified semantic hash changed during {case['id']}",
            )
            require(native["headers"].get("x-ngkg-native-cutover-mode") == "required", "native endpoint is not in required cutover mode")
            observed_snapshot = native["headers"].get("x-ngkg-snapshot-id")
            require(bool(observed_snapshot), "native response omitted snapshot identity")
            require(snapshot_id in {None, observed_snapshot}, f"snapshot changed during case {case['id']}")
            snapshot_id = observed_snapshot
            repetitions.append({
                "ordinal": repetition,
                "nativeElapsedMs": native["elapsedMs"],
                "oracleElapsedMs": oracle["elapsedMs"],
                "nativeBytes": native["bytes"],
                "oracleBytes": oracle["bytes"],
                "semanticResultSha256": native_hash,
                "nativeSemanticResultSha256": reported_hash,
                "nativeQueryExecutionId": native["headers"].get("x-ngkg-query-execution-id"),
            })
        ended = epoch_ms()
        recorder.record(case["id"], {
            "querySha256": case["querySha256"],
            "form": form,
            "snapshotId": snapshot_id,
            "semanticResultSha256": expected_hash,
            "reasoningRoute": case["reasoningRoute"],
            "repetitions": repetitions,
            "mismatchCount": 0,
        }, started, ended)
        semantic_roots.append({"id": case["id"], "semanticResultSha256": str(expected_hash)})

    required_forms = {"SELECT", "ASK", "CONSTRUCT", "DESCRIBE"}
    required_routes = {"CERTIFIED_CLOSURE", "EXACT_HERMIT"}
    require({case["form"] for case in cases} == required_forms, "differential corpus must cover every query form")
    require(required_routes <= {case["reasoningRoute"] for case in cases}, "differential corpus lacks reasoning route coverage")
    output = {
        "formatVersion": 1,
        "kind": "Phase6DifferentialEvidence",
        "subjectSha256": subject,
        "nativeCutoverMode": "required",
        "oracleIsolation": "QUALIFICATION_ONLY",
        "oracleProductionDependency": False,
        "caseCount": len(cases),
        "measuredRepetitions": minimum_repetitions,
        "mismatchCount": 0,
        "semanticRootSha256": sha256_bytes(canonical(sorted(semantic_roots, key=lambda row: row["id"]))),
        "scenarios": recorder.rows,
        "status": "PASS",
        "synthetic": False,
        "complete": True,
    }
    atomic_json(args.output.resolve() / "differential-evidence.json", output)
    print(json.dumps({"status": "PASS", "caseCount": len(cases), "semanticRootSha256": output["semanticRootSha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        print(f"Phase 6 differential qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
