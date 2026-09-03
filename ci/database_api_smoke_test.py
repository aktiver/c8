#!/usr/bin/env python3
"""Smoke-test the deployed NGKG control and online database APIs.

This test deliberately treats ontology alignment and OWL certification as
upstream responsibilities. It verifies only the NGKG database boundary:
service health, OpenAPI exposure, immutable TriG upload, and SPARQL execution
against an already-published dataset.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class SmokeFailure(RuntimeError):
    """Raised when a deployed endpoint violates its smoke-test contract."""


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        try:
            return json.loads(self.body)
        except json.JSONDecodeError as exc:
            raise SmokeFailure(f"expected JSON response, received: {self.body[:300]!r}") from exc


def request(
    method: str,
    url: str,
    *,
    token: str | None = None,
    body: bytes | None = None,
    content_type: str | None = None,
    accept: str | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 30.0,
) -> Response:
    request_headers = {"User-Agent": "ngkg-database-smoke/1.0", **(headers or {})}
    if token:
        request_headers["Authorization"] = f"Bearer {token}"
    if content_type:
        request_headers["Content-Type"] = content_type
    if accept:
        request_headers["Accept"] = accept
    req = urllib.request.Request(url, data=body, headers=request_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as result:
            return Response(
                status=result.status,
                headers={key.lower(): value for key, value in result.headers.items()},
                body=result.read(),
            )
    except urllib.error.HTTPError as exc:
        response_body = exc.read()
        raise SmokeFailure(
            f"{method} {url} returned HTTP {exc.code}: {response_body[:1000].decode(errors='replace')}"
        ) from exc
    except urllib.error.URLError as exc:
        raise SmokeFailure(f"{method} {url} failed: {exc.reason}") from exc


def expect_status(response: Response, expected: int, label: str) -> None:
    if response.status != expected:
        raise SmokeFailure(f"{label} returned HTTP {response.status}; expected {expected}")


def url(base: str, path: str) -> str:
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def check_public_surfaces(control_url: str, online_url: str, timeout: float) -> list[str]:
    passed: list[str] = []
    checks = (
        (control_url, "/health/live", 204, "control liveness"),
        (control_url, "/health/ready", 204, "control readiness"),
        (control_url, "/docs", 200, "control Swagger"),
        (control_url, "/openapi.json", 200, "control OpenAPI"),
        (online_url, "/health/live", 204, "online liveness"),
        (online_url, "/health/ready", 204, "online readiness"),
        (online_url, "/docs", 200, "online Swagger"),
        (online_url, "/openapi.json", 200, "online OpenAPI"),
    )
    for base, path, expected, label in checks:
        expect_status(request("GET", url(base, path), timeout=timeout), expected, label)
        passed.append(label)
    return passed


def create_dataset(
    control_url: str,
    token: str,
    dataset_id: str,
    identity_namespace: str,
    policy_version: str,
    timeout: float,
) -> None:
    payload = json.dumps(
        {"identityNamespace": identity_namespace, "policyVersion": policy_version},
        separators=(",", ":"),
    ).encode()
    response = request(
        "PUT",
        url(control_url, f"/v1/datasets/{dataset_id}"),
        token=token,
        body=payload,
        content_type="application/json",
        timeout=timeout,
    )
    expect_status(response, 204, "dataset creation")


def upload_trig(
    control_url: str,
    token: str,
    dataset_id: str,
    source_id: str,
    trig_path: Path,
    timeout: float,
) -> dict[str, Any]:
    trig = trig_path.read_bytes()
    digest = hashlib.sha256(trig).hexdigest()
    response = request(
        "PUT",
        url(control_url, f"/v1/datasets/{dataset_id}/sources/{source_id}"),
        token=token,
        body=trig,
        content_type="application/trig; charset=utf-8",
        accept="application/json",
        headers={"X-NGKG-Content-SHA256": digest},
        timeout=timeout,
    )
    expect_status(response, 201, "TriG upload")
    result = response.json()
    required = {
        "sourceId",
        "datasetId",
        "objectKey",
        "sha256",
        "bytes",
        "parsedQuadCount",
        "namedGraphs",
        "metadataObjectKey",
        "metadataSha256",
    }
    missing = sorted(required - set(result)) if isinstance(result, dict) else sorted(required)
    if missing:
        raise SmokeFailure(f"TriG upload response is missing fields: {', '.join(missing)}")
    if result["datasetId"] != dataset_id or result["sourceId"] != source_id:
        raise SmokeFailure("TriG upload response changed the requested dataset or source identity")
    if result["sha256"] != digest or result["bytes"] != len(trig):
        raise SmokeFailure("TriG upload response is not bound to the exact uploaded bytes")
    if result["parsedQuadCount"] < 1 or not result["namedGraphs"]:
        raise SmokeFailure("TriG upload did not preserve a non-empty named-graph dataset")
    return result


def execute_sparql(
    online_url: str,
    token: str,
    dataset_id: str,
    query: str,
    timeout: float,
) -> Response:
    response = request(
        "POST",
        url(online_url, f"/v1/datasets/{dataset_id}/sparql"),
        token=token,
        body=query.encode(),
        content_type="application/sparql-query; charset=utf-8",
        accept="application/sparql-results+json",
        timeout=timeout,
    )
    expect_status(response, 200, "SPARQL query")
    if "x-ngkg-query-execution-id" not in response.headers:
        raise SmokeFailure("SPARQL response is missing x-ngkg-query-execution-id")
    response.json()
    return response


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-url", required=True, help="Control-plane base URL")
    parser.add_argument("--online-url", required=True, help="Online query-plane base URL")
    parser.add_argument("--token", help="Bearer token for authenticated tests")
    parser.add_argument("--dataset-id", type=uuid.UUID, help="Existing or new dataset UUID")
    parser.add_argument("--identity-namespace", type=uuid.UUID, help="Immutable identity namespace")
    parser.add_argument("--source-id", type=uuid.UUID, help="Source UUID for --trig")
    parser.add_argument("--trig", type=Path, help="Already-aligned TriG file to upload")
    parser.add_argument("--policy-version", default="staging-v1")
    parser.add_argument(
        "--query",
        default="SELECT (COUNT(*) AS ?count) WHERE { GRAPH ?g { ?s ?p ?o } }",
        help="SPARQL smoke query; executed only with --query-published-dataset",
    )
    parser.add_argument("--query-published-dataset", action="store_true")
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    results: dict[str, Any] = {
        "scope": "deployed-database-smoke",
        "alignmentOrCertificationPerformed": False,
        "checks": check_public_surfaces(args.control_url, args.online_url, args.timeout),
    }
    authenticated = args.trig is not None or args.query_published_dataset
    if authenticated and (not args.token or args.dataset_id is None):
        raise SmokeFailure("authenticated tests require --token and --dataset-id")
    if args.trig is not None:
        if args.identity_namespace is None or args.source_id is None:
            raise SmokeFailure("--trig requires --identity-namespace and --source-id")
        if not args.trig.is_file():
            raise SmokeFailure(f"TriG file does not exist: {args.trig}")
        create_dataset(
            args.control_url,
            args.token,
            str(args.dataset_id),
            str(args.identity_namespace),
            args.policy_version,
            args.timeout,
        )
        upload = upload_trig(
            args.control_url,
            args.token,
            str(args.dataset_id),
            str(args.source_id),
            args.trig,
            args.timeout,
        )
        results["checks"].extend(["dataset creation", "immutable TriG upload"])
        results["upload"] = upload
    if args.query_published_dataset:
        sparql = execute_sparql(
            args.online_url,
            args.token,
            str(args.dataset_id),
            args.query,
            args.timeout,
        )
        results["checks"].append("published snapshot SPARQL")
        results["queryExecutionId"] = sparql.headers["x-ngkg-query-execution-id"]
    results["status"] = "PASS"
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SmokeFailure as exc:
        print(json.dumps({"status": "FAIL", "error": str(exc)}, indent=2), file=sys.stderr)
        raise SystemExit(1) from exc
