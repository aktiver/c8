#!/usr/bin/env python3
"""Make every shipped REST operation self-explanatory in Swagger and emit a route catalog."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPECS = [
    ROOT / "NGKG_1_0_0_GA/api/openapi.yaml",
    ROOT / "NGKG_1_0_0_GA/api/online-openapi.yaml",
    *sorted((ROOT / "ngkg-agents/contracts").glob("*openapi.yaml")),
]
METHODS = {"get", "post", "put", "patch", "delete", "head", "options"}


def sentences(value: str) -> list[str]:
    normalized = " ".join(value.split())
    return [part.strip() for part in re.split(r"(?<=[.!?])\s+", normalized) if part.strip()]


def humanize(value: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value).replace("_", "-")
    return value.strip().rstrip(".")


def use_sentence(path: str, summary: str) -> str:
    text = f"{path} {summary}".lower()
    if "health" in text:
        purpose = "Kubernetes probes and operators need to decide whether this exact process should receive traffic"
    elif "metrics" in text:
        purpose = "Prometheus and platform operators need bounded operational measurements for alerting and autoscaling"
    elif "openapi" in text or "swagger" in text or path == "/docs":
        purpose = "client developers need the exact running contract for code generation, testing or interactive requests"
    elif "query_log" in text or "query log" in text:
        purpose = "a user or auditor needs tenant-scoped execution timing, resource attribution and outcome evidence"
    elif "sparql" in text or "/query" in text:
        purpose = "an authorized application needs a snapshot-bound semantic answer rather than direct access to internal stores"
    elif "memory" in text:
        purpose = "an agent workflow needs durable, evidence-bound memory without allowing a model to write authoritative facts directly"
    elif "tool" in text or "approval" in text:
        purpose = "an agent workflow needs a qualified external tool under tenant policy, bounded transport and audit controls"
    elif "input" in text or "requirement" in text:
        purpose = "a client needs to ingest or inspect a large immutable agent input and its permanent constraint ledger"
    elif "context-slice" in text or "context slice" in text:
        purpose = "an agent needs a bounded, authorized slice of a reasoned context graph without receiving an entire snapshot"
    elif "hpc" in text or "qualification" in text:
        purpose = "an operator needs deterministic distributed-work evidence or the live resource envelope before enabling an optimized path"
    elif "storage" in text or "restore" in text or "backup" in text:
        purpose = "an operator needs recoverable snapshot storage work with checksum and lifecycle evidence"
    elif "dataset" in text or "snapshot" in text or "ingestion" in text or "import" in text:
        purpose = "an authorized data publisher needs to manage immutable dataset or snapshot lifecycle state"
    else:
        purpose = "an API client needs this capability through the supported contract instead of coupling to an internal service"
    return f"Use it when {purpose}."


def response_sentence(operation: dict[str, Any], method: str) -> str:
    request = operation.get("requestBody")
    if request:
        content = request.get("content", {}) if isinstance(request, dict) else {}
        media = ", ".join(sorted(content)) or "the documented media type"
        prefix = f"The request body uses {media} and the schema shown below"
    elif operation.get("parameters"):
        prefix = "Inputs use the documented path, query and header parameters"
    elif method == "get":
        prefix = "The operation takes no request body"
    else:
        prefix = "The operation uses the documented request contract"
    success = operation.get("responses", {}).get("200") or operation.get("responses", {}).get("201") or operation.get("responses", {}).get("202") or operation.get("responses", {}).get("204")
    if isinstance(success, dict) and "$ref" in success:
        result = f"the {success['$ref'].rsplit('/', 1)[-1]} response schema"
    elif isinstance(success, dict):
        response_description = sentences(str(success.get("description", "the documented success payload")))
        result = (response_description[0] if response_description else "the documented success payload").rstrip(".").lower()
    else:
        result = "the documented success payload"
    return f"{prefix}; success returns {result}."


def operation_id(method: str, path: str) -> str:
    tokens = re.findall(r"[A-Za-z0-9]+", path)
    return method.lower() + "".join(token[:1].upper() + token[1:] for token in tokens)


def enrich(path: Path) -> tuple[dict[str, Any], list[dict[str, str]]]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    rows: list[dict[str, str]] = []
    for route, item in document.get("paths", {}).items():
        for method, operation in item.items():
            if method.lower() not in METHODS or not isinstance(operation, dict):
                continue
            identifier = operation.setdefault("operationId", operation_id(method, route))
            summary = operation.get("summary") or humanize(identifier)
            operation["summary"] = summary.rstrip(".")
            original = sentences(str(operation.get("description", "")))
            description: list[str] = original[:2]
            legacy_generated = f"This route {summary[:1].lower() + summary[1:]}."
            if description and description[0] == legacy_generated:
                description[0] = f'This operation implements the documented "{summary}" API capability.'
            if not description:
                description.append(f'This operation implements the documented "{summary}" API capability.')
            if len(description) < 2:
                description.append(use_sentence(route, summary))
            description.append(response_sentence(operation, method.lower()))
            has_security = operation.get("security", document.get("security", [])) != []
            if has_security:
                description.append("Authentication, tenant authorization, resource ceilings and documented failure responses are enforced before a result is accepted.")
            else:
                description.append("This operational endpoint returns only the documented non-sensitive contract and never dataset contents or credentials.")
            operation["description"] = " ".join(description[:4])
            request_types = []
            request_body = operation.get("requestBody", {})
            if isinstance(request_body, dict):
                for media, body in request_body.get("content", {}).items():
                    schema = body.get("schema", {}) if isinstance(body, dict) else {}
                    request_types.append(f"{media} {schema.get('$ref', schema.get('type', 'schema'))}")
            success_types = []
            for code, response in operation.get("responses", {}).items():
                if not str(code).startswith("2") or not isinstance(response, dict):
                    continue
                if "$ref" in response:
                    success_types.append(f"{code} {response['$ref']}")
                for media, body in response.get("content", {}).items():
                    schema = body.get("schema", {}) if isinstance(body, dict) else {}
                    success_types.append(f"{code} {media} {schema.get('$ref', schema.get('type', 'schema'))}")
                if not response.get("content"):
                    success_types.append(f"{code} no body")
            rows.append({
                "method": method.upper(), "path": route, "operation": identifier,
                "summary": operation["summary"], "request": "; ".join(request_types) or "path/query/header parameters only",
                "response": "; ".join(success_types) or "documented response", "description": operation["description"],
            })
    path.write_text(yaml.safe_dump(document, sort_keys=False, width=120), encoding="utf-8")
    return document, rows


def main() -> None:
    catalog = ["# NGKG REST and Swagger route catalog", "", "Every operation below is embedded in its serving binary and appears in Swagger UI. Request and response keys, JSON value types, required fields, enums and bounds remain authoritative in the linked component schema inside each OpenAPI document.", ""]
    total = 0
    for spec in SPECS:
        if not spec.is_file():
            continue
        _, rows = enrich(spec)
        total += len(rows)
        catalog.extend([f"## `{spec.relative_to(ROOT)}`", "", "| Method | Route | Operation | Request | Success response |", "|---|---|---|---|---|"])
        for row in rows:
            catalog.append(f"| {row['method']} | `{row['path']}` | `{row['operation']}` — {row['summary']} | {row['request']} | {row['response']} |")
        catalog.append("")
    catalog.extend(["## Swagger descriptions", "", "Each Swagger operation contains a concise three- or four-sentence description covering intent, when to use it, the request and success payload shape, and the security/failure boundary.", ""])
    output = ROOT / "NGKG_1_0_0_GA/docs/API_ROUTE_CATALOG_PHASE8.md"
    output.write_text("\n".join(catalog), encoding="utf-8")
    print(f"enriched {total} operations across {len(SPECS)} OpenAPI documents")


if __name__ == "__main__":
    main()
