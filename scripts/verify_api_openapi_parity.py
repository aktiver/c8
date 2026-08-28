#!/usr/bin/env python3
"""Verify that every runtime REST route is represented in the shipped OpenAPI contracts."""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from typing import Iterable

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
HTTP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options", "trace"}


def _balanced_route_calls(text: str) -> Iterable[str]:
    needle = ".route("
    pos = 0
    while True:
        start = text.find(needle, pos)
        if start < 0:
            return
        i = start + len(needle)
        depth = 1
        in_string = False
        escape = False
        while i < len(text) and depth:
            ch = text[i]
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
            else:
                if ch == '"':
                    in_string = True
                elif ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
            i += 1
        if depth:
            raise RuntimeError("unbalanced .route(...) expression")
        yield text[start + len(needle): i - 1]
        pos = i


def _snake_to_camel(value: str) -> str:
    head, *tail = value.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in tail)


def normalize_path(path: str) -> str:
    def repl(match: re.Match[str]) -> str:
        raw = match.group(1)
        if raw.startswith("*"):
            return "{" + raw + "}"
        return "{" + _snake_to_camel(raw) + "}"
    return re.sub(r"\{([^{}]+)\}", repl, path)


def runtime_routes(source: pathlib.Path) -> set[tuple[str, str]]:
    text = source.read_text(encoding="utf-8")
    routes: set[tuple[str, str]] = set()
    for call in _balanced_route_calls(text):
        match = re.match(r'\s*"([^"]+)"\s*,(.*)\Z', call, re.S)
        if not match:
            continue
        path, handler_expr = match.groups()
        # Swagger UI assets are implementation support, not REST operations.
        if path == "/docs/{*asset}":
            continue
        methods = set(re.findall(r"\b(get|post|put|patch|delete|head|options|trace)\s*\(", handler_expr))
        if not methods:
            raise RuntimeError(f"could not determine HTTP method for {source}:{path}")
        normalized = normalize_path(path)
        routes.update((method, normalized) for method in methods)
    return routes


def openapi_routes(path: pathlib.Path) -> set[tuple[str, str]]:
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict) or not isinstance(doc.get("paths"), dict):
        raise RuntimeError(f"{path} does not contain an OpenAPI paths object")
    routes: set[tuple[str, str]] = set()
    for route, item in doc["paths"].items():
        if not isinstance(item, dict):
            continue
        for method in item:
            if method.lower() in HTTP_METHODS:
                routes.add((method.lower(), route))
    return routes



def _resolve_pointer(document: object, ref: str) -> object:
    if not ref.startswith("#/"):
        raise RuntimeError(f"only local OpenAPI refs are permitted by this verifier: {ref}")
    current = document
    for raw in ref[2:].split("/"):
        token = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(current, dict) and token in current:
            current = current[token]
        else:
            raise RuntimeError(f"broken OpenAPI ref {ref}")
    return current


def validate_openapi_document(path: pathlib.Path) -> None:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise RuntimeError(f"{path} is not an OpenAPI object")
    if document.get("openapi") != "3.1.0":
        raise RuntimeError(f"{path} must remain OpenAPI 3.1.0")
    for route, item in document.get("paths", {}).items():
        if not isinstance(item, dict):
            continue
        for method, operation in item.items():
            if method.lower() not in HTTP_METHODS:
                continue
            if not isinstance(operation, dict):
                raise RuntimeError(f"{path}:{method.upper()} {route} is not an operation object")
            if not operation.get("summary") and not operation.get("operationId"):
                raise RuntimeError(f"{path}:{method.upper()} {route} lacks a Swagger summary/operationId")
    stack = [document]
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            ref = value.get("$ref")
            if isinstance(ref, str):
                _resolve_pointer(document, ref)
            stack.extend(value.values())
        elif isinstance(value, list):
            stack.extend(value)

def check_pair(label: str, source_rel: str, openapi_rel: str) -> dict[str, object]:
    source = ROOT / source_rel
    contract = ROOT / openapi_rel
    runtime = runtime_routes(source)
    validate_openapi_document(contract)
    declared = openapi_routes(contract)
    missing = sorted(runtime - declared)
    stale = sorted(declared - runtime)
    if missing or stale:
        raise RuntimeError(
            f"{label} route/OpenAPI mismatch: missing={missing or '[]'} stale={stale or '[]'}"
        )
    required_docs = {("get", "/docs"), ("get", "/openapi.yaml"), ("get", "/openapi.json")}
    absent_docs = sorted(required_docs - runtime)
    if absent_docs:
        raise RuntimeError(f"{label} runtime is missing Swagger/OpenAPI routes: {absent_docs}")
    return {
        "label": label,
        "source": source_rel,
        "openapi": openapi_rel,
        "operationCount": len(runtime),
        "operations": [f"{method.upper()} {route}" for method, route in sorted(runtime)],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args()
    records = [
        check_pair("control-plane", "services/api/src/main.rs", "api/openapi.yaml"),
        check_pair("online-data-plane", "services/online-serving/src/main.rs", "api/online-openapi.yaml"),
    ]
    report = {"formatVersion": 1, "status": "pass", "services": records}
    if args.report:
        output = args.report if args.report.is_absolute() else ROOT / args.report
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for record in records:
        print(f"{record['label']}: {record['operationCount']} OpenAPI-covered REST operations")
    print("Swagger/OpenAPI route parity verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, TypeError, ValueError, yaml.YAMLError) as exc:
        print(f"API/OpenAPI parity verification failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
