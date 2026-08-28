#!/usr/bin/env python3
"""Validate the SPARQL 1.1 feature inventory and its local evidence paths."""
from __future__ import annotations

import json
import pathlib
import sys

import jsonschema


ROOT = pathlib.Path(__file__).resolve().parents[1]


def main() -> int:
    matrix_path = ROOT / "conformance/sparql11-feature-matrix.json"
    schema_path = ROOT / "contracts/sparql11-feature-matrix.schema.json"
    matrix = json.loads(matrix_path.read_text(encoding="utf-8"))
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator(schema).validate(matrix)
    identifiers: set[str] = set()
    for feature in matrix["features"]:
        identifier = feature["id"]
        if identifier in identifiers:
            raise RuntimeError(f"duplicate feature id {identifier}")
        identifiers.add(identifier)
        for evidence in feature["evidence"]:
            path = (ROOT / evidence).resolve()
            try:
                path.relative_to(ROOT)
            except ValueError as exc:
                raise RuntimeError(f"evidence escapes repository: {evidence}") from exc
            if not path.is_file():
                raise RuntimeError(f"missing evidence for {identifier}: {evidence}")
    if matrix["claim"] != "inventory":
        raise RuntimeError("matrix may become qualified only after all layers pass native gates")
    print(f"validated {len(identifiers)} SPARQL 1.1 functional inventory entries")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (json.JSONDecodeError, jsonschema.ValidationError, RuntimeError) as error:
        print(f"SPARQL feature-matrix validation failed: {error}", file=sys.stderr)
        raise SystemExit(1)
