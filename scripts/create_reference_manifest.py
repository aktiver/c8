#!/usr/bin/env python3
"""Create a checksum-complete reference manifest from real local artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def artifact(path: Path) -> dict[str, str]:
    resolved = path.resolve(strict=True)
    if not resolved.is_file():
        raise ValueError(f"artifact is not a file: {resolved}")
    return {"path": str(resolved), "sha256": sha256(resolved)}


def positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("value must be greater than zero")
    return number


def boolean(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise argparse.ArgumentTypeError("value must be true or false")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--ontology", type=Path, action="append", required=True)
    parser.add_argument("--projection-policy", type=Path, required=True)
    parser.add_argument("--query", type=Path, required=True)
    parser.add_argument("--expected", type=Path, required=True)
    parser.add_argument("--query-id", required=True)
    parser.add_argument("--ordered", type=boolean, required=True)
    parser.add_argument("--required-source-iri", action="append", default=[])
    parser.add_argument("--closure-graph-iri", required=True)
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument("--dataset-namespace", required=True)
    parser.add_argument("--source-guid", required=True)
    parser.add_argument("--source-snapshot", required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--manifest-output", type=Path, required=True)
    parser.add_argument("--max-input-bytes", type=positive, required=True)
    parser.add_argument("--max-quads", type=positive, required=True)
    parser.add_argument("--max-dictionary-terms", type=positive, required=True)
    parser.add_argument("--max-reasoner-seconds", type=positive, required=True)
    parser.add_argument("--parquet-row-group-rows", type=positive, required=True)
    parser.add_argument("--max-named-individuals", type=positive, required=True)
    parser.add_argument("--max-properties", type=positive, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    policy: dict[str, Any] = json.loads(args.projection_policy.read_text(encoding="utf-8"))
    output_directory = args.output_directory.resolve(strict=True)
    if not output_directory.is_dir():
        raise ValueError("output-directory must already exist")
    manifest = {
        "formatVersion": 1,
        "datasetId": args.dataset_id,
        "snapshotId": args.snapshot_id,
        "parentSnapshotId": None,
        "datasetNamespace": args.dataset_namespace,
        "sourceGuid": args.source_guid,
        "sourceSnapshot": args.source_snapshot,
        "source": artifact(args.source),
        "ontologyBundle": [artifact(path) for path in args.ontology],
        "outputDirectory": str(output_directory),
        "projection": policy,
        "reasoning": {
            "closureGraphIri": args.closure_graph_iri,
            "maxNamedIndividuals": args.max_named_individuals,
            "maxProperties": args.max_properties,
        },
        "certifiedQueries": [
            {
                "queryId": args.query_id,
                "ordered": args.ordered,
                "query": artifact(args.query),
                "expected": artifact(args.expected),
                "requiredSourceIris": sorted(set(args.required_source_iri)),
            }
        ],
        "limits": {
            "maxInputBytes": args.max_input_bytes,
            "maxQuads": args.max_quads,
            "maxDictionaryTerms": args.max_dictionary_terms,
            "maxReasonerSeconds": args.max_reasoner_seconds,
            "parquetRowGroupRows": args.parquet_row_group_rows,
        },
    }
    destination = args.manifest_output.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.partial")
    if temporary.exists():
        raise FileExistsError(temporary)
    temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary.replace(destination)
    print(destination)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
