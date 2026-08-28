#!/usr/bin/env python3
"""Stage a checksum-addressed Phase 14 bundle in a local object-store root."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import uuid
from pathlib import Path
from typing import Any


OBJECT_SEGMENT = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._-]*$")
RESERVED_NAMES = {"compilation-bundle.json", "reference-compile.json"}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def positive(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def boolean(value: str) -> bool:
    if value == "true":
        return True
    if value == "false":
        return False
    raise argparse.ArgumentTypeError("value must be true or false")


def uuid_text(value: str) -> str:
    return str(uuid.UUID(value))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--object-root", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--ontology", type=Path, action="append", required=True)
    parser.add_argument("--projection-policy", type=Path, required=True)
    parser.add_argument("--query", type=Path, required=True)
    parser.add_argument("--expected", type=Path, required=True)
    parser.add_argument("--query-id", required=True)
    parser.add_argument("--ordered", type=boolean, required=True)
    parser.add_argument("--required-source-iri", action="append", default=[])
    parser.add_argument("--closure-graph-iri", required=True)
    parser.add_argument("--dataset-id", type=uuid_text, required=True)
    parser.add_argument("--snapshot-id", type=uuid_text, required=True)
    parser.add_argument("--parent-snapshot-id", type=uuid_text)
    parser.add_argument("--dataset-namespace", type=uuid_text, required=True)
    parser.add_argument("--source-guid", type=uuid_text, required=True)
    parser.add_argument("--source-snapshot", required=True)
    parser.add_argument("--max-input-bytes", type=positive, required=True)
    parser.add_argument("--max-quads", type=positive, required=True)
    parser.add_argument("--max-dictionary-terms", type=positive, required=True)
    parser.add_argument("--max-reasoner-seconds", type=positive, required=True)
    parser.add_argument("--parquet-row-group-rows", type=positive, required=True)
    parser.add_argument("--max-named-individuals", type=positive, required=True)
    parser.add_argument("--max-properties", type=positive, required=True)
    return parser.parse_args()


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish_file(source: Path, object_root: Path) -> dict[str, str]:
    source = source.resolve(strict=True)
    if (
        not source.is_file()
        or not OBJECT_SEGMENT.fullmatch(source.name)
        or source.name in RESERVED_NAMES
    ):
        raise ValueError(f"invalid artifact: {source}")
    digest = sha256(source)
    key = f"inputs/sha256/{digest}/{source.name}"
    destination = object_root / key
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if sha256(destination) != digest:
            raise ValueError(f"immutable object conflicts with {key}")
    else:
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.partial")
        with source.open("rb") as reader, temporary.open("xb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
            writer.flush()
            os.fsync(writer.fileno())
        if sha256(temporary) != digest:
            temporary.unlink()
            raise ValueError(f"copy checksum mismatch for {source}")
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if sha256(destination) != digest:
                raise ValueError(f"concurrent immutable object conflicts with {key}")
        finally:
            temporary.unlink(missing_ok=True)
        fsync_directory(destination.parent)
    return {"objectKey": key, "sha256": digest, "fileName": source.name}


def main() -> int:
    args = parse_args()
    root = args.object_root.resolve(strict=True)
    if not root.is_dir():
        raise ValueError("object-root must already be an existing directory")
    input_paths = [args.source, *args.ontology, args.query, args.expected]
    names = [path.resolve(strict=True).name for path in input_paths]
    if len(names) != len(set(names)):
        raise ValueError("source, ontology, query and expected files require unique base names")
    source = publish_file(args.source, root)
    ontologies = [publish_file(path, root) for path in args.ontology]
    query = publish_file(args.query, root)
    expected = publish_file(args.expected, root)
    policy: dict[str, Any] = json.loads(args.projection_policy.read_text(encoding="utf-8"))
    bundle = {
        "formatVersion": 1,
        "datasetId": args.dataset_id,
        "snapshotId": args.snapshot_id,
        "parentSnapshotId": args.parent_snapshot_id,
        "datasetNamespace": args.dataset_namespace,
        "sourceGuid": args.source_guid,
        "sourceSnapshot": args.source_snapshot,
        "source": source,
        "ontologyBundle": ontologies,
        "projection": policy,
        "reasoning": {
            "closureGraphIri": args.closure_graph_iri,
            "maxNamedIndividuals": args.max_named_individuals,
            "maxProperties": args.max_properties,
        },
        "certifiedQueries": [{
            "queryId": args.query_id,
            "ordered": args.ordered,
            "query": query,
            "expected": expected,
            "requiredSourceIris": sorted(set(args.required_source_iri)),
        }],
        "limits": {
            "maxInputBytes": args.max_input_bytes,
            "maxQuads": args.max_quads,
            "maxDictionaryTerms": args.max_dictionary_terms,
            "maxReasonerSeconds": args.max_reasoner_seconds,
            "parquetRowGroupRows": args.parquet_row_group_rows,
        },
    }
    bundle_bytes = (json.dumps(bundle, indent=2) + "\n").encode()
    bundle_hash = hashlib.sha256(bundle_bytes).hexdigest()
    bundle_key = f"bundles/sha256/{bundle_hash}/compilation-bundle.json"
    destination = root / bundle_key
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if destination.read_bytes() != bundle_bytes:
            raise ValueError(f"immutable bundle conflicts with {bundle_key}")
    else:
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.partial")
        with temporary.open("xb") as handle:
            handle.write(bundle_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, destination)
        except FileExistsError:
            if destination.read_bytes() != bundle_bytes:
                raise ValueError(f"concurrent immutable bundle conflicts with {bundle_key}")
        finally:
            temporary.unlink(missing_ok=True)
        fsync_directory(destination.parent)
    print(json.dumps({"bundleObjectKey": bundle_key, "bundleSha256": bundle_hash}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
