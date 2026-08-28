#!/usr/bin/env python3
"""Validate the Phase 40.1 OWL signature contract, including deterministic ordering."""
from __future__ import annotations
import argparse, hashlib, json, pathlib, re, sys
from jsonschema import Draft202012Validator, FormatChecker

ROOT = pathlib.Path(__file__).resolve().parents[1]
SHA256 = re.compile(r"^[0-9a-f]{64}$")
IRI_ARRAYS = (
    "imports", "classes", "objectProperties", "dataProperties",
    "annotationProperties", "namedIndividuals", "datatypes",
)

def sha256(path: pathlib.Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024*1024), b""):
            h.update(block)
    return h.hexdigest()

def strictly_sorted_unique(values: list[str]) -> bool:
    return all(left < right for left,right in zip(values, values[1:]))

def main() -> int:
    ap=argparse.ArgumentParser()
    ap.add_argument("signature", type=pathlib.Path)
    ap.add_argument("--expected-sha256")
    args=ap.parse_args()
    raw=args.signature.read_bytes(); value=json.loads(raw)
    schema=json.loads((ROOT/"contracts/owl-signature.schema.json").read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    errors=sorted(Draft202012Validator(schema,format_checker=FormatChecker()).iter_errors(value),key=lambda e:list(e.path))
    if errors:
        raise RuntimeError("schema validation failed: "+"; ".join(error.message for error in errors[:8]))
    if args.expected_sha256 and (not SHA256.fullmatch(args.expected_sha256) or sha256(args.signature)!=args.expected_sha256):
        raise RuntimeError("signature SHA-256 does not match expected digest")
    for key in IRI_ARRAYS:
        if not strictly_sorted_unique(value[key]):
            raise RuntimeError(f"{key} must be strictly sorted and unique")
    documents=value["ontologyDocuments"]
    normalized=[]
    for document in documents:
        iris=document["ontologyIris"]
        if not strictly_sorted_unique(iris):
            raise RuntimeError("ontologyDocuments[].ontologyIris must be strictly sorted and unique")
        normalized.append((document["sha256"], tuple(iris)))
    if normalized != sorted(normalized) or len(normalized)!=len(set(normalized)):
        raise RuntimeError("ontologyDocuments must be deterministically sorted and unique")
    print(json.dumps({
        "status":"valid",
        "sha256":sha256(args.signature),
        "classes":len(value["classes"]),
        "objectProperties":len(value["objectProperties"]),
        "dataProperties":len(value["dataProperties"]),
        "annotationProperties":len(value["annotationProperties"]),
        "namedIndividuals":len(value["namedIndividuals"]),
        "datatypes":len(value["datatypes"]),
        "imports":len(value["imports"]),
        "ontologyDocuments":len(documents),
    },sort_keys=True))
    return 0

if __name__ == "__main__":
    try: raise SystemExit(main())
    except (OSError,ValueError,RuntimeError,json.JSONDecodeError) as exc:
        print(f"OWL signature validation failed: {exc}",file=sys.stderr); raise SystemExit(1)
