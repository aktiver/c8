#!/usr/bin/env python3
"""Fail when a Helm image value has no Docker build or a build is unused."""
from pathlib import Path
import argparse
import json
import re
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--lock", type=Path)
args = parser.parse_args()
root = Path(__file__).resolve().parent.parent
catalog = json.loads((root / "docker_repos/images.json").read_text(encoding="utf-8"))["images"]
names = [row["name"] for row in catalog]
if len(names) != 13 or len(names) != len(set(names)):
    raise SystemExit("image catalog must contain the 13 unique Phase 8 release images")
for row in catalog:
    dockerfile = root / "docker_repos" / row["dockerfile"]
    if not dockerfile.is_file() or not dockerfile.read_text(encoding="utf-8").lstrip().startswith("ARG"):
        raise SystemExit(f"missing buildable Dockerfile: {dockerfile}")
    if not row.get("helm"):
        raise SystemExit(f"image has no Helm consumer: {row['name']}")
phase3 = json.loads((root / "phase3/config/images.json").read_text(encoding="utf-8"))["images"]
if {row["name"] for row in phase3} != set(names):
    raise SystemExit("phase3 and docker_repos image catalogs differ")
templates = "\n".join(path.read_text(encoding="utf-8") for path in root.glob("**/charts/*/templates/*.yaml"))
for row in catalog:
    for _, dotted in row["helm"]:
        leaf = dotted.split(".")[-1]
        if leaf not in {"image"} and not re.search(rf"\.Values\.[A-Za-z0-9_.]*{re.escape(leaf)}", templates):
            raise SystemExit(f"Helm value is not consumed: {dotted}")
if args.lock is not None:
    lock = json.loads(args.lock.read_text(encoding="utf-8"))
    rows = lock.get("images", [])
    locked_names = {row.get("name") for row in rows}
    if locked_names != set(names):
        raise SystemExit("image lock does not contain the exact catalog image set")
    digest = re.compile(r"^sha256:[0-9a-f]{64}$")
    for row in rows:
        if not digest.fullmatch(str(row.get("digest", ""))):
            raise SystemExit(f"image lock has an invalid digest: {row.get('name')}")
print(json.dumps({"status":"PASS","images":len(names),"dockerfiles":len(names)}, sort_keys=True))
