#!/usr/bin/env python3
"""Write a deterministic SLSA-style provenance predicate for one OCI image."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--digest", required=True)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--builder-id", required=True)
    parser.add_argument("--platforms", required=True)
    parser.add_argument("--base", action="append", default=[])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.digest.startswith("sha256:") or len(args.digest) != 71:
        raise ValueError("invalid image digest")
    materials = []
    for reference in sorted(args.base):
        uri, digest = reference.rsplit("@sha256:", 1)
        if len(digest) != 64:
            raise ValueError("invalid base-image digest")
        materials.append({"uri": uri, "digest": {"sha256": digest}})
    predicate = {
        "buildDefinition": {
            "buildType": "https://c8-next-generation.io/buildtypes/oci-buildx-offline/v1",
            "externalParameters": {
                "image": args.name,
                "platforms": sorted(set(args.platforms.split(","))),
                "network": "none",
                "dependencyMode": "locked-offline",
            },
            "internalParameters": {},
            "resolvedDependencies": materials
            + [{"uri": args.source_uri, "digest": {"gitCommit": args.source_revision}}],
        },
        "runDetails": {
            "builder": {"id": args.builder_id},
            "metadata": {"invocationId": f"{args.source_revision}:{args.name}"},
            "byproducts": [],
        },
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(predicate, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
