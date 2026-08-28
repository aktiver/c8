#!/usr/bin/env python3
"""Fail-closed semantic and hydration checks for the reference corpus."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    args = parser.parse_args()
    snapshot = json.loads(args.snapshot.read_text(encoding="utf-8"))
    result = json.loads(args.result.read_text(encoding="utf-8"))
    if snapshot["snapshotId"] != result["snapshotId"]:
        raise AssertionError("result snapshot does not match compiled snapshot")
    bindings = result["bindings"]
    if len(bindings) != 1:
        raise AssertionError(f"expected one exact binding, observed {len(bindings)}")
    binding = bindings[0]
    expected = {
        "observation": "https://ngkg.io/id/observation-1",
        "node": "https://ngkg.io/id/node-1",
        "failure": "https://ngkg.io/id/failure-1",
    }
    observed = {name: binding[name]["value"] for name in expected}
    if observed != expected:
        raise AssertionError(f"unexpected exact binding: {observed}")
    payload = result["hydratedPayload"]
    messages = [row for row in payload if row["predicateIri"] == "https://ngkg.io/ontology/rawMessage"]
    if [row["lexicalValue"] for row in messages] != ["latency threshold exceeded"]:
        raise AssertionError("GUID-directed payload hydration did not return the expected raw message")
    print(json.dumps({"status": "passed", "bindings": len(bindings), "hydratedPayloadRows": len(payload)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
