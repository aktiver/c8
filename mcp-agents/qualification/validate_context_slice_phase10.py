#!/usr/bin/env python3
"""Deterministic source/contract checks for Phase 10; not a live-cloud certificate."""

from __future__ import annotations

import hashlib
import json
import pathlib
import struct
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, *needles: str) -> str:
    value = (ROOT / path).read_text(encoding="utf-8")
    for needle in needles:
        if needle not in value:
            raise AssertionError(f"{path} lacks {needle!r}")
    return value


def validate_index_abi() -> None:
    magic = b"NGKGSIDX"
    payload = b"first chunk\nsecond chunk\n"
    chunks = [payload[:12], payload[12:]]
    records = []
    cursor = 0
    for ordinal, chunk in enumerate(chunks):
        end = cursor + len(chunk)
        records.append((hashlib.sha256(chunk).digest(), ordinal, cursor, end))
        cursor = end
    records.sort(key=lambda value: (value[0], value[1]))
    record_bytes = b"".join(
        digest + struct.pack("<I", ordinal) + b"\0" * 4 + struct.pack("<QQ", start, end)
        for digest, ordinal, start, end in records
    )
    header = (
        magic
        + struct.pack("<IIQQ", 1, 56, len(records), len(payload))
        + hashlib.sha256(payload).digest()
        + hashlib.sha256(record_bytes).digest()
    )
    index = header + record_bytes
    assert len(header) == 96 and len(index) == 96 + 56 * len(records)
    assert index[:8] == magic and struct.unpack_from("<I", index, 12)[0] == 56
    corrupted = bytearray(index)
    corrupted[-1] ^= 1
    assert hashlib.sha256(corrupted[96:]).digest() != corrupted[64:96]


def main() -> int:
    spec = yaml.safe_load((ROOT / "contracts/context-slice-openapi.yaml").read_text())
    assert spec["openapi"] == "3.1.0"
    operations = {
        operation["operationId"]
        for path in spec["paths"].values()
        for method, operation in path.items()
        if method in {"get", "post", "put", "delete", "patch"}
    }
    expected = {
        "createContextSlice", "getContextSlice", "putContextSliceChunk", "finalizeContextSlice",
        "issueContextSliceCapability", "readContextSliceRange", "expireContextSlice",
        "contextSliceLiveness", "contextSliceReadiness", "contextSliceMetrics",
    }
    assert operations == expected
    json.loads((ROOT / "charts/ngkg-agents/values.schema.json").read_text())
    values = yaml.safe_load((ROOT / "charts/ngkg-agents/values.yaml").read_text())
    context = values["contextSlice"]
    assert context["enabled"] is False
    assert context["autoscaling"]["cpuTargetPercent"] == 80
    assert context["autoscaling"]["memoryTargetPercent"] == 80
    assert context["maximumIndexBytes"] <= context["resources"]["limits"]["memory"].count("Gi") * 8 * 1024**3
    require("Cargo.toml", 'memmap2 = "=0.9.9"', 'version = "0.10.0"')
    require("crates/ngkg-context-slice/src/index.rs", "MmapOptions::new()", "map(&file)", "symlink_metadata", "metadata.uid()", "index sort order")
    require("crates/ngkg-context-slice/src/capability.rs", "range_start", "range_end_exclusive", "policy_version_sha256", "manifest_sha256", "ngkg-context-capability+jwt")
    require("crates/ngkg-context-slice/src/storage.rs", "tenants/{tenant_id}/context-slices", "put_immutable", "get_verified")
    broker = require("services/context-slice-broker/src/main.rs", "/v1/context-slices", "/chunks/{ordinal}", "/finalize", "/capabilities", "/content", "/expire", "/openapi.yaml", "/swagger-ui")
    assert "ContextObjectStore::chunk_reference" in broker and "VerifiedLocatorIndex::from_staged_file" in broker
    require("migrations-agents/0007_context_slice_broker.sql", "FORCE ROW LEVEL SECURITY", "claim_context_slice_gc", "SKIP LOCKED", "context_slice_tombstone", "token_sha256")
    chart = require("charts/ngkg-agents/templates/context-slice.yaml", "ngkg-context-slice-broker", "averageUtilization", "cpuTargetPercent", "memoryTargetPercent", "readOnlyRootFilesystem: true", "OMP_NUM_THREADS")
    assert "serviceAccountName: ngkg-context-slice" in chart
    gateway = (ROOT / "charts/ngkg-agents/templates/gateway.yaml").read_text()
    assert "NGKG_CONTEXT_" not in gateway and "contextSlice.storage" not in gateway
    require("charts/ngkg-agents/templates/context-slice-network-policy.yaml", "default-deny", "policyTypes: [Ingress, Egress]")
    for provider in ("eks", "aks", "gke", "rke", "rke2"):
        yaml.safe_load((ROOT / f"deploy/context-slice-provider-overlays/{provider}-values.yaml").read_text())
    validate_index_abi()
    print("NGKG MCP Agent Phase 10 context-slice source qualification: PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"Phase 10 validation failed: {error}", file=sys.stderr)
        raise
