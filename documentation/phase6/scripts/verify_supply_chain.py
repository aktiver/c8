#!/usr/bin/env python3
"""Verify immutable images, signatures, two SBOM formats and vulnerability policy."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from phase6_common import atomic_json, canonical, load_json, require, run, sha256_bytes, sha256_file, valid_sha256


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--image-lock", required=True, type=Path)
    parser.add_argument("--scan-report", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--subject-sha256", required=True)
    parser.add_argument("--certificate-identity-regexp", required=True)
    parser.add_argument("--certificate-oidc-issuer", required=True)
    args = parser.parse_args()
    lock = load_json(args.image_lock.resolve())
    scan = load_json(args.scan_report.resolve())
    require(lock.get("formatVersion") == 1, "unsupported image lock")
    require(valid_sha256(args.subject_sha256), "invalid release subject")
    images = lock.get("images")
    require(isinstance(images, list) and len(images) == 12, "exactly twelve release images are required")
    require(scan.get("complete") is True and scan.get("unapprovedCritical") == 0 and scan.get("unapprovedHigh") == 0, "vulnerability policy failed")
    require(scan.get("imageLockSha256") == sha256_file(args.image_lock.resolve()), "scan report used a different image lock")
    verified = []
    for image in images:
        reference = image.get("reference") or (f"{image.get('repository')}@{image.get('digest')}")
        require(isinstance(reference, str) and "@sha256:" in reference and not reference.endswith("@sha256:"), "image is not digest pinned")
        common = ["--certificate-identity-regexp", args.certificate_identity_regexp, "--certificate-oidc-issuer", args.certificate_oidc_issuer]
        signature = run(["cosign", "verify", *common, "--output", "json", reference], timeout=600)
        spdx = run(["cosign", "verify-attestation", *common, "--type", "spdxjson", "--output", "json", reference], timeout=600)
        cyclone = run(["cosign", "verify-attestation", *common, "--type", "cyclonedx", "--output", "json", reference], timeout=600)
        require(json.loads(signature) and json.loads(spdx) and json.loads(cyclone), f"empty signature/SBOM evidence: {reference}")
        verified.append({"name": image["name"], "reference": reference, "signatureSha256": sha256_bytes(signature), "spdxAttestationSha256": sha256_bytes(spdx), "cycloneDxAttestationSha256": sha256_bytes(cyclone)})
    evidence = {
        "formatVersion": 1,
        "kind": "Phase6SupplyChainEvidence",
        "subjectSha256": args.subject_sha256,
        "imageLockSha256": sha256_file(args.image_lock.resolve()),
        "imageCount": len(verified),
        "images": verified,
        "vulnerabilityReportSha256": sha256_file(args.scan_report.resolve()),
        "unapprovedCritical": 0,
        "unapprovedHigh": 0,
        "signed": True,
        "spdxComplete": True,
        "cycloneDxComplete": True,
        "synthetic": False,
        "status": "PASS",
        "complete": True,
    }
    evidence["evidenceRootSha256"] = sha256_bytes(canonical(verified))
    atomic_json(args.output.resolve(), evidence)
    print(json.dumps({"status": "PASS", "imageCount": len(verified), "evidenceRootSha256": evidence["evidenceRootSha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError, json.JSONDecodeError) as error:
        print(f"Phase 6 supply-chain qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
