#!/usr/bin/env python3
"""Verify immutable images, signatures, two SBOM formats and vulnerability policy."""

from __future__ import annotations

import argparse
import base64
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

from phase6_common import atomic_json, canonical, load_json, require, run, sha256_bytes, sha256_file, valid_sha256

RELEASE_IMAGES = {
    "ngkg-api", "ngkg-catalog-migrator", "ngkg-distributed-operator",
    "ngkg-distributed-worker", "ngkg-operator", "ngkg-storage-recovery-operator",
    "ngkg-storage-recovery-worker", "ngkg-direct-reasoner-worker",
    "ngkg-reference-worker", "ngkg-online-serving", "ngkg-agents", "ngkg-vllm",
    "ngkg-hpc-worker",
}


def attestation_binds_digest(raw: bytes, digest: str) -> bool:
    for row in json.loads(raw):
        payload = row.get("payload")
        if not isinstance(payload, str):
            continue
        statement = json.loads(base64.b64decode(payload))
        if any(subject.get("digest", {}).get("sha256") == digest.removeprefix("sha256:") for subject in statement.get("subject", [])):
            return True
    return False


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
    require(isinstance(images, list) and len(images) == 13, "exactly thirteen Phase 8 release images are required")
    require({image.get("name") for image in images} == RELEASE_IMAGES, "release image names do not match the approved product set")
    require(scan.get("complete") is True and scan.get("unapprovedCritical") == 0 and scan.get("unapprovedHigh") == 0, "vulnerability policy failed")
    require(scan.get("imageLockSha256") == sha256_file(args.image_lock.resolve()), "scan report used a different image lock")
    scan_rows = scan.get("images")
    require(isinstance(scan_rows, list) and {row.get("name") for row in scan_rows} == RELEASE_IMAGES, "vulnerability report lacks per-image coverage")
    scan_by_name = {row["name"]: row for row in scan_rows}
    verified = []
    for image in images:
        reference = image.get("reference") or (f"{image.get('repository')}@{image.get('digest')}")
        require(isinstance(reference, str) and "@sha256:" in reference and not reference.endswith("@sha256:"), "image is not digest pinned")
        digest = reference.rsplit("@", 1)[1]
        require(image.get("digest") == digest and valid_sha256(digest.removeprefix("sha256:")), "image-lock digest and reference differ")
        scan_row = scan_by_name[image["name"]]
        require(scan_row.get("reference") == reference and valid_sha256(scan_row.get("scannerDatabaseSha256")), f"scan subject/database is invalid: {image['name']}")
        require(int(scan_row.get("unapprovedCritical", -1)) == 0 and int(scan_row.get("unapprovedHigh", -1)) == 0, f"unapproved vulnerabilities remain: {image['name']}")
        for exception in scan_row.get("exceptions", []):
            require(isinstance(exception.get("id"), str) and exception["id"] and isinstance(exception.get("signer"), str) and exception["signer"], "vulnerability exception lacks approval identity")
            expiry = datetime.fromisoformat(exception["expiresAt"].replace("Z", "+00:00"))
            require(expiry > datetime.now(timezone.utc), f"expired vulnerability exception: {exception['id']}")
        common = ["--certificate-identity-regexp", args.certificate_identity_regexp, "--certificate-oidc-issuer", args.certificate_oidc_issuer]
        signature = run(["cosign", "verify", *common, "--output", "json", reference], timeout=600)
        spdx = run(["cosign", "verify-attestation", *common, "--type", "spdxjson", "--output", "json", reference], timeout=600)
        cyclone = run(["cosign", "verify-attestation", *common, "--type", "cyclonedx", "--output", "json", reference], timeout=600)
        require(json.loads(signature) and json.loads(spdx) and json.loads(cyclone), f"empty signature/SBOM evidence: {reference}")
        require(attestation_binds_digest(spdx, digest), f"SPDX attestation is bound to another image: {reference}")
        require(attestation_binds_digest(cyclone, digest), f"CycloneDX attestation is bound to another image: {reference}")
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
