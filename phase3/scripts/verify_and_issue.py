#!/usr/bin/env python3
"""Fail-closed verification and issuance for the Phase 3 evidence bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import time
from pathlib import Path
from typing import Any


HEX = set("0123456789abcdef")


def load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha_file(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def valid_sha(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= HEX


def merkle_root(domain: bytes, documents: list[dict[str, Any]]) -> str:
    leaves = [hashlib.sha256(domain + canonical(item)).digest() for item in documents]
    require(bool(leaves), "empty evidence set")
    while len(leaves) > 1:
        if len(leaves) % 2:
            leaves.append(leaves[-1])
        leaves = [
            hashlib.sha256(domain + leaves[index] + leaves[index + 1]).digest()
            for index in range(0, len(leaves), 2)
        ]
    return leaves[0].hex()


class CosignVerifier:
    def __init__(self, key: str | None, identity: str | None, issuer: str | None, signatures: Path) -> None:
        require(bool(key) != bool(identity or issuer), "choose exactly one cosign verification mode")
        require(bool(key) or (bool(identity) and bool(issuer)), "keyless verification requires identity and issuer")
        self.key = key
        self.identity = identity
        self.issuer = issuer
        self.signatures = signatures

    def _identity_args(self) -> list[str]:
        if self.key:
            return ["--key", self.key]
        return ["--certificate-identity-regexp", self.identity or "", "--certificate-oidc-issuer", self.issuer or ""]

    def image(self, reference: str) -> None:
        for command in (
            ["cosign", "verify", *self._identity_args(), reference],
            ["cosign", "verify-attestation", *self._identity_args(), "--type", "spdxjson", reference],
            ["cosign", "verify-attestation", *self._identity_args(), "--type", "slsaprovenance", reference],
        ):
            result = subprocess.run(command, capture_output=True, check=False)
            require(result.returncode == 0, f"cosign image/attestation verification failed: {reference}")

    def blob(self, path: Path, name: str) -> None:
        signature = self.signatures / f"{name}.sig"
        require(signature.is_file(), f"missing evidence signature: {signature}")
        command = ["cosign", "verify-blob", "--signature", str(signature)]
        if self.key:
            command.extend(["--key", self.key])
        else:
            certificate = self.signatures / f"{name}.pem"
            require(certificate.is_file(), f"missing evidence certificate: {certificate}")
            command.extend(["--certificate", str(certificate), *self._identity_args()])
        command.append(str(path))
        result = subprocess.run(command, capture_output=True, check=False)
        require(result.returncode == 0, f"cosign evidence verification failed: {name}")


def verify_supply_chain(root: Path, catalog_path: Path, verifier: CosignVerifier) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    lock_path = root / "image-lock.json"
    lock = load(lock_path)
    catalog = load(catalog_path)
    expected = sorted(item["name"] for item in catalog["images"])
    observed = sorted(item["name"] for item in lock["images"])
    require(lock.get("formatVersion") == 1 and observed == expected, "image lock does not exactly cover the catalog")
    require(len(observed) == 13 and len(set(observed)) == 13, "Phase 3 requires exactly thirteen unique deployable images")
    require(set(lock.get("platforms", [])) == {"linux/amd64", "linux/arm64"}, "multi-architecture lock is incomplete")
    require(valid_sha(lock.get("toolchainEvidenceSha256")) and sha_file(root / "toolchain-evidence.json") == lock["toolchainEvidenceSha256"], "controlled toolchain evidence mismatch")
    evidence = []
    by_name = {item["name"]: item for item in lock["images"]}
    for name in expected:
        directory = root / "images" / name
        item = load(directory / "evidence.json")
        required = {
            "formatVersion", "sourceRevision", "image", "digest", "platforms",
            "spdxSha256", "cycloneDxSha256", "grypeSha256", "trivySha256",
            "provenanceSha256", "signatureVerified", "highVulnerabilities",
            "criticalVulnerabilities", "complete",
        }
        require(set(item) == required, f"unexpected image evidence shape: {name}")
        require(item["sourceRevision"] == lock["sourceRevision"], f"source mismatch: {name}")
        require(item["image"] == by_name[name]["repository"] and item["digest"] == by_name[name]["digest"], f"image lock mismatch: {name}")
        require(set(item["platforms"]) == {"linux/amd64", "linux/arm64"}, f"platform mismatch: {name}")
        require(item["signatureVerified"] is True and item["complete"] is True, f"unverified image: {name}")
        require(item["highVulnerabilities"] == 0 and item["criticalVulnerabilities"] == 0, f"vulnerability policy failed: {name}")
        artifacts = {
            "spdxSha256": directory / "sbom.spdx.json",
            "cycloneDxSha256": directory / "sbom.cyclonedx.json",
            "grypeSha256": directory / "grype.json",
            "trivySha256": directory / "trivy.json",
            "provenanceSha256": directory / "provenance.json",
        }
        for field, path in artifacts.items():
            require(valid_sha(item[field]) and sha_file(path) == item[field], f"artifact checksum mismatch: {name}/{field}")
        verifier.image(f"{item['image']}@{item['digest']}")
        evidence.append(item)
    return lock, sorted(evidence, key=lambda item: item["image"])


def verify_postgres(path: Path) -> dict[str, Any]:
    item = load(path)
    required = {
        "formatVersion", "toolchainEvidenceSha256", "serverVersion", "primary", "streamingReplicas",
        "coreMigrationCount", "agentMigrationCount", "tenantRlsForced",
        "immutabilityVerified", "crossTenantDenied", "schemaBeforeSha256",
        "schemaAfterSha256", "complete",
    }
    require(set(item) == required, "unexpected PostgreSQL evidence shape")
    require(item["formatVersion"] == 1 and item["primary"] is True and item["streamingReplicas"] >= 1, "PostgreSQL is not qualified HA primary")
    require(item["coreMigrationCount"] >= 9 and item["agentMigrationCount"] >= 7, "migration coverage is incomplete")
    require(item["tenantRlsForced"] is True and item["immutabilityVerified"] is True and item["crossTenantDenied"] is True, "PostgreSQL security invariants failed")
    require(valid_sha(item["schemaBeforeSha256"]) and valid_sha(item["schemaAfterSha256"]), "invalid PostgreSQL schema hash")
    require(valid_sha(item["toolchainEvidenceSha256"]) and sha_file(path.parent / "toolchain-evidence.json") == item["toolchainEvidenceSha256"], "PostgreSQL toolchain evidence mismatch")
    require(item["complete"] is True, "PostgreSQL evidence is incomplete")
    return item


def verify_clusters(root: Path, required_path: Path, image_lock_sha: str) -> list[dict[str, Any]]:
    policy = load(required_path)
    expected_providers = set(policy["providers"])
    expected_scenarios = set(policy["scenarios"])
    reports = [load(path) for path in sorted(root.glob("*.json"))]
    require({item.get("provider") for item in reports} == expected_providers, "provider evidence matrix is incomplete")
    verified = []
    for item in reports:
        provider = item["provider"]
        require(item.get("formatVersion") == 1 and item.get("complete") is True, f"incomplete cluster evidence: {provider}")
        require(item.get("readyNodes", 0) >= 3 and item.get("zones", 0) >= 3 and item.get("gpuNodes", 0) >= 1, f"cluster is not HA/GPU qualified: {provider}")
        require(item.get("imageLockSha256") == image_lock_sha, f"cluster image lock mismatch: {provider}")
        scenarios = item.get("scenarios", [])
        require({scenario.get("id") for scenario in scenarios} == expected_scenarios, f"scenario coverage mismatch: {provider}")
        require(len(scenarios) == len(expected_scenarios), f"duplicate scenario evidence: {provider}")
        for scenario in scenarios:
            require(scenario.get("complete") is True, f"scenario incomplete: {provider}/{scenario.get('id')}")
            require(scenario.get("endedEpochMs", 0) >= scenario.get("startedEpochMs", 1), f"invalid scenario time: {provider}/{scenario.get('id')}")
            require(valid_sha(scenario.get("evidenceSha256")), f"invalid scenario checksum: {provider}/{scenario.get('id')}")
        verified.append(item)
    return sorted(verified, key=lambda item: item["provider"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--supply-chain", type=Path, required=True)
    parser.add_argument("--postgres", type=Path, required=True)
    parser.add_argument("--clusters", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--issued-epoch-ms", type=int)
    parser.add_argument("--cosign-key")
    parser.add_argument("--certificate-identity-regexp")
    parser.add_argument("--oidc-issuer")
    parser.add_argument("--signatures", type=Path, required=True)
    args = parser.parse_args()
    phase3 = Path(__file__).resolve().parents[1]
    verifier = CosignVerifier(args.cosign_key, args.certificate_identity_regexp, args.oidc_issuer, args.signatures)
    lock, images = verify_supply_chain(args.supply_chain, phase3 / "config/images.json", verifier)
    postgres = verify_postgres(args.postgres)
    verifier.blob(args.postgres, "postgres-evidence.json")
    image_lock_sha = sha_file(args.supply_chain / "image-lock.json")
    clusters = verify_clusters(args.clusters, phase3 / "config/required-scenarios.json", image_lock_sha)
    for cluster in clusters:
        verifier.blob(args.clusters / f"{cluster['provider']}.json", f"{cluster['provider']}.json")
    certificate = {
        "formatVersion": 1,
        "sourceRevision": lock["sourceRevision"],
        "imageLockSha256": image_lock_sha,
        "imageEvidenceRootSha256": merkle_root(b"ngkg-phase3-image-v1\0", images),
        "postgresEvidenceSha256": sha_bytes(canonical(postgres)),
        "clusterEvidenceRootSha256": merkle_root(b"ngkg-phase3-cluster-v1\0", clusters),
        "providers": sorted(item["provider"] for item in clusters),
        "issuedEpochMs": args.issued_epoch_ms or int(time.time() * 1000),
        "certificateSha256": "",
        "complete": True,
    }
    certificate["certificateSha256"] = sha_bytes(canonical(certificate))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical(certificate) + b"\n")
    print(json.dumps({"certificateSha256": certificate["certificateSha256"], "complete": True}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
