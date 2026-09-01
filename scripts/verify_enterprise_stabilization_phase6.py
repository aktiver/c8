#!/usr/bin/env python3
"""Fail-closed source/contract gate for Enterprise Stabilization Phase 6."""

from __future__ import annotations

import json
from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
PHASE6 = ROOT.parent / "phase6"


def require(path: Path, *needles: str) -> str:
    value = path.read_text(encoding="utf-8")
    if not value:
        raise RuntimeError(f"empty required file: {path}")
    for needle in needles:
        if needle not in value:
            raise RuntimeError(f"{path}: missing {needle!r}")
    return value


def main() -> int:
    differential = require(PHASE6 / "scripts/run_differential.py", "canonical_sparql_json", "CERTIFIED_CLOSURE", "EXACT_HERMIT", "x-ngkg-complete", "x-ngkg-native-cutover-mode", "x-ngkg-semantic-result-sha256", "oracleProductionTraffic")
    if "oracleProductionTraffic\") is True" in differential:
        raise RuntimeError("differential harness permits production oracle traffic")
    require(PHASE6 / "scripts/qualify_provider.py", "RUN_SATURATION_MATRIX", "INJECT_AND_RECOVER", '"cpuPercent": 80', "peakRssBytes", "podUid", "nodeUid", "nodeScaleFromZero")
    require(PHASE6 / "scripts/verify_supply_chain.py", "len(images) == 12", "spdxjson", "cyclonedx", "subject-sha256", "unapprovedCritical", "unapprovedHigh")
    require(ROOT.parent / "phase3/scripts/build_supply_chain.sh", "--type spdxjson", "--type cyclonedx", "--type slsaprovenance")
    require(PHASE6 / "scripts/verify_reproducible_build.py", "distinct builders", "networkControlled", "timestampsNormalized", "rows_a == rows_b")
    issuer = require(PHASE6 / "scripts/verify_and_issue.py", "phase3-certificate.json", "phase4-live-certificate.json", "phase5-live-certificate.json", "semanticMismatchCount", "failedScenarioCount")
    for provider in ("rke2", "eks", "aks", "gke"):
        if provider not in issuer:
            raise RuntimeError(f"Phase 6 issuer is missing provider: {provider}")
    require(PHASE6 / "scripts/seal_certificate.py", "cosign", "verify-blob", "sigstore-keyless")
    require(ROOT / "services/online-serving/src/main.rs", "x-ngkg-native-cutover-mode", "x-ngkg-semantic-result-sha256", "protocol_ordered")
    require(ROOT / "api/online-openapi.yaml", "x-ngkg-native-cutover-mode", "x-ngkg-semantic-result-sha256")
    require(PHASE6 / "acceptance.sh", "NGKG_PHASE6_EXECUTE_LIVE", "run_controlled.py")
    schema = json.loads(require(PHASE6 / "schemas/phase6-certificate.schema.json", "EnterpriseStabilizationPhase6Certificate"))
    if schema.get("additionalProperties") is not False:
        raise RuntimeError("Phase 6 certificate schema must be closed")
    yaml.safe_load(require(ROOT.parent / ".github/workflows/phase6-controlled-release.yml", "ngkg-phase6-release", "matrix", "rke2", "eks", "aks", "gke"))
    source = json.loads(require(ROOT / "qualification/enterprise-stabilization-phase6-source.json", '"SOURCE_ONLY"'))
    if source.get("productionQualified") is not False or source.get("liveEvidenceIncluded") is not False:
        raise RuntimeError("source evidence overstates production qualification")
    print("Enterprise Stabilization Phase 6 source/contract gate passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, TypeError, ValueError, RuntimeError) as error:
        print(f"Phase 6 gate failed: {error}", file=sys.stderr)
        raise SystemExit(1)
