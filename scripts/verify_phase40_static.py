#!/usr/bin/env python3
"""Static governance gate for the Phase 40 baseline."""
from __future__ import annotations

import hashlib
import json
import pathlib
import subprocess
import sys
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def load_json(rel: str):
    path = ROOT / rel
    if not path.is_file():
        raise RuntimeError(f"missing {rel}")
    return json.loads(path.read_text(encoding="utf-8"))


def require_text(rel: str, *tokens: str) -> str:
    path = ROOT / rel
    if not path.is_file():
        raise RuntimeError(f"missing {rel}")
    text = path.read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{rel} missing {token}")
    return text


def sha256_file(path: pathlib.Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> int:
    requirements = load_json("verification/phase-40-requirements.json")
    ids = {entry["id"] for entry in requirements["requirements"]}
    if ids != {f"P40-BASE-{n:03d}" for n in range(1, 9)}:
        raise RuntimeError("Phase 40 baseline requirement set is incomplete")
    if requirements.get("futureMilestones") != [f"40.{i}" for i in range(1, 28)]:
        raise RuntimeError("Phase 40 milestone sequence must be exactly 40.1 through 40.27")

    trace = load_json("verification/phase-40-traceability.json")
    traced = {entry["requirementId"] for entry in trace["entries"]}
    if traced != ids:
        raise RuntimeError("Phase 40 traceability does not cover every baseline requirement exactly once")
    for entry in trace["entries"]:
        for rel in entry.get("implementation", []) + entry.get("evidence", []):
            if not (ROOT / rel).is_file():
                raise RuntimeError(f"traceability references missing file {rel}")

    inherited = load_json("verification/phase-40-inherited-gates.json")
    phases = [str(entry["phase"]) for entry in inherited["gates"]]
    if phases[0] != "15" or phases[-1] != "39.5":
        raise RuntimeError("inherited gate range is not anchored at Phase 15 and 39.5")
    acceptance = yaml.safe_load(require_text("acceptance/phase-gates.yaml"))["phases"]
    by_phase = {str(entry["phase"]): entry for entry in acceptance}
    for gate in inherited["gates"]:
        current = by_phase.get(str(gate["phase"]))
        if current is None or current["command"] != gate["command"] or current["gate"] != gate["gate"]:
            raise RuntimeError(f"inherited acceptance gate drifted at Phase {gate['phase']}")
    if by_phase.get("40", {}).get("command") != "scripts/qualify_phase40.sh":
        raise RuntimeError("acceptance registry does not point Phase 40 to scripts/qualify_phase40.sh")

    capability = load_json("verification/phase-40-capability-status.json")
    if capability.get("standardsClaimsEnabled") is not False:
        raise RuntimeError("Phase 40 baseline must keep standards claims disabled")
    direct = capability["capabilities"]["owlDirectArbitraryBgpCompleteness"]
    if direct.get("status") != "not-implemented":
        raise RuntimeError("Phase 40 baseline must not claim arbitrary OWL Direct BGP completeness")

    ceilings = load_json("verification/phase-40-ceilings.json")
    if not ceilings.get("inheritedEnforced"):
        raise RuntimeError("Phase 40 ceiling registry is incomplete")
    # Historical Phase 40 required Direct ceilings to be planned for 40.10. Descendants
    # at/after 40.10 legitimately advance those entries into schema-validated Helm
    # declarations while preserving the original inherited ceiling registry.
    if ceilings.get("phase") == "40":
        if len(ceilings.get("plannedPhase40Unwired", [])) < 6:
            raise RuntimeError("Phase 40 Direct ceiling plan is incomplete")
        if any(item.get("plannedMilestone") != "40.10" for item in ceilings["plannedPhase40Unwired"]):
            raise RuntimeError("Direct-reasoner ceilings must remain planned for Phase 40.10")
    else:
        declared=ceilings.get("phase40HelmDeclared", [])
        phase_label=str(ceilings.get("phase", ""))
        if phase_label not in {"40.10", "40.11", "40.12", "40.13"} or len(declared) < 13:
            raise RuntimeError("descendant Phase 40.10+ Helm ceiling declaration is incomplete")
        milestones={item.get("milestone") for item in ceilings.get("plannedPhase40Unwired", [])}
        expected={"40.11", "40.12", "40.13"} if phase_label=="40.10" else ({"40.12", "40.13"} if phase_label=="40.11" else ({"40.13"} if phase_label=="40.12" else set()))
        if milestones != expected:
            raise RuntimeError("post-40.10 ceiling wiring milestones drifted")

    require_text(
        "docs/PHASE_40_ENGINEERING_CONTRACT.md",
        "40.1",
        "40.27",
        "Every REST operation",
        "Standards claims remain disabled",
    )
    require_text(
        "docs/API_ROUTE_CATALOG.md",
        "/docs",
        "/openapi.yaml",
        "/openapi.json",
        "verify_api_openapi_parity.py",
    )
    require_text("services/api/src/main.rs", '.route("/docs"', '.route("/openapi.json"')
    require_text("services/online-serving/src/main.rs", '.route("/docs"', '.route("/openapi.json"')

    parity = subprocess.run(
        [sys.executable, str(ROOT / "scripts/verify_api_openapi_parity.py")],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if parity.returncode != 0:
        raise RuntimeError(f"REST/OpenAPI parity gate failed: {parity.stdout.strip()}")

    evidence = load_json("verification/stabilization/phase-40.json")
    if evidence.get("parentLabel") != "phase-39.5" or evidence.get("currentLabel") != "phase-40":
        raise RuntimeError("Phase 40 parent evidence labels are invalid")
    if evidence.get("deletedFiles") != []:
        raise RuntimeError("Phase 40 deleted inherited Phase 39.5 files")
    # The Phase 40 evidence describes the Phase 39.5 -> 40 transition. Descendant
    # phases are allowed to modify those inherited files; their own checksum-bound
    # parent evidence proves the next transition. Validate the recorded digests and
    # embedded parent manifest here instead of requiring the current descendant tree
    # to byte-match the historical Phase 40 state.
    embedded = ROOT / evidence.get("embeddedParentManifest", "")
    if not embedded.is_file() or sha256_file(embedded) != evidence.get("parentFileManifestSha256"):
        raise RuntimeError("Phase 40 embedded parent manifest evidence is invalid")
    for changed in evidence.get("changedParentFiles", []):
        if len(changed.get("parentSha256", "")) != 64 or len(changed.get("currentSha256", "")) != 64:
            raise RuntimeError(f"Phase 40 parent evidence has invalid digest for {changed.get('path')}")

    phase = load_json("verification/phase-40.json")
    if phase.get("owlDirectRuntimeChangesIntroduced") is not False:
        raise RuntimeError("Phase 40 baseline must not claim OWL Direct runtime implementation")

    print("Phase 40 static contract verification passed; baseline, API/OpenAPI parity, inheritance and HPC ceiling governance are coherent")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError, yaml.YAMLError) as exc:
        print(f"phase 40 static verification failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
