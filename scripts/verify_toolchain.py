#!/usr/bin/env python3
"""Run available build tools and record blocked checks without converting them to success."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import shutil
import subprocess


def run(command: list[str], root: pathlib.Path) -> dict[str, object]:
    executable = shutil.which(command[0])
    if executable is None:
        return {"status": "blocked", "reason": f"{command[0]} is not installed", "command": command}
    completed = subprocess.run(command, cwd=root, capture_output=True, text=True, check=False)
    return {
        "status": "passed" if completed.returncode == 0 else "failed",
        "command": command,
        "exitCode": completed.returncode,
        "stdout": completed.stdout[-12000:],
        "stderr": completed.stderr[-12000:],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=pathlib.Path, default=pathlib.Path.cwd())
    parser.add_argument("--phase", required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve()
    checks = {
        "structural": run(["python3", "scripts/structural_validate.py", "--root", "."], root),
        "cargoFmt": run(["cargo", "fmt", "--all", "--check"], root),
        "cargoCheck": run(["cargo", "check", "--workspace", "--all-targets", "--all-features"], root),
        "cargoTest": run(["cargo", "test", "--workspace", "--all-features"], root),
    }
    if (root / "charts" / "ngkg-workloads").is_dir():
        checks["helmValues"] = run(
            ["python3", "scripts/validate_helm_values.py", "charts/ngkg-workloads/values.yaml"], root
        )
    if (root / "charts" / "ngkg-crds").is_dir():
        checks["helmCrds"] = run(["helm", "lint", "charts/ngkg-crds"], root)
    if (root / "charts" / "ngkg-platform").is_dir():
        checks["helmPlatform"] = run(["helm", "lint", "charts/ngkg-platform"], root)
    if (root / "charts" / "ngkg-workloads").is_dir():
        checks["helmWorkloads"] = run(["helm", "lint", "charts/ngkg-workloads"], root)
    result = {
        "phase": args.phase,
        "generatedAt": dt.datetime.now(dt.timezone.utc).isoformat(),
        "checks": checks,
        "gateStatus": "failed" if any(v["status"] == "failed" for v in checks.values()) else "blocked" if any(v["status"] == "blocked" for v in checks.values()) else "passed",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 1 if result["gateStatus"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
