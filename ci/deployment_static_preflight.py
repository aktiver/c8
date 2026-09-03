#!/usr/bin/env python3
"""Fail fast on source-level blockers before an NGKG image or Helm deployment run."""

from __future__ import annotations

import json
from pathlib import Path
import re
import shutil
import sys

import yaml


ROOT = Path(__file__).resolve().parents[1]
DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


class PreflightError(RuntimeError):
    """A source-level deployment contract is inconsistent."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise PreflightError(message)


def read(relative: str) -> str:
    path = ROOT / relative
    require(path.is_file(), f"missing file: {relative}")
    return path.read_text(encoding="utf-8")


def validate_rust_lints() -> dict[str, int]:
    workspaces = ("NGKG_1_0_0_GA", "ngkg-agents")
    unsafe_blocks = 0
    expect_calls = 0
    for workspace in workspaces:
        manifest = read(f"{workspace}/Cargo.toml")
        require('unsafe_code = "forbid"' not in manifest, f"{workspace} forbids its required mmap unsafe blocks")
        require('unsafe_code = "deny"' in manifest, f"{workspace} must deny unscoped unsafe code")
        require((ROOT / workspace / "rust-toolchain.toml").is_file(), f"{workspace} has no pinned Rust toolchain")
        for source in (ROOT / workspace).rglob("*.rs"):
            if "vendor" in source.parts:
                continue
            text = source.read_text(encoding="utf-8")
            unsafe_blocks += len(re.findall(r"\bunsafe\s*\{", text))
            expect_calls += text.count(".expect(")
            if "unsafe {" in text:
                require("#[allow(unsafe_code)]" in text, f"unscoped unsafe block: {source.relative_to(ROOT)}")
    require(unsafe_blocks == 2, f"expected two reviewed mmap unsafe blocks, found {unsafe_blocks}")
    require(expect_calls == 0, f"Clippy-denied .expect calls remain: {expect_calls}")
    return {"reviewedUnsafeBlocks": unsafe_blocks, "clippyExpectCalls": expect_calls}


def validate_images() -> dict[str, int]:
    catalog = json.loads(read("docker_repos/images.json"))
    images = catalog.get("images", [])
    require(len(images) == 13, f"expected 13 release images, found {len(images)}")
    require(len({image["name"] for image in images}) == len(images), "release image names are not unique")
    build_dockerfiles = 0
    for image in images:
        dockerfile = ROOT / "docker_repos" / image["dockerfile"]
        require(dockerfile.is_file(), f"missing Dockerfile for {image['name']}: {dockerfile.relative_to(ROOT)}")
        text = dockerfile.read_text(encoding="utf-8")
        if image["name"] == "ngkg-vllm":
            require("VLLM_SOURCE_IMAGE" in text, "vLLM source image is not an explicit build input")
            continue
        build_dockerfiles += 1
        require("ARG BUILD_OFFLINE=false" in text, f"{image['name']} lacks selectable dependency mode")
        require('true) set -- --offline' in text, f"{image['name']} lacks an offline build branch")
        require("USER 65532:65532" in text, f"{image['name']} does not select the non-root runtime UID")
    builder = read("docker_repos/build_and_push_local.py")
    require('"BUILD_OFFLINE"' in builder, "local image builder does not pass BUILD_OFFLINE")
    require('command.append("--push"' in builder, "local image builder does not push build results")
    require("imagetools\", \"inspect" in builder, "local image builder does not resolve registry digests")
    controlled = read("phase3/scripts/build_supply_chain.sh")
    for marker in ("BUILD_OFFLINE=true", "MPI_BUILDER_IMAGE", "MPI_RUNTIME_IMAGE"):
        require(marker in controlled, f"controlled image build is missing {marker}")
    deploy = read("phase3/scripts/deploy_cluster.sh")
    require("images.hpcWorker.repository" in deploy, "controlled Helm deploy omits the HPC image lock entry")
    require("serverVersion.gitVersion" in deploy, "controlled Helm render does not use the target cluster version")
    return {"images": len(images), "compiledImages": build_dockerfiles}


def validate_charts() -> dict[str, int]:
    charts = (
        "NGKG_1_0_0_GA/charts/ngkg-platform",
        "NGKG_1_0_0_GA/charts/ngkg-workloads",
        "ngkg-agents/charts/ngkg-agents",
    )
    for chart in charts:
        schema = json.loads(read(f"{chart}/values.schema.json"))
        properties = schema.get("properties", {})
        require("imagePullSecrets" in properties, f"{chart} has no private-registry pull-secret value")
        pull_secret = properties["imagePullSecrets"]
        require(pull_secret.get("type") == "array", f"{chart} imagePullSecrets is not an array")
        templates = "\n".join(path.read_text(encoding="utf-8") for path in (ROOT / chart / "templates").glob("*.yaml"))
        require("imagePullSecrets:" in templates, f"{chart} never renders imagePullSecrets")
        require("imagePullPolicy:" in templates, f"{chart} never renders imagePullPolicy")
        values = yaml.safe_load(read(f"{chart}/values.yaml"))
        image_values = values.get("images", {"image": values.get("image", {})})
        require(
            all(image.get("pullPolicy") in {"Always", "IfNotPresent", "Never"} for image in image_values.values()),
            f"{chart} default image values do not all declare a valid pullPolicy",
        )
        if chart.endswith(("ngkg-platform", "ngkg-workloads")):
            image_schema = schema.get("$defs", {}).get("image", {})
            image_properties = image_schema.get("properties", {})
            require("pullPolicy" in image_properties, f"{chart} rejects generated pullPolicy values")
            require(set(image_properties["pullPolicy"].get("enum", [])) == {"Always", "IfNotPresent", "Never"}, f"{chart} pullPolicy enum is incomplete")
    crds = list((ROOT / "NGKG_1_0_0_GA/charts/ngkg-crds/crds").glob("*.yaml"))
    require(len(crds) == 3, f"expected three NGKG CRDs, found {len(crds)}")
    return {"charts": len(charts), "crds": len(crds)}


def validate_apis() -> dict[str, int]:
    control = read("NGKG_1_0_0_GA/api/openapi.yaml")
    online = read("NGKG_1_0_0_GA/api/online-openapi.yaml")
    require(control.startswith("openapi: 3.1.0"), "control API is not OpenAPI 3.1.0")
    require(online.startswith("openapi: 3.1.0"), "online API is not OpenAPI 3.1.0")
    for marker in ("/docs:", "/v1/datasets:", "/v1/datasets/{datasetId}/sources/{sourceId}:", "application/trig"):
        require(marker in control, f"control API marker is missing: {marker}")
    for marker in ("/docs:", "/v1/datasets/{datasetId}/sparql:", "application/sparql-query", "bearerAuth"):
        require(marker in online, f"online API marker is missing: {marker}")
    return {
        "controlOperations": len(re.findall(r"^      operationId:", control, re.MULTILINE)),
        "onlineOperations": len(re.findall(r"^      operationId:", online, re.MULTILINE)),
    }


def validate_live_test_gates() -> dict[str, bool]:
    cluster_preflight = ROOT / "scripts/cluster_preflight.sh"
    api_smoke = ROOT / "scripts/database_api_smoke_test.py"
    require(cluster_preflight.is_file(), "missing read-only live cluster preflight")
    require(api_smoke.is_file(), "missing deployed database API smoke test")
    require(cluster_preflight.stat().st_mode & 0o111 != 0, "cluster preflight is not executable")
    require(api_smoke.stat().st_mode & 0o111 != 0, "database API smoke test is not executable")
    cluster_text = cluster_preflight.read_text(encoding="utf-8")
    smoke_text = api_smoke.read_text(encoding="utf-8")
    require("kubectl apply" not in cluster_text, "cluster preflight must remain read-only")
    require("alignmentOrCertificationPerformed\": False" in smoke_text, "database smoke scope is ambiguous")
    for marker in ("/health/ready", "/openapi.json", "application/trig", "application/sparql-query"):
        require(marker in smoke_text, f"database smoke test is missing {marker}")
    return {"clusterPreflight": True, "databaseApiSmoke": True}


def main() -> int:
    result = {
        "status": "PASS",
        "scope": "source-static-only",
        "rust": validate_rust_lints(),
        "images": validate_images(),
        "helm": validate_charts(),
        "apis": validate_apis(),
        "liveTestGates": validate_live_test_gates(),
        "availableExternalTools": {
            command: shutil.which(command) is not None
            for command in ("cargo", "rustc", "docker", "helm", "kubectl", "mpicc", "mpirun")
        },
        "liveClusterQualification": "NOT_RUN",
    }
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, PreflightError) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}, sort_keys=True), file=sys.stderr)
        raise SystemExit(1)
