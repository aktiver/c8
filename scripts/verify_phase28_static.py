#!/usr/bin/env python3
"""Fail-closed static inspection for Phase 28 tenant admission isolation."""

from __future__ import annotations

import json
import pathlib
import sys

import yaml


ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(path: str, tokens: tuple[str, ...] = ()) -> str:
    target = ROOT / path
    if not target.is_file():
        raise RuntimeError(f"missing required file: {path}")
    text = target.read_text(encoding="utf-8")
    for token in tokens:
        if token not in text:
            raise RuntimeError(f"{path} is missing required token: {token}")
    return text


def main() -> int:
    policy = require(
        "services/online-serving/src/tenant_admission.rs",
        (
            "struct TenantAdmissionRegistry",
            "expected_sha256",
            "policy_tenants != *authorized_tenants",
            "fragment_worker_max_in_flight",
            "tenant_limits_cannot_exceed_global_envelopes",
        ),
    )
    if "BTreeMap<Uuid, Arc<TenantAdmissionLanes>>" not in policy:
        raise RuntimeError("tenant policy is not a finite precompiled registry")

    serving = require(
        "services/online-serving/src/main.rs",
        (
            "TenantAdmissionRegistry::load",
            "TENANT_ADMISSION_CAPACITY_EXHAUSTED",
            "ngkg_admission_rejections_by_scope_total",
            "ngkg_tenant_admission_configured",
            "saturated_tenant_cannot_consume_another_tenants_lane",
            "NGKG_AUTH_TOKENS_FILE_SHA256",
        ),
    )
    middleware = serving.index("async fn admission_middleware")
    authentication = serving.index("state.authorizer.authorize(request.headers())", middleware)
    acquire = serving.index("state.admission.acquire(class, identity.tenant_id)", authentication)
    next_run = serving.index("let response = next.run(request).await", acquire)
    wrap = serving.index("hold_admission_through_body(response, lease)", next_run)
    if not middleware < authentication < acquire < next_run < wrap:
        raise RuntimeError("tenant and global permits do not surround the complete response")

    schema = json.loads(require("contracts/tenant-admission-policy.schema.json"))
    tenant_required = set(schema["$defs"]["tenant"]["required"])
    if "tenantId" not in tenant_required or "fragmentWorkerMaxInFlight" not in tenant_required:
        raise RuntimeError("tenant policy schema omits tenant identity or shared worker ceiling")

    values = yaml.safe_load(require("charts/ngkg-workloads/values.yaml"))
    online = values["onlineServing"]
    for key in (
        "authTokensFileSha256",
        "tenantAdmissionSecret",
        "tenantAdmissionPolicySha256",
        "maxAdmissionTenants",
    ):
        if key not in online:
            raise RuntimeError(f"Helm values omit {key}")
    template = require("charts/ngkg-workloads/templates/online-data-plane.yaml")
    for token in (
        "NGKG_TENANT_ADMISSION_POLICY_FILE",
        "NGKG_TENANT_ADMISSION_POLICY_SHA256",
        "NGKG_MAX_ADMISSION_TENANTS",
        "ngkg.io/tenant-admission-policy-sha256",
        "NGKG_AUTH_TOKENS_FILE_SHA256",
        "ngkg.io/auth-tokens-file-sha256",
    ):
        if template.count(token) != 4:
            raise RuntimeError(f"{token} is not wired to all four online roles")
    if values["metrics"]["cpuUtilizationTargetPercent"] > 80 or values["metrics"]["memoryUtilizationTargetPercent"] > 80:
        raise RuntimeError("resource autoscaling exceeds the 80-percent boundary")

    openapi = yaml.safe_load(require("api/online-openapi.yaml"))
    version = tuple(int(part) for part in str(openapi["info"]["version"]).split("."))
    # The public 1.0.0 GA line resets the historical internal API version
    # after freezing the already-present tenant admission contract.
    if version != (1, 0, 0) and version < (1, 2, 0):
        raise RuntimeError("online OpenAPI version does not include tenant overload")
    require(
        "scripts/qualify_phase28.sh",
        (
            "TENANT_ADMISSION_CAPACITY_EXHAUSTED",
            "tenant B was starved",
            "cmp",
            "ngkg_admission_rejections_by_scope_total",
        ),
    )
    require(
        "docs/phases/PHASE_28.md",
        ("Acceptance criteria", "Intentional boundary", "RDF graph database", "Parquet", "mmap"),
    )
    require("verification/phase-28.json")
    print("Phase 28 static contract verification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (KeyError, RuntimeError, TypeError, ValueError) as error:
        print(f"phase 28 static verification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
