#!/usr/bin/env python3
"""Fail-closed source/deployment checks for Phase 40.13.21 query audit."""

from __future__ import annotations

import json
import pathlib
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]


def require(relative: str, *tokens: str) -> str:
    value = (ROOT / relative).read_text(encoding="utf-8")
    for token in tokens:
        if token not in value:
            raise RuntimeError(f"{relative} is missing {token!r}")
    return value


def main() -> int:
    migration = require(
        "migrations/0009_enterprise_query_audit.sql",
        "CREATE TABLE query_execution_log",
        "ENABLE ROW LEVEL SECURITY",
        "FORCE ROW LEVEL SECURITY",
        "ngkg_query_execution_log_guard",
        "may be finalized exactly once",
        "query_execution_log_no_delete",
    )
    catalog = require(
        "crates/ngkg-catalog/src/lib.rs",
        "BeginQueryExecutionLog",
        "FinalizeQueryExecutionLog",
        "begin_query_execution_log",
        "finalize_query_execution_log",
        "list_query_execution_logs",
        "catalog migrations through version 9 are required",
    )
    auth = require(
        "services/online-serving/src/auth.rs",
        '"query-logs:read"',
        '"query-logs:read:text"',
        "can_read_all_query_logs",
        "can_read_query_text",
        "authorize_query_logs",
    )
    online = require(
        "services/online-serving/src/main.rs",
        '"/v1/query_logs"',
        '"/v1/query_logs/{query_execution_id}"',
        "x-ngkg-query-execution-id",
        "start_time_epoch_ms",
        "end_time_epoch_ms",
        "participating_nodes",
        "allocated_cpu_millicores",
        "allocated_ram_bytes",
        'format!("{}min {}s"',
        '"1min 30s"',
        '"230min 12s"',
    )
    openapi = yaml.safe_load((ROOT / "api/online-openapi.yaml").read_text(encoding="utf-8"))
    for path in ("/v1/query_logs", "/v1/query_logs/{queryExecutionId}"):
        if path not in openapi["paths"]:
            raise RuntimeError(f"OpenAPI is missing {path}")
    for schema in ("QueryLog", "QueryLogPage", "QueryLogResources", "QueryLogTiming"):
        if schema not in openapi["components"]["schemas"]:
            raise RuntimeError(f"OpenAPI is missing {schema}")
    values = yaml.safe_load((ROOT / "charts/ngkg-workloads/values.yaml").read_text(encoding="utf-8"))
    query_logs = values["onlineServing"]["queryLogs"]
    if int(query_logs["coordinatorCpuMillis"]) != 16000 \
            or int(query_logs["coordinatorMemoryBytes"]) != 64 * 1024**3:
        raise RuntimeError("query-log coordinator resource envelope differs from the pod request")
    template = require(
        "charts/ngkg-workloads/templates/online-data-plane.yaml",
        "NGKG_QUERY_LOG_STORE_QUERY_TEXT",
        "NGKG_QUERY_LOG_MAX_PAGE_SIZE",
        "NGKG_QUERY_LOG_COORDINATOR_CPU_MILLIS",
        "NGKG_QUERY_LOG_FRAGMENT_MEMORY_BYTES",
        "NGKG_QUERY_LOG_HYDRATION_MEMORY_BYTES",
    )
    schema = json.loads((ROOT / "contracts/api-auth-tokens.schema.json").read_text(encoding="utf-8"))
    scope_enum = schema["properties"]["tokens"]["items"]["properties"]["scopes"]["items"]["enum"]
    if not {"query-logs:read", "query-logs:read:text"} <= set(scope_enum):
        raise RuntimeError("token contract lacks query-log scopes")
    combined = migration + catalog + auth + online + template
    if "align_ontology" in combined or "raw_data_mapping" in combined:
        raise RuntimeError("ontology alignment or raw-data mapping entered enterprise query audit")
    print("phase 40.13.21 static qualification passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"phase 40.13.21 static qualification failed: {error}", file=sys.stderr)
        raise SystemExit(1)
