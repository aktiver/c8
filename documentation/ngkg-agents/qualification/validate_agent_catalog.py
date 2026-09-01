#!/usr/bin/env python3
"""Static structural gate for the Phase 2 PostgreSQL trust boundary."""

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SQL = "\n".join(
    path.read_text(encoding="utf-8")
    for path in sorted((ROOT / "migrations-agents").glob("*.sql"))
).lower()

TABLES = (
    "tool_provider",
    "tool_catalog",
    "agent_profile",
    "retention_policy",
    "agent_execution",
    "model_call",
    "tool_call",
    "claim_validation",
    "execution_resource_observation",
    "approval",
    "agent_audit_chain",
    "audit_seal",
)

failures: list[str] = []
for table in TABLES:
    if f"create table ngkg_agents.{table}" not in SQL:
        failures.append(f"missing table {table}")
    if f"'create policy tenant_isolation on ngkg_agents.%i" not in SQL:
        failures.append("tenant RLS policy generator is missing")
        break

for marker in (
    "enable row level security",
    "force row level security",
    "current_setting('ngkg.tenant_id', true)",
    "pg_advisory_xact_lock",
    "audit_chain_immutable",
    "enforce_execution_transition",
    "enforce_model_call_finalize_once",
    "enforce_tool_call_finalize_once",
    "revoke all on all tables in schema ngkg_agents from public",
):
    if marker not in SQL:
        failures.append(f"missing security marker: {marker}")

if failures:
    for failure in sorted(set(failures)):
        print(failure, file=sys.stderr)
    raise SystemExit(1)

print("agent catalog SQL structural gate: PASS")
