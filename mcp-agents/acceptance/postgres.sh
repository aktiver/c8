#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for command in cargo psql; do
  if ! command -v "${command}" >/dev/null; then
    echo "required PostgreSQL qualification command is unavailable: ${command}" >&2
    exit 2
  fi
done

: "${NGKG_AGENT_TEST_MIGRATION_DATABASE_URL:?migration-owner PostgreSQL URL is required}"
: "${NGKG_AGENT_TEST_RUNTIME_DATABASE_URL:?runtime-role PostgreSQL URL is required}"
: "${NGKG_AGENT_TEST_RUNTIME_DATABASE_ROLE:?runtime PostgreSQL role is required}"

export NGKG_AGENT_DATABASE_URL="${NGKG_AGENT_TEST_MIGRATION_DATABASE_URL}"
export NGKG_AGENT_DATABASE_ALLOW_INSECURE_LOOPBACK="${NGKG_AGENT_TEST_ALLOW_INSECURE_LOOPBACK:-false}"
export NGKG_AGENT_RUNTIME_DATABASE_ROLE="${NGKG_AGENT_TEST_RUNTIME_DATABASE_ROLE}"

cd "${ROOT}"
cargo run --locked --package ngkg-agent-catalog-migrator
psql "${NGKG_AGENT_TEST_RUNTIME_DATABASE_URL}" \
  --set=ON_ERROR_STOP=1 \
  --file="${ROOT}/qualification/postgres/rls_immutability.sql"

echo "NGKG MCP Agent 0.8.0 PostgreSQL acceptance: PASS"
