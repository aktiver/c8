#!/usr/bin/env bash
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

for command in docker psql pg_dump jq sha256sum; do
  require_command "${command}"
done

: "${NGKG_CORE_MIGRATION_DATABASE_URL:?core migration-owner PostgreSQL URL is required}"
: "${NGKG_CORE_RUNTIME_DATABASE_URL:?core runtime-role PostgreSQL URL is required}"
: "${NGKG_AGENT_MIGRATION_DATABASE_URL:?agent migration-owner PostgreSQL URL is required}"
: "${NGKG_AGENT_RUNTIME_DATABASE_URL:?agent runtime-role PostgreSQL URL is required}"
: "${NGKG_AGENT_RUNTIME_DATABASE_ROLE:?agent runtime database role is required}"
: "${NGKG_PHASE3_IMAGE_LOCK:?Phase 3 image-lock.json is required}"
: "${NGKG_PHASE3_TOOLCHAIN_LOCK:?approved controlled-runner toolchain lock is required}"

for url_name in NGKG_CORE_MIGRATION_DATABASE_URL NGKG_CORE_RUNTIME_DATABASE_URL NGKG_AGENT_MIGRATION_DATABASE_URL NGKG_AGENT_RUNTIME_DATABASE_URL; do
  url="${!url_name}"
  [[ "${url}" != *"sslmode=disable"* ]] || die "${url_name} disables PostgreSQL TLS"
done
require_file "${NGKG_PHASE3_IMAGE_LOCK}"
evidence_dir="${NGKG_PHASE3_POSTGRES_EVIDENCE_DIR:-${candidate_root}/phase3-evidence/postgres}"
mkdir -p "${evidence_dir}"
chmod 0700 "${evidence_dir}"
python3 "${phase3_root}/scripts/verify_toolchain.py" --lock "${NGKG_PHASE3_TOOLCHAIN_LOCK}" \
  --require docker --require psql --require pg_dump --require jq --require python3 \
  --output "${evidence_dir}/toolchain-evidence.json"

core_migrator="$(jq -r '.images[] | select(.name=="ngkg-catalog-migrator") | .repository+"@"+.digest' "${NGKG_PHASE3_IMAGE_LOCK}")"
agent_image="$(jq -r '.images[] | select(.name=="ngkg-agents") | .repository+"@"+.digest' "${NGKG_PHASE3_IMAGE_LOCK}")"
require_digest_ref core_migrator "${core_migrator}"
require_digest_ref agent_image "${agent_image}"

pg_dump --schema-only --no-owner --no-privileges "${NGKG_CORE_MIGRATION_DATABASE_URL}" >"${evidence_dir}/core-before.sql"
pg_dump --schema-only --no-owner --no-privileges "${NGKG_AGENT_MIGRATION_DATABASE_URL}" >"${evidence_dir}/agent-before.sql"
schema_before="$(cat "${evidence_dir}/core-before.sql" "${evidence_dir}/agent-before.sql" | sha256sum | awk '{print $1}')"

export NGKG_DATABASE_URL="${NGKG_CORE_MIGRATION_DATABASE_URL}"
docker run --rm --network "${NGKG_MIGRATION_DOCKER_NETWORK:-host}" \
  -e NGKG_DATABASE_URL -e NGKG_MIGRATION_DIRECTORY=/opt/ngkg/migrations \
  "${core_migrator}"
unset NGKG_DATABASE_URL

export NGKG_AGENT_DATABASE_URL="${NGKG_AGENT_MIGRATION_DATABASE_URL}"
export NGKG_AGENT_RUNTIME_DATABASE_ROLE
export NGKG_AGENT_DATABASE_ALLOW_INSECURE_LOOPBACK=false
docker run --rm --network "${NGKG_MIGRATION_DOCKER_NETWORK:-host}" \
  --entrypoint /usr/local/bin/ngkg-agent-catalog-migrator \
  -e NGKG_AGENT_DATABASE_URL -e NGKG_AGENT_RUNTIME_DATABASE_ROLE \
  -e NGKG_AGENT_DATABASE_ALLOW_INSECURE_LOOPBACK "${agent_image}"
unset NGKG_AGENT_DATABASE_URL NGKG_AGENT_DATABASE_ALLOW_INSECURE_LOOPBACK

pg_dump --schema-only --no-owner --no-privileges "${NGKG_CORE_MIGRATION_DATABASE_URL}" >"${evidence_dir}/core-after.sql"
pg_dump --schema-only --no-owner --no-privileges "${NGKG_AGENT_MIGRATION_DATABASE_URL}" >"${evidence_dir}/agent-after.sql"
schema_after="$(cat "${evidence_dir}/core-after.sql" "${evidence_dir}/agent-after.sql" | sha256sum | awk '{print $1}')"

primary="$(psql "${NGKG_CORE_MIGRATION_DATABASE_URL}" -AtX --set=ON_ERROR_STOP=1 -c 'SELECT NOT pg_is_in_recovery()')"
[[ "${primary}" == t ]] || die "migration endpoint is not a PostgreSQL primary"
streaming_replicas="$(psql "${NGKG_CORE_MIGRATION_DATABASE_URL}" -AtX --set=ON_ERROR_STOP=1 -c "SELECT count(*) FROM pg_stat_replication WHERE state='streaming'")"
[[ "${streaming_replicas}" =~ ^[0-9]+$ && "${streaming_replicas}" -ge 1 ]] || die "PostgreSQL has no visible streaming replica"
server_version="$(psql "${NGKG_CORE_MIGRATION_DATABASE_URL}" -AtX --set=ON_ERROR_STOP=1 -c 'SHOW server_version')"
core_migrations="$(psql "${NGKG_CORE_MIGRATION_DATABASE_URL}" -AtX --set=ON_ERROR_STOP=1 -c 'SELECT count(*) FROM _sqlx_migrations WHERE success')"
agent_migrations="$(psql "${NGKG_AGENT_MIGRATION_DATABASE_URL}" -AtX --set=ON_ERROR_STOP=1 -c 'SELECT count(*) FROM _sqlx_migrations WHERE success')"
[[ "${core_migrations}" -ge 9 && "${agent_migrations}" -ge 7 ]] || die "migration ledger is incomplete"

psql "${NGKG_CORE_RUNTIME_DATABASE_URL}" --set=ON_ERROR_STOP=1 --file="${phase3_root}/sql/core_rls_immutability.sql" >/dev/null
psql "${NGKG_AGENT_RUNTIME_DATABASE_URL}" --set=ON_ERROR_STOP=1 --file="${candidate_root}/ngkg-agents/qualification/postgres/rls_immutability.sql" >/dev/null

jq -n -cS --arg toolchain "$(sha256_file "${evidence_dir}/toolchain-evidence.json")" \
  --arg server "${server_version}" --argjson replicas "${streaming_replicas}" \
  --argjson core "${core_migrations}" --argjson agent "${agent_migrations}" \
  --arg before "${schema_before}" --arg after "${schema_after}" \
  '{formatVersion:1,toolchainEvidenceSha256:$toolchain,serverVersion:$server,primary:true,streamingReplicas:$replicas,coreMigrationCount:$core,agentMigrationCount:$agent,tenantRlsForced:true,immutabilityVerified:true,crossTenantDenied:true,schemaBeforeSha256:$before,schemaAfterSha256:$after,complete:true}' \
  >"${evidence_dir}/postgres-evidence.json"

echo "Phase 3 PostgreSQL migration and RLS qualification: PASS"
