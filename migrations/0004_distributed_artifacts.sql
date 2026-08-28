ALTER TYPE ngkg_work_kind ADD VALUE IF NOT EXISTS 'ARTIFACT';

CREATE TABLE distributed_artifact_plan (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  source_plan_object_key TEXT NOT NULL CHECK (
    length(source_plan_object_key) BETWEEN 1 AND 1024
    AND source_plan_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  source_plan_sha256 BYTEA NOT NULL CHECK (octet_length(source_plan_sha256) = 32),
  dictionary_object_key TEXT NOT NULL CHECK (
    length(dictionary_object_key) BETWEEN 1 AND 1024
    AND dictionary_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  dictionary_sha256 BYTEA NOT NULL CHECK (octet_length(dictionary_sha256) = 32),
  artifact_plan_object_key TEXT NOT NULL CHECK (
    length(artifact_plan_object_key) BETWEEN 1 AND 1024
    AND artifact_plan_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  artifact_plan_sha256 BYTEA NOT NULL CHECK (octet_length(artifact_plan_sha256) = 32),
  partition_count INTEGER NOT NULL CHECK (partition_count BETWEEN 1 AND 65536),
  row_group_rows INTEGER NOT NULL CHECK (row_group_rows BETWEEN 1 AND 2147483647),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id),
  FOREIGN KEY (tenant_id, operation_id) REFERENCES distributed_root (tenant_id, operation_id)
);

CREATE TABLE distributed_artifact_root (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  root_manifest_object_key TEXT NOT NULL CHECK (
    length(root_manifest_object_key) BETWEEN 1 AND 1024
    AND root_manifest_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  root_manifest_sha256 BYTEA NOT NULL CHECK (octet_length(root_manifest_sha256) = 32),
  locator_object_key TEXT NOT NULL CHECK (
    length(locator_object_key) BETWEEN 1 AND 1024
    AND locator_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  locator_sha256 BYTEA NOT NULL CHECK (octet_length(locator_sha256) = 32),
  semantic_content_sha256 BYTEA NOT NULL CHECK (octet_length(semantic_content_sha256) = 32),
  fact_count BIGINT NOT NULL CHECK (fact_count >= 0),
  semantic_row_count BIGINT NOT NULL CHECK (semantic_row_count >= 0),
  payload_row_count BIGINT NOT NULL CHECK (payload_row_count >= 0),
  locator_record_count BIGINT NOT NULL CHECK (locator_record_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id),
  FOREIGN KEY (tenant_id, operation_id) REFERENCES distributed_artifact_plan (tenant_id, operation_id),
  CHECK (payload_row_count = locator_record_count),
  CHECK (semantic_row_count + payload_row_count = fact_count)
);

ALTER TABLE distributed_artifact_plan ENABLE ROW LEVEL SECURITY;
ALTER TABLE distributed_artifact_plan FORCE ROW LEVEL SECURITY;
CREATE POLICY distributed_artifact_plan_tenant_policy ON distributed_artifact_plan
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

ALTER TABLE distributed_artifact_root ENABLE ROW LEVEL SECURITY;
ALTER TABLE distributed_artifact_root FORCE ROW LEVEL SECURITY;
CREATE POLICY distributed_artifact_root_tenant_policy ON distributed_artifact_root
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE TRIGGER distributed_artifact_plan_immutable
BEFORE UPDATE OR DELETE ON distributed_artifact_plan
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_audit_guard();

CREATE TRIGGER distributed_artifact_root_immutable
BEFORE UPDATE OR DELETE ON distributed_artifact_root
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_audit_guard();
