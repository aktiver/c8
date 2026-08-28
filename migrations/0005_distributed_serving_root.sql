CREATE TABLE distributed_serving_root (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  serving_root_object_key TEXT NOT NULL CHECK (
    length(serving_root_object_key) BETWEEN 1 AND 1024
    AND serving_root_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  serving_root_sha256 BYTEA NOT NULL CHECK (octet_length(serving_root_sha256) = 32),
  binary_locator_object_key TEXT NOT NULL CHECK (
    length(binary_locator_object_key) BETWEEN 1 AND 1024
    AND binary_locator_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  binary_locator_sha256 BYTEA NOT NULL CHECK (octet_length(binary_locator_sha256) = 32),
  source_locator_sha256 BYTEA NOT NULL CHECK (octet_length(source_locator_sha256) = 32),
  semantic_content_sha256 BYTEA NOT NULL CHECK (octet_length(semantic_content_sha256) = 32),
  partition_count INTEGER NOT NULL CHECK (partition_count BETWEEN 1 AND 65536),
  row_group_rows INTEGER NOT NULL CHECK (row_group_rows > 0),
  locator_record_count BIGINT NOT NULL CHECK (locator_record_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id),
  FOREIGN KEY (tenant_id, operation_id)
    REFERENCES distributed_artifact_root (tenant_id, operation_id)
);

CREATE TABLE distributed_serving_certification (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  report_object_key TEXT NOT NULL CHECK (
    length(report_object_key) BETWEEN 1 AND 1024
    AND report_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  report_sha256 BYTEA NOT NULL CHECK (octet_length(report_sha256) = 32),
  serving_root_sha256 BYTEA NOT NULL CHECK (octet_length(serving_root_sha256) = 32),
  binary_locator_sha256 BYTEA NOT NULL CHECK (octet_length(binary_locator_sha256) = 32),
  reference_manifest_object_key TEXT NOT NULL CHECK (
    length(reference_manifest_object_key) BETWEEN 1 AND 1024
    AND reference_manifest_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  reference_manifest_sha256 BYTEA NOT NULL CHECK (octet_length(reference_manifest_sha256) = 32),
  certified_query_count INTEGER NOT NULL CHECK (certified_query_count > 0),
  hydrated_row_count BIGINT NOT NULL CHECK (hydrated_row_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id),
  FOREIGN KEY (tenant_id, operation_id)
    REFERENCES distributed_serving_root (tenant_id, operation_id)
);

ALTER TABLE distributed_serving_root ENABLE ROW LEVEL SECURITY;
ALTER TABLE distributed_serving_root FORCE ROW LEVEL SECURITY;
CREATE POLICY distributed_serving_root_tenant_policy ON distributed_serving_root
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

ALTER TABLE distributed_serving_certification ENABLE ROW LEVEL SECURITY;
ALTER TABLE distributed_serving_certification FORCE ROW LEVEL SECURITY;
CREATE POLICY distributed_serving_certification_tenant_policy
  ON distributed_serving_certification
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE TRIGGER distributed_serving_root_immutable
BEFORE UPDATE OR DELETE ON distributed_serving_root
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_audit_guard();

CREATE TRIGGER distributed_serving_certification_immutable
BEFORE UPDATE OR DELETE ON distributed_serving_certification
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_audit_guard();
