CREATE TYPE ngkg_work_kind AS ENUM ('PROJECTION', 'REDUCER');
CREATE TYPE ngkg_work_state AS ENUM ('PENDING', 'SUCCEEDED', 'FAILED');

CREATE TABLE distributed_plan (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  source_plan_object_key TEXT NOT NULL CHECK (
    length(source_plan_object_key) BETWEEN 1 AND 1024
    AND source_plan_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  source_plan_sha256 BYTEA NOT NULL CHECK (octet_length(source_plan_sha256) = 32),
  logical_partition_count INTEGER NOT NULL CHECK (logical_partition_count BETWEEN 1 AND 65536),
  reducer_count INTEGER NOT NULL CHECK (reducer_count BETWEEN 1 AND logical_partition_count),
  fact_count BIGINT NOT NULL CHECK (fact_count >= 0),
  layout_profile TEXT NOT NULL CHECK (length(layout_profile) BETWEEN 1 AND 128),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id),
  FOREIGN KEY (tenant_id, operation_id) REFERENCES operation (tenant_id, operation_id)
);

CREATE TABLE distributed_work (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  work_kind ngkg_work_kind NOT NULL,
  work_index INTEGER NOT NULL CHECK (work_index >= 0),
  stable_work_id TEXT NOT NULL CHECK (stable_work_id ~ '^blake3:[0-9a-f]{64}$'),
  input_object_key TEXT NOT NULL CHECK (
    length(input_object_key) BETWEEN 1 AND 1024
    AND input_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  input_sha256 BYTEA NOT NULL CHECK (octet_length(input_sha256) = 32),
  state ngkg_work_state NOT NULL DEFAULT 'PENDING',
  output_manifest_object_key TEXT CHECK (
    output_manifest_object_key IS NULL OR (
      length(output_manifest_object_key) BETWEEN 1 AND 1024
      AND output_manifest_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
    )
  ),
  output_manifest_sha256 BYTEA CHECK (
    output_manifest_sha256 IS NULL OR octet_length(output_manifest_sha256) = 32
  ),
  error_code TEXT,
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (tenant_id, operation_id, work_kind, work_index),
  UNIQUE (tenant_id, operation_id, stable_work_id),
  FOREIGN KEY (tenant_id, operation_id) REFERENCES distributed_plan (tenant_id, operation_id),
  CHECK (
    (state = 'PENDING' AND output_manifest_object_key IS NULL AND output_manifest_sha256 IS NULL AND error_code IS NULL AND completed_at IS NULL)
    OR (state = 'SUCCEEDED' AND output_manifest_object_key IS NOT NULL AND output_manifest_sha256 IS NOT NULL AND error_code IS NULL AND completed_at IS NOT NULL)
    OR (state = 'FAILED' AND output_manifest_object_key IS NULL AND output_manifest_sha256 IS NULL AND error_code IS NOT NULL AND completed_at IS NOT NULL)
  )
);

CREATE INDEX distributed_work_pending_idx
  ON distributed_work (tenant_id, operation_id, work_kind, state, work_index);

CREATE TABLE distributed_root (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  root_manifest_object_key TEXT NOT NULL CHECK (
    length(root_manifest_object_key) BETWEEN 1 AND 1024
    AND root_manifest_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  root_manifest_sha256 BYTEA NOT NULL CHECK (octet_length(root_manifest_sha256) = 32),
  canonical_source_object_key TEXT NOT NULL CHECK (
    length(canonical_source_object_key) BETWEEN 1 AND 1024
    AND canonical_source_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  canonical_source_sha256 BYTEA NOT NULL CHECK (octet_length(canonical_source_sha256) = 32),
  dictionary_object_key TEXT NOT NULL CHECK (
    length(dictionary_object_key) BETWEEN 1 AND 1024
    AND dictionary_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  dictionary_sha256 BYTEA NOT NULL CHECK (octet_length(dictionary_sha256) = 32),
  semantic_content_sha256 BYTEA NOT NULL CHECK (octet_length(semantic_content_sha256) = 32),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id),
  FOREIGN KEY (tenant_id, operation_id) REFERENCES distributed_plan (tenant_id, operation_id)
);

ALTER TABLE distributed_plan ENABLE ROW LEVEL SECURITY;
ALTER TABLE distributed_plan FORCE ROW LEVEL SECURITY;
CREATE POLICY distributed_plan_tenant_policy ON distributed_plan
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

ALTER TABLE distributed_work ENABLE ROW LEVEL SECURITY;
ALTER TABLE distributed_work FORCE ROW LEVEL SECURITY;
CREATE POLICY distributed_work_tenant_policy ON distributed_work
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

ALTER TABLE distributed_root ENABLE ROW LEVEL SECURITY;
ALTER TABLE distributed_root FORCE ROW LEVEL SECURITY;
CREATE POLICY distributed_root_tenant_policy ON distributed_root
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE TRIGGER distributed_plan_immutable
BEFORE UPDATE OR DELETE ON distributed_plan
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_audit_guard();

CREATE OR REPLACE FUNCTION ngkg_distributed_work_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id
    OR NEW.operation_id <> OLD.operation_id
    OR NEW.work_kind <> OLD.work_kind
    OR NEW.work_index <> OLD.work_index
    OR NEW.stable_work_id <> OLD.stable_work_id
    OR NEW.input_object_key <> OLD.input_object_key
    OR NEW.input_sha256 <> OLD.input_sha256
  THEN
    RAISE EXCEPTION 'distributed work identity and input are immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state = NEW.state THEN
    IF NEW.output_manifest_object_key IS DISTINCT FROM OLD.output_manifest_object_key
      OR NEW.output_manifest_sha256 IS DISTINCT FROM OLD.output_manifest_sha256
      OR NEW.error_code IS DISTINCT FROM OLD.error_code
      OR NEW.completed_at IS DISTINCT FROM OLD.completed_at
    THEN
      RAISE EXCEPTION 'distributed work output cannot change without state transition'
        USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
  END IF;
  IF OLD.state <> 'PENDING' OR NEW.state NOT IN ('SUCCEEDED', 'FAILED') THEN
    RAISE EXCEPTION 'distributed work has one terminal transition'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER distributed_work_transition_guard
BEFORE UPDATE ON distributed_work
FOR EACH ROW
EXECUTE FUNCTION ngkg_distributed_work_guard();

CREATE TRIGGER distributed_work_no_delete
BEFORE DELETE ON distributed_work
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_audit_guard();

CREATE TRIGGER distributed_root_immutable
BEFORE UPDATE OR DELETE ON distributed_root
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_audit_guard();
