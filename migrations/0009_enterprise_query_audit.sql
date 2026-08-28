CREATE TYPE ngkg_query_execution_status AS ENUM (
  'RUNNING', 'COMPLETED', 'FAILED', 'TIMED_OUT', 'CANCELLED'
);

CREATE TABLE query_execution_log (
  tenant_id UUID NOT NULL,
  query_execution_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  snapshot_id UUID,
  principal_id TEXT NOT NULL CHECK (length(principal_id) BETWEEN 1 AND 256),
  request_id TEXT NOT NULL CHECK (length(request_id) BETWEEN 1 AND 128),
  query_sha256 BYTEA NOT NULL CHECK (octet_length(query_sha256) = 32),
  query_text TEXT,
  query_form TEXT CHECK (query_form IS NULL OR query_form IN ('SELECT','ASK','CONSTRUCT','DESCRIBE')),
  execution_mode TEXT CHECK (execution_mode IS NULL OR length(execution_mode) BETWEEN 1 AND 128),
  status ngkg_query_execution_status NOT NULL DEFAULT 'RUNNING',
  participating_nodes INTEGER CHECK (participating_nodes IS NULL OR participating_nodes > 0),
  allocated_cpu_millis BIGINT CHECK (allocated_cpu_millis IS NULL OR allocated_cpu_millis > 0),
  allocated_memory_bytes BIGINT CHECK (allocated_memory_bytes IS NULL OR allocated_memory_bytes > 0),
  result_rows BIGINT CHECK (result_rows IS NULL OR result_rows >= 0),
  result_bytes BIGINT CHECK (result_bytes IS NULL OR result_bytes >= 0),
  cache_hit BOOLEAN,
  start_time_epoch_ms BIGINT NOT NULL CHECK (start_time_epoch_ms > 0),
  end_time_epoch_ms BIGINT,
  total_duration_ms BIGINT,
  error_code TEXT CHECK (error_code IS NULL OR length(error_code) BETWEEN 1 AND 128),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finalized_at TIMESTAMPTZ,
  PRIMARY KEY (tenant_id, query_execution_id),
  FOREIGN KEY (tenant_id, dataset_id) REFERENCES dataset (tenant_id, dataset_id),
  FOREIGN KEY (tenant_id, dataset_id, snapshot_id)
    REFERENCES snapshot (tenant_id, dataset_id, snapshot_id),
  CHECK ((status = 'RUNNING') = (end_time_epoch_ms IS NULL AND total_duration_ms IS NULL)),
  CHECK (end_time_epoch_ms IS NULL OR end_time_epoch_ms >= start_time_epoch_ms),
  CHECK (total_duration_ms IS NULL OR total_duration_ms = end_time_epoch_ms - start_time_epoch_ms),
  CHECK ((status IN ('FAILED','TIMED_OUT','CANCELLED')) = (error_code IS NOT NULL)),
  CHECK ((status = 'RUNNING') = (finalized_at IS NULL))
);

CREATE INDEX query_execution_log_time_idx
  ON query_execution_log (tenant_id, start_time_epoch_ms DESC, query_execution_id DESC);
CREATE INDEX query_execution_log_principal_idx
  ON query_execution_log (tenant_id, principal_id, start_time_epoch_ms DESC);
CREATE INDEX query_execution_log_dataset_idx
  ON query_execution_log (tenant_id, dataset_id, start_time_epoch_ms DESC);

ALTER TABLE query_execution_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE query_execution_log FORCE ROW LEVEL SECURITY;
CREATE POLICY query_execution_log_tenant_policy ON query_execution_log
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE OR REPLACE FUNCTION ngkg_query_execution_log_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id
    OR NEW.query_execution_id <> OLD.query_execution_id
    OR NEW.dataset_id <> OLD.dataset_id
    OR NEW.principal_id <> OLD.principal_id
    OR NEW.request_id <> OLD.request_id
    OR NEW.query_sha256 <> OLD.query_sha256
    OR NEW.query_text IS DISTINCT FROM OLD.query_text
    OR NEW.start_time_epoch_ms <> OLD.start_time_epoch_ms
    OR OLD.status <> 'RUNNING'
    OR NEW.status = 'RUNNING'
  THEN
    RAISE EXCEPTION 'query execution identity is immutable and may be finalized exactly once'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER query_execution_log_finalize_once
BEFORE UPDATE ON query_execution_log
FOR EACH ROW EXECUTE FUNCTION ngkg_query_execution_log_guard();

CREATE TRIGGER query_execution_log_no_delete
BEFORE DELETE ON query_execution_log
FOR EACH ROW EXECUTE FUNCTION ngkg_operation_audit_guard();
