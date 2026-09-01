-- Enterprise Stabilization Phase 4: durable orchestration and measured resource evidence.

CREATE TYPE ngkg_orchestration_stage_state AS ENUM (
  'RESERVED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED'
);

CREATE TABLE orchestration_stage (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  stage_name TEXT NOT NULL CHECK (length(stage_name) BETWEEN 1 AND 128),
  stage_spec_sha256 BYTEA NOT NULL CHECK (octet_length(stage_spec_sha256) = 32),
  state ngkg_orchestration_stage_state NOT NULL DEFAULT 'RESERVED',
  job_namespace TEXT NOT NULL CHECK (length(job_namespace) BETWEEN 1 AND 253),
  job_name TEXT NOT NULL CHECK (length(job_name) BETWEEN 1 AND 253),
  job_uid TEXT,
  terminal_evidence JSONB,
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (tenant_id, operation_id, stage_name),
  FOREIGN KEY (tenant_id, operation_id) REFERENCES operation (tenant_id, operation_id),
  CHECK ((state = 'SUCCEEDED') = (completed_at IS NOT NULL)),
  CHECK (state <> 'SUCCEEDED' OR terminal_evidence IS NOT NULL)
);

CREATE INDEX orchestration_stage_reconcile_idx
  ON orchestration_stage (tenant_id, state, updated_at, operation_id);

ALTER TABLE orchestration_stage ENABLE ROW LEVEL SECURITY;
ALTER TABLE orchestration_stage FORCE ROW LEVEL SECURITY;
CREATE POLICY orchestration_stage_tenant_policy ON orchestration_stage
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE OR REPLACE FUNCTION ngkg_orchestration_stage_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id
    OR NEW.operation_id <> OLD.operation_id
    OR NEW.stage_name <> OLD.stage_name
    OR NEW.stage_spec_sha256 <> OLD.stage_spec_sha256
    OR OLD.state IN ('SUCCEEDED', 'CANCELLED')
  THEN
    RAISE EXCEPTION 'orchestration stage identity or terminal state is immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state = 'FAILED' AND NEW.state NOT IN ('FAILED', 'RUNNING') THEN
    RAISE EXCEPTION 'failed orchestration stage may only be retried explicitly'
      USING ERRCODE = 'check_violation';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

CREATE TRIGGER orchestration_stage_transition_guard
BEFORE UPDATE ON orchestration_stage
FOR EACH ROW EXECUTE FUNCTION ngkg_orchestration_stage_guard();

CREATE TABLE source_upload_reservation (
  tenant_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  source_id UUID NOT NULL,
  content_sha256 BYTEA NOT NULL CHECK (octet_length(content_sha256) = 32),
  state TEXT NOT NULL DEFAULT 'RESERVED' CHECK (state IN ('RESERVED','PUBLISHED')),
  response_payload JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  PRIMARY KEY (tenant_id, dataset_id, source_id),
  FOREIGN KEY (tenant_id, dataset_id) REFERENCES dataset (tenant_id, dataset_id),
  CHECK ((state = 'PUBLISHED') = (response_payload IS NOT NULL AND published_at IS NOT NULL))
);

ALTER TABLE source_upload_reservation ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_upload_reservation FORCE ROW LEVEL SECURITY;
CREATE POLICY source_upload_reservation_tenant_policy ON source_upload_reservation
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE OR REPLACE FUNCTION ngkg_source_upload_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id OR NEW.dataset_id <> OLD.dataset_id
     OR NEW.source_id <> OLD.source_id OR NEW.content_sha256 <> OLD.content_sha256
     OR OLD.state = 'PUBLISHED' THEN
    RAISE EXCEPTION 'source upload identity and published result are immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER source_upload_transition_guard
BEFORE UPDATE ON source_upload_reservation
FOR EACH ROW EXECUTE FUNCTION ngkg_source_upload_guard();

ALTER TABLE query_execution_log
  ADD COLUMN requested_cpu_millis BIGINT CHECK (requested_cpu_millis IS NULL OR requested_cpu_millis > 0),
  ADD COLUMN requested_memory_bytes BIGINT CHECK (requested_memory_bytes IS NULL OR requested_memory_bytes > 0),
  ADD COLUMN measured_cpu_time_millis BIGINT CHECK (measured_cpu_time_millis IS NULL OR measured_cpu_time_millis >= 0),
  ADD COLUMN measured_peak_rss_bytes BIGINT CHECK (measured_peak_rss_bytes IS NULL OR measured_peak_rss_bytes >= 0),
  ADD COLUMN measured_gpu_time_millis BIGINT CHECK (measured_gpu_time_millis IS NULL OR measured_gpu_time_millis >= 0),
  ADD COLUMN measured_gpu_peak_memory_bytes BIGINT CHECK (measured_gpu_peak_memory_bytes IS NULL OR measured_gpu_peak_memory_bytes >= 0),
  ADD COLUMN participating_pod_uids TEXT[] NOT NULL DEFAULT '{}',
  ADD COLUMN participating_node_uids TEXT[] NOT NULL DEFAULT '{}',
  ADD COLUMN autoscaling_events JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN measurement_scope TEXT NOT NULL DEFAULT 'UNAVAILABLE'
    CHECK (measurement_scope IN ('COORDINATOR_CGROUP_INTERVAL', 'ALL_PARTICIPANTS', 'UNAVAILABLE'));

ALTER TABLE query_execution_log
  ADD CONSTRAINT query_execution_log_autoscaling_events_array
  CHECK (jsonb_typeof(autoscaling_events) = 'array');
