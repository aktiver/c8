CREATE TYPE ngkg_job_state AS ENUM (
  'REGISTERED', 'SOURCE_PLANNED', 'MAPPING_VALIDATED', 'PARTITIONED',
  'PROJECTED', 'IDENTIFIED', 'SPINE_WRITTEN', 'INDEXED', 'REASONED',
  'CERTIFIED', 'PUBLISHED', 'FAILED', 'CANCELLED'
);

CREATE TABLE dataset (
  tenant_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  identity_namespace UUID NOT NULL,
  active_snapshot_id UUID,
  policy_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, dataset_id),
  UNIQUE (tenant_id, identity_namespace)
);

ALTER TABLE dataset ENABLE ROW LEVEL SECURITY;
ALTER TABLE dataset FORCE ROW LEVEL SECURITY;
CREATE POLICY dataset_tenant_policy ON dataset
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE TABLE operation (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 16 AND 128),
  request_hash BYTEA NOT NULL CHECK (octet_length(request_hash) = 32),
  state ngkg_job_state NOT NULL,
  target_snapshot_id UUID,
  error_code TEXT,
  error_artifact_uri TEXT,
  revision BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id),
  UNIQUE (tenant_id, idempotency_key),
  FOREIGN KEY (tenant_id, dataset_id) REFERENCES dataset (tenant_id, dataset_id),
  CHECK ((state <> 'FAILED') OR error_code IS NOT NULL)
);

ALTER TABLE operation ENABLE ROW LEVEL SECURITY;
ALTER TABLE operation FORCE ROW LEVEL SECURITY;
CREATE POLICY operation_tenant_policy ON operation
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE TABLE stage_partition (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  stage_name TEXT NOT NULL,
  partition_id TEXT NOT NULL,
  input_hash BYTEA NOT NULL CHECK (octet_length(input_hash) = 32),
  expected_output_uri TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('PENDING','LEASED','SUCCEEDED','FAILED')),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  output_hash BYTEA,
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
  PRIMARY KEY (tenant_id, operation_id, stage_name, partition_id),
  FOREIGN KEY (tenant_id, operation_id) REFERENCES operation (tenant_id, operation_id)
);

CREATE INDEX stage_partition_lease_idx
  ON stage_partition (tenant_id, operation_id, stage_name, state, lease_expires_at);

ALTER TABLE stage_partition ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_partition FORCE ROW LEVEL SECURITY;
CREATE POLICY stage_partition_tenant_policy ON stage_partition
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE TABLE operation_audit (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  revision BIGINT NOT NULL,
  previous_state ngkg_job_state,
  new_state ngkg_job_state NOT NULL,
  actor TEXT NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id, revision)
);

