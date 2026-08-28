CREATE TYPE ngkg_storage_operation_kind AS ENUM (
  'REPLICATE', 'RELOCATE', 'NODE_LOSS', 'BACKUP', 'RESTORE'
);

CREATE TYPE ngkg_storage_operation_state AS ENUM (
  'REGISTERED', 'PLANNED', 'RUNNING', 'VERIFYING', 'SUCCEEDED', 'FAILED', 'CANCELLED'
);

CREATE TYPE ngkg_replica_state AS ENUM (
  'COPYING', 'VERIFYING', 'READY', 'QUARANTINED', 'RETIRING', 'LOST'
);

CREATE TABLE storage_recovery_operation (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  source_snapshot_id UUID NOT NULL,
  restored_snapshot_id UUID,
  kind ngkg_storage_operation_kind NOT NULL,
  idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 16 AND 128),
  request_sha256 BYTEA NOT NULL CHECK (octet_length(request_sha256) = 32),
  plan_object_key TEXT NOT NULL CHECK (
    length(plan_object_key) BETWEEN 1 AND 1024
    AND plan_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  plan_sha256 BYTEA NOT NULL CHECK (octet_length(plan_sha256) = 32),
  task_count INTEGER NOT NULL CHECK (task_count BETWEEN 0 AND 10000000),
  max_in_flight_bytes BIGINT NOT NULL CHECK (max_in_flight_bytes > 0),
  state ngkg_storage_operation_state NOT NULL DEFAULT 'REGISTERED',
  recovery_certificate_object_key TEXT,
  recovery_certificate_sha256 BYTEA CHECK (octet_length(recovery_certificate_sha256) = 32),
  error_code TEXT,
  revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id),
  UNIQUE (tenant_id, idempotency_key),
  FOREIGN KEY (tenant_id, dataset_id, source_snapshot_id)
    REFERENCES snapshot (tenant_id, dataset_id, snapshot_id),
  CHECK ((kind = 'RESTORE') = (restored_snapshot_id IS NOT NULL)),
  CHECK ((state = 'SUCCEEDED') = (
    recovery_certificate_object_key IS NOT NULL AND recovery_certificate_sha256 IS NOT NULL
  )),
  CHECK ((state = 'FAILED') = (error_code IS NOT NULL))
);

CREATE TABLE snapshot_artifact_replica (
  tenant_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  snapshot_id UUID NOT NULL,
  artifact_sha256 BYTEA NOT NULL CHECK (octet_length(artifact_sha256) = 32),
  artifact_bytes BIGINT NOT NULL CHECK (artifact_bytes >= 0),
  storage_target TEXT NOT NULL CHECK (length(storage_target) BETWEEN 1 AND 63),
  failure_domain TEXT NOT NULL CHECK (length(failure_domain) BETWEEN 1 AND 253),
  object_key TEXT NOT NULL CHECK (
    length(object_key) BETWEEN 1 AND 1024
    AND object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  state ngkg_replica_state NOT NULL,
  verified_at TIMESTAMPTZ,
  quarantine_code TEXT,
  recovery_operation_id UUID NOT NULL,
  PRIMARY KEY (tenant_id, dataset_id, snapshot_id, artifact_sha256, storage_target, object_key),
  FOREIGN KEY (tenant_id, dataset_id, snapshot_id)
    REFERENCES snapshot (tenant_id, dataset_id, snapshot_id),
  FOREIGN KEY (tenant_id, recovery_operation_id)
    REFERENCES storage_recovery_operation (tenant_id, operation_id),
  CHECK ((state IN ('READY','RETIRING')) = (verified_at IS NOT NULL)),
  CHECK ((state = 'QUARANTINED') = (quarantine_code IS NOT NULL))
);

CREATE INDEX snapshot_replica_health_idx
  ON snapshot_artifact_replica (tenant_id, snapshot_id, state, failure_domain);

CREATE TABLE snapshot_backup (
  tenant_id UUID NOT NULL,
  backup_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  source_snapshot_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  destination_target TEXT NOT NULL CHECK (length(destination_target) BETWEEN 1 AND 63),
  backup_manifest_object_key TEXT NOT NULL CHECK (
    length(backup_manifest_object_key) BETWEEN 1 AND 1024
    AND backup_manifest_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  backup_manifest_sha256 BYTEA NOT NULL CHECK (octet_length(backup_manifest_sha256) = 32),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, backup_id),
  UNIQUE (tenant_id, operation_id),
  FOREIGN KEY (tenant_id, dataset_id, source_snapshot_id)
    REFERENCES snapshot (tenant_id, dataset_id, snapshot_id),
  FOREIGN KEY (tenant_id, operation_id)
    REFERENCES storage_recovery_operation (tenant_id, operation_id)
);

ALTER TABLE storage_recovery_operation ENABLE ROW LEVEL SECURITY;
ALTER TABLE storage_recovery_operation FORCE ROW LEVEL SECURITY;
CREATE POLICY storage_recovery_operation_tenant_policy ON storage_recovery_operation
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

ALTER TABLE snapshot_artifact_replica ENABLE ROW LEVEL SECURITY;
ALTER TABLE snapshot_artifact_replica FORCE ROW LEVEL SECURITY;
CREATE POLICY snapshot_artifact_replica_tenant_policy ON snapshot_artifact_replica
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

ALTER TABLE snapshot_backup ENABLE ROW LEVEL SECURITY;
ALTER TABLE snapshot_backup FORCE ROW LEVEL SECURITY;
CREATE POLICY snapshot_backup_tenant_policy ON snapshot_backup
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE OR REPLACE FUNCTION ngkg_storage_recovery_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id
    OR NEW.operation_id <> OLD.operation_id
    OR NEW.dataset_id <> OLD.dataset_id
    OR NEW.source_snapshot_id <> OLD.source_snapshot_id
    OR NEW.restored_snapshot_id IS DISTINCT FROM OLD.restored_snapshot_id
    OR NEW.kind <> OLD.kind
    OR NEW.idempotency_key <> OLD.idempotency_key
    OR NEW.request_sha256 <> OLD.request_sha256
    OR NEW.plan_object_key <> OLD.plan_object_key
    OR NEW.plan_sha256 <> OLD.plan_sha256
    OR NEW.task_count <> OLD.task_count
    OR NEW.max_in_flight_bytes <> OLD.max_in_flight_bytes
  THEN
    RAISE EXCEPTION 'storage recovery identity and plan are immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state <> NEW.state AND NOT (
    (OLD.state = 'REGISTERED' AND NEW.state IN ('PLANNED','FAILED','CANCELLED'))
    OR (OLD.state = 'PLANNED' AND NEW.state IN ('RUNNING','VERIFYING','FAILED','CANCELLED'))
    OR (OLD.state = 'RUNNING' AND NEW.state IN ('VERIFYING','FAILED','CANCELLED'))
    OR (OLD.state = 'VERIFYING' AND NEW.state IN ('SUCCEEDED','FAILED'))
  ) THEN
    RAISE EXCEPTION 'illegal storage recovery state transition'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state <> NEW.state AND NEW.revision <> OLD.revision + 1 THEN
    RAISE EXCEPTION 'storage recovery transition must increment revision exactly once'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state = NEW.state AND (
    NEW.recovery_certificate_object_key IS DISTINCT FROM OLD.recovery_certificate_object_key
    OR NEW.recovery_certificate_sha256 IS DISTINCT FROM OLD.recovery_certificate_sha256
    OR NEW.error_code IS DISTINCT FROM OLD.error_code
    OR NEW.revision <> OLD.revision
  ) THEN
    RAISE EXCEPTION 'storage recovery outcome cannot change without a state transition'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER storage_recovery_transition_guard
BEFORE UPDATE ON storage_recovery_operation
FOR EACH ROW EXECUTE FUNCTION ngkg_storage_recovery_guard();

CREATE OR REPLACE FUNCTION ngkg_snapshot_replica_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id
    OR NEW.dataset_id <> OLD.dataset_id
    OR NEW.snapshot_id <> OLD.snapshot_id
    OR NEW.artifact_sha256 <> OLD.artifact_sha256
    OR NEW.artifact_bytes <> OLD.artifact_bytes
    OR NEW.storage_target <> OLD.storage_target
    OR NEW.failure_domain <> OLD.failure_domain
    OR NEW.object_key <> OLD.object_key
    OR NEW.recovery_operation_id <> OLD.recovery_operation_id
  THEN
    RAISE EXCEPTION 'snapshot replica identity and content are immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state <> NEW.state AND NOT (
    (OLD.state = 'COPYING' AND NEW.state IN ('VERIFYING','QUARANTINED','LOST'))
    OR (OLD.state = 'VERIFYING' AND NEW.state IN ('READY','QUARANTINED','LOST'))
    OR (OLD.state = 'READY' AND NEW.state IN ('RETIRING','QUARANTINED','LOST'))
  ) THEN
    RAISE EXCEPTION 'illegal snapshot replica transition'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state = NEW.state AND (
    NEW.verified_at IS DISTINCT FROM OLD.verified_at
    OR NEW.quarantine_code IS DISTINCT FROM OLD.quarantine_code
  ) THEN
    RAISE EXCEPTION 'snapshot replica evidence cannot change without a state transition'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER snapshot_replica_transition_guard
BEFORE UPDATE ON snapshot_artifact_replica
FOR EACH ROW EXECUTE FUNCTION ngkg_snapshot_replica_guard();

CREATE TRIGGER snapshot_backup_immutable
BEFORE UPDATE OR DELETE ON snapshot_backup
FOR EACH ROW EXECUTE FUNCTION ngkg_operation_audit_guard();
