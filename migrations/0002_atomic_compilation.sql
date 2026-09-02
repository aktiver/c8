CREATE TYPE ngkg_publication_policy AS ENUM (
  'MANUAL_AFTER_CERTIFICATION',
  'AUTOMATIC_AFTER_CERTIFICATION'
);

CREATE TYPE ngkg_snapshot_state AS ENUM ('CERTIFIED', 'PUBLISHED', 'RETIRED');

CREATE TABLE compilation_request (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  bundle_object_key TEXT NOT NULL CHECK (
    length(bundle_object_key) BETWEEN 1 AND 1024
    AND bundle_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  bundle_sha256 BYTEA NOT NULL CHECK (octet_length(bundle_sha256) = 32),
  parent_snapshot_id UUID,
  target_snapshot_id UUID NOT NULL,
  publication_policy ngkg_publication_policy NOT NULL,
  resource_profile TEXT NOT NULL CHECK (length(resource_profile) BETWEEN 1 AND 128),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id),
  UNIQUE (tenant_id, target_snapshot_id),
  FOREIGN KEY (tenant_id, operation_id) REFERENCES operation (tenant_id, operation_id)
);

ALTER TABLE compilation_request ENABLE ROW LEVEL SECURITY;
ALTER TABLE compilation_request FORCE ROW LEVEL SECURITY;
CREATE POLICY compilation_request_tenant_policy ON compilation_request
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE TABLE snapshot (
  tenant_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  snapshot_id UUID NOT NULL,
  parent_snapshot_id UUID,
  operation_id UUID NOT NULL,
  manifest_object_key TEXT NOT NULL CHECK (
    length(manifest_object_key) BETWEEN 1 AND 1024
    AND manifest_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  manifest_sha256 BYTEA NOT NULL CHECK (octet_length(manifest_sha256) = 32),
  state ngkg_snapshot_state NOT NULL,
  certified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ,
  PRIMARY KEY (tenant_id, dataset_id, snapshot_id),
  UNIQUE (tenant_id, operation_id),
  FOREIGN KEY (tenant_id, dataset_id) REFERENCES dataset (tenant_id, dataset_id),
  FOREIGN KEY (tenant_id, operation_id) REFERENCES operation (tenant_id, operation_id),
  CHECK ((state <> 'PUBLISHED') OR published_at IS NOT NULL)
);

CREATE INDEX snapshot_operation_idx ON snapshot (tenant_id, operation_id);

ALTER TABLE snapshot
  ADD CONSTRAINT snapshot_parent_fk
  FOREIGN KEY (tenant_id, dataset_id, parent_snapshot_id)
  REFERENCES snapshot (tenant_id, dataset_id, snapshot_id);

ALTER TABLE dataset
  ADD CONSTRAINT dataset_active_snapshot_fk
  FOREIGN KEY (tenant_id, dataset_id, active_snapshot_id)
  REFERENCES snapshot (tenant_id, dataset_id, snapshot_id);

ALTER TABLE snapshot ENABLE ROW LEVEL SECURITY;
ALTER TABLE snapshot FORCE ROW LEVEL SECURITY;
CREATE POLICY snapshot_tenant_policy ON snapshot
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

ALTER TABLE operation_audit ENABLE ROW LEVEL SECURITY;
ALTER TABLE operation_audit FORCE ROW LEVEL SECURITY;
CREATE POLICY operation_audit_tenant_policy ON operation_audit
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

ALTER TABLE operation_audit
  ADD CONSTRAINT operation_audit_operation_fk
  FOREIGN KEY (tenant_id, operation_id) REFERENCES operation (tenant_id, operation_id);

CREATE OR REPLACE FUNCTION ngkg_operation_transition_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id
    OR NEW.operation_id <> OLD.operation_id
    OR NEW.dataset_id <> OLD.dataset_id
    OR NEW.idempotency_key <> OLD.idempotency_key
    OR NEW.request_hash <> OLD.request_hash
    OR NEW.target_snapshot_id IS DISTINCT FROM OLD.target_snapshot_id
  THEN
    RAISE EXCEPTION 'operation identity and request fields are immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state = NEW.state THEN
    IF NEW.revision <> OLD.revision
      OR NEW.error_code IS DISTINCT FROM OLD.error_code
      OR NEW.error_artifact_uri IS DISTINCT FROM OLD.error_artifact_uri
    THEN
      RAISE EXCEPTION 'operation fields cannot change without a legal state transition'
        USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
  END IF;
  IF NOT (
    (OLD.state = 'REGISTERED' AND NEW.state IN ('SOURCE_PLANNED', 'FAILED', 'CANCELLED'))
    OR (OLD.state = 'SOURCE_PLANNED' AND NEW.state IN ('MAPPING_VALIDATED', 'FAILED', 'CANCELLED'))
    OR (OLD.state = 'MAPPING_VALIDATED' AND NEW.state IN ('PARTITIONED', 'FAILED', 'CANCELLED'))
    OR (OLD.state = 'PARTITIONED' AND NEW.state IN ('PROJECTED', 'FAILED', 'CANCELLED'))
    OR (OLD.state = 'PROJECTED' AND NEW.state IN ('IDENTIFIED', 'FAILED', 'CANCELLED'))
    OR (OLD.state = 'IDENTIFIED' AND NEW.state IN ('SPINE_WRITTEN', 'FAILED', 'CANCELLED'))
    OR (OLD.state = 'SPINE_WRITTEN' AND NEW.state IN ('INDEXED', 'FAILED', 'CANCELLED'))
    OR (OLD.state = 'INDEXED' AND NEW.state IN ('REASONED', 'FAILED', 'CANCELLED'))
    OR (OLD.state = 'REASONED' AND NEW.state IN ('CERTIFIED', 'FAILED', 'CANCELLED'))
    OR (OLD.state = 'CERTIFIED' AND NEW.state = 'PUBLISHED')
  ) THEN
    RAISE EXCEPTION 'illegal NGKG operation transition from % to %', OLD.state, NEW.state
      USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.revision <> OLD.revision + 1 THEN
    RAISE EXCEPTION 'state transition must increment revision exactly once'
      USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.state <> 'FAILED'
    AND (
      NEW.error_code IS DISTINCT FROM OLD.error_code
      OR NEW.error_artifact_uri IS DISTINCT FROM OLD.error_artifact_uri
    )
  THEN
    RAISE EXCEPTION 'error fields may change only on a FAILED transition'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER operation_transition_guard
BEFORE UPDATE OF state ON operation
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_transition_guard();

CREATE OR REPLACE FUNCTION ngkg_operation_audit_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'operation audit rows are immutable' USING ERRCODE = 'check_violation';
END;
$$;

CREATE TRIGGER operation_audit_immutable_update
BEFORE UPDATE OR DELETE ON operation_audit
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_audit_guard();

CREATE TRIGGER compilation_request_immutable
BEFORE UPDATE OR DELETE ON compilation_request
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_audit_guard();

CREATE OR REPLACE FUNCTION ngkg_snapshot_guard()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id
    OR NEW.dataset_id <> OLD.dataset_id
    OR NEW.snapshot_id <> OLD.snapshot_id
    OR NEW.parent_snapshot_id IS DISTINCT FROM OLD.parent_snapshot_id
    OR NEW.operation_id <> OLD.operation_id
    OR NEW.manifest_object_key <> OLD.manifest_object_key
    OR NEW.manifest_sha256 <> OLD.manifest_sha256
    OR NEW.certified_at <> OLD.certified_at
  THEN
    RAISE EXCEPTION 'certified snapshot identity and content are immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  IF NOT (
    OLD.state = NEW.state
    OR (OLD.state = 'CERTIFIED' AND NEW.state = 'PUBLISHED')
    OR (OLD.state = 'PUBLISHED' AND NEW.state = 'RETIRED')
  ) THEN
    RAISE EXCEPTION 'illegal snapshot transition from % to %', OLD.state, NEW.state
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state = NEW.state
    AND NEW.published_at IS DISTINCT FROM OLD.published_at
  THEN
    RAISE EXCEPTION 'published_at may change only when publishing'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state = 'PUBLISHED'
    AND NEW.state = 'RETIRED'
    AND NEW.published_at IS DISTINCT FROM OLD.published_at
  THEN
    RAISE EXCEPTION 'retirement cannot rewrite published_at'
      USING ERRCODE = 'check_violation';
  END IF;
  IF NEW.state = 'PUBLISHED' AND NEW.published_at IS NULL THEN
    RAISE EXCEPTION 'published snapshot requires published_at'
      USING ERRCODE = 'check_violation';
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER snapshot_transition_guard
BEFORE UPDATE ON snapshot
FOR EACH ROW
EXECUTE FUNCTION ngkg_snapshot_guard();
