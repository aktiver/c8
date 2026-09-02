-- Enterprise Remediation Phase 7.
-- Historical migrations 0002 and 0006 are frozen at their published hashes.
-- Apply their corrective behavior here so both upgraded and empty databases
-- converge through the same forward-only history.

CREATE OR REPLACE FUNCTION ngkg_operation_transition_guard()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.tenant_id <> OLD.tenant_id
    OR NEW.operation_id <> OLD.operation_id
    OR NEW.kind <> OLD.kind
    OR NEW.dataset_id <> OLD.dataset_id
    OR NEW.idempotency_key <> OLD.idempotency_key
    OR NEW.request_hash <> OLD.request_hash
    OR NEW.target_snapshot_id IS DISTINCT FROM OLD.target_snapshot_id
    OR NEW.created_at IS DISTINCT FROM OLD.created_at
  THEN
    RAISE EXCEPTION 'operation identity and request fields are immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  IF OLD.state IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND NEW.state <> OLD.state THEN
    RAISE EXCEPTION 'terminal operation state is immutable'
      USING ERRCODE = 'check_violation';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS operation_transition_guard ON operation;
CREATE TRIGGER operation_transition_guard
BEFORE UPDATE ON operation
FOR EACH ROW
EXECUTE FUNCTION ngkg_operation_transition_guard();

-- The table owner must see all tenants for this deterministic migration-only
-- backfill. ENABLE RLS remains active for non-owner roles during the transaction.
ALTER TABLE dataset NO FORCE ROW LEVEL SECURITY;
UPDATE dataset
SET dataset_name = dataset_id::text
WHERE dataset_name IS NULL;
ALTER TABLE dataset FORCE ROW LEVEL SECURITY;
