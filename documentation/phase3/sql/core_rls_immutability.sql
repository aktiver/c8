\set ON_ERROR_STOP on
BEGIN;

DO $qualification$
DECLARE
    unqualified integer;
BEGIN
    SELECT count(*) INTO unqualified
      FROM pg_class relation
      JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
     WHERE relation.relkind = 'r'
       AND namespace.nspname = 'public'
       AND EXISTS (
           SELECT 1 FROM pg_attribute attribute
            WHERE attribute.attrelid = relation.oid
              AND attribute.attname = 'tenant_id'
              AND NOT attribute.attisdropped
       )
       AND NOT (relation.relrowsecurity AND relation.relforcerowsecurity);
    IF unqualified <> 0 THEN
        RAISE EXCEPTION '% tenant tables do not force RLS', unqualified;
    END IF;
END
$qualification$;

SELECT set_config('ngkg.tenant_id', '81000000-0000-4000-8000-000000000001', true);
INSERT INTO dataset
    (tenant_id, dataset_id, identity_namespace, policy_version, dataset_name)
VALUES
    ('81000000-0000-4000-8000-000000000001',
     '82000000-0000-4000-8000-000000000001',
     '83000000-0000-4000-8000-000000000001',
     'phase3-qualification', 'phase3_qualification');
INSERT INTO operation
    (tenant_id, operation_id, dataset_id, idempotency_key, request_hash, state)
VALUES
    ('81000000-0000-4000-8000-000000000001',
     '84000000-0000-4000-8000-000000000001',
     '82000000-0000-4000-8000-000000000001',
     'phase3-qualification-idempotency', decode(repeat('11', 32), 'hex'),
     'REGISTERED');
INSERT INTO operation_audit
    (tenant_id, operation_id, revision, previous_state, new_state, actor)
VALUES
    ('81000000-0000-4000-8000-000000000001',
     '84000000-0000-4000-8000-000000000001', 0, NULL, 'REGISTERED',
     'phase3-qualification');

DO $qualification$
BEGIN
    BEGIN
        UPDATE operation_audit SET actor = 'mutated'
         WHERE operation_id = '84000000-0000-4000-8000-000000000001';
        RAISE EXCEPTION 'operation audit mutation was accepted';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;
END
$qualification$;

SELECT set_config('ngkg.tenant_id', '81000000-0000-4000-8000-000000000002', true);
DO $qualification$
BEGIN
    IF EXISTS (
        SELECT 1 FROM dataset
         WHERE dataset_id = '82000000-0000-4000-8000-000000000001'
    ) THEN
        RAISE EXCEPTION 'cross-tenant dataset row became visible';
    END IF;
    IF EXISTS (
        SELECT 1 FROM operation_audit
         WHERE operation_id = '84000000-0000-4000-8000-000000000001'
    ) THEN
        RAISE EXCEPTION 'cross-tenant audit row became visible';
    END IF;
END
$qualification$;

ROLLBACK;
