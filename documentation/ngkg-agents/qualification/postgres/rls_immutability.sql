\set ON_ERROR_STOP on
BEGIN;

DO $qualification$
DECLARE
    unqualified integer;
BEGIN
    SELECT count(*) INTO unqualified
      FROM pg_class
     WHERE oid IN (
         to_regclass('ngkg_agents.tool_provider'),
         to_regclass('ngkg_agents.agent_profile'),
         to_regclass('ngkg_agents.agent_execution'),
         to_regclass('ngkg_agents.model_call'),
         to_regclass('ngkg_agents.tool_call'),
         to_regclass('ngkg_agents.claim_validation'),
         to_regclass('ngkg_agents.execution_resource_observation'),
         to_regclass('ngkg_agents.approval'),
         to_regclass('ngkg_agents.agent_audit_chain'),
         to_regclass('ngkg_agents.audit_seal'),
         to_regclass('ngkg_agents.agent_memory'),
         to_regclass('ngkg_agents.agent_memory_version'),
         to_regclass('ngkg_agents.agent_memory_transition'),
         to_regclass('ngkg_agents.agent_memory_edge'),
         to_regclass('ngkg_agents.agent_memory_publication')
         ,to_regclass('ngkg_agents.context_slice')
         ,to_regclass('ngkg_agents.context_slice_chunk')
         ,to_regclass('ngkg_agents.context_slice_capability')
         ,to_regclass('ngkg_agents.context_slice_tombstone')
     )
       AND NOT (relrowsecurity AND relforcerowsecurity);
    IF unqualified <> 0 THEN
        RAISE EXCEPTION 'one or more tenant tables do not force RLS';
    END IF;
END
$qualification$;

SELECT set_config('ngkg.tenant_id', '10000000-0000-4000-8000-000000000001', true);

INSERT INTO ngkg_agents.tool_provider
    (tenant_id, provider_id, version, name, endpoint, auth_reference, policy,
     state, spec_sha256, created_by, created_at_epoch_ms)
VALUES
    ('10000000-0000-4000-8000-000000000001',
     '20000000-0000-4000-8000-000000000001', 1, 'qualification-provider',
     'https://tools.example.test/mcp', 'secret://qualification/provider', '{}'::jsonb,
     'PENDING', decode(repeat('11', 32), 'hex'), 'qualification', 1);

INSERT INTO ngkg_agents.agent_profile
    (tenant_id, profile_id, version, name, dataset_constraints, model_allowlist,
     tool_catalog_sha256s, limits, approval_policy, profile_sha256, created_by,
     created_at_epoch_ms)
VALUES
    ('10000000-0000-4000-8000-000000000001',
     '30000000-0000-4000-8000-000000000001', 1, 'qualification-profile',
     '{}'::jsonb, '[]'::jsonb, '[]'::jsonb, '{}'::jsonb, '{}'::jsonb,
     decode(repeat('22', 32), 'hex'), 'qualification', 1);

INSERT INTO ngkg_agents.agent_memory
    (tenant_id, memory_id, memory_class, owner_subject, audience, state,
     state_version, current_version, retention_days, legal_hold,
     idempotency_sha256, idempotency_request_sha256, created_at_epoch_ms, updated_at_epoch_ms)
VALUES
    ('10000000-0000-4000-8000-000000000001',
     '35000000-0000-4000-8000-000000000001', 'EPISODIC',
     'qualification-user', 'OWNER', 'PROPOSED', 0, 1, 30, false,
     decode(repeat('23', 32), 'hex'), decode(repeat('25', 32), 'hex'), 1, 1);
INSERT INTO ngkg_agents.agent_memory_version
    (tenant_id, memory_id, version, content_type, content, content_sha256,
     provenance, created_by, created_at_epoch_ms)
VALUES
    ('10000000-0000-4000-8000-000000000001',
     '35000000-0000-4000-8000-000000000001', 1, 'text/plain',
     'qualification memory', decode(repeat('24', 32), 'hex'), '{}'::jsonb,
     'qualification-user', 1);

INSERT INTO ngkg_agents.context_slice
    (tenant_id,slice_id,subject,dataset_id,snapshot_id,authorized_graph_set_sha256,
     semantic_result_sha256,media_type,state,state_version,chunk_size_bytes,
     expected_total_bytes,total_triples,kms_key_id_sha256,created_at_epoch_ms,
     expires_at_epoch_ms,delete_after_epoch_ms,updated_at_epoch_ms)
VALUES
    ('10000000-0000-4000-8000-000000000001','36000000-0000-4000-8000-000000000001',
     'qualification-user','50000000-0000-4000-8000-000000000001',
     '51000000-0000-4000-8000-000000000001',decode(repeat('31',32),'hex'),
     decode(repeat('32',32),'hex'),'application/n-triples','UPLOADING',1,65536,10,1,
     decode(repeat('33',32),'hex'),1,60001,120001,1);

DO $qualification$
BEGIN
    BEGIN
        UPDATE ngkg_agents.context_slice
           SET dataset_id='50000000-0000-4000-8000-000000000002',state_version=2,updated_at_epoch_ms=2
         WHERE slice_id='36000000-0000-4000-8000-000000000001';
        RAISE EXCEPTION 'context slice semantic identity was mutable';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;
END
$qualification$;

UPDATE ngkg_agents.agent_memory
   SET state='VALIDATING', state_version=1, updated_at_epoch_ms=2
 WHERE memory_id='35000000-0000-4000-8000-000000000001'
   AND state='PROPOSED' AND state_version=0;

DO $qualification$
BEGIN
    BEGIN
        DELETE FROM ngkg_agents.agent_memory
         WHERE memory_id='35000000-0000-4000-8000-000000000001';
        RAISE EXCEPTION 'agent memory delete was accepted';
    EXCEPTION WHEN object_not_in_prerequisite_state OR insufficient_privilege THEN
        NULL;
    END;
END
$qualification$;

DO $qualification$
BEGIN
    BEGIN
        UPDATE ngkg_agents.tool_provider SET name='mutated'
         WHERE provider_id='20000000-0000-4000-8000-000000000001';
        RAISE EXCEPTION 'immutable provider update was accepted';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
END
$qualification$;

SELECT set_config('ngkg.tenant_id', '10000000-0000-4000-8000-000000000002', true);
DO $qualification$
BEGIN
    IF EXISTS (
        SELECT 1 FROM ngkg_agents.tool_provider
         WHERE provider_id='20000000-0000-4000-8000-000000000001'
    ) THEN
        RAISE EXCEPTION 'cross-tenant provider row became visible';
    END IF;
    IF EXISTS (
        SELECT 1 FROM ngkg_agents.agent_memory
         WHERE memory_id='35000000-0000-4000-8000-000000000001'
    ) THEN
        RAISE EXCEPTION 'cross-tenant memory row became visible';
    END IF;
    IF EXISTS (
        SELECT 1 FROM ngkg_agents.context_slice
         WHERE slice_id='36000000-0000-4000-8000-000000000001'
    ) THEN
        RAISE EXCEPTION 'cross-tenant context slice became visible';
    END IF;
END
$qualification$;

SELECT set_config('ngkg.tenant_id', '10000000-0000-4000-8000-000000000001', true);
INSERT INTO ngkg_agents.agent_execution
    (tenant_id, execution_id, subject, dataset_id, profile_id, profile_version,
     model_provider, model_id, state, state_version, started_at_epoch_ms)
VALUES
    ('10000000-0000-4000-8000-000000000001',
     '40000000-0000-4000-8000-000000000001', 'qualification-user',
     '50000000-0000-4000-8000-000000000001',
     '30000000-0000-4000-8000-000000000001', 1,
     'qualification', 'qualification-model', 'ADMITTED', 0, 1);

UPDATE ngkg_agents.agent_execution
   SET state='RUNNING', state_version=1
 WHERE execution_id='40000000-0000-4000-8000-000000000001'
   AND state='ADMITTED' AND state_version=0;

DO $qualification$
BEGIN
    BEGIN
        UPDATE ngkg_agents.agent_execution
           SET state='COMPLETED', state_version=2, ended_at_epoch_ms=2,
               result_sha256=decode(repeat('33', 32), 'hex')
         WHERE execution_id='40000000-0000-4000-8000-000000000001';
        RAISE EXCEPTION 'illegal execution transition was accepted';
    EXCEPTION WHEN check_violation THEN
        NULL;
    END;
END
$qualification$;

INSERT INTO ngkg_agents.model_call
    (tenant_id, model_call_id, execution_id, ordinal, provider, model_id,
     request_sha256, started_at_epoch_ms)
VALUES
    ('10000000-0000-4000-8000-000000000001',
     '60000000-0000-4000-8000-000000000001',
     '40000000-0000-4000-8000-000000000001', 0, 'qualification',
     'qualification-model', decode(repeat('44', 32), 'hex'), 1);
UPDATE ngkg_agents.model_call
   SET response_sha256=decode(repeat('55', 32), 'hex'), ended_at_epoch_ms=2,
       outcome='COMPLETED'
 WHERE model_call_id='60000000-0000-4000-8000-000000000001';

DO $qualification$
BEGIN
    BEGIN
        UPDATE ngkg_agents.model_call SET output_tokens=1
         WHERE model_call_id='60000000-0000-4000-8000-000000000001';
        RAISE EXCEPTION 'finalized model call was mutable';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        NULL;
    END;
END
$qualification$;

INSERT INTO ngkg_agents.agent_audit_chain
    (tenant_id, sequence, event_id, event_type, subject, request_id, outcome,
     policy_version_sha256, service_build_sha256, redacted_payload_sha256,
     previous_event_sha256, event_sha256, event_time_epoch_ms)
VALUES
    ('10000000-0000-4000-8000-000000000001', 0,
     '70000000-0000-4000-8000-000000000001', 'QUALIFICATION',
     'qualification-user', 'qualification-request', 'COMPLETED',
     decode(repeat('66', 32), 'hex'), decode(repeat('77', 32), 'hex'),
     decode(repeat('88', 32), 'hex'), decode(repeat('00', 32), 'hex'),
     decode(repeat('99', 32), 'hex'), 1);

DO $qualification$
BEGIN
    BEGIN
        INSERT INTO ngkg_agents.agent_audit_chain
            (tenant_id, sequence, event_id, event_type, subject, request_id, outcome,
             policy_version_sha256, service_build_sha256, redacted_payload_sha256,
             previous_event_sha256, event_sha256, event_time_epoch_ms)
        VALUES
            ('10000000-0000-4000-8000-000000000001', 2,
             '70000000-0000-4000-8000-000000000002', 'QUALIFICATION',
             'qualification-user', 'qualification-request-2', 'COMPLETED',
             decode(repeat('66', 32), 'hex'), decode(repeat('77', 32), 'hex'),
             decode(repeat('88', 32), 'hex'), decode(repeat('99', 32), 'hex'),
             decode(repeat('aa', 32), 'hex'), 2);
        RAISE EXCEPTION 'invalid audit chain sequence was accepted';
    EXCEPTION WHEN serialization_failure THEN
        NULL;
    END;
END
$qualification$;

ROLLBACK;
