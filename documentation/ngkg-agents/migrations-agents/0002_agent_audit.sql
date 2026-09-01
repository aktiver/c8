CREATE TABLE ngkg_agents.agent_execution (
    tenant_id uuid NOT NULL,
    execution_id uuid NOT NULL,
    subject text NOT NULL CHECK (length(subject) BETWEEN 1 AND 256),
    actor text CHECK (actor IS NULL OR length(actor) BETWEEN 1 AND 256),
    dataset_id uuid NOT NULL,
    snapshot_id uuid,
    authorized_graph_set_sha256 bytea CHECK (authorized_graph_set_sha256 IS NULL OR octet_length(authorized_graph_set_sha256) = 32),
    active_dataset_sha256 bytea CHECK (active_dataset_sha256 IS NULL OR octet_length(active_dataset_sha256) = 32),
    serving_root_sha256 bytea CHECK (serving_root_sha256 IS NULL OR octet_length(serving_root_sha256) = 32),
    profile_id uuid NOT NULL,
    profile_version bigint NOT NULL CHECK (profile_version > 0),
    model_provider text NOT NULL CHECK (length(model_provider) BETWEEN 1 AND 128),
    model_id text NOT NULL CHECK (length(model_id) BETWEEN 1 AND 512),
    state text NOT NULL CHECK (state IN ('ADMITTED', 'RUNNING', 'WAITING_APPROVAL', 'VALIDATING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    state_version bigint NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    started_at_epoch_ms bigint NOT NULL CHECK (started_at_epoch_ms >= 0),
    ended_at_epoch_ms bigint,
    result_sha256 bytea CHECK (result_sha256 IS NULL OR octet_length(result_sha256) = 32),
    failure_code text CHECK (failure_code IS NULL OR length(failure_code) BETWEEN 1 AND 128),
    PRIMARY KEY (tenant_id, execution_id),
    FOREIGN KEY (tenant_id, profile_id, profile_version)
        REFERENCES ngkg_agents.agent_profile (tenant_id, profile_id, version),
    CHECK ((ended_at_epoch_ms IS NULL AND result_sha256 IS NULL AND failure_code IS NULL)
        OR (ended_at_epoch_ms IS NOT NULL AND ended_at_epoch_ms >= started_at_epoch_ms))
);

CREATE TABLE ngkg_agents.model_call (
    tenant_id uuid NOT NULL,
    model_call_id uuid NOT NULL,
    execution_id uuid NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    provider text NOT NULL CHECK (length(provider) BETWEEN 1 AND 128),
    model_id text NOT NULL CHECK (length(model_id) BETWEEN 1 AND 512),
    request_sha256 bytea NOT NULL CHECK (octet_length(request_sha256) = 32),
    response_sha256 bytea CHECK (response_sha256 IS NULL OR octet_length(response_sha256) = 32),
    input_tokens bigint CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens bigint CHECK (output_tokens IS NULL OR output_tokens >= 0),
    started_at_epoch_ms bigint NOT NULL CHECK (started_at_epoch_ms >= 0),
    ended_at_epoch_ms bigint,
    outcome text CHECK (outcome IS NULL OR outcome IN ('COMPLETED', 'FAILED', 'CANCELLED')),
    error_code text CHECK (error_code IS NULL OR length(error_code) BETWEEN 1 AND 128),
    PRIMARY KEY (tenant_id, model_call_id),
    UNIQUE (tenant_id, execution_id, ordinal),
    FOREIGN KEY (tenant_id, execution_id)
        REFERENCES ngkg_agents.agent_execution (tenant_id, execution_id)
);

CREATE TABLE ngkg_agents.tool_call (
    tenant_id uuid NOT NULL,
    tool_call_id uuid NOT NULL,
    execution_id uuid NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    provider_id uuid,
    tool_name text NOT NULL CHECK (length(tool_name) BETWEEN 1 AND 512),
    catalog_sha256 bytea CHECK (catalog_sha256 IS NULL OR octet_length(catalog_sha256) = 32),
    arguments_sha256 bytea NOT NULL CHECK (octet_length(arguments_sha256) = 32),
    result_sha256 bytea CHECK (result_sha256 IS NULL OR octet_length(result_sha256) = 32),
    query_execution_id uuid,
    approval_id uuid,
    started_at_epoch_ms bigint NOT NULL CHECK (started_at_epoch_ms >= 0),
    ended_at_epoch_ms bigint,
    outcome text CHECK (outcome IS NULL OR outcome IN ('COMPLETED', 'FAILED', 'CANCELLED', 'DENIED')),
    error_code text CHECK (error_code IS NULL OR length(error_code) BETWEEN 1 AND 128),
    PRIMARY KEY (tenant_id, tool_call_id),
    UNIQUE (tenant_id, execution_id, ordinal),
    FOREIGN KEY (tenant_id, execution_id)
        REFERENCES ngkg_agents.agent_execution (tenant_id, execution_id)
);

CREATE TABLE ngkg_agents.claim_validation (
    tenant_id uuid NOT NULL,
    validation_id uuid NOT NULL,
    execution_id uuid NOT NULL,
    claim_sha256 bytea NOT NULL CHECK (octet_length(claim_sha256) = 32),
    verdict text NOT NULL CHECK (verdict IN ('ENTAILED', 'CONTRADICTED', 'UNKNOWN', 'INVALID')),
    query_execution_id uuid,
    proof_support_ids jsonb NOT NULL,
    reason_code text NOT NULL CHECK (length(reason_code) BETWEEN 1 AND 128),
    evidence_sha256 bytea NOT NULL CHECK (octet_length(evidence_sha256) = 32),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id, validation_id),
    FOREIGN KEY (tenant_id, execution_id)
        REFERENCES ngkg_agents.agent_execution (tenant_id, execution_id)
);

CREATE TABLE ngkg_agents.execution_resource_observation (
    tenant_id uuid NOT NULL,
    observation_id uuid NOT NULL,
    execution_id uuid NOT NULL,
    resource_semantics text NOT NULL CHECK (resource_semantics IN ('CONFIGURED_ALLOCATION', 'OBSERVED_USAGE')),
    source text NOT NULL CHECK (length(source) BETWEEN 1 AND 128),
    participating_pods integer NOT NULL CHECK (participating_pods >= 0),
    distinct_physical_nodes integer CHECK (distinct_physical_nodes IS NULL OR distinct_physical_nodes >= 0),
    cpu_millicores bigint NOT NULL CHECK (cpu_millicores >= 0),
    memory_bytes bigint NOT NULL CHECK (memory_bytes >= 0),
    interval_start_epoch_ms bigint NOT NULL CHECK (interval_start_epoch_ms >= 0),
    interval_end_epoch_ms bigint NOT NULL CHECK (interval_end_epoch_ms >= interval_start_epoch_ms),
    evidence_sha256 bytea NOT NULL CHECK (octet_length(evidence_sha256) = 32),
    PRIMARY KEY (tenant_id, observation_id),
    FOREIGN KEY (tenant_id, execution_id)
        REFERENCES ngkg_agents.agent_execution (tenant_id, execution_id),
    CHECK (resource_semantics = 'OBSERVED_USAGE' OR distinct_physical_nodes IS NULL)
);

CREATE TABLE ngkg_agents.approval (
    tenant_id uuid NOT NULL,
    approval_id uuid NOT NULL,
    execution_id uuid NOT NULL,
    tool_name text NOT NULL CHECK (length(tool_name) BETWEEN 1 AND 512),
    approver text NOT NULL CHECK (length(approver) BETWEEN 1 AND 256),
    policy_sha256 bytea NOT NULL CHECK (octet_length(policy_sha256) = 32),
    catalog_sha256 bytea CHECK (catalog_sha256 IS NULL OR octet_length(catalog_sha256) = 32),
    decision text NOT NULL CHECK (decision IN ('APPROVED', 'DENIED')),
    expires_at_epoch_ms bigint NOT NULL CHECK (expires_at_epoch_ms >= 0),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id, approval_id),
    FOREIGN KEY (tenant_id, execution_id)
        REFERENCES ngkg_agents.agent_execution (tenant_id, execution_id),
    CHECK (expires_at_epoch_ms >= created_at_epoch_ms)
);

ALTER TABLE ngkg_agents.tool_call
    ADD CONSTRAINT tool_call_approval_fk
    FOREIGN KEY (tenant_id, approval_id)
    REFERENCES ngkg_agents.approval (tenant_id, approval_id)
    DEFERRABLE INITIALLY IMMEDIATE;

CREATE TABLE ngkg_agents.agent_audit_chain (
    tenant_id uuid NOT NULL,
    sequence bigint NOT NULL CHECK (sequence >= 0),
    event_id uuid NOT NULL,
    event_type text NOT NULL CHECK (event_type ~ '^[A-Z][A-Z0-9_]{0,127}$'),
    subject text NOT NULL CHECK (length(subject) BETWEEN 1 AND 256),
    actor text CHECK (actor IS NULL OR length(actor) BETWEEN 1 AND 256),
    request_id text NOT NULL CHECK (length(request_id) BETWEEN 1 AND 128),
    outcome text NOT NULL CHECK (outcome IN ('STARTED', 'COMPLETED', 'FAILED', 'DENIED', 'CANCELLED')),
    policy_version_sha256 bytea NOT NULL CHECK (octet_length(policy_version_sha256) = 32),
    service_build_sha256 bytea NOT NULL CHECK (octet_length(service_build_sha256) = 32),
    redacted_payload_sha256 bytea NOT NULL CHECK (octet_length(redacted_payload_sha256) = 32),
    previous_event_sha256 bytea NOT NULL CHECK (octet_length(previous_event_sha256) = 32),
    event_sha256 bytea NOT NULL CHECK (octet_length(event_sha256) = 32),
    event_time_epoch_ms bigint NOT NULL CHECK (event_time_epoch_ms >= 0),
    PRIMARY KEY (tenant_id, sequence),
    UNIQUE (tenant_id, event_id),
    UNIQUE (tenant_id, event_sha256)
);

CREATE TABLE ngkg_agents.audit_seal (
    tenant_id uuid NOT NULL,
    seal_id uuid NOT NULL,
    through_sequence bigint NOT NULL CHECK (through_sequence >= 0),
    chain_head_sha256 bytea NOT NULL CHECK (octet_length(chain_head_sha256) = 32),
    external_target text NOT NULL CHECK (length(external_target) BETWEEN 1 AND 1024),
    external_receipt_sha256 bytea NOT NULL CHECK (octet_length(external_receipt_sha256) = 32),
    sealed_by text NOT NULL CHECK (length(sealed_by) BETWEEN 1 AND 256),
    sealed_at_epoch_ms bigint NOT NULL CHECK (sealed_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id, seal_id),
    UNIQUE (tenant_id, through_sequence)
);

CREATE OR REPLACE FUNCTION ngkg_agents.enforce_execution_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(NEW.tenant_id, NEW.execution_id, NEW.subject, NEW.actor, NEW.dataset_id,
           NEW.profile_id, NEW.profile_version, NEW.model_provider, NEW.model_id,
           NEW.started_at_epoch_ms)
       IS DISTINCT FROM
       ROW(OLD.tenant_id, OLD.execution_id, OLD.subject, OLD.actor, OLD.dataset_id,
           OLD.profile_id, OLD.profile_version, OLD.model_provider, OLD.model_id,
           OLD.started_at_epoch_ms) THEN
        RAISE EXCEPTION 'agent execution identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF OLD.state IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN
        RAISE EXCEPTION 'terminal agent execution is immutable' USING ERRCODE = '55000';
    END IF;
    IF NEW.state_version <> OLD.state_version + 1 THEN
        RAISE EXCEPTION 'agent execution state version must increment by one' USING ERRCODE = '40001';
    END IF;
    IF NOT CASE OLD.state
        WHEN 'ADMITTED' THEN NEW.state IN ('RUNNING', 'FAILED', 'CANCELLED')
        WHEN 'RUNNING' THEN NEW.state IN ('WAITING_APPROVAL', 'VALIDATING', 'FAILED', 'CANCELLED')
        WHEN 'WAITING_APPROVAL' THEN NEW.state IN ('RUNNING', 'FAILED', 'CANCELLED')
        WHEN 'VALIDATING' THEN NEW.state IN ('COMPLETED', 'FAILED', 'CANCELLED')
        ELSE false
    END THEN
        RAISE EXCEPTION 'illegal agent execution state transition % -> %', OLD.state, NEW.state
            USING ERRCODE = '23514';
    END IF;
    IF NEW.state IN ('COMPLETED', 'FAILED', 'CANCELLED') AND NEW.ended_at_epoch_ms IS NULL THEN
        RAISE EXCEPTION 'terminal agent execution requires end time' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION ngkg_agents.enforce_model_call_finalize_once()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.ended_at_epoch_ms IS NOT NULL OR OLD.outcome IS NOT NULL THEN
        RAISE EXCEPTION 'terminal call record is immutable' USING ERRCODE = '55000';
    END IF;
    IF ROW(NEW.tenant_id, NEW.model_call_id, NEW.execution_id, NEW.ordinal, NEW.provider,
           NEW.model_id, NEW.request_sha256, NEW.started_at_epoch_ms)
       IS DISTINCT FROM
       ROW(OLD.tenant_id, OLD.model_call_id, OLD.execution_id, OLD.ordinal, OLD.provider,
           OLD.model_id, OLD.request_sha256, OLD.started_at_epoch_ms) THEN
        RAISE EXCEPTION 'call identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF NEW.ended_at_epoch_ms IS NULL OR NEW.outcome IS NULL OR NEW.ended_at_epoch_ms < OLD.started_at_epoch_ms THEN
        RAISE EXCEPTION 'call finalization is incomplete' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION ngkg_agents.enforce_tool_call_finalize_once()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.ended_at_epoch_ms IS NOT NULL OR OLD.outcome IS NOT NULL THEN
        RAISE EXCEPTION 'terminal call record is immutable' USING ERRCODE = '55000';
    END IF;
    IF ROW(NEW.tenant_id, NEW.tool_call_id, NEW.execution_id, NEW.ordinal, NEW.provider_id,
           NEW.tool_name, NEW.catalog_sha256, NEW.arguments_sha256, NEW.approval_id,
           NEW.started_at_epoch_ms)
       IS DISTINCT FROM
       ROW(OLD.tenant_id, OLD.tool_call_id, OLD.execution_id, OLD.ordinal, OLD.provider_id,
           OLD.tool_name, OLD.catalog_sha256, OLD.arguments_sha256, OLD.approval_id,
           OLD.started_at_epoch_ms) THEN
        RAISE EXCEPTION 'call identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF NEW.ended_at_epoch_ms IS NULL OR NEW.outcome IS NULL OR NEW.ended_at_epoch_ms < OLD.started_at_epoch_ms THEN
        RAISE EXCEPTION 'call finalization is incomplete' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION ngkg_agents.enforce_audit_chain()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    prior_sequence bigint;
    prior_hash bytea;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(NEW.tenant_id::text, 0));
    SELECT sequence, event_sha256 INTO prior_sequence, prior_hash
      FROM ngkg_agents.agent_audit_chain
     WHERE tenant_id = NEW.tenant_id
     ORDER BY sequence DESC
     LIMIT 1;
    IF prior_sequence IS NULL THEN
        IF NEW.sequence <> 0 OR NEW.previous_event_sha256 <> decode(repeat('00', 32), 'hex') THEN
            RAISE EXCEPTION 'invalid audit chain genesis' USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.sequence <> prior_sequence + 1 OR NEW.previous_event_sha256 <> prior_hash THEN
        RAISE EXCEPTION 'invalid audit chain linkage' USING ERRCODE = '40001';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER execution_transition BEFORE UPDATE ON ngkg_agents.agent_execution
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.enforce_execution_transition();
CREATE TRIGGER execution_no_delete BEFORE DELETE ON ngkg_agents.agent_execution
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation();
CREATE TRIGGER model_call_finalize BEFORE UPDATE ON ngkg_agents.model_call
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.enforce_model_call_finalize_once();
CREATE TRIGGER model_call_no_delete BEFORE DELETE ON ngkg_agents.model_call
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation();
CREATE TRIGGER tool_call_finalize BEFORE UPDATE ON ngkg_agents.tool_call
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.enforce_tool_call_finalize_once();
CREATE TRIGGER tool_call_no_delete BEFORE DELETE ON ngkg_agents.tool_call
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation();
CREATE TRIGGER audit_chain_link BEFORE INSERT ON ngkg_agents.agent_audit_chain
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.enforce_audit_chain();

DO $$
DECLARE
    relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY['agent_execution', 'model_call', 'tool_call', 'claim_validation', 'execution_resource_observation', 'approval', 'agent_audit_chain', 'audit_seal']
    LOOP
        EXECUTE format('ALTER TABLE ngkg_agents.%I ENABLE ROW LEVEL SECURITY', relation_name);
        EXECUTE format('ALTER TABLE ngkg_agents.%I FORCE ROW LEVEL SECURITY', relation_name);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON ngkg_agents.%I USING (tenant_id = ngkg_agents.current_tenant_id()) WITH CHECK (tenant_id = ngkg_agents.current_tenant_id())',
            relation_name
        );
    END LOOP;
END
$$;

CREATE TRIGGER claim_validation_immutable BEFORE UPDATE OR DELETE ON ngkg_agents.claim_validation
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation();
CREATE TRIGGER resource_observation_immutable BEFORE UPDATE OR DELETE ON ngkg_agents.execution_resource_observation
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation();
CREATE TRIGGER approval_immutable BEFORE UPDATE OR DELETE ON ngkg_agents.approval
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation();
CREATE TRIGGER audit_chain_immutable BEFORE UPDATE OR DELETE ON ngkg_agents.agent_audit_chain
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation();
CREATE TRIGGER audit_seal_immutable BEFORE UPDATE OR DELETE ON ngkg_agents.audit_seal
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation();

CREATE INDEX agent_execution_state_idx ON ngkg_agents.agent_execution (tenant_id, state, started_at_epoch_ms);
CREATE INDEX model_call_execution_idx ON ngkg_agents.model_call (tenant_id, execution_id, ordinal);
CREATE INDEX tool_call_execution_idx ON ngkg_agents.tool_call (tenant_id, execution_id, ordinal);
CREATE INDEX claim_validation_execution_idx ON ngkg_agents.claim_validation (tenant_id, execution_id);
CREATE INDEX resource_observation_execution_idx ON ngkg_agents.execution_resource_observation (tenant_id, execution_id, interval_start_epoch_ms);
CREATE INDEX audit_chain_time_idx ON ngkg_agents.agent_audit_chain (tenant_id, event_time_epoch_ms);

REVOKE ALL ON ALL TABLES IN SCHEMA ngkg_agents FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.enforce_execution_transition() FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.enforce_model_call_finalize_once() FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.enforce_tool_call_finalize_once() FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.enforce_audit_chain() FROM PUBLIC;
