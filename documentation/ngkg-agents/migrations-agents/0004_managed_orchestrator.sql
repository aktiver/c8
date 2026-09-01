-- Phase 5: managed model execution and reasoning-bound answer certificates.

CREATE TABLE ngkg_agents.agent_execution_input (
    tenant_id uuid NOT NULL,input_id uuid NOT NULL,execution_id uuid NOT NULL,
    source_root_sha256 bytea NOT NULL CHECK(octet_length(source_root_sha256)=32),
    compiled_root_sha256 bytea NOT NULL CHECK(octet_length(compiled_root_sha256)=32),
    requirement_root_sha256 bytea NOT NULL CHECK(octet_length(requirement_root_sha256)=32),
    context_query_sha256 bytea NOT NULL CHECK(octet_length(context_query_sha256)=32),
    created_at_epoch_ms bigint NOT NULL CHECK(created_at_epoch_ms>=0),
    PRIMARY KEY(tenant_id,execution_id),
    FOREIGN KEY(tenant_id,input_id) REFERENCES ngkg_agents.prompt_input(tenant_id,input_id),
    FOREIGN KEY(tenant_id,execution_id) REFERENCES ngkg_agents.agent_execution(tenant_id,execution_id)
);

CREATE TABLE ngkg_agents.agent_answer_certificate (
    tenant_id uuid NOT NULL,certificate_id uuid NOT NULL,execution_id uuid NOT NULL,
    dataset_id uuid NOT NULL,snapshot_id uuid NOT NULL,query_execution_id uuid NOT NULL,
    authorized_graph_set_sha256 bytea NOT NULL CHECK(octet_length(authorized_graph_set_sha256)=32),
    active_dataset_sha256 bytea NOT NULL CHECK(octet_length(active_dataset_sha256)=32),
    serving_root_sha256 bytea NOT NULL CHECK(octet_length(serving_root_sha256)=32),
    semantic_result_sha256 bytea NOT NULL CHECK(octet_length(semantic_result_sha256)=32),
    source_root_sha256 bytea NOT NULL CHECK(octet_length(source_root_sha256)=32),
    compiled_root_sha256 bytea NOT NULL CHECK(octet_length(compiled_root_sha256)=32),
    requirement_root_sha256 bytea NOT NULL CHECK(octet_length(requirement_root_sha256)=32),
    model_request_sha256 bytea NOT NULL CHECK(octet_length(model_request_sha256)=32),
    model_response_sha256 bytea NOT NULL CHECK(octet_length(model_response_sha256)=32),
    answer_sha256 bytea NOT NULL CHECK(octet_length(answer_sha256)=32),
    certificate_sha256 bytea NOT NULL CHECK(octet_length(certificate_sha256)=32),
    claim_validation_ids jsonb NOT NULL CHECK(jsonb_typeof(claim_validation_ids)='array'),
    proof_support_ids jsonb NOT NULL CHECK(jsonb_typeof(proof_support_ids)='array'),
    certificate jsonb NOT NULL CHECK(jsonb_typeof(certificate)='object'),
    issued_at_epoch_ms bigint NOT NULL CHECK(issued_at_epoch_ms>=0),
    PRIMARY KEY(tenant_id,certificate_id),UNIQUE(tenant_id,execution_id),UNIQUE(tenant_id,certificate_sha256),
    FOREIGN KEY(tenant_id,execution_id) REFERENCES ngkg_agents.agent_execution(tenant_id,execution_id)
);

DO $$ DECLARE relation_name text; BEGIN
  FOREACH relation_name IN ARRAY ARRAY['agent_execution_input','agent_answer_certificate'] LOOP
    EXECUTE format('ALTER TABLE ngkg_agents.%I ENABLE ROW LEVEL SECURITY',relation_name);
    EXECUTE format('ALTER TABLE ngkg_agents.%I FORCE ROW LEVEL SECURITY',relation_name);
    EXECUTE format('CREATE POLICY tenant_isolation ON ngkg_agents.%I USING (tenant_id=ngkg_agents.current_tenant_id()) WITH CHECK (tenant_id=ngkg_agents.current_tenant_id())',relation_name);
    EXECUTE format('CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON ngkg_agents.%I FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation()',relation_name);
  END LOOP;
END $$;

REVOKE ALL ON ALL TABLES IN SCHEMA ngkg_agents FROM PUBLIC;
