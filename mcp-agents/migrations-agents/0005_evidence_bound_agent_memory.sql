-- Phase 7: tenant-isolated, evidence-bound five-class agent memory.

CREATE TABLE ngkg_agents.agent_memory (
    tenant_id uuid NOT NULL,
    memory_id uuid NOT NULL,
    memory_class text NOT NULL CHECK (memory_class IN ('WORKING','EPISODIC','SEMANTIC','PROCEDURAL','EVIDENCE')),
    owner_subject text NOT NULL CHECK (length(owner_subject) BETWEEN 1 AND 256),
    audience text NOT NULL CHECK (audience IN ('OWNER','TENANT')),
    state text NOT NULL CHECK (state IN ('PROPOSED','VALIDATING','VALIDATED','ENTAILED','CONTRADICTED','UNKNOWN','APPROVAL_REQUIRED','APPROVED','PUBLISHED','SUPERSEDED','REVOKED','REJECTED','EXPIRED')),
    state_version bigint NOT NULL CHECK (state_version >= 0),
    current_version bigint NOT NULL CHECK (current_version > 0),
    retention_days integer NOT NULL CHECK (retention_days BETWEEN 1 AND 3650),
    legal_hold boolean NOT NULL,
    expires_at_epoch_ms bigint,
    idempotency_sha256 bytea NOT NULL CHECK (octet_length(idempotency_sha256)=32),
    idempotency_request_sha256 bytea NOT NULL CHECK (octet_length(idempotency_request_sha256)=32),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    updated_at_epoch_ms bigint NOT NULL CHECK (updated_at_epoch_ms >= created_at_epoch_ms),
    PRIMARY KEY (tenant_id,memory_id),
    UNIQUE (tenant_id,idempotency_sha256),
    CHECK ((memory_class='WORKING' AND expires_at_epoch_ms IS NOT NULL) OR memory_class<>'WORKING')
);

CREATE TABLE ngkg_agents.agent_memory_version (
    tenant_id uuid NOT NULL,
    memory_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    content_type text NOT NULL CHECK (content_type IN ('text/plain','application/json','application/n-triples')),
    content text NOT NULL CHECK (octet_length(content) BETWEEN 1 AND 1048576),
    content_sha256 bytea NOT NULL CHECK (octet_length(content_sha256)=32),
    source_execution_id uuid,
    dataset_id uuid,
    snapshot_id uuid,
    authorized_graph_set_sha256 bytea CHECK (authorized_graph_set_sha256 IS NULL OR octet_length(authorized_graph_set_sha256)=32),
    semantic_result_sha256 bytea CHECK (semantic_result_sha256 IS NULL OR octet_length(semantic_result_sha256)=32),
    answer_certificate_sha256 bytea CHECK (answer_certificate_sha256 IS NULL OR octet_length(answer_certificate_sha256)=32),
    provenance jsonb NOT NULL CHECK (jsonb_typeof(provenance)='object'),
    created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    search_vector tsvector GENERATED ALWAYS AS (to_tsvector('simple',content)) STORED,
    PRIMARY KEY (tenant_id,memory_id,version),
    UNIQUE (tenant_id,content_sha256,memory_id),
    FOREIGN KEY (tenant_id,memory_id) REFERENCES ngkg_agents.agent_memory(tenant_id,memory_id),
    CHECK ((content_type='application/n-triples' AND dataset_id IS NOT NULL AND snapshot_id IS NOT NULL AND authorized_graph_set_sha256 IS NOT NULL AND semantic_result_sha256 IS NOT NULL AND answer_certificate_sha256 IS NOT NULL) OR content_type<>'application/n-triples')
);

CREATE INDEX agent_memory_version_search_idx ON ngkg_agents.agent_memory_version USING gin(search_vector);

CREATE TABLE ngkg_agents.agent_memory_transition (
    tenant_id uuid NOT NULL,
    transition_id uuid NOT NULL,
    memory_id uuid NOT NULL,
    state_version bigint NOT NULL CHECK (state_version > 0),
    from_state text NOT NULL,
    to_state text NOT NULL,
    actor text NOT NULL CHECK (length(actor) BETWEEN 1 AND 256),
    reason_code text NOT NULL CHECK (length(reason_code) BETWEEN 1 AND 128),
    evidence_sha256 bytea NOT NULL CHECK (octet_length(evidence_sha256)=32),
    query_execution_ids jsonb NOT NULL CHECK (jsonb_typeof(query_execution_ids)='array'),
    proof_support_ids jsonb NOT NULL CHECK (jsonb_typeof(proof_support_ids)='array'),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id,transition_id),
    UNIQUE (tenant_id,memory_id,state_version),
    FOREIGN KEY (tenant_id,memory_id) REFERENCES ngkg_agents.agent_memory(tenant_id,memory_id)
);

CREATE TABLE ngkg_agents.agent_memory_edge (
    tenant_id uuid NOT NULL,
    edge_id uuid NOT NULL,
    source_memory_id uuid NOT NULL,
    source_version bigint NOT NULL,
    target_memory_id uuid NOT NULL,
    target_version bigint NOT NULL,
    edge_type text NOT NULL CHECK (edge_type IN ('SUPERSEDES','REVOKES','DERIVED_FROM')),
    actor text NOT NULL CHECK (length(actor) BETWEEN 1 AND 256),
    evidence_sha256 bytea NOT NULL CHECK (octet_length(evidence_sha256)=32),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id,edge_id),
    UNIQUE (tenant_id,source_memory_id,source_version,target_memory_id,target_version,edge_type),
    FOREIGN KEY (tenant_id,source_memory_id,source_version) REFERENCES ngkg_agents.agent_memory_version(tenant_id,memory_id,version),
    FOREIGN KEY (tenant_id,target_memory_id,target_version) REFERENCES ngkg_agents.agent_memory_version(tenant_id,memory_id,version)
);

CREATE TABLE ngkg_agents.agent_memory_publication (
    tenant_id uuid NOT NULL,
    publication_id uuid NOT NULL,
    memory_id uuid NOT NULL,
    memory_version bigint NOT NULL,
    ngkg_operation_id uuid NOT NULL,
    published_snapshot_id uuid NOT NULL,
    validation_evidence_sha256 bytea NOT NULL CHECK (octet_length(validation_evidence_sha256)=32),
    query_execution_ids jsonb NOT NULL CHECK (jsonb_typeof(query_execution_ids)='array'),
    proof_support_ids jsonb NOT NULL CHECK (jsonb_typeof(proof_support_ids)='array'),
    published_by text NOT NULL CHECK (length(published_by) BETWEEN 1 AND 256),
    published_at_epoch_ms bigint NOT NULL CHECK (published_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id,publication_id),
    UNIQUE (tenant_id,memory_id,memory_version),
    FOREIGN KEY (tenant_id,memory_id,memory_version) REFERENCES ngkg_agents.agent_memory_version(tenant_id,memory_id,version)
);

CREATE OR REPLACE FUNCTION ngkg_agents.validate_agent_memory_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.tenant_id<>OLD.tenant_id OR NEW.memory_id<>OLD.memory_id OR NEW.memory_class<>OLD.memory_class
     OR NEW.owner_subject<>OLD.owner_subject OR NEW.audience<>OLD.audience
     OR NEW.current_version<>OLD.current_version OR NEW.retention_days<>OLD.retention_days
     OR NEW.legal_hold<>OLD.legal_hold OR NEW.expires_at_epoch_ms IS DISTINCT FROM OLD.expires_at_epoch_ms
     OR NEW.idempotency_sha256<>OLD.idempotency_sha256 OR NEW.idempotency_request_sha256<>OLD.idempotency_request_sha256
     OR NEW.created_at_epoch_ms<>OLD.created_at_epoch_ms THEN
    RAISE EXCEPTION 'agent memory identity is immutable' USING ERRCODE='55000';
  END IF;
  IF NEW.state_version<>OLD.state_version+1 OR NEW.updated_at_epoch_ms<OLD.updated_at_epoch_ms THEN
    RAISE EXCEPTION 'agent memory CAS version or timestamp is invalid' USING ERRCODE='40001';
  END IF;
  IF NOT ((OLD.state='PROPOSED' AND NEW.state IN ('VALIDATING','REJECTED','REVOKED'))
       OR (OLD.state='VALIDATING' AND NEW.state IN ('VALIDATED','ENTAILED','CONTRADICTED','UNKNOWN','REJECTED','REVOKED'))
       OR (OLD.state IN ('VALIDATED','ENTAILED') AND NEW.state IN ('APPROVAL_REQUIRED','APPROVED','SUPERSEDED','REVOKED','EXPIRED'))
       OR (OLD.state='APPROVAL_REQUIRED' AND NEW.state IN ('APPROVED','REJECTED','REVOKED'))
       OR (OLD.state='APPROVED' AND NEW.state IN ('PUBLISHED','SUPERSEDED','REVOKED','EXPIRED'))
       OR (OLD.state='PUBLISHED' AND NEW.state IN ('SUPERSEDED','REVOKED'))
       OR (OLD.state IN ('CONTRADICTED','UNKNOWN') AND NEW.state IN ('PROPOSED','REJECTED','REVOKED'))) THEN
    RAISE EXCEPTION 'illegal agent memory transition % -> %',OLD.state,NEW.state USING ERRCODE='23514';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER validate_memory_transition BEFORE UPDATE ON ngkg_agents.agent_memory
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.validate_agent_memory_transition();

CREATE OR REPLACE FUNCTION ngkg_agents.reject_agent_memory_delete()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'agent memory is retained by transition, never deleted' USING ERRCODE='55000';
END $$;

CREATE TRIGGER reject_agent_memory_delete BEFORE DELETE ON ngkg_agents.agent_memory
FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_agent_memory_delete();

DO $$ DECLARE relation_name text; BEGIN
  FOREACH relation_name IN ARRAY ARRAY['agent_memory','agent_memory_version','agent_memory_transition','agent_memory_edge','agent_memory_publication'] LOOP
    EXECUTE format('ALTER TABLE ngkg_agents.%I ENABLE ROW LEVEL SECURITY',relation_name);
    EXECUTE format('ALTER TABLE ngkg_agents.%I FORCE ROW LEVEL SECURITY',relation_name);
    EXECUTE format('CREATE POLICY tenant_isolation ON ngkg_agents.%I USING (tenant_id=ngkg_agents.current_tenant_id()) WITH CHECK (tenant_id=ngkg_agents.current_tenant_id())',relation_name);
  END LOOP;
  FOREACH relation_name IN ARRAY ARRAY['agent_memory_version','agent_memory_transition','agent_memory_edge','agent_memory_publication'] LOOP
    EXECUTE format('CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON ngkg_agents.%I FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation()',relation_name);
  END LOOP;
END $$;

REVOKE ALL ON FUNCTION ngkg_agents.validate_agent_memory_transition() FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.reject_agent_memory_delete() FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA ngkg_agents FROM PUBLIC;
