-- Phase 4: resumable long-input ingestion and deterministic context compilation.
-- Raw bytes are never stored in PostgreSQL; object_reference is an opaque,
-- content-addressed key owned by the input storage adapter.

CREATE TABLE ngkg_agents.prompt_input (
    tenant_id uuid NOT NULL,
    input_id uuid NOT NULL,
    subject text NOT NULL CHECK (length(subject) BETWEEN 1 AND 256),
    actor text CHECK (actor IS NULL OR length(actor) BETWEEN 1 AND 256),
    source_name text NOT NULL CHECK (length(source_name) BETWEEN 1 AND 1024),
    media_type text NOT NULL CHECK (length(media_type) BETWEEN 1 AND 256),
    state text NOT NULL CHECK (state IN ('UPLOADING','COMPILING','COMPILED','FAILED','CANCELLED')),
    state_version bigint NOT NULL CHECK (state_version >= 0),
    maximum_parts integer NOT NULL CHECK (maximum_parts BETWEEN 1 AND 100000),
    maximum_bytes bigint NOT NULL CHECK (maximum_bytes BETWEEN 1 AND 1099511627776),
    expected_parts integer CHECK (expected_parts BETWEEN 1 AND 100000),
    total_bytes bigint CHECK (total_bytes BETWEEN 1 AND 1099511627776),
    source_root_sha256 bytea CHECK (source_root_sha256 IS NULL OR octet_length(source_root_sha256)=32),
    compiled_root_sha256 bytea CHECK (compiled_root_sha256 IS NULL OR octet_length(compiled_root_sha256)=32),
    requirement_root_sha256 bytea CHECK (requirement_root_sha256 IS NULL OR octet_length(requirement_root_sha256)=32),
    failure_code text CHECK (failure_code IS NULL OR length(failure_code) BETWEEN 1 AND 128),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    finalized_at_epoch_ms bigint,
    completed_at_epoch_ms bigint,
    PRIMARY KEY (tenant_id,input_id),
    CHECK ((state='UPLOADING' AND expected_parts IS NULL AND total_bytes IS NULL AND source_root_sha256 IS NULL AND finalized_at_epoch_ms IS NULL)
        OR (state<>'UPLOADING' AND expected_parts IS NOT NULL AND total_bytes IS NOT NULL AND source_root_sha256 IS NOT NULL AND finalized_at_epoch_ms IS NOT NULL)),
    CHECK ((state='COMPILED' AND compiled_root_sha256 IS NOT NULL AND requirement_root_sha256 IS NOT NULL AND completed_at_epoch_ms IS NOT NULL)
        OR state<>'COMPILED')
);

CREATE TABLE ngkg_agents.prompt_part (
    tenant_id uuid NOT NULL,
    input_id uuid NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    byte_length bigint NOT NULL CHECK (byte_length > 0),
    media_type text NOT NULL CHECK (length(media_type) BETWEEN 1 AND 256),
    source_sha256 bytea NOT NULL CHECK (octet_length(source_sha256)=32),
    object_reference text NOT NULL CHECK (length(object_reference) BETWEEN 1 AND 2048 AND object_reference !~ '[[:cntrl:]]'),
    PRIMARY KEY (tenant_id,input_id,ordinal),
    UNIQUE (tenant_id,input_id,object_reference),
    FOREIGN KEY (tenant_id,input_id) REFERENCES ngkg_agents.prompt_input (tenant_id,input_id)
);

CREATE TABLE ngkg_agents.prompt_compilation_shard (
    tenant_id uuid NOT NULL,
    input_id uuid NOT NULL,
    part_ordinal integer NOT NULL,
    state text NOT NULL CHECK (state IN ('READY','LEASED','COMPLETED','FAILED')),
    attempt integer NOT NULL CHECK (attempt BETWEEN 0 AND 100),
    lease_owner text CHECK (lease_owner IS NULL OR length(lease_owner) BETWEEN 1 AND 256),
    lease_token uuid,
    lease_expires_at_epoch_ms bigint,
    compiled_sha256 bytea CHECK (compiled_sha256 IS NULL OR octet_length(compiled_sha256)=32),
    completed_at_epoch_ms bigint,
    failure_code text CHECK (failure_code IS NULL OR length(failure_code) BETWEEN 1 AND 128),
    PRIMARY KEY (tenant_id,input_id,part_ordinal),
    FOREIGN KEY (tenant_id,input_id,part_ordinal) REFERENCES ngkg_agents.prompt_part (tenant_id,input_id,ordinal),
    CHECK ((state='LEASED') = (lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at_epoch_ms IS NOT NULL)),
    CHECK ((state='COMPLETED') = (compiled_sha256 IS NOT NULL AND completed_at_epoch_ms IS NOT NULL))
);
CREATE INDEX prompt_compilation_ready_idx ON ngkg_agents.prompt_compilation_shard (state,lease_expires_at_epoch_ms,tenant_id,input_id,part_ordinal);

-- Opaque dispatcher queue. It contains no prompt content and receives no
-- runtime table privileges; workers can access it only through the narrow
-- functions below. This avoids requiring a BYPASSRLS worker role.
CREATE TABLE ngkg_agents.prompt_compilation_queue (
    tenant_id uuid NOT NULL,
    input_id uuid NOT NULL,
    part_ordinal integer NOT NULL,
    state text NOT NULL CHECK (state IN ('READY','LEASED','COMPLETED','FAILED')),
    lease_owner text,
    lease_token uuid,
    lease_expires_at_epoch_ms bigint,
    PRIMARY KEY (tenant_id,input_id,part_ordinal)
);
CREATE INDEX prompt_queue_ready_idx ON ngkg_agents.prompt_compilation_queue (state,lease_expires_at_epoch_ms,tenant_id,input_id,part_ordinal);

CREATE TABLE ngkg_agents.prompt_chunk (
    tenant_id uuid NOT NULL,
    input_id uuid NOT NULL,
    part_ordinal integer NOT NULL,
    chunk_id text NOT NULL CHECK (chunk_id ~ '^CHK-[0-9a-f]{64}$'),
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    byte_start bigint NOT NULL CHECK (byte_start >= 0),
    byte_end bigint NOT NULL CHECK (byte_end > byte_start),
    heading_path jsonb NOT NULL CHECK (jsonb_typeof(heading_path)='array'),
    text_sha256 bytea NOT NULL CHECK (octet_length(text_sha256)=32),
    PRIMARY KEY (tenant_id,input_id,chunk_id),
    UNIQUE (tenant_id,input_id,part_ordinal,ordinal),
    FOREIGN KEY (tenant_id,input_id,part_ordinal) REFERENCES ngkg_agents.prompt_part (tenant_id,input_id,ordinal)
);

CREATE TABLE ngkg_agents.prompt_requirement (
    tenant_id uuid NOT NULL,
    input_id uuid NOT NULL,
    requirement_id text NOT NULL CHECK (requirement_id ~ '^REQ-[0-9a-f]{64}$'),
    part_ordinal integer NOT NULL,
    source_chunk_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('INSTRUCTION','PROHIBITION','ACCEPTANCECRITERION','REQUIREDOUTPUT','IDENTIFIER')),
    mandatory boolean NOT NULL,
    byte_start bigint NOT NULL CHECK (byte_start >= 0),
    byte_end bigint NOT NULL CHECK (byte_end > byte_start),
    normalized_text text NOT NULL CHECK (length(normalized_text)>0),
    text_sha256 bytea NOT NULL CHECK (octet_length(text_sha256)=32),
    PRIMARY KEY (tenant_id,input_id,requirement_id),
    FOREIGN KEY (tenant_id,input_id,source_chunk_id) REFERENCES ngkg_agents.prompt_chunk (tenant_id,input_id,chunk_id)
);

CREATE TABLE ngkg_agents.compiled_context (
    tenant_id uuid NOT NULL,
    context_id uuid NOT NULL,
    input_id uuid NOT NULL,
    budget_bytes bigint NOT NULL CHECK (budget_bytes > 0),
    selected_chunk_ids jsonb NOT NULL CHECK (jsonb_typeof(selected_chunk_ids)='array'),
    requirement_ids jsonb NOT NULL CHECK (jsonb_typeof(requirement_ids)='array'),
    context_sha256 bytea NOT NULL CHECK (octet_length(context_sha256)=32),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id,context_id),
    UNIQUE (tenant_id,input_id,budget_bytes,context_sha256),
    FOREIGN KEY (tenant_id,input_id) REFERENCES ngkg_agents.prompt_input (tenant_id,input_id)
);

CREATE TABLE ngkg_agents.prompt_requirement_coverage (
    tenant_id uuid NOT NULL,
    input_id uuid NOT NULL,
    execution_id uuid NOT NULL,
    requirement_id text NOT NULL,
    status text NOT NULL CHECK (status IN ('SATISFIED','PARTIAL','UNSATISFIED','NOT_APPLICABLE')),
    evidence_refs jsonb NOT NULL CHECK (jsonb_typeof(evidence_refs)='array'),
    evidence_sha256 bytea NOT NULL CHECK (octet_length(evidence_sha256)=32),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id,input_id,execution_id,requirement_id),
    FOREIGN KEY (tenant_id,input_id,requirement_id) REFERENCES ngkg_agents.prompt_requirement (tenant_id,input_id,requirement_id),
    FOREIGN KEY (tenant_id,execution_id) REFERENCES ngkg_agents.agent_execution (tenant_id,execution_id)
);

CREATE FUNCTION ngkg_agents.enforce_prompt_input_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.tenant_id,NEW.input_id,NEW.subject,NEW.actor,NEW.source_name,NEW.media_type,NEW.maximum_parts,NEW.maximum_bytes,NEW.created_at_epoch_ms)
     IS DISTINCT FROM ROW(OLD.tenant_id,OLD.input_id,OLD.subject,OLD.actor,OLD.source_name,OLD.media_type,OLD.maximum_parts,OLD.maximum_bytes,OLD.created_at_epoch_ms)
  THEN RAISE EXCEPTION 'prompt input identity is immutable' USING ERRCODE='55000'; END IF;
  IF OLD.state IN ('COMPILED','FAILED','CANCELLED') THEN RAISE EXCEPTION 'terminal prompt input is immutable' USING ERRCODE='55000'; END IF;
  IF NEW.state_version <> OLD.state_version+1 THEN RAISE EXCEPTION 'state version must increment by one' USING ERRCODE='40001'; END IF;
  IF NOT CASE OLD.state WHEN 'UPLOADING' THEN NEW.state IN ('COMPILING','CANCELLED') WHEN 'COMPILING' THEN NEW.state IN ('COMPILED','FAILED','CANCELLED') ELSE false END
  THEN RAISE EXCEPTION 'invalid prompt input transition' USING ERRCODE='23514'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER prompt_input_transition BEFORE UPDATE ON ngkg_agents.prompt_input FOR EACH ROW EXECUTE FUNCTION ngkg_agents.enforce_prompt_input_transition();

CREATE FUNCTION ngkg_agents.enforce_prompt_shard_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.tenant_id,NEW.input_id,NEW.part_ordinal) IS DISTINCT FROM ROW(OLD.tenant_id,OLD.input_id,OLD.part_ordinal)
  THEN RAISE EXCEPTION 'compilation shard identity is immutable' USING ERRCODE='55000'; END IF;
  IF OLD.state IN ('COMPLETED','FAILED') THEN RAISE EXCEPTION 'terminal compilation shard is immutable' USING ERRCODE='55000'; END IF;
  IF NEW.state='LEASED' AND NEW.attempt<>OLD.attempt+1 THEN RAISE EXCEPTION 'new lease must increment attempt' USING ERRCODE='40001'; END IF;
  IF NEW.state IN ('COMPLETED','FAILED') AND OLD.state<>'LEASED' THEN RAISE EXCEPTION 'only a lease owner may finish a shard' USING ERRCODE='55000'; END IF;
  IF NEW.state NOT IN ('LEASED','COMPLETED','FAILED') THEN RAISE EXCEPTION 'invalid shard transition' USING ERRCODE='23514'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER prompt_shard_transition BEFORE UPDATE ON ngkg_agents.prompt_compilation_shard FOR EACH ROW EXECUTE FUNCTION ngkg_agents.enforce_prompt_shard_transition();

CREATE FUNCTION ngkg_agents.enqueue_prompt_compilation_shards(p_tenant uuid,p_input uuid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
BEGIN
  IF p_tenant IS DISTINCT FROM ngkg_agents.current_tenant_id() THEN RAISE EXCEPTION 'tenant mismatch' USING ERRCODE='42501'; END IF;
  INSERT INTO ngkg_agents.prompt_compilation_queue(tenant_id,input_id,part_ordinal,state)
  SELECT tenant_id,input_id,part_ordinal,'READY' FROM ngkg_agents.prompt_compilation_shard
  WHERE tenant_id=p_tenant AND input_id=p_input ON CONFLICT DO NOTHING;
END $$;

-- Workers lease across tenants through an opaque queue, then this function
-- installs that exact tenant before touching the FORCE-RLS shard table.
CREATE FUNCTION ngkg_agents.claim_prompt_compilation_shard(p_worker text,p_token uuid,p_lease_ms bigint)
RETURNS TABLE(tenant_id uuid,input_id uuid,part_ordinal integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
DECLARE c record; shard_state text;
BEGIN
  IF length(p_worker) NOT BETWEEN 1 AND 256 OR p_lease_ms NOT BETWEEN 1000 AND 3600000 THEN RAISE EXCEPTION 'invalid lease request' USING ERRCODE='22023'; END IF;
  LOOP
    SELECT q.tenant_id,q.input_id,q.part_ordinal INTO c FROM ngkg_agents.prompt_compilation_queue q
      WHERE q.state='READY' OR (q.state='LEASED' AND q.lease_expires_at_epoch_ms < (extract(epoch from clock_timestamp())*1000)::bigint)
      ORDER BY q.tenant_id,q.input_id,q.part_ordinal FOR UPDATE SKIP LOCKED LIMIT 1;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM set_config('ngkg.tenant_id',c.tenant_id::text,true);
    SELECT s.state INTO shard_state FROM ngkg_agents.prompt_compilation_shard s
      WHERE (s.tenant_id,s.input_id,s.part_ordinal)=(c.tenant_id,c.input_id,c.part_ordinal) FOR UPDATE;
    IF shard_state IN ('COMPLETED','FAILED') THEN
      UPDATE ngkg_agents.prompt_compilation_queue q SET state=shard_state,lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL
        WHERE (q.tenant_id,q.input_id,q.part_ordinal)=(c.tenant_id,c.input_id,c.part_ordinal);
      CONTINUE;
    END IF;
    UPDATE ngkg_agents.prompt_compilation_queue q SET state='LEASED',lease_owner=p_worker,lease_token=p_token,
      lease_expires_at_epoch_ms=(extract(epoch from clock_timestamp())*1000)::bigint+p_lease_ms
      WHERE (q.tenant_id,q.input_id,q.part_ordinal)=(c.tenant_id,c.input_id,c.part_ordinal);
    UPDATE ngkg_agents.prompt_compilation_shard s SET state='LEASED',lease_owner=p_worker,lease_token=p_token,
      lease_expires_at_epoch_ms=(extract(epoch from clock_timestamp())*1000)::bigint+p_lease_ms,attempt=attempt+1
      WHERE (s.tenant_id,s.input_id,s.part_ordinal)=(c.tenant_id,c.input_id,c.part_ordinal);
    RETURN QUERY SELECT c.tenant_id,c.input_id,c.part_ordinal;
    RETURN;
  END LOOP;
END $$;

CREATE FUNCTION ngkg_agents.finish_prompt_compilation_claim(p_tenant uuid,p_input uuid,p_part integer,p_token uuid,p_state text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
BEGIN
  IF p_tenant IS DISTINCT FROM ngkg_agents.current_tenant_id() OR p_state NOT IN ('COMPLETED','FAILED') THEN RAISE EXCEPTION 'invalid queue finalization' USING ERRCODE='42501'; END IF;
  UPDATE ngkg_agents.prompt_compilation_queue SET state=p_state,lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL
    WHERE tenant_id=p_tenant AND input_id=p_input AND part_ordinal=p_part AND state='LEASED' AND lease_token=p_token;
  IF NOT FOUND THEN RAISE EXCEPTION 'queue lease lost' USING ERRCODE='40001'; END IF;
END $$;
REVOKE ALL ON FUNCTION ngkg_agents.claim_prompt_compilation_shard(text,uuid,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.enqueue_prompt_compilation_shards(uuid,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.finish_prompt_compilation_claim(uuid,uuid,integer,uuid,text) FROM PUBLIC;

CREATE FUNCTION ngkg_agents.reject_prompt_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'prompt evidence is immutable' USING ERRCODE='55000'; END $$;
DO $$ DECLARE t text; BEGIN FOREACH t IN ARRAY ARRAY['prompt_part','prompt_chunk','prompt_requirement','compiled_context','prompt_requirement_coverage'] LOOP EXECUTE format('CREATE TRIGGER %I_immutable BEFORE UPDATE OR DELETE ON ngkg_agents.%I FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_prompt_mutation()',t,t); END LOOP; END $$;

DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY['prompt_input','prompt_part','prompt_compilation_shard','prompt_chunk','prompt_requirement','compiled_context','prompt_requirement_coverage'] LOOP
    EXECUTE format('ALTER TABLE ngkg_agents.%I ENABLE ROW LEVEL SECURITY',t); EXECUTE format('ALTER TABLE ngkg_agents.%I FORCE ROW LEVEL SECURITY',t);
    EXECUTE format('CREATE POLICY %I_tenant ON ngkg_agents.%I USING (tenant_id=ngkg_agents.current_tenant_id()) WITH CHECK (tenant_id=ngkg_agents.current_tenant_id())',t,t);
  END LOOP;
END $$;

REVOKE ALL ON ALL TABLES IN SCHEMA ngkg_agents FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA ngkg_agents FROM PUBLIC;
