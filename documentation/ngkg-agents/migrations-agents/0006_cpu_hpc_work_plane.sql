-- Phase 8: Kubernetes-native multinode CPU work, bounded spill and checkpoints.

CREATE TABLE ngkg_agents.cpu_workload (
    tenant_id uuid NOT NULL,
    workload_id uuid NOT NULL,
    component text NOT NULL CHECK (component IN ('QUALIFICATION')),
    kernel text NOT NULL CHECK (kernel IN ('CANONICAL_LINESET_V1')),
    subject text NOT NULL CHECK (length(subject) BETWEEN 1 AND 256),
    state text NOT NULL CHECK (state IN ('READY','RUNNING','COMPLETED','FAILED','CANCELLED')),
    state_version bigint NOT NULL CHECK (state_version >= 0),
    total_partitions integer NOT NULL CHECK (total_partitions BETWEEN 1 AND 100000),
    completed_partitions integer NOT NULL CHECK (completed_partitions BETWEEN 0 AND total_partitions),
    failed_partitions integer NOT NULL CHECK (failed_partitions BETWEEN 0 AND total_partitions),
    maximum_attempts integer NOT NULL CHECK (maximum_attempts BETWEEN 1 AND 20),
    maximum_partition_bytes bigint NOT NULL CHECK (maximum_partition_bytes BETWEEN 1 AND 1073741824),
    maximum_spill_bytes bigint NOT NULL CHECK (maximum_spill_bytes BETWEEN 1 AND 4398046511104),
    idempotency_sha256 bytea NOT NULL CHECK (octet_length(idempotency_sha256)=32),
    request_sha256 bytea NOT NULL CHECK (octet_length(request_sha256)=32),
    result_root_sha256 bytea CHECK (result_root_sha256 IS NULL OR octet_length(result_root_sha256)=32),
    failure_code text CHECK (failure_code IS NULL OR length(failure_code) BETWEEN 1 AND 128),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    updated_at_epoch_ms bigint NOT NULL CHECK (updated_at_epoch_ms >= created_at_epoch_ms),
    PRIMARY KEY (tenant_id,workload_id),
    UNIQUE (tenant_id,idempotency_sha256),
    CHECK (completed_partitions + failed_partitions <= total_partitions),
    CHECK ((state='COMPLETED' AND result_root_sha256 IS NOT NULL AND completed_partitions=total_partitions AND failed_partitions=0) OR state<>'COMPLETED'),
    CHECK ((state='FAILED' AND failure_code IS NOT NULL) OR state<>'FAILED')
);

CREATE TABLE ngkg_agents.cpu_work_partition (
    tenant_id uuid NOT NULL,
    workload_id uuid NOT NULL,
    partition_ordinal integer NOT NULL CHECK (partition_ordinal >= 0),
    state text NOT NULL CHECK (state IN ('READY','LEASED','COMPLETED','FAILED','CANCELLED')),
    attempt integer NOT NULL CHECK (attempt >= 0),
    object_reference text NOT NULL CHECK (length(object_reference) BETWEEN 1 AND 2048),
    source_sha256 bytea NOT NULL CHECK (octet_length(source_sha256)=32),
    byte_length bigint NOT NULL CHECK (byte_length > 0),
    lease_owner text,
    lease_token uuid,
    lease_expires_at_epoch_ms bigint,
    result_sha256 bytea CHECK (result_sha256 IS NULL OR octet_length(result_sha256)=32),
    failure_code text CHECK (failure_code IS NULL OR length(failure_code) BETWEEN 1 AND 128),
    records_completed bigint NOT NULL DEFAULT 0 CHECK (records_completed >= 0),
    bytes_completed bigint NOT NULL DEFAULT 0 CHECK (bytes_completed >= 0),
    spill_bytes bigint NOT NULL DEFAULT 0 CHECK (spill_bytes >= 0),
    threads_used integer NOT NULL DEFAULT 0 CHECK (threads_used >= 0),
    peak_memory_bytes bigint NOT NULL DEFAULT 0 CHECK (peak_memory_bytes >= 0),
    completed_at_epoch_ms bigint,
    PRIMARY KEY (tenant_id,workload_id,partition_ordinal),
    FOREIGN KEY (tenant_id,workload_id) REFERENCES ngkg_agents.cpu_workload(tenant_id,workload_id),
    CHECK ((state='LEASED' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at_epoch_ms IS NOT NULL) OR state<>'LEASED'),
    CHECK ((state='COMPLETED' AND result_sha256 IS NOT NULL AND completed_at_epoch_ms IS NOT NULL) OR state<>'COMPLETED'),
    CHECK ((state='FAILED' AND failure_code IS NOT NULL AND completed_at_epoch_ms IS NOT NULL) OR state<>'FAILED')
);

-- Opaque cross-tenant claim queue. It has no direct runtime grants or RLS policy.
CREATE TABLE ngkg_agents.cpu_work_queue (
    tenant_id uuid NOT NULL,
    workload_id uuid NOT NULL,
    partition_ordinal integer NOT NULL,
    component text NOT NULL,
    state text NOT NULL CHECK (state IN ('READY','LEASED','COMPLETED','FAILED','CANCELLED')),
    lease_owner text,
    lease_token uuid,
    lease_expires_at_epoch_ms bigint,
    PRIMARY KEY (tenant_id,workload_id,partition_ordinal)
);
CREATE INDEX cpu_work_claim_idx ON ngkg_agents.cpu_work_queue(component,state,lease_expires_at_epoch_ms,tenant_id,workload_id,partition_ordinal);

CREATE TABLE ngkg_agents.cpu_checkpoint (
    tenant_id uuid NOT NULL,
    workload_id uuid NOT NULL,
    partition_ordinal integer NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    checkpoint_sha256 bytea NOT NULL CHECK (octet_length(checkpoint_sha256)=32),
    records_completed bigint NOT NULL CHECK (records_completed >= 0),
    bytes_completed bigint NOT NULL CHECK (bytes_completed >= 0),
    spill_bytes bigint NOT NULL CHECK (spill_bytes >= 0),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id,workload_id,partition_ordinal,sequence),
    FOREIGN KEY (tenant_id,workload_id,partition_ordinal) REFERENCES ngkg_agents.cpu_work_partition(tenant_id,workload_id,partition_ordinal)
);

CREATE TABLE ngkg_agents.cpu_work_event (
    tenant_id uuid NOT NULL,
    event_id uuid NOT NULL,
    workload_id uuid NOT NULL,
    partition_ordinal integer,
    event_type text NOT NULL CHECK (length(event_type) BETWEEN 1 AND 64),
    worker_id text,
    evidence_sha256 bytea NOT NULL CHECK (octet_length(evidence_sha256)=32),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id,event_id),
    FOREIGN KEY (tenant_id,workload_id) REFERENCES ngkg_agents.cpu_workload(tenant_id,workload_id)
);

CREATE FUNCTION ngkg_agents.enforce_cpu_workload_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.tenant_id,NEW.workload_id,NEW.component,NEW.kernel,NEW.subject,NEW.total_partitions,NEW.maximum_attempts,NEW.maximum_partition_bytes,NEW.maximum_spill_bytes,NEW.idempotency_sha256,NEW.request_sha256,NEW.created_at_epoch_ms)
     IS DISTINCT FROM
     ROW(OLD.tenant_id,OLD.workload_id,OLD.component,OLD.kernel,OLD.subject,OLD.total_partitions,OLD.maximum_attempts,OLD.maximum_partition_bytes,OLD.maximum_spill_bytes,OLD.idempotency_sha256,OLD.request_sha256,OLD.created_at_epoch_ms)
  THEN RAISE EXCEPTION 'CPU workload identity is immutable' USING ERRCODE='55000'; END IF;
  IF NEW.state_version<>OLD.state_version AND NEW.state_version<>OLD.state_version+1
  THEN RAISE EXCEPTION 'CPU workload CAS version invalid' USING ERRCODE='40001'; END IF;
  IF NEW.state<>OLD.state AND NOT ((OLD.state='READY' AND NEW.state IN ('RUNNING','CANCELLED','FAILED')) OR (OLD.state='RUNNING' AND NEW.state IN ('COMPLETED','FAILED','CANCELLED')))
  THEN RAISE EXCEPTION 'illegal CPU workload transition % -> %',OLD.state,NEW.state USING ERRCODE='23514'; END IF;
  IF OLD.state IN ('COMPLETED','FAILED','CANCELLED') THEN RAISE EXCEPTION 'terminal CPU workload is immutable' USING ERRCODE='55000'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER cpu_workload_transition BEFORE UPDATE ON ngkg_agents.cpu_workload FOR EACH ROW EXECUTE FUNCTION ngkg_agents.enforce_cpu_workload_transition();

CREATE FUNCTION ngkg_agents.enforce_cpu_partition_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.tenant_id,NEW.workload_id,NEW.partition_ordinal,NEW.object_reference,NEW.source_sha256,NEW.byte_length)
     IS DISTINCT FROM ROW(OLD.tenant_id,OLD.workload_id,OLD.partition_ordinal,OLD.object_reference,OLD.source_sha256,OLD.byte_length)
  THEN RAISE EXCEPTION 'CPU partition identity is immutable' USING ERRCODE='55000'; END IF;
  IF OLD.state IN ('COMPLETED','FAILED','CANCELLED') THEN RAISE EXCEPTION 'terminal CPU partition is immutable' USING ERRCODE='55000'; END IF;
  IF OLD.state='READY' AND NEW.state='LEASED' AND NEW.attempt<>OLD.attempt+1 THEN RAISE EXCEPTION 'lease attempt invalid' USING ERRCODE='40001'; END IF;
  IF OLD.state='LEASED' AND NEW.state='LEASED' AND NOT (
       (NEW.lease_token=OLD.lease_token AND NEW.attempt=OLD.attempt AND NEW.lease_expires_at_epoch_ms>OLD.lease_expires_at_epoch_ms)
       OR (OLD.lease_expires_at_epoch_ms<(extract(epoch from clock_timestamp())*1000)::bigint AND NEW.lease_token<>OLD.lease_token AND NEW.attempt=OLD.attempt+1)
     ) THEN RAISE EXCEPTION 'lease heartbeat or recovery invalid' USING ERRCODE='40001'; END IF;
  IF NOT ((OLD.state='READY' AND NEW.state IN ('LEASED','FAILED','CANCELLED')) OR (OLD.state='LEASED' AND NEW.state IN ('LEASED','COMPLETED','FAILED','CANCELLED')))
  THEN RAISE EXCEPTION 'illegal CPU partition transition % -> %',OLD.state,NEW.state USING ERRCODE='23514'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER cpu_partition_transition BEFORE UPDATE ON ngkg_agents.cpu_work_partition FOR EACH ROW EXECUTE FUNCTION ngkg_agents.enforce_cpu_partition_transition();

CREATE FUNCTION ngkg_agents.enqueue_cpu_workload(p_tenant uuid,p_workload uuid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
BEGIN
  IF p_tenant IS DISTINCT FROM ngkg_agents.current_tenant_id() THEN RAISE EXCEPTION 'tenant mismatch' USING ERRCODE='42501'; END IF;
  INSERT INTO ngkg_agents.cpu_work_queue(tenant_id,workload_id,partition_ordinal,component,state)
  SELECT p.tenant_id,p.workload_id,p.partition_ordinal,w.component,'READY'
    FROM ngkg_agents.cpu_work_partition p JOIN ngkg_agents.cpu_workload w USING(tenant_id,workload_id)
   WHERE p.tenant_id=p_tenant AND p.workload_id=p_workload ON CONFLICT DO NOTHING;
END $$;

CREATE FUNCTION ngkg_agents.claim_cpu_partition(p_worker text,p_token uuid,p_lease_ms bigint,p_component text)
RETURNS TABLE(tenant_id uuid,workload_id uuid,partition_ordinal integer,kernel text,object_reference text,source_sha256 bytea,byte_length bigint,maximum_partition_bytes bigint,maximum_spill_bytes bigint,attempt integer)
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
DECLARE c record; now_ms bigint := (extract(epoch from clock_timestamp())*1000)::bigint; partition_attempt integer; maximum_attempt integer;
BEGIN
  IF length(p_worker) NOT BETWEEN 1 AND 256 OR p_lease_ms NOT BETWEEN 1000 AND 3600000 OR p_component<>'QUALIFICATION' THEN RAISE EXCEPTION 'invalid CPU lease request' USING ERRCODE='22023'; END IF;
  LOOP
    SELECT q.* INTO c FROM ngkg_agents.cpu_work_queue q
     WHERE q.component=p_component AND (q.state='READY' OR (q.state='LEASED' AND q.lease_expires_at_epoch_ms<now_ms))
     ORDER BY q.tenant_id,q.workload_id,q.partition_ordinal FOR UPDATE SKIP LOCKED LIMIT 1;
    IF NOT FOUND THEN RETURN; END IF;
    PERFORM set_config('ngkg.tenant_id',c.tenant_id::text,true);
    SELECT p.attempt,w.maximum_attempts INTO partition_attempt,maximum_attempt
      FROM ngkg_agents.cpu_work_partition p JOIN ngkg_agents.cpu_workload w USING(tenant_id,workload_id)
     WHERE (p.tenant_id,p.workload_id,p.partition_ordinal)=(c.tenant_id,c.workload_id,c.partition_ordinal) FOR UPDATE OF p,w;
    IF partition_attempt>=maximum_attempt THEN
      UPDATE ngkg_agents.cpu_work_queue SET state='FAILED',lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL WHERE (tenant_id,workload_id,partition_ordinal)=(c.tenant_id,c.workload_id,c.partition_ordinal);
      UPDATE ngkg_agents.cpu_work_partition SET state='FAILED',failure_code='MAXIMUM_ATTEMPTS_EXCEEDED',completed_at_epoch_ms=now_ms,lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL WHERE (tenant_id,workload_id,partition_ordinal)=(c.tenant_id,c.workload_id,c.partition_ordinal);
      UPDATE ngkg_agents.cpu_workload SET state='FAILED',state_version=state_version+1,failed_partitions=failed_partitions+1,failure_code='MAXIMUM_ATTEMPTS_EXCEEDED',updated_at_epoch_ms=now_ms WHERE tenant_id=c.tenant_id AND workload_id=c.workload_id AND state IN ('READY','RUNNING');
      CONTINUE;
    END IF;
    UPDATE ngkg_agents.cpu_work_queue SET state='LEASED',lease_owner=p_worker,lease_token=p_token,lease_expires_at_epoch_ms=now_ms+p_lease_ms WHERE (tenant_id,workload_id,partition_ordinal)=(c.tenant_id,c.workload_id,c.partition_ordinal);
    UPDATE ngkg_agents.cpu_work_partition SET state='LEASED',attempt=attempt+1,lease_owner=p_worker,lease_token=p_token,lease_expires_at_epoch_ms=now_ms+p_lease_ms WHERE (tenant_id,workload_id,partition_ordinal)=(c.tenant_id,c.workload_id,c.partition_ordinal);
    UPDATE ngkg_agents.cpu_workload SET state='RUNNING',state_version=state_version+1,updated_at_epoch_ms=now_ms WHERE tenant_id=c.tenant_id AND workload_id=c.workload_id AND state='READY';
    RETURN QUERY SELECT p.tenant_id,p.workload_id,p.partition_ordinal,w.kernel,p.object_reference,p.source_sha256,p.byte_length,w.maximum_partition_bytes,w.maximum_spill_bytes,p.attempt
      FROM ngkg_agents.cpu_work_partition p JOIN ngkg_agents.cpu_workload w USING(tenant_id,workload_id)
     WHERE (p.tenant_id,p.workload_id,p.partition_ordinal)=(c.tenant_id,c.workload_id,c.partition_ordinal);
    RETURN;
  END LOOP;
END $$;

CREATE FUNCTION ngkg_agents.cpu_ready_partition_count(p_component text)
RETURNS bigint LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
  SELECT count(*)::bigint
    FROM ngkg_agents.cpu_work_queue
   WHERE component=p_component
     AND (state='READY' OR (state='LEASED' AND lease_expires_at_epoch_ms<(extract(epoch from clock_timestamp())*1000)::bigint))
$$;

CREATE FUNCTION ngkg_agents.checkpoint_cpu_partition(p_tenant uuid,p_workload uuid,p_partition integer,p_token uuid,p_hash bytea,p_records bigint,p_bytes bigint,p_spill bigint,p_lease_ms bigint,p_now bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
DECLARE next_sequence bigint;
BEGIN
  IF p_tenant IS DISTINCT FROM ngkg_agents.current_tenant_id() OR octet_length(p_hash)<>32 OR p_records<0 OR p_bytes<0 OR p_spill<0 OR p_lease_ms NOT BETWEEN 1000 AND 3600000 THEN RAISE EXCEPTION 'invalid checkpoint' USING ERRCODE='22023'; END IF;
  PERFORM 1 FROM ngkg_agents.cpu_work_partition WHERE tenant_id=p_tenant AND workload_id=p_workload AND partition_ordinal=p_partition AND state='LEASED' AND lease_token=p_token FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'CPU lease lost' USING ERRCODE='40001'; END IF;
  SELECT COALESCE(max(sequence),0)+1 INTO next_sequence FROM ngkg_agents.cpu_checkpoint WHERE tenant_id=p_tenant AND workload_id=p_workload AND partition_ordinal=p_partition;
  INSERT INTO ngkg_agents.cpu_checkpoint VALUES(p_tenant,p_workload,p_partition,next_sequence,p_hash,p_records,p_bytes,p_spill,p_now);
  UPDATE ngkg_agents.cpu_work_partition SET records_completed=p_records,bytes_completed=p_bytes,spill_bytes=p_spill,lease_expires_at_epoch_ms=p_now+p_lease_ms WHERE tenant_id=p_tenant AND workload_id=p_workload AND partition_ordinal=p_partition AND lease_token=p_token;
  UPDATE ngkg_agents.cpu_work_queue SET lease_expires_at_epoch_ms=p_now+p_lease_ms WHERE tenant_id=p_tenant AND workload_id=p_workload AND partition_ordinal=p_partition AND lease_token=p_token;
END $$;

CREATE FUNCTION ngkg_agents.finish_cpu_partition(p_tenant uuid,p_workload uuid,p_partition integer,p_token uuid,p_state text,p_result bytea,p_failure text,p_records bigint,p_bytes bigint,p_spill bigint,p_threads integer,p_peak_memory bigint,p_now bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
BEGIN
  IF p_tenant IS DISTINCT FROM ngkg_agents.current_tenant_id() OR p_state NOT IN ('COMPLETED','FAILED') OR (p_state='COMPLETED' AND octet_length(p_result)<>32) OR (p_state='FAILED' AND (p_failure IS NULL OR length(p_failure) NOT BETWEEN 1 AND 128)) THEN RAISE EXCEPTION 'invalid CPU completion' USING ERRCODE='22023'; END IF;
  UPDATE ngkg_agents.cpu_work_partition SET state=p_state,result_sha256=p_result,failure_code=p_failure,records_completed=p_records,bytes_completed=p_bytes,spill_bytes=p_spill,threads_used=p_threads,peak_memory_bytes=p_peak_memory,completed_at_epoch_ms=p_now,lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL WHERE tenant_id=p_tenant AND workload_id=p_workload AND partition_ordinal=p_partition AND state='LEASED' AND lease_token=p_token;
  IF NOT FOUND THEN RAISE EXCEPTION 'CPU lease lost' USING ERRCODE='40001'; END IF;
  UPDATE ngkg_agents.cpu_work_queue SET state=p_state,lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL WHERE tenant_id=p_tenant AND workload_id=p_workload AND partition_ordinal=p_partition AND state='LEASED' AND lease_token=p_token;
  IF NOT FOUND THEN RAISE EXCEPTION 'CPU queue lease lost' USING ERRCODE='40001'; END IF;
  IF p_state='COMPLETED' THEN UPDATE ngkg_agents.cpu_workload SET completed_partitions=completed_partitions+1,updated_at_epoch_ms=p_now WHERE tenant_id=p_tenant AND workload_id=p_workload AND state='RUNNING';
  ELSE UPDATE ngkg_agents.cpu_workload SET state='FAILED',state_version=state_version+1,failed_partitions=failed_partitions+1,failure_code=p_failure,updated_at_epoch_ms=p_now WHERE tenant_id=p_tenant AND workload_id=p_workload AND state='RUNNING'; END IF;
END $$;

CREATE FUNCTION ngkg_agents.cancel_cpu_workload(p_tenant uuid,p_workload uuid)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
BEGIN
  IF p_tenant IS DISTINCT FROM ngkg_agents.current_tenant_id() THEN RAISE EXCEPTION 'tenant mismatch' USING ERRCODE='42501'; END IF;
  UPDATE ngkg_agents.cpu_work_partition SET state='CANCELLED',lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL WHERE tenant_id=p_tenant AND workload_id=p_workload AND state IN ('READY','LEASED');
  UPDATE ngkg_agents.cpu_work_queue SET state='CANCELLED',lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL WHERE tenant_id=p_tenant AND workload_id=p_workload AND state IN ('READY','LEASED');
END $$;

DO $$ DECLARE table_name text; BEGIN
  FOREACH table_name IN ARRAY ARRAY['cpu_workload','cpu_work_partition','cpu_checkpoint','cpu_work_event'] LOOP
    EXECUTE format('ALTER TABLE ngkg_agents.%I ENABLE ROW LEVEL SECURITY',table_name);
    EXECUTE format('ALTER TABLE ngkg_agents.%I FORCE ROW LEVEL SECURITY',table_name);
    EXECUTE format('CREATE POLICY tenant_isolation ON ngkg_agents.%I USING (tenant_id=ngkg_agents.current_tenant_id()) WITH CHECK (tenant_id=ngkg_agents.current_tenant_id())',table_name);
  END LOOP;
  FOREACH table_name IN ARRAY ARRAY['cpu_checkpoint','cpu_work_event'] LOOP
    EXECUTE format('CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON ngkg_agents.%I FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation()',table_name);
  END LOOP;
END $$;

REVOKE ALL ON FUNCTION ngkg_agents.enqueue_cpu_workload(uuid,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.claim_cpu_partition(text,uuid,bigint,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.cpu_ready_partition_count(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.checkpoint_cpu_partition(uuid,uuid,integer,uuid,bytea,bigint,bigint,bigint,bigint,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.finish_cpu_partition(uuid,uuid,integer,uuid,text,bytea,text,bigint,bigint,bigint,integer,bigint,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.cancel_cpu_workload(uuid,uuid) FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA ngkg_agents FROM PUBLIC;
