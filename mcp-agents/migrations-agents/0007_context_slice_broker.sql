-- Phase 10: immutable, capability-scoped large context slices.

CREATE TABLE ngkg_agents.context_slice (
  tenant_id uuid NOT NULL,
  slice_id uuid NOT NULL,
  subject text NOT NULL CHECK(length(subject) BETWEEN 1 AND 256),
  dataset_id uuid NOT NULL,
  snapshot_id uuid NOT NULL,
  authorized_graph_set_sha256 bytea NOT NULL CHECK(octet_length(authorized_graph_set_sha256)=32),
  semantic_result_sha256 bytea NOT NULL CHECK(octet_length(semantic_result_sha256)=32),
  media_type text NOT NULL CHECK(length(media_type) BETWEEN 1 AND 128),
  state text NOT NULL CHECK(state IN ('UPLOADING','ABANDONED','ACTIVE','EXPIRED_PENDING_DELETE','DELETING','DELETED','CORRUPT')),
  state_version bigint NOT NULL DEFAULT 1 CHECK(state_version>0),
  chunk_size_bytes bigint NOT NULL CHECK(chunk_size_bytes BETWEEN 65536 AND 268435456),
  expected_total_bytes bigint NOT NULL CHECK(expected_total_bytes BETWEEN 1 AND 10995116277760),
  total_bytes bigint CHECK(total_bytes IS NULL OR total_bytes=expected_total_bytes),
  total_triples bigint NOT NULL CHECK(total_triples>=0),
  content_sha256 bytea CHECK(content_sha256 IS NULL OR octet_length(content_sha256)=32),
  manifest_sha256 bytea CHECK(manifest_sha256 IS NULL OR octet_length(manifest_sha256)=32),
  manifest_object_reference text,
  index_sha256 bytea CHECK(index_sha256 IS NULL OR octet_length(index_sha256)=32),
  index_bytes bigint CHECK(index_bytes IS NULL OR index_bytes>0),
  index_object_reference text,
  kms_key_id_sha256 bytea NOT NULL CHECK(octet_length(kms_key_id_sha256)=32),
  created_at_epoch_ms bigint NOT NULL CHECK(created_at_epoch_ms>=0),
  expires_at_epoch_ms bigint NOT NULL CHECK(expires_at_epoch_ms>created_at_epoch_ms),
  delete_after_epoch_ms bigint NOT NULL CHECK(delete_after_epoch_ms>=expires_at_epoch_ms),
  updated_at_epoch_ms bigint NOT NULL CHECK(updated_at_epoch_ms>=created_at_epoch_ms),
  PRIMARY KEY(tenant_id,slice_id),
  CHECK ((state='UPLOADING' AND content_sha256 IS NULL AND manifest_sha256 IS NULL AND index_sha256 IS NULL)
      OR (state IN ('ACTIVE','EXPIRED_PENDING_DELETE','CORRUPT') AND content_sha256 IS NOT NULL AND manifest_sha256 IS NOT NULL AND index_sha256 IS NOT NULL)
      OR (state IN ('ABANDONED','DELETING','DELETED') AND ((content_sha256 IS NULL AND manifest_sha256 IS NULL AND index_sha256 IS NULL) OR (content_sha256 IS NOT NULL AND manifest_sha256 IS NOT NULL AND index_sha256 IS NOT NULL))))
);

CREATE TABLE ngkg_agents.context_slice_chunk (
  tenant_id uuid NOT NULL,
  slice_id uuid NOT NULL,
  ordinal integer NOT NULL CHECK(ordinal>=0),
  byte_start bigint NOT NULL CHECK(byte_start>=0),
  byte_end_exclusive bigint NOT NULL CHECK(byte_end_exclusive>byte_start),
  chunk_sha256 bytea NOT NULL CHECK(octet_length(chunk_sha256)=32),
  object_reference text NOT NULL CHECK(length(object_reference) BETWEEN 1 AND 2048),
  created_at_epoch_ms bigint NOT NULL CHECK(created_at_epoch_ms>=0),
  PRIMARY KEY(tenant_id,slice_id,ordinal),
  UNIQUE(tenant_id,slice_id,chunk_sha256,ordinal),
  FOREIGN KEY(tenant_id,slice_id) REFERENCES ngkg_agents.context_slice(tenant_id,slice_id)
);

CREATE TABLE ngkg_agents.context_slice_capability (
  tenant_id uuid NOT NULL,
  capability_id uuid NOT NULL,
  slice_id uuid NOT NULL,
  subject text NOT NULL CHECK(length(subject) BETWEEN 1 AND 256),
  audience text NOT NULL CHECK(length(audience) BETWEEN 1 AND 256),
  nonce uuid NOT NULL,
  range_start bigint NOT NULL CHECK(range_start>=0),
  range_end_exclusive bigint NOT NULL CHECK(range_end_exclusive>range_start),
  token_sha256 bytea NOT NULL CHECK(octet_length(token_sha256)=32),
  policy_version_sha256 bytea NOT NULL CHECK(octet_length(policy_version_sha256)=32),
  issued_at_epoch_ms bigint NOT NULL CHECK(issued_at_epoch_ms>=0),
  expires_at_epoch_ms bigint NOT NULL CHECK(expires_at_epoch_ms>issued_at_epoch_ms),
  revoked_at_epoch_ms bigint,
  PRIMARY KEY(tenant_id,capability_id),
  UNIQUE(tenant_id,nonce),
  FOREIGN KEY(tenant_id,slice_id) REFERENCES ngkg_agents.context_slice(tenant_id,slice_id)
);

-- This queue has no runtime table grant and intentionally has no tenant policy;
-- only SECURITY DEFINER lifecycle functions expose opaque claims.
CREATE TABLE ngkg_agents.context_slice_gc_queue (
  tenant_id uuid NOT NULL,
  slice_id uuid NOT NULL,
  delete_after_epoch_ms bigint NOT NULL,
  state text NOT NULL CHECK(state IN ('SCHEDULED','LEASED','DELETED','FAILED')),
  lease_owner text,
  lease_token uuid,
  lease_expires_at_epoch_ms bigint,
  attempt integer NOT NULL DEFAULT 0 CHECK(attempt>=0),
  PRIMARY KEY(tenant_id,slice_id)
);
CREATE INDEX context_slice_gc_claim_idx ON ngkg_agents.context_slice_gc_queue(state,delete_after_epoch_ms,lease_expires_at_epoch_ms);

CREATE TABLE ngkg_agents.context_slice_tombstone (
  tenant_id uuid NOT NULL,
  tombstone_id uuid NOT NULL,
  slice_id uuid NOT NULL,
  manifest_sha256 bytea CHECK(manifest_sha256 IS NULL OR octet_length(manifest_sha256)=32),
  content_sha256 bytea CHECK(content_sha256 IS NULL OR octet_length(content_sha256)=32),
  deleted_object_count integer NOT NULL CHECK(deleted_object_count>=0),
  deletion_evidence_sha256 bytea NOT NULL CHECK(octet_length(deletion_evidence_sha256)=32),
  deleted_at_epoch_ms bigint NOT NULL CHECK(deleted_at_epoch_ms>=0),
  PRIMARY KEY(tenant_id,tombstone_id),
  UNIQUE(tenant_id,slice_id)
);

CREATE FUNCTION ngkg_agents.validate_context_slice_transition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.tenant_id,NEW.slice_id,NEW.subject,NEW.dataset_id,NEW.snapshot_id,NEW.authorized_graph_set_sha256,NEW.semantic_result_sha256,NEW.media_type,NEW.chunk_size_bytes,NEW.expected_total_bytes,NEW.total_triples,NEW.kms_key_id_sha256,NEW.created_at_epoch_ms,NEW.expires_at_epoch_ms,NEW.delete_after_epoch_ms)
     IS DISTINCT FROM ROW(OLD.tenant_id,OLD.slice_id,OLD.subject,OLD.dataset_id,OLD.snapshot_id,OLD.authorized_graph_set_sha256,OLD.semantic_result_sha256,OLD.media_type,OLD.chunk_size_bytes,OLD.expected_total_bytes,OLD.total_triples,OLD.kms_key_id_sha256,OLD.created_at_epoch_ms,OLD.expires_at_epoch_ms,OLD.delete_after_epoch_ms)
  THEN RAISE EXCEPTION 'context slice identity is immutable' USING ERRCODE='55000'; END IF;
  IF NEW.state_version<>OLD.state_version+1 OR NEW.updated_at_epoch_ms<OLD.updated_at_epoch_ms
  THEN RAISE EXCEPTION 'context slice CAS version invalid' USING ERRCODE='40001'; END IF;
  IF OLD.state NOT IN ('UPLOADING','ABANDONED') AND ROW(NEW.total_bytes,NEW.content_sha256,NEW.manifest_sha256,NEW.index_sha256,NEW.index_bytes)
     IS DISTINCT FROM ROW(OLD.total_bytes,OLD.content_sha256,OLD.manifest_sha256,OLD.index_sha256,OLD.index_bytes)
  THEN RAISE EXCEPTION 'active context slice content identity is immutable' USING ERRCODE='55000'; END IF;
  IF NOT ((OLD.state='UPLOADING' AND NEW.state IN ('ACTIVE','ABANDONED'))
       OR (OLD.state='ABANDONED' AND NEW.state='DELETING')
       OR (OLD.state='ACTIVE' AND NEW.state IN ('EXPIRED_PENDING_DELETE','DELETING','CORRUPT'))
       OR (OLD.state IN ('EXPIRED_PENDING_DELETE','CORRUPT') AND NEW.state='DELETING')
       OR (OLD.state='DELETING' AND NEW.state IN ('DELETED','EXPIRED_PENDING_DELETE')))
  THEN RAISE EXCEPTION 'illegal context slice transition % -> %',OLD.state,NEW.state USING ERRCODE='23514'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER context_slice_transition BEFORE UPDATE ON ngkg_agents.context_slice FOR EACH ROW EXECUTE FUNCTION ngkg_agents.validate_context_slice_transition();
CREATE TRIGGER context_slice_no_delete BEFORE DELETE ON ngkg_agents.context_slice FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation();

CREATE FUNCTION ngkg_agents.validate_context_capability_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(NEW.tenant_id,NEW.capability_id,NEW.slice_id,NEW.subject,NEW.audience,NEW.nonce,NEW.range_start,NEW.range_end_exclusive,NEW.token_sha256,NEW.policy_version_sha256,NEW.issued_at_epoch_ms,NEW.expires_at_epoch_ms)
     IS DISTINCT FROM ROW(OLD.tenant_id,OLD.capability_id,OLD.slice_id,OLD.subject,OLD.audience,OLD.nonce,OLD.range_start,OLD.range_end_exclusive,OLD.token_sha256,OLD.policy_version_sha256,OLD.issued_at_epoch_ms,OLD.expires_at_epoch_ms)
     OR OLD.revoked_at_epoch_ms IS NOT NULL OR NEW.revoked_at_epoch_ms IS NULL OR NEW.revoked_at_epoch_ms<OLD.issued_at_epoch_ms
  THEN RAISE EXCEPTION 'context capability is immutable except one-way revocation' USING ERRCODE='55000'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER context_capability_update BEFORE UPDATE ON ngkg_agents.context_slice_capability FOR EACH ROW EXECUTE FUNCTION ngkg_agents.validate_context_capability_update();
CREATE TRIGGER context_capability_no_delete BEFORE DELETE ON ngkg_agents.context_slice_capability FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation();

CREATE FUNCTION ngkg_agents.schedule_context_slice_gc(p_tenant uuid,p_slice uuid,p_delete_after bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
BEGIN
  IF p_tenant IS DISTINCT FROM ngkg_agents.current_tenant_id() THEN RAISE EXCEPTION 'tenant mismatch' USING ERRCODE='42501'; END IF;
  INSERT INTO ngkg_agents.context_slice_gc_queue(tenant_id,slice_id,delete_after_epoch_ms,state)
  SELECT tenant_id,slice_id,p_delete_after,'SCHEDULED' FROM ngkg_agents.context_slice
   WHERE tenant_id=p_tenant AND slice_id=p_slice AND delete_after_epoch_ms=p_delete_after;
  IF NOT FOUND THEN RAISE EXCEPTION 'slice lifecycle mismatch' USING ERRCODE='23514'; END IF;
END $$;

CREATE FUNCTION ngkg_agents.claim_context_slice_gc(p_worker text,p_token uuid,p_lease_ms bigint,p_now bigint)
RETURNS TABLE(tenant_id uuid,slice_id uuid) LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
DECLARE c record;
BEGIN
  IF length(p_worker) NOT BETWEEN 1 AND 256 OR p_lease_ms NOT BETWEEN 1000 AND 3600000 THEN RAISE EXCEPTION 'invalid GC lease request' USING ERRCODE='22023'; END IF;
  SELECT q.* INTO c FROM ngkg_agents.context_slice_gc_queue q
   WHERE q.delete_after_epoch_ms<=p_now AND (q.state='SCHEDULED' OR (q.state='LEASED' AND q.lease_expires_at_epoch_ms<p_now))
   ORDER BY q.delete_after_epoch_ms,q.tenant_id,q.slice_id FOR UPDATE SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN RETURN; END IF;
  UPDATE ngkg_agents.context_slice_gc_queue SET state='LEASED',lease_owner=p_worker,lease_token=p_token,lease_expires_at_epoch_ms=p_now+p_lease_ms,attempt=attempt+1
   WHERE (context_slice_gc_queue.tenant_id,context_slice_gc_queue.slice_id)=(c.tenant_id,c.slice_id);
  PERFORM set_config('ngkg.tenant_id',c.tenant_id::text,true);
  UPDATE ngkg_agents.context_slice SET state='ABANDONED',state_version=state_version+1,updated_at_epoch_ms=p_now
   WHERE context_slice.tenant_id=c.tenant_id AND context_slice.slice_id=c.slice_id AND state='UPLOADING';
  UPDATE ngkg_agents.context_slice SET state='DELETING',state_version=state_version+1,updated_at_epoch_ms=p_now
   WHERE context_slice.tenant_id=c.tenant_id AND context_slice.slice_id=c.slice_id AND state IN ('ABANDONED','ACTIVE','EXPIRED_PENDING_DELETE','CORRUPT');
  RETURN QUERY SELECT c.tenant_id,c.slice_id;
END $$;

CREATE FUNCTION ngkg_agents.finish_context_slice_gc(p_tenant uuid,p_slice uuid,p_token uuid,p_tombstone uuid,p_evidence bytea,p_count integer,p_now bigint)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,ngkg_agents AS $$
BEGIN
  IF p_tenant IS DISTINCT FROM ngkg_agents.current_tenant_id() OR octet_length(p_evidence)<>32 OR p_count<0 THEN RAISE EXCEPTION 'invalid GC completion' USING ERRCODE='22023'; END IF;
  PERFORM 1 FROM ngkg_agents.context_slice_gc_queue WHERE tenant_id=p_tenant AND slice_id=p_slice AND state='LEASED' AND lease_token=p_token FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'GC lease lost' USING ERRCODE='40001'; END IF;
  INSERT INTO ngkg_agents.context_slice_tombstone(tenant_id,tombstone_id,slice_id,manifest_sha256,content_sha256,deleted_object_count,deletion_evidence_sha256,deleted_at_epoch_ms)
  SELECT tenant_id,p_tombstone,slice_id,manifest_sha256,content_sha256,p_count,p_evidence,p_now FROM ngkg_agents.context_slice WHERE tenant_id=p_tenant AND slice_id=p_slice;
  UPDATE ngkg_agents.context_slice SET state='DELETED',state_version=state_version+1,updated_at_epoch_ms=p_now,manifest_object_reference=NULL,index_object_reference=NULL
   WHERE tenant_id=p_tenant AND slice_id=p_slice AND state='DELETING';
  UPDATE ngkg_agents.context_slice_gc_queue SET state='DELETED',lease_owner=NULL,lease_token=NULL,lease_expires_at_epoch_ms=NULL WHERE tenant_id=p_tenant AND slice_id=p_slice;
END $$;

DO $$ DECLARE relation_name text; BEGIN
  FOREACH relation_name IN ARRAY ARRAY['context_slice','context_slice_chunk','context_slice_capability','context_slice_tombstone'] LOOP
    EXECUTE format('ALTER TABLE ngkg_agents.%I ENABLE ROW LEVEL SECURITY',relation_name);
    EXECUTE format('ALTER TABLE ngkg_agents.%I FORCE ROW LEVEL SECURITY',relation_name);
    EXECUTE format('CREATE POLICY tenant_isolation ON ngkg_agents.%I USING (tenant_id=ngkg_agents.current_tenant_id()) WITH CHECK (tenant_id=ngkg_agents.current_tenant_id())',relation_name);
  END LOOP;
  FOREACH relation_name IN ARRAY ARRAY['context_slice_chunk','context_slice_tombstone'] LOOP
    EXECUTE format('CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON ngkg_agents.%I FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation()',relation_name);
  END LOOP;
END $$;

REVOKE ALL ON FUNCTION ngkg_agents.claim_context_slice_gc(text,uuid,bigint,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.schedule_context_slice_gc(uuid,uuid,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.finish_context_slice_gc(uuid,uuid,uuid,uuid,bytea,integer,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.validate_context_slice_transition() FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.validate_context_capability_update() FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA ngkg_agents FROM PUBLIC;
