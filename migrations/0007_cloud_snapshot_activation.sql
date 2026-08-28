CREATE TABLE cloud_snapshot_activation (
  tenant_id UUID NOT NULL,
  operation_id UUID NOT NULL,
  dataset_id UUID NOT NULL,
  snapshot_id UUID NOT NULL,
  activation_manifest_object_key TEXT NOT NULL CHECK (
    length(activation_manifest_object_key) BETWEEN 1 AND 1024
    AND activation_manifest_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  activation_manifest_sha256 BYTEA NOT NULL CHECK (octet_length(activation_manifest_sha256) = 32),
  reference_manifest_object_key TEXT NOT NULL CHECK (
    length(reference_manifest_object_key) BETWEEN 1 AND 1024
    AND reference_manifest_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  reference_manifest_sha256 BYTEA NOT NULL CHECK (octet_length(reference_manifest_sha256) = 32),
  semantic_root_object_key TEXT NOT NULL CHECK (
    length(semantic_root_object_key) BETWEEN 1 AND 1024
    AND semantic_root_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  semantic_root_sha256 BYTEA NOT NULL CHECK (octet_length(semantic_root_sha256) = 32),
  qualification_root_object_key TEXT NOT NULL CHECK (
    length(qualification_root_object_key) BETWEEN 1 AND 1024
    AND qualification_root_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  qualification_root_sha256 BYTEA NOT NULL CHECK (octet_length(qualification_root_sha256) = 32),
  offline_root_object_key TEXT NOT NULL CHECK (
    length(offline_root_object_key) BETWEEN 1 AND 1024
    AND offline_root_object_key ~ '^[A-Za-z0-9_][A-Za-z0-9._-]{0,254}(/[A-Za-z0-9_][A-Za-z0-9._-]{0,254})*$'
  ),
  offline_root_sha256 BYTEA NOT NULL CHECK (octet_length(offline_root_sha256) = 32),
  semantic_content_sha256 BYTEA NOT NULL CHECK (octet_length(semantic_content_sha256) = 32),
  authorized_graph_set_sha256 BYTEA NOT NULL CHECK (octet_length(authorized_graph_set_sha256) = 32),
  datatype_policy_sha256 BYTEA NOT NULL CHECK (octet_length(datatype_policy_sha256) = 32),
  ontology_sha256 BYTEA NOT NULL CHECK (octet_length(ontology_sha256) = 32),
  finite_closure_sha256 BYTEA NOT NULL CHECK (octet_length(finite_closure_sha256) = 32),
  proof_support_root_sha256 BYTEA NOT NULL CHECK (octet_length(proof_support_root_sha256) = 32),
  query_dataset_sha256 BYTEA NOT NULL CHECK (octet_length(query_dataset_sha256) = 32),
  query_dataset_bytes BIGINT NOT NULL CHECK (query_dataset_bytes >= 0),
  fact_count BIGINT NOT NULL CHECK (fact_count >= 0),
  consequence_count BIGINT NOT NULL CHECK (consequence_count >= 0),
  semantic_partition_count INTEGER NOT NULL CHECK (semantic_partition_count BETWEEN 1 AND 65536),
  reasoning_partition_count INTEGER NOT NULL CHECK (reasoning_partition_count BETWEEN 1 AND 65536),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, operation_id),
  UNIQUE (tenant_id, dataset_id, snapshot_id),
  FOREIGN KEY (tenant_id, operation_id) REFERENCES operation (tenant_id, operation_id),
  FOREIGN KEY (tenant_id, dataset_id) REFERENCES dataset (tenant_id, dataset_id)
);

ALTER TABLE cloud_snapshot_activation ENABLE ROW LEVEL SECURITY;
ALTER TABLE cloud_snapshot_activation FORCE ROW LEVEL SECURITY;
CREATE POLICY cloud_snapshot_activation_tenant_policy ON cloud_snapshot_activation
  USING (tenant_id = current_setting('ngkg.tenant_id', true)::uuid)
  WITH CHECK (tenant_id = current_setting('ngkg.tenant_id', true)::uuid);

CREATE TRIGGER cloud_snapshot_activation_immutable
BEFORE UPDATE OR DELETE ON cloud_snapshot_activation
FOR EACH ROW EXECUTE FUNCTION ngkg_operation_audit_guard();
