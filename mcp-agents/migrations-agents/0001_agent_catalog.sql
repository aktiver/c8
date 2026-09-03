CREATE SCHEMA IF NOT EXISTS ngkg_agents;

CREATE OR REPLACE FUNCTION ngkg_agents.current_tenant_id()
RETURNS uuid
LANGUAGE sql
STABLE
PARALLEL SAFE
AS $$
    SELECT NULLIF(current_setting('ngkg.tenant_id', true), '')::uuid
$$;

CREATE OR REPLACE FUNCTION ngkg_agents.reject_immutable_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'immutable table % does not permit %', TG_TABLE_NAME, TG_OP
        USING ERRCODE = '55000';
END
$$;

CREATE TABLE ngkg_agents.tool_provider (
    tenant_id uuid NOT NULL,
    provider_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 256),
    endpoint text NOT NULL CHECK (length(endpoint) BETWEEN 1 AND 8192),
    auth_reference text NOT NULL CHECK (length(auth_reference) BETWEEN 1 AND 1024),
    policy jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('PENDING', 'QUALIFIED', 'DISABLED', 'REVOKED')),
    spec_sha256 bytea NOT NULL CHECK (octet_length(spec_sha256) = 32),
    created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id, provider_id, version),
    UNIQUE (tenant_id, name, version)
);

CREATE TABLE ngkg_agents.tool_catalog (
    tenant_id uuid NOT NULL,
    provider_id uuid NOT NULL,
    provider_version bigint NOT NULL CHECK (provider_version > 0),
    catalog_sha256 bytea NOT NULL CHECK (octet_length(catalog_sha256) = 32),
    protocol_version text NOT NULL CHECK (length(protocol_version) BETWEEN 1 AND 64),
    discovered_tools jsonb NOT NULL,
    qualification_evidence_sha256 bytea NOT NULL CHECK (octet_length(qualification_evidence_sha256) = 32),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id, provider_id, catalog_sha256),
    FOREIGN KEY (tenant_id, provider_id, provider_version)
        REFERENCES ngkg_agents.tool_provider (tenant_id, provider_id, version)
);

CREATE TABLE ngkg_agents.agent_profile (
    tenant_id uuid NOT NULL,
    profile_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 256),
    dataset_constraints jsonb NOT NULL,
    model_allowlist jsonb NOT NULL,
    tool_catalog_sha256s jsonb NOT NULL,
    limits jsonb NOT NULL,
    approval_policy jsonb NOT NULL,
    profile_sha256 bytea NOT NULL CHECK (octet_length(profile_sha256) = 32),
    created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id, profile_id, version),
    UNIQUE (tenant_id, profile_sha256)
);

CREATE TABLE ngkg_agents.retention_policy (
    tenant_id uuid NOT NULL,
    policy_id uuid NOT NULL,
    version bigint NOT NULL CHECK (version > 0),
    minimum_retention_days integer NOT NULL CHECK (minimum_retention_days >= 1),
    legal_hold boolean NOT NULL,
    external_worm_required boolean NOT NULL,
    policy_sha256 bytea NOT NULL CHECK (octet_length(policy_sha256) = 32),
    created_by text NOT NULL CHECK (length(created_by) BETWEEN 1 AND 256),
    created_at_epoch_ms bigint NOT NULL CHECK (created_at_epoch_ms >= 0),
    PRIMARY KEY (tenant_id, policy_id, version),
    UNIQUE (tenant_id, policy_sha256)
);

DO $$
DECLARE
    relation_name text;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY['tool_provider', 'tool_catalog', 'agent_profile', 'retention_policy']
    LOOP
        EXECUTE format('ALTER TABLE ngkg_agents.%I ENABLE ROW LEVEL SECURITY', relation_name);
        EXECUTE format('ALTER TABLE ngkg_agents.%I FORCE ROW LEVEL SECURITY', relation_name);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON ngkg_agents.%I USING (tenant_id = ngkg_agents.current_tenant_id()) WITH CHECK (tenant_id = ngkg_agents.current_tenant_id())',
            relation_name
        );
        EXECUTE format(
            'CREATE TRIGGER immutable_rows BEFORE UPDATE OR DELETE ON ngkg_agents.%I FOR EACH ROW EXECUTE FUNCTION ngkg_agents.reject_immutable_mutation()',
            relation_name
        );
    END LOOP;
END
$$;

REVOKE ALL ON SCHEMA ngkg_agents FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA ngkg_agents FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.current_tenant_id() FROM PUBLIC;
REVOKE ALL ON FUNCTION ngkg_agents.reject_immutable_mutation() FROM PUBLIC;
