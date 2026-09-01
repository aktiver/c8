ALTER TABLE dataset ADD COLUMN dataset_name TEXT;

-- The catalog intentionally forces tenant RLS for application traffic. Schema
-- migrations run as the table owner and temporarily relax FORCE only inside
-- this transaction so the deterministic all-tenant backfill can see existing
-- rows. ENABLE RLS remains in effect for non-owner roles throughout.
ALTER TABLE dataset NO FORCE ROW LEVEL SECURITY;

UPDATE dataset
SET dataset_name = dataset_id::text
WHERE dataset_name IS NULL;

ALTER TABLE dataset ALTER COLUMN dataset_name SET NOT NULL;
ALTER TABLE dataset ADD CONSTRAINT dataset_name_format
  CHECK (dataset_name ~ '^[a-z][a-z0-9_]{0,62}$' OR dataset_name = dataset_id::text);
ALTER TABLE dataset ADD CONSTRAINT dataset_tenant_name_unique
  UNIQUE (tenant_id, dataset_name);

ALTER TABLE dataset FORCE ROW LEVEL SECURITY;
