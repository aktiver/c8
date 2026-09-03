# NGKG Agents 0.2.0 build status

**Classification:** source-implemented Phase 2 candidate; not a production-qualified release

**Supplied Phase 1 archive SHA-256:** `8c63da88a2e84403d93bcac28241022b79e8eef097006d517ed4989117d8b49e`

## Executed successfully in this environment

1. The supplied archive contained 1,215 safe entries, had no path traversal, and passed its 1,214-entry bundle manifest and Phase 1 static acceptance before modification.
2. The frozen NGKG GA sibling passed its cumulative GA acceptance both before and after Phase 2 implementation, including the Phase 40.13.24 prerequisite gate, GA freeze/defect/runtime checks, 19 control-plane routes, and 20 online routes.
3. `acceptance/static.sh` passed against a freshly regenerated source manifest and the frozen GA OpenAPI input hashes.
4. All add-on JSON documents parse, including the chart values schema and the new audit/execution contracts.
5. Non-template chart defaults and production values parse as YAML.
6. The Phase 2 SQL structural gate confirms all required relations, forced RLS generation, tenant binding, immutable triggers, serialized audit chaining, and revoked public privileges.
7. Static source scanning found no Rust `unsafe` block or `unwrap`, `expect`, or `panic` call.
8. All Bash acceptance entry points pass `bash -n` validation.

## Blocked and not claimed

This environment does not provide Rust/Cargo, Helm, PostgreSQL/psql, kubectl, a container builder, a live NGKG service, an MCP interoperability client, or an HA Kubernetes cluster. Therefore:

- no Phase 2 `Cargo.lock` was generated and the Rust workspace was not formatted, compiled, linted, or executed;
- Rust unit and database repository tests were not executed;
- the real PostgreSQL forced-RLS, immutability, finalize-once, CAS, audit-concurrency, failover, and retention suite was not run;
- Helm was not linted or rendered and Kubernetes server-side dry run was not performed;
- no migration/rollback was run against an HA PostgreSQL deployment;
- no MCP call was exercised end to end against a live NGKG query service;
- no image was built, scanned, signed, or deployed; and
- HPA at 80%, node provisioning, disruption, network enforcement, and multicloud behavior were not requalified for this add-on revision.

`acceptance/native.sh` and `acceptance/postgres.sh` intentionally fail when their required toolchains or credentials are absent. Static success is not a substitute for these mandatory promotion gates.

## Promotion gate

Before deployment, use the pinned Rust 1.97.1 toolchain and controlled dependency mirror to generate and review `Cargo.lock`; run native acceptance; provision distinct migration/runtime roles in disposable PostgreSQL; run PostgreSQL acceptance under concurrent tenants; render and server-dry-run Helm; test install/upgrade/rollback; run MCP interoperability and live NGKG semantic fixtures; build with immutable base images; scan/sign artifacts; and qualify HA, network policy, audit durability, and autoscaling on every supported Kubernetes profile.
