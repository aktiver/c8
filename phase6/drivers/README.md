# Phase 6 site drivers

`api_driver.py` is the included runner-neutral driver. Capacity, chaos and provider-integration sections may point to separate installed copies of this exact file and pin each copy's SHA-256. The driver sends the controlled request to a site-owned HTTPS qualification API using a private bearer-token file, optional mTLS client certificate, and a pinned CA. It rejects redirects by deployment policy, bounds request/response bytes, and binds the response to action, provider and release subject.

The site API performs real operations: submit benchmark executions, capture Prometheus/cgroup/container-runtime samples, watch HPA/KEDA and node-provisioner events, allocate/drain a GPU pod, inject approved failures, test cross-tenant denials, and return the evidence fields enforced by `qualify_provider.py`. It must retain its raw content-addressed evidence independently. Redirects are disabled. The driver cannot turn a synthetic result into a pass because the issuer requires Kubernetes identities, exact trial cardinality, GPU time, semantic hashes and referenced evidence.

Required environment:

- `NGKG_PHASE6_DRIVER_API`
- `NGKG_PHASE6_DRIVER_CA_FILE`
- `NGKG_PHASE6_DRIVER_TOKEN_FILE`
- optional `NGKG_PHASE6_DRIVER_CLIENT_CERT` and `NGKG_PHASE6_DRIVER_CLIENT_KEY`
- optional `NGKG_PHASE6_DRIVER_TIMEOUT_SECONDS`
