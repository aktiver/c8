# NGKG Agents 0.1.0 build status

**Classification:** source-implemented first slice; not a production-qualified release

**Frozen NGKG baseline archive SHA-256:** `749f753bdce355ad8022ba027979ecd37de617c26502c4dc33b6c1a780d0eb58`

**Engineering-plan SHA-256:** `0ccf6d2ae57fed5fa8ac2f9aec9de23ea3bd374657b60f944a3386b7887c84d0`

## Passed in this environment

1. The original NGKG GA tree remained separate and unchanged. Its cumulative `acceptance/ga.sh` passed, including Phase 40.13.24 prerequisites, GA freeze/defect/runtime checks, OpenAPI parity with 19 control-plane and 20 online operations, and structural validation.
2. `acceptance/static.sh` passed. It verified required source/contracts/chart files, JSON syntax, frozen OpenAPI input hashes, the public-client route boundary, forbidden internal dependencies, Host/Origin guards, disabled redirects, semantic status vocabulary, 80% CPU and memory targets, read-only root filesystem, and default-deny policy.
3. All add-on JSON documents parsed successfully.
4. Chart metadata, defaults, and example values parsed as YAML.
5. Both acceptance scripts passed `bash -n` structural validation.
6. No Rust `unsafe` block, `unwrap`, `expect`, or `panic` call was found by source scan.

## Blocked and therefore not claimed

The environment does not provide `rustc`, Cargo, Helm, kubectl, a container builder, an NGKG runtime, an MCP client, PostgreSQL, or an HA Kubernetes cluster. Consequently:

- no `Cargo.lock` was generated;
- the Rust crates were not formatted, compiled, linted, or executed;
- Rust unit tests were not run;
- MCP initialize/list/call interoperability was not run;
- the Helm templates were not rendered or linted;
- Kubernetes API dry-run, HPA behavior, topology placement, disruption, and network-policy behavior were not tested;
- no container was built or scanned; and
- no end-to-end semantic comparison against a live NGKG query was performed.

`acceptance/native.sh` intentionally exits nonzero until Cargo, a generated lockfile, and Helm are available. Passing static gates is not a substitute for those blocked gates.

## Required promotion gate

Before any deployment, generate `Cargo.lock` using Rust/Cargo 1.97.1 from a controlled dependency mirror, run the full native acceptance script, build with immutable reviewed base-image digests, render and server-dry-run the chart, run official MCP client interoperability, execute the semantic corpus against a live certified NGKG snapshot, and qualify HA/autoscaling on supported Kubernetes providers.
