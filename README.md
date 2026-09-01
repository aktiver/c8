# NGKG MCP Agent Enterprise Stabilization Phase 6

This candidate builds on the Phase 5 native distributed query and reasoning cutover and adds production differential, multinode capacity, saturation, chaos, four-provider Kubernetes and signed release-evidence closure.

The Phase 5 native cutover remains unchanged: production serving uses the Rust-native lane in `required` mode and cannot silently fall back to the scalar oracle. Phase 6 adds a qualification-only oracle comparison, exact SPARQL multiset and RDF graph checks, physical pod/node resource evidence, 80% CPU/RAM autoscaling verification, serialized recovery tests, twelve-image signature/SBOM checks and two-builder reproducibility checks. Apache Jena remains outside the production product and may appear only behind an isolated qualification endpoint.

The new `ngkg-native-runtime` crate contains no Oxigraph or reference-runtime dependency. It provides immutable native-plan admission, scalar-oracle exclusion in required mode, checksum-verified bounded Parquet leaf scans, exact partition barriers, idempotent duplicate completion, conflict rejection, cancellation, and snapshot-bound OWL closure/exact-reasoner coverage contracts. Fragment workers expose the authenticated leaf scan through REST and Swagger/OpenAPI. Graph IDs are derived from the authenticated active dataset on the worker and cannot be supplied as an authorization decision by a caller.

The query service now supports `disabled`, `shadow`, and `required` cutover policies. The ordinary chart defaults to `shadow` for controlled differential qualification. The enterprise overlay uses `required`; unsupported native algebra, missing distributed plans, missing exact-reasoner workers, and incomplete evidence return `NATIVE_CUTOVER_UNAVAILABLE` rather than silently invoking the ordinary scalar runtime. Pure native SELECT bag operators can finalize exact HermiT BGP relations without opening an Oxigraph store.

Run the source gates with:

```bash
cd NGKG_1_0_0_GA
python3 scripts/verify_enterprise_stabilization_phase4.py
python3 scripts/verify_enterprise_stabilization_phase5.py
python3 scripts/verify_enterprise_stabilization_phase6.py
python3 scripts/verify_api_openapi_parity.py
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
```

Production acceptance is deliberately separate:

```bash
cd ..
./phase3/acceptance.sh
./phase5/acceptance.sh /absolute/path/to/signed-evidence
NGKG_PHASE6_EXECUTE_LIVE=YES ./phase6/acceptance.sh /absolute/config-root /absolute/evidence-root
```

The Phase 6 command requires signed Phase 3, affected Phase 4 and Phase 5 live certificates plus real RKE2, EKS, AKS and GKE evidence. This archive contains no synthetic replacement certificate and must not be called production-qualified until that workflow passes.
