# NGKG Phase 36/37 Build Status

Generated from the cumulative Phase 35 baseline during the Phase 36/37 compliance implementation.

## Status

- Phase 36: implementation candidate; not production-qualified.
- Phase 37: implementation candidate; not production-qualified.
- Public SPARQL 1.1 / union-default / OWL Direct / OWL 2 DL feature claims remain disabled.

## Executed successfully in this build environment

- Repository structural validation.
- Phase 30 through Phase 35 cumulative static compatibility checks.
- Phase 36 static contract validation.
- Phase 37 static contract validation.
- Platform autoscaling/value cross-field validation.
- RKE2 workload overlay/value validation.
- Python syntax validation for the immutable W3C conformance fetcher.
- Shell syntax validation for Phase 36, Phase 37, and release qualification scripts.
- JSON Schema meta-validation for all checked-in contract schemas.
- OpenAPI 3.1 parsing and local-reference validation for the control and online APIs.
- Git whitespace/error validation with `git diff --check`.
- Changed-source placeholder/fake-logic scan.

## Live gates not executable in this environment

The following commands/toolchains are not installed here and therefore are not recorded as passing:

- Cargo / rustc / rustfmt / Clippy.
- Maven.
- Helm.
- kubectl and a live RKE2 API server.
- The pinned W3C suite checkout, because this container cannot resolve the upstream Git host directly.

`Cargo.lock` is intentionally not fabricated. The release gate requires Cargo to generate it and all Rust release commands run with `--locked`.

## Release rule

A release remains blocked until `scripts/ci_release.sh` and `scripts/qualify_phase37.sh` execute successfully in a toolchain-complete RKE2 qualification environment. Missing dependencies, unavailable workers, incomplete results, checksum mismatches, authorization mismatches, and conformance failures are errors rather than successful partial qualification.
