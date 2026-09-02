# NGKG Phase 39.1 Build Status

Status: **implementation-candidate-not-production-qualified**.

Phase 39.1 hardens Rust reproducibility without altering Phase 39 query semantics. `scripts/generate_cargo_lock.sh` requires the exact workspace Rust/Cargo 1.97.1 resolver, generates `Cargo.lock`, and immediately verifies locked metadata resolution. Release qualification fails closed if the exact resolver or the lock is unavailable.

The archive produced in an environment without Cargo intentionally does not fabricate `Cargo.lock`; such an archive remains a stabilization implementation candidate until the pinned resolver is run and the resulting lockfile is committed into the next qualified candidate.
