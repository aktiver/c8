# NGKG 1.0.0 General Availability release boundary

This directory defines the final GA version, compatibility freeze, live qualification ledger, defect disposition, runtime isolation audit, signed artifact set, support matrix, acceptance plan, and go/no-go certificate.

`ga-readiness.json` is intentionally fail-closed in this source candidate. A production operator supplies live same-release certificates and independently generated security, reproducibility, support-matrix, signature, and publication evidence. Static tests may exercise the barrier only in test-harness mode and can never issue a publishable certificate.
