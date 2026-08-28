# NGKG Phase 40 Build Status

Status: **phase-40-baseline-implementation-candidate-not-production-qualified**.

Phase 40 is the control baseline built directly on the Phase 39.5 stabilization candidate. It freezes checksum inheritance, publishes the authoritative 40.1–40.27 implementation sequence, records requirement traceability and inherited Phase 15–39.5 gates, declares current OWL Direct capability honestly, and separates inherited HPC ceilings from the future Direct-reasoner ceilings scheduled for Phase 40.10.

The control-plane API now serves a vendored Swagger UI at `/docs` and the embedded contract at both `/openapi.yaml` and `/openapi.json`. The online plane already served those endpoints. `scripts/verify_api_openapi_parity.py` proves that every Axum REST operation in both services is represented in its OpenAPI contract and rejects stale OpenAPI operations.

This phase intentionally introduces **no OWL Direct runtime semantic changes**. OWL signature, datatype policy, Direct-BGP result/certificate contracts, legal-BGP validation and exact Direct reasoner fallback remain subsequent Phase 40.x milestones. Standards claims remain disabled until their executable qualification gates pass.
