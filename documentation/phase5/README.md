# Enterprise Stabilization Phase 5 qualification

Phase 5 separates source implementation from production qualification. Run `acceptance.sh` only on a controlled runner after the Phase 3 image, SBOM, signing, PostgreSQL and four-cluster workflow has completed and after every affected Phase 4 runtime regression has been exercised live.

```bash
./phase3/acceptance.sh
./phase5/acceptance.sh /absolute/path/to/signed-evidence
```

The evidence directory must contain signed `phase3-certificate.json`, `phase4-live-certificate.json`, and `phase5-live-certificate.json` documents. The verifier requires every listed scenario to pass and refuses partial, unsigned, synthetic-only, or missing evidence. This source archive intentionally does not contain pre-issued live certificates.

The enterprise Helm overlay enables `nativeCutoverMode: required`. The ordinary default remains `shadow` until those certificates exist so a source build cannot imply that native cutover has been qualified. Required mode returns `NATIVE_CUTOVER_UNAVAILABLE` instead of silently selecting a scalar evaluator when a native operator, distributed plan, exact reasoning worker, or complete partition set is unavailable.
