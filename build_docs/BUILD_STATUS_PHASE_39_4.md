# NGKG Phase 39.4 Build Status

Status: **implementation-candidate-not-production-qualified**.

Phase 39.4 removes the offline query-hash whitelist as a correctness prerequisite for ordinary SPARQL 1.1 evaluation. A query that has a valid Phase 39 certificate keeps the existing cache/routed/distributed path. A supported query without a certificate is executed by the bounded, cancellable scalar evaluator over the authorization- and dataset-resolved immutable full query dataset. Deployment ceilings, timeout behavior, hydration authorization, snapshot identity, and protocol serialization remain enforced.

The ad-hoc scalar path is explicitly labeled `phase39-exact-rdf-plus-qualified-finite-closure-v1`; it does **not** claim arbitrary OWL 2 Direct-Semantics completeness. Phase 40 must replace/augment this semantic boundary with legal-Direct-BGP qualification and complete reasoner fallback.
