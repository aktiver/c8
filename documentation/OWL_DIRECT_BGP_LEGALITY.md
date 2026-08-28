# OWL 2 Direct-Semantics BGP Legality — Phase 40.7

Phase 40.7 is the query-admission boundary between typed SPARQL and the exact OWL Direct reasoner path. It does not answer a BGP. It decides whether each SPARQL basic graph pattern can be admitted for OWL 2 Direct-Semantics processing and emits deterministic snapshot-bound evidence consumed by Phase 40.8.

## Standards model

The classifier follows the SPARQL 1.1 Entailment Regimes OWL Direct mapping model:

1. SPARQL is parsed once into typed algebra by `ngkg-sparql-compiler`.
2. Declarations from the checksum-bound Phase 40.1 OWL signature disambiguate constant class/property/datatype/entity IRIs.
3. BGP-local variable declarations are isolated per BGP; no typing state crosses `UNION`, `OPTIONAL`, subquery or other BGP boundaries.
4. Variables used as classes, object/data/annotation properties or datatypes require compatible declarations; undeclared variables may only resolve unambiguously through individual/literal positions.
5. Variables in cardinality numbers and datatype-facet values fail closed.
6. Unknown predicates, conflicting roles and unsupported/ambiguous OWL structural shapes fail closed.
7. Property-path algebra is recorded separately because SPARQL entailment regimes extend BGP matching, not arbitrary path algebra.

The report deliberately retains `groundedOwl2dlCheckRequired=true`. Phase 40.8 must instantiate each candidate and prove the resulting axioms remain legal OWL 2 DL when added to the active ontology (the W3C C3 condition) before any solution can become exact/complete.

## Active graph and authorization

Authorization is applied before the combined semantic signature can be loaded. The report binds both `authorizedGraphSetSha256` and `activeDatasetSha256`, and every BGP records whether it belongs to the active default graph, an explicit named graph or `GRAPH ?g`.

Phase 40.7 is an admission preflight. Named-graph/`GRAPH ?g` candidate execution in Phase 40.8 must reconstruct and validate the correct active-graph ontology for each candidate graph; the combined signature is a declaration index, not permission to merge named graphs during entailment.

## Deterministic HPC classification

Independent BGP leaves are classified across bounded CPU lanes using `available_parallelism()`, capped at 32 in Phase 40.7. Each lane works on immutable typed algebra and immutable `BTreeSet` signature indexes. Results are sorted back to typed-algebra preorder before serialization. CPU count, scheduling order and Kubernetes node placement therefore cannot change a legality report.

Phase 40.10 replaces temporary classification ceilings with authoritative Helm/operator values. Distributing this small compiler step across Kubernetes nodes would add network overhead without useful throughput; multi-node distribution begins where candidate entailment work is expensive in Phase 40.8/40.15.

## Runtime API

`POST /v1/datasets/{datasetId}/sparql/direct/validate`

The endpoint applies authentication, graph authorization, SPARQL Protocol/query dataset precedence and requested-snapshot checks before classification. It returns `DirectBgpLegalityReport` and is exposed in `/docs`, `/openapi.yaml` and `/openapi.json`.

This endpoint does not enable the OWL Direct Service Description claim. Standards claims remain disabled until exact fallback, proof/certificate and conformance gates pass.
