# Phase 40.10 — Authoritative Helm ceilings

Phase 40.10 replaces the temporary Phase 40.7–40.9 semantic/HPC default registry with closed, schema-validated Helm values. It deliberately **does not consume or propagate those values yet**: reference-worker wiring is Phase 40.11, distributed/online worker wiring is Phase 40.12, and operator/distributed-operator propagation is Phase 40.13.

## Workloads chart — Direct-BGP admission

`phase40.directAdmission` declares the maximum BGP leaves, triples per BGP, and bounded CPU classification lanes. The defaults exactly match the Phase 40.7 hard bounds and remain scheduling-independent because the classifier restores typed-algebra order after parallel work.

## Platform chart — exact Direct reasoning

`phase40.direct` declares the complete Phase 40.8/40.9 exact-path budget: candidate bindings, candidates per partition, maximum partition count, grounded OWL axioms/RDF bytes, concurrent HermiT lanes, JVM heap per lane, partition timeout, certificate bytes, and proof-support IDs.

The cross-field validator requires the candidate-space budget to fit within the partition-count budget and checks the default lane×heap allocation against the reference-worker pod memory with an 80% safety threshold. It also prevents the Helm proof-support ceiling from exceeding the Phase 40.9 one-million-record runtime hard cap.

## HPC invariant

Helm controls ceilings; CPU count controls throughput only. The declared values do not change candidate ordinals, BGP ordering, exact entailment semantics, proof support IDs, or certificate identity.
