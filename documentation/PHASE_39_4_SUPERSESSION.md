# Phase 39.4 Strict Supersession Record

## Phase 21 selective-residency invariant

Phase 21 prohibited materializing the monolithic `data/query-dataset.nq` in the online semantic state because every admitted query was required to use a pre-certified selective route. Phase 39.4 adds a bounded scalar path for supported ad-hoc SPARQL queries that have no offline query certificate.

The old invariant is therefore strictly superseded by two stronger concurrent invariants:

1. certified query hashes continue to use `routed_runtime(...)` and selective query artifacts; and
2. uncertified supported queries may open the immutable full query dataset only through `full_runtime(...)`, with the same authorization, active-dataset, timeout, cancellation, result, graph, and hydration ceilings as the certified scalar path.

This does not claim OWL 2 Direct completeness. The Phase 39.4 fallback is exact for the active RDF dataset plus the already-qualified finite closure; Phase 40 owns legal Direct-BGP classification and exact Direct-reasoner fallback.
