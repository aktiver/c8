# Phase 9 — Distributed OWL-aware SPARQL

Phase 9 authorizes first, pins one snapshot, builds the exact RDF dataset, lowers SPARQL algebra, routes relevant graph/table dependencies, and emits inspectable graph, virtual-RDF, and reasoner fragments. Required but unauthorized dependencies fail instead of disappearing from the plan.

Workers exchange compact encoded bindings. Exact semijoins preserve multiplicity; joins multiply bag counts with overflow checks; Bloom filters may only prune before an exact step. Variable-length paths distribute `(EntityID64, automaton_state)` frontiers. A partial Flight stream is an error, not end-of-data, and final success still requires the Phase 7 coverage gate.

