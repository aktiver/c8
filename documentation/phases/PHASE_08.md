# Phase 8 — Atomic snapshots and publication

Phase 8 binds every physical and semantic artifact into one immutable manifest. It includes exact table snapshots, ontology, mapping, plans, spine, dictionaries, indexes, locators, proofs, coverage, routing, policy, stage manifests, and a verification report.

Publication first proves the target is certified, then compare-and-swaps the dataset pointer against the parent observed at compilation start. Concurrent updates produce one winner and an explicit conflict. Readers follow only the published NGKG manifest; independently moving Iceberg pointers never define the logical database.

