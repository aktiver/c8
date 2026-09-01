# Phase 7 — Certified OWL 2 DL reasoning

Phase 7 adds a real, versioned, mTLS reasoner boundary and query-plan-specific coverage certificates. Reasoner input contains TBox, RBox, `core` ABox, bridges, identity, and reasoning-critical datatypes for the selected modules.

The client rejects plain HTTP, snapshot/input/version mismatches, inconsistent ontologies under the exact policy, and incomplete artifacts. The planner has only three outcomes: certified compiled execution, dependency expansion and recertification, or an exact reasoner path. There is no partial exact result.

