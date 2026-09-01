# Phase 0 — Freeze semantics before optimizing

Phase 0 establishes what later software must preserve. The corpus contains connected named graphs, OWL vocabulary, virtual literal filters, optional bindings, exact expected result multisets, negative syntax/import cases, and a benchmark contract.

The small checked-in corpus is the executable smoke layer. The manifest also records which feature families must be added and independently certified before a Phase 12 production release. The project refuses to market performance from this smoke corpus alone.

## Gate evidence

- Structural validation runs locally with `scripts/structural_validate.py`.
- Rust formatting, compilation, Clippy, and tests require the pinned Rust toolchain.
- Independent OWL/SPARQL reference reproduction requires the certified reference environment described by the workload contract.
- Any unavailable gate remains `blocked`, never `passed`.

