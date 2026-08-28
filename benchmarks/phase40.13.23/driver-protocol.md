# Performance driver protocol

The qualification coordinator invokes a driver argument vector directly and appends one absolute request JSON path. It never invokes a shell. The NGKG driver must identify itself as `ngkg-rust`; an applicable competitor driver identifies itself as `external-apache-jena`. Versions must match the content-bound inventory.

The request fixes the run, scenario, family, warm-up/measured phase, trial index, cache state, concurrency, hardware/pricing/autoscaling hashes, resource envelope, and content-addressed operation descriptor. The driver must reset or prove the requested cache state before it starts its monotonic clock.

The driver emits exactly one bounded JSON object containing duration, completed operations, family-specific work items, input/output volume, CPU time, peak RSS, bytes read/written, activated nodes/CPU/RAM, canonical semantic-result SHA-256, optional artifact-root SHA-256, autoscaling evidence SHA-256, cost, and terminal completeness. Failed or partial execution is an error, not a measurement. All warm-ups are retained with `trialPhase: warmup`; only the predetermined measured indices enter percentiles, and no post-hoc outlier removal exists.

For ingestion, `workItems` is decoded input bytes. For semantic compilation it is input triples. For reasoning it is asserted plus derived axioms processed. For property paths it is scanned edges. For SPARQL it is completed queries. For recovery it is checksum-verified restored bytes. These units make throughput and cost normalization explicit rather than mixing unrelated counters.

Apache Jena runs outside the NGKG product boundary and is used only where the inventory requires a competitor comparison. The baseline must receive the same dataset, snapshot semantics, cache state, client concurrency, hardware class, CPU/RAM ceiling, and correctness expectation. Jena code is never linked into a Rust crate or copied into a production NGKG image.
