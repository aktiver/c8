# Phase 11A — Operator and distributed stage execution

Phase 11A turns immutable work envelopes into restart-safe multi-node stages. The operator watches concise desired state, loads catalog truth, creates deterministic Indexed Jobs, and requeues without treating pod history as success. Job completion indexes select catalog envelopes; they are never source byte offsets.

Projection pods request equal CPU/memory limits and requests, target a responsibility-specific label/taint, use digest-pinned images, and run through Kueue quota. Duplicate pods are expected; immutable output keys and catalog compare-and-swap choose one logical winner. Reducers and subsequent stages are admitted only after every expected partition manifest validates.

