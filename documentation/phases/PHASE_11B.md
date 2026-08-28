# Phase 11B — Helm and RKE2 deployment

Phase 11B separates CRD, platform, and workload chart lifecycles. Values reject unpinned images, missing dependency Secret references, invalid responsibility names/counts, incompatible provisioners, and competing scaling owners. Cross-field validation runs before Helm.

RKE2 uses distinct Rancher worker machine pools for projection, reasoning, indexing, query, hydration, and maintenance. HPA/KEDA/operator create work, Kueue admits it, and the externally managed Rancher Cluster Autoscaler supplies matching nodes. The chart validates this chain but does not own machine credentials or cluster-wide autoscaler lifecycle.

