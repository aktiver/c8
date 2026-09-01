# Provider overlays

These files are incomplete, non-secret starting points. Replace every placeholder, bind the service account to a prefix-limited workload identity, configure bucket-default KMS encryption, place exact provider egress CIDRs in `contextSlice.storage.egressIpBlocks`, and supply the separately created capability and database Secrets. RKE/RKE2 may use S3-compatible storage only after its endpoint, TLS chain, identity provider, HA behavior, conditional-write behavior, and encryption controls pass the same qualification suite.

The cluster/node autoscaler is intentionally external to this chart. Broker CPU and memory requests plus the `ngkg.io/workload-class=cpu-hpc` selector are the scheduling contract used by Karpenter, EKS/AKS/GKE Cluster Autoscaler, or RKE2 node provisioning.
