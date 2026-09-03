# MCP Agent Phase 9 delivery report

Phase 9 implements the source and Kubernetes configuration for the complete vLLM/GPU serving plane requested above the Phase 8 CPU/HPC candidate.

The key architectural correction is a separate HA CPU admission gateway. Direct-to-GPU routing cannot safely scale from zero because the first request arrives before a Service endpoint or GPU node exists. `ngkg-inference-gateway` now owns a bounded in-memory cold-start queue, publishes its real waiting depth to Prometheus, and waits within a finite deadline for a ready backend. A completion POST is delivered to a backend once; an ambiguous failure is never retried.

Every GPU pod now contains `ngkg-vllm-pod-agent`. vLLM binds only loopback, while the pod agent verifies engine health and the exact configured model, gates readiness, limits request concurrency and response bytes, and records backend metrics. Its drain controller is loopback-only. During termination the endpoint becomes unready, admitted requests finish, and a shared completion marker releases the vLLM pre-stop hold without imposing the full drain timeout on an idle pod.

The Helm chart creates independent CPU admission and GPU backend Deployments. The CPU tier remains available with at least two replicas and scales at 80% CPU or 80% RAM. KEDA owns the GPU replica count, defaults to zero, activates from `sum(ngkg_inference_waiting_requests)`, and retains 80% CPU and memory triggers after activation. A missing activation signal times out queued calls rather than waiting indefinitely; Prometheus is therefore an HA prerequisite. Whole-GPU request/limit equality and tensor-parallel size are enforced during Helm rendering. Backend replicas are spread across zones and hosts and run only on labeled, tainted GPU pools.

RKE, RKE2, EKS, AKS and GKE scheduling profiles are included. EKS includes a Karpenter NodePool/EC2NodeClass template; AKS and GKE include scale-to-zero node-pool commands; RKE/RKE2 includes the node registration contract. These artifacts create pending-pod demand or infrastructure setup, but NGKG itself never calls cloud scaling APIs.

Both internal inference services are REST-driven and ship OpenAPI 3.1 documents. Operational status is instance-scoped and cannot be used as NGKG semantic evidence. The serving plane has no PostgreSQL, NGKG query/reasoner, raw-input object, or tenant-tool credential access. Model answers remain untrusted until the existing orchestrator completes same-snapshot claim and coverage validation.

Static Phase 1–9 validators, JSON/TOML/non-template YAML parsing, Python compilation and all shell syntax checks passed. Native Rust compilation, Helm rendering, Kubernetes API validation and live GPU/provider tests were unavailable and are explicitly unqualified in the build-status report.

After Phase 9, **four planned source phases remain: Phase 10 through Phase 13**. Next is **Phase 10: Optional Large Context-Slice Broker and Verified mmap Index**—capability-scoped immutable graph artifacts, checksum-verified bounded reads, expiry/garbage collection, corruption handling and HA recovery, enabled only when measurements justify results larger than the inline context ceiling.
