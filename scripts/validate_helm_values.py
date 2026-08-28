#!/usr/bin/env python3
"""Cross-field NGKG Helm validation that JSON Schema alone cannot express."""

from __future__ import annotations

import argparse
import pathlib
import re
import sys

import yaml


RESPONSIBILITIES = {
    "semanticProjection": "semantic_projection_num_of_nodes",
    "semanticArtifactBuild": "semantic_artifact_build_num_of_nodes",
    "reasoning": "reasoning_num_of_nodes",
    "indexBuild": "index_build_num_of_nodes",
    "sparqlQueryProcessing": "sparql_query_processing_num_of_nodes",
    "sparqlFragmentProcessing": "sparql_fragment_processing_num_of_nodes",
    "parquetHydration": "parquet_hydration_num_of_nodes",
    "maintenanceExport": "maintenance_export_num_of_nodes",
    "storageRecovery": "storage_recovery_num_of_nodes",
}


def binary_quantity(value: str) -> int:
    match = re.fullmatch(r"([1-9][0-9]*)(Ki|Mi|Gi|Ti)", value)
    if not match:
        raise ValueError(f"unsupported binary Kubernetes quantity: {value}")
    shifts = {"Ki": 10, "Mi": 20, "Gi": 30, "Ti": 40}
    return int(match.group(1)) << shifts[match.group(2)]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("values", type=pathlib.Path)
    parser.add_argument("--overlay", type=pathlib.Path, action="append", default=[])
    args = parser.parse_args()
    values = yaml.safe_load(args.values.read_text(encoding="utf-8"))
    for overlay_path in args.overlay:
        overlay = yaml.safe_load(overlay_path.read_text(encoding="utf-8"))
        merge(values, overlay)
    errors: list[str] = []
    groups = values["hpcNodeGroups"]
    for responsibility, count_name in RESPONSIBILITIES.items():
        initial = groups[count_name]
        policy = values["autoscaling"][responsibility]
        if initial < policy["minNodes"] or initial > policy["maxNodes"]:
            errors.append(f"{count_name}={initial} is outside [{policy['minNodes']}, {policy['maxNodes']}]")
    owners = [values["autoscaling"][name]["owner"] for name in RESPONSIBILITIES]
    if any(owner not in {"operator", "hpa", "keda"} for owner in owners):
        errors.append("every workload must have exactly one supported scaling owner")
    distribution = values["platform"]["kubernetesDistribution"]
    provisioner = groups["provisioner"]
    if distribution == "rke2" and provisioner not in {"existing", "cluster-autoscaler"}:
        errors.append("RKE2 requires existing capacity or Cluster Autoscaler in the validated base profile")
    autoscaler = values["rke2"]["clusterAutoscaler"]
    if autoscaler["provider"] == "rancher" and not autoscaler["existingCloudConfigSecret"]:
        errors.append("Rancher Cluster Autoscaler requires an existing cloud-config secret reference")
    if values["hpcRuntime"]["nestedParallelism"]:
        errors.append("nested native parallelism is forbidden")
    saturation = values["hpcRuntime"]["nodeSaturationTargetPercent"]
    if saturation < 1 or saturation > 80:
        errors.append("nodeSaturationTargetPercent must be between 1 and 80")
    for metric in ("cpuUtilizationTargetPercent", "memoryUtilizationTargetPercent"):
        target = values["metrics"][metric]
        if target < 1 or target > saturation:
            errors.append(f"{metric} must be between 1 and nodeSaturationTargetPercent")
    production = values["productionAutoscaling"]
    if production["enabled"]:
        node_provider = values["nodeProvisioning"]["provider"]
        allowed = {
            "rke": ({"rancher-cluster-autoscaler"}, {"cluster-autoscaler"}),
            "rke2": ({"rancher-cluster-autoscaler"}, {"cluster-autoscaler"}),
            "eks": ({"eks-karpenter", "eks-cluster-autoscaler"}, {"karpenter", "cluster-autoscaler"}),
            "aks": ({"aks-managed-cluster-autoscaler"}, {"cluster-autoscaler"}),
            "gke": ({"gke-managed-cluster-autoscaler"}, {"cluster-autoscaler"}),
            "generic": ({"external-cluster-autoscaler"}, {"cluster-autoscaler"}),
        }
        providers, provisioners = allowed[distribution]
        if node_provider not in providers:
            errors.append(f"{distribution} production autoscaling has an incompatible node provider")
        if provisioner not in provisioners:
            errors.append(f"{distribution} production autoscaling has an incompatible provisioner")
        if not values["nodeProvisioning"]["workloadName"]:
            errors.append("production autoscaling requires an observable node-provisioner workload name")
        if not values["nodeProvisioning"]["discoveryResource"]:
            errors.append("production autoscaling requires a provider discovery resource")
        if not values["metricsApis"]["requireResourceMetrics"]:
            errors.append("production autoscaling requires metrics.k8s.io")
        if not values["metricsApis"]["requireCustomMetrics"]:
            errors.append("production autoscaling requires custom.metrics.k8s.io")
        if values["batchScheduling"]["mode"] != "kueue":
            errors.append("production autoscaling requires Kueue admission")
        if not values["metrics"]["workloadAwareAutoscalingEnabled"]:
            errors.append("production autoscaling requires workload-aware metrics")
        if "keda" not in owners:
            errors.append("production autoscaling qualification requires one KEDA-owned workload")
        if production["cpuTargetPercent"] != 80 or production["memoryTargetPercent"] != 80:
            errors.append("production autoscaling CPU and memory targets must equal 80")
        zero_pools = ("sourceIngestion", "semanticProjection", "semanticArtifactBuild", "reasoning", "indexBuild", "storageRecovery")
        if any(values["autoscaling"][name]["minNodes"] != 0 for name in zero_pools):
            errors.append("all qualified batch responsibility pools must scale from zero")
    online = values["onlineServing"]
    fragment_bytes = int(online["maxFragmentResponseBytes"])
    exchange_bytes = int(online["maxDistributedExchangeBytes"])
    fragment_response_spool_limit = binary_quantity(online["fragmentResponseSpoolSizeLimit"])
    fragment_response_spool_bytes = int(online["maxFragmentResponseSpoolBytes"])
    if fragment_bytes > exchange_bytes:
        errors.append("maxFragmentResponseBytes cannot exceed maxDistributedExchangeBytes")
    if fragment_response_spool_bytes > fragment_response_spool_limit:
        errors.append("maxFragmentResponseSpoolBytes cannot exceed fragmentResponseSpoolSizeLimit")
    if exchange_bytes > fragment_response_spool_bytes:
        errors.append("maxDistributedExchangeBytes cannot exceed maxFragmentResponseSpoolBytes")
    if int(online["fragmentExchangeConcurrency"]) > int(online["maxDistributedFragments"]):
        errors.append("fragmentExchangeConcurrency cannot exceed maxDistributedFragments")
    algebra_replicas = int(online["distributedAlgebraReplicas"])
    if online["distributedAlgebraEnabled"] and algebra_replicas < 2:
        errors.append("distributedAlgebraReplicas must be at least two when enabled")
    if algebra_replicas > int(online["fragmentExchangeConcurrency"]):
        errors.append("distributedAlgebraReplicas cannot exceed fragmentExchangeConcurrency")
    if algebra_replicas > int(online["maxDistributedFragments"]):
        errors.append("distributedAlgebraReplicas cannot exceed maxDistributedFragments")
    if algebra_replicas > int(values["autoscaling"]["sparqlFragmentProcessing"]["minNodes"]):
        errors.append("sparqlFragmentProcessing.minNodes must cover the algebra replica barrier")
    path_threads = int(online["propertyPathWorkerThreads"])
    if online["partitionNativePathsEnabled"] and path_threads > int(online["fragmentExchangeConcurrency"]):
        errors.append("propertyPathWorkerThreads cannot exceed fragmentExchangeConcurrency")
    if int(online["propertyPathMaxScanRows"]) < int(online["propertyPathMaxFrontierItems"]):
        errors.append("propertyPathMaxScanRows cannot be smaller than propertyPathMaxFrontierItems")
    if int(online["fragmentArrowBatchRows"]) > int(online["maxDistributedIntermediateRows"]):
        errors.append("fragmentArrowBatchRows cannot exceed maxDistributedIntermediateRows")
    if int(online["maxQueryResultRows"]) > int(online["maxDistributedIntermediateRows"]):
        errors.append("maxQueryResultRows cannot exceed maxDistributedIntermediateRows for distributed SELECT equivalence")
    if int(online["maxQueryGraphBlankNodes"]) > 2 * int(online["maxQueryGraphTriples"]):
        errors.append("maxQueryGraphBlankNodes cannot exceed twice maxQueryGraphTriples")
    shuffle_partitions = int(online["shufflePartitions"])
    shuffle_concurrency = int(online["shuffleExchangeConcurrency"])
    shuffle_request_bytes = int(online["maxShuffleRequestBytes"])
    shuffle_response_bytes = int(online["maxShuffleResponseBytes"])
    shuffle_exchange_bytes = int(online["maxShuffleExchangeBytes"])
    if shuffle_partitions < 2:
        errors.append("shufflePartitions must be at least two")
    if shuffle_concurrency > shuffle_partitions:
        errors.append("shuffleExchangeConcurrency cannot exceed shufflePartitions")
    if shuffle_request_bytes > int(online["maxRequestBytes"]):
        errors.append("maxShuffleRequestBytes cannot exceed maxRequestBytes")
    if shuffle_request_bytes > shuffle_exchange_bytes:
        errors.append("maxShuffleRequestBytes cannot exceed maxShuffleExchangeBytes")
    if shuffle_response_bytes > shuffle_exchange_bytes:
        errors.append("maxShuffleResponseBytes cannot exceed maxShuffleExchangeBytes")
    if shuffle_exchange_bytes > fragment_response_spool_bytes:
        errors.append("maxShuffleExchangeBytes cannot exceed maxFragmentResponseSpoolBytes")
    shuffle_spill_bytes = int(online["maxShuffleSpillBytes"])
    shuffle_spill_limit = binary_quantity(online["shuffleSpillSizeLimit"])
    if shuffle_spill_bytes > shuffle_spill_limit:
        errors.append("maxShuffleSpillBytes cannot exceed shuffleSpillSizeLimit")
    if int(online["maxShuffleOpenFiles"]) < 2 * shuffle_partitions:
        errors.append("maxShuffleOpenFiles must allow two writers per shuffle partition")
    path_frontier = int(online["propertyPathMaxFrontierItems"])
    path_visited = int(online["propertyPathMaxVisitedItems"])
    path_checkpoint = int(online["propertyPathMaxCheckpointBytes"])
    path_spill = int(online["propertyPathMaxSpillBytes"])
    if path_visited < path_frontier:
        errors.append("propertyPathMaxVisitedItems cannot be smaller than the frontier ceiling")
    if path_checkpoint > path_spill:
        errors.append("propertyPathMaxCheckpointBytes cannot exceed propertyPathMaxSpillBytes")
    if path_spill > shuffle_spill_bytes:
        errors.append("propertyPathMaxSpillBytes must fit the shared shuffle spill budget")
    if path_frontier * 128 > shuffle_request_bytes:
        errors.append("propertyPathMaxFrontierItems cannot fit the bounded internal request envelope")
    if int(online["propertyPathMaxHotVertexSplits"]) < 2:
        errors.append("propertyPathMaxHotVertexSplits must be at least two")
    shuffle_cache_limit = binary_quantity(online["shuffleCacheSizeLimit"])
    shuffle_cache_bytes = int(online["maxShuffleCacheBytes"])
    shuffle_cache_entry_bytes = int(online["maxShuffleCacheEntryBytes"])
    if shuffle_cache_bytes > shuffle_cache_limit:
        errors.append("maxShuffleCacheBytes cannot exceed shuffleCacheSizeLimit")
    if shuffle_cache_entry_bytes > shuffle_cache_bytes:
        errors.append("maxShuffleCacheEntryBytes cannot exceed maxShuffleCacheBytes")
    worker_join_limit = binary_quantity(online["workerJoinSpillSizeLimit"])
    worker_join_total = int(online["maxWorkerJoinSpillBytes"])
    worker_join_request = int(online["maxWorkerJoinSpillBytesPerRequest"])
    worker_join_buckets = int(online["workerJoinBuckets"])
    worker_join_build = int(online["maxWorkerJoinBuildRows"])
    worker_join_probe = int(online["maxWorkerJoinProbeRows"])
    if worker_join_total > worker_join_limit:
        errors.append("maxWorkerJoinSpillBytes cannot exceed workerJoinSpillSizeLimit")
    if worker_join_request > worker_join_total:
        errors.append("maxWorkerJoinSpillBytesPerRequest cannot exceed maxWorkerJoinSpillBytes")
    if worker_join_buckets < 2:
        errors.append("workerJoinBuckets must be at least two")
    if int(online["maxWorkerJoinOpenFiles"]) < 2 * worker_join_buckets:
        errors.append("maxWorkerJoinOpenFiles must allow two writers per worker join bucket")
    if worker_join_build > int(online["maxDistributedIntermediateRows"]):
        errors.append("maxWorkerJoinBuildRows cannot exceed maxDistributedIntermediateRows")
    if worker_join_probe > int(online["maxDistributedIntermediateRows"]):
        errors.append("maxWorkerJoinProbeRows cannot exceed maxDistributedIntermediateRows")
    if int(online["inMemoryJoinBuildRows"]) > worker_join_build:
        errors.append("inMemoryJoinBuildRows cannot exceed maxWorkerJoinBuildRows")
    if int(online["maxWorkerJoinRowBytes"]) > shuffle_request_bytes:
        errors.append("maxWorkerJoinRowBytes cannot exceed maxShuffleRequestBytes")
    request_spool_limit = binary_quantity(online["streamingRequestSpoolSizeLimit"])
    request_spool_total = int(online["maxStreamingRequestSpoolBytes"])
    if request_spool_total > request_spool_limit:
        errors.append("maxStreamingRequestSpoolBytes cannot exceed streamingRequestSpoolSizeLimit")
    if shuffle_request_bytes > request_spool_total:
        errors.append("maxShuffleRequestBytes cannot exceed maxStreamingRequestSpoolBytes")
    query_cache_limit = binary_quantity(online["queryResultCacheSizeLimit"])
    query_cache_bytes = int(online["maxQueryResultCacheBytes"])
    query_cache_entry_bytes = int(online["maxQueryResultCacheEntryBytes"])
    if query_cache_bytes > query_cache_limit:
        errors.append("maxQueryResultCacheBytes cannot exceed queryResultCacheSizeLimit")
    if query_cache_entry_bytes > query_cache_bytes:
        errors.append("maxQueryResultCacheEntryBytes cannot exceed maxQueryResultCacheBytes")
    if query_cache_entry_bytes < int(online["maxQueryResponseBytes"]) + 80:
        errors.append("maxQueryResultCacheEntryBytes must cover maxQueryResponseBytes plus the cache header")
    if int(online["maxQueryResultCacheEntries"]) > 100000:
        errors.append("maxQueryResultCacheEntries cannot exceed the reviewed Helm ceiling of 100000")
    fragment_worker_in_flight = int(online["maxFragmentWorkerInFlight"])
    if int(online["maxFragmentInFlight"]) > fragment_worker_in_flight:
        errors.append("maxFragmentInFlight cannot exceed maxFragmentWorkerInFlight")
    if int(online["maxShuffleInFlight"]) > fragment_worker_in_flight:
        errors.append("maxShuffleInFlight cannot exceed maxFragmentWorkerInFlight")
    if int(online["admissionWaitMilliseconds"]) > 5000:
        errors.append("admissionWaitMilliseconds cannot exceed 5000")
    for checksum_name in ("authTokensFileSha256", "tenantAdmissionPolicySha256"):
        checksum = online[checksum_name]
        if checksum and not re.fullmatch(r"[0-9a-f]{64}", checksum):
            errors.append(f"{checksum_name} must be lowercase SHA-256")
    if int(online["maxAdmissionTenants"]) > 100000:
        errors.append("maxAdmissionTenants cannot exceed the reviewed Helm ceiling of 100000")
    for name in (
        "maxQueryInFlight", "maxFragmentWorkerInFlight", "maxFragmentInFlight",
        "maxShuffleInFlight", "maxLocatorInFlight", "maxHydrationInFlight",
        "maxQueryPending", "maxFragmentPending", "maxShufflePending",
        "maxLocatorPending", "maxHydrationPending",
    ):
        if int(online[name]) > 100000:
            errors.append(f"{name} cannot exceed the reviewed Helm ceiling of 100000")
    arrow_channel_bytes = int(online["fragmentArrowHttpChunkBytes"]) * int(online["fragmentArrowChannelCapacity"])
    if arrow_channel_bytes > fragment_bytes:
        errors.append("Arrow HTTP chunk bytes multiplied by channel capacity cannot exceed maxFragmentResponseBytes")
    if arrow_channel_bytes > shuffle_response_bytes:
        errors.append("Arrow HTTP chunk bytes multiplied by channel capacity cannot exceed maxShuffleResponseBytes")
    if arrow_channel_bytes > shuffle_request_bytes:
        errors.append("Arrow HTTP chunk bytes multiplied by channel capacity cannot exceed maxShuffleRequestBytes")
    if int(online["maxQueryResponseBytes"]) < int(online["maxHydrationResponseBytes"]):
        errors.append("maxQueryResponseBytes cannot be smaller than maxHydrationResponseBytes")
    if values["networking"]["fragmentIpFamily"] not in {"IPv4", "IPv6"}:
        errors.append("fragmentIpFamily must be IPv4 or IPv6")
    if values["networking"]["internalExchange"] != "certified-arrow-ipc-rest":
        errors.append("internalExchange must match the implemented certified Arrow IPC REST transport")
    if values["hpcRuntime"]["guaranteedQos"]:
        for workload, resources in values["resources"].items():
            if resources["requests"] != resources["limits"]:
                errors.append(f"{workload} requests and limits must match for Guaranteed QoS")
    query_ephemeral = values["resources"]["query"]["requests"].get("ephemeral-storage")
    if query_ephemeral is None:
        errors.append("query resources must reserve ephemeral-storage for cache and shuffle spill")
    elif binary_quantity(query_ephemeral) < binary_quantity(online["cacheSizeLimit"]) + shuffle_spill_limit + query_cache_limit + fragment_response_spool_limit:
        errors.append("query ephemeral-storage request must cover cacheSizeLimit plus shuffleSpillSizeLimit plus queryResultCacheSizeLimit plus fragmentResponseSpoolSizeLimit")
    fragment_ephemeral = values["resources"]["fragment"]["requests"].get("ephemeral-storage")
    if fragment_ephemeral is None:
        errors.append("fragment resources must reserve ephemeral-storage for immutable and shuffle caches")
    elif binary_quantity(fragment_ephemeral) < binary_quantity(online["cacheSizeLimit"]) + shuffle_cache_limit + worker_join_limit + request_spool_limit:
        errors.append("fragment ephemeral-storage request must cover cacheSizeLimit plus shuffleCacheSizeLimit plus workerJoinSpillSizeLimit plus streamingRequestSpoolSizeLimit")
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


def merge(base: dict, overlay: dict) -> None:
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merge(base[key], value)
        else:
            base[key] = value


if __name__ == "__main__":
    raise SystemExit(main())
