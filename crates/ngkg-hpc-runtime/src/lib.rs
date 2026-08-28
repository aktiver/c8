//! CPU-set-aware per-pod thread budgeting for Rust, OpenMP and BLAS.

use std::{env, fs, path::Path};

use serde::Serialize;
use thiserror::Error;

/// Mutually budgeted execution resources inside one Kubernetes cpuset.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ThreadBudget {
    pub rust_compute: usize,
    pub blocking_io: usize,
    pub openmp: usize,
    pub blas: usize,
    pub control: usize,
}

/// Startup report exported to logs and metrics.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CapabilityReport {
    pub cpuset_cores: usize,
    pub cpuset_source: String,
    pub budget: ThreadBudget,
    pub node_saturation_target_percent: u8,
    pub omp_num_threads: usize,
    pub openblas_num_threads: usize,
    pub mkl_num_threads: usize,
}

/// Cgroup-derived CPU and memory envelope used by sparse workers before they
/// allocate multipart, Arrow, Parquet, spill, or reasoning lanes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResourceEnvelopeReport {
    pub cpuset_cores: usize,
    pub cpuset_source: String,
    pub memory_limit_bytes: u64,
    pub memory_current_bytes: u64,
    pub usable_memory_bytes: u64,
    pub memory_headroom_bytes: u64,
    pub saturation_target_percent: u8,
    pub memory_saturated: bool,
}

/// Invalid runtime topology is a startup failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ThreadBudgetError {
    #[error("thread budget {requested} exceeds assigned CPU set {available}")]
    Oversubscribed { requested: usize, available: usize },
    #[error("nested native parallelism is not allowed")]
    NestedParallelism,
    #[error("CPU set is unavailable or malformed: {0}")]
    CpuSet(String),
    #[error("environment variable {name} must be a positive integer")]
    InvalidEnvironment { name: &'static str },
    #[error("native runtime thread configuration disagrees with NGKG thread budget")]
    NativeRuntimeMismatch,
    #[error("node saturation target must be between 1 and 80 percent")]
    InvalidSaturationTarget,
    #[error("cgroup memory limit or usage is unavailable or malformed: {0}")]
    CgroupMemory(String),
    #[error("bounded worker buffers require {requested} bytes but only {available} bytes remain inside the 80-percent envelope")]
    MemoryBudget { requested: u64, available: u64 },
}

/// Highest supported steady-state utilization before NGKG asks Kubernetes to scale.
pub const MAX_NODE_SATURATION_PERCENT: u8 = 80;

/// Read and validate the shared HPA/worker saturation target.
pub fn node_saturation_target() -> Result<u8, ThreadBudgetError> {
    let value = env::var("NGKG_NODE_SATURATION_TARGET_PERCENT")
        .map_err(|_| ThreadBudgetError::InvalidSaturationTarget)?
        .parse::<u8>()
        .map_err(|_| ThreadBudgetError::InvalidSaturationTarget)?;
    if value == 0 || value > MAX_NODE_SATURATION_PERCENT {
        return Err(ThreadBudgetError::InvalidSaturationTarget);
    }
    Ok(value)
}

/// Validate that all independently runnable pools fit in the assigned cores.
pub fn validate_thread_budget(
    budget: &ThreadBudget,
    cpuset_cores: usize,
    nested_parallelism: bool,
) -> Result<(), ThreadBudgetError> {
    if nested_parallelism {
        return Err(ThreadBudgetError::NestedParallelism);
    }
    let requested = budget
        .rust_compute
        .checked_add(budget.blocking_io)
        .and_then(|value| value.checked_add(budget.openmp))
        .and_then(|value| value.checked_add(budget.blas))
        .and_then(|value| value.checked_add(budget.control))
        .ok_or(ThreadBudgetError::Oversubscribed { requested: usize::MAX, available: cpuset_cores })?;
    if requested > cpuset_cores {
        return Err(ThreadBudgetError::Oversubscribed { requested, available: cpuset_cores });
    }
    Ok(())
}

/// Inspect cgroup/proc CPU assignment and native thread environment before workers start.
pub fn capability_report(budget: ThreadBudget) -> Result<CapabilityReport, ThreadBudgetError> {
    let (cpuset_cores, cpuset_source) = detect_cpuset()?;
    validate_thread_budget(&budget, cpuset_cores, false)?;
    let omp = positive_env("OMP_NUM_THREADS")?;
    let openblas = positive_env("OPENBLAS_NUM_THREADS")?;
    let mkl = positive_env("MKL_NUM_THREADS")?;
    let saturation = node_saturation_target()?;
    if omp != budget.openmp || openblas != budget.blas || mkl != budget.blas {
        return Err(ThreadBudgetError::NativeRuntimeMismatch);
    }
    Ok(CapabilityReport {
        cpuset_cores,
        cpuset_source,
        budget,
        node_saturation_target_percent: saturation,
        omp_num_threads: omp,
        openblas_num_threads: openblas,
        mkl_num_threads: mkl,
    })
}

/// Inspect the effective cpuset and cgroup-v2 memory controller. This function
/// intentionally rejects an unlimited memory cgroup: production pods must have
/// equal requests and limits so Kubernetes and the process enforce one budget.
pub fn resource_envelope_report() -> Result<ResourceEnvelopeReport, ThreadBudgetError> {
    let (cpuset_cores, cpuset_source) = detect_cpuset()?;
    let target = node_saturation_target()?;
    let memory_limit_bytes = read_cgroup_u64("/sys/fs/cgroup/memory.max")?;
    let memory_current_bytes = read_cgroup_u64("/sys/fs/cgroup/memory.current")?;
    if memory_limit_bytes == 0 || memory_current_bytes > memory_limit_bytes {
        return Err(ThreadBudgetError::CgroupMemory(
            "usage exceeds the configured finite limit".to_owned(),
        ));
    }
    let usable_u128 = u128::from(memory_limit_bytes)
        .saturating_mul(u128::from(target))
        / 100;
    let usable_memory_bytes = u64::try_from(usable_u128)
        .map_err(|_| ThreadBudgetError::CgroupMemory("usable memory exceeds u64".to_owned()))?;
    let memory_headroom_bytes = usable_memory_bytes.saturating_sub(memory_current_bytes);
    Ok(ResourceEnvelopeReport {
        cpuset_cores,
        cpuset_source,
        memory_limit_bytes,
        memory_current_bytes,
        usable_memory_bytes,
        memory_headroom_bytes,
        saturation_target_percent: target,
        memory_saturated: memory_current_bytes >= usable_memory_bytes,
    })
}

/// Verify that bounded concurrent buffers fit before a worker begins I/O.
pub fn validate_buffer_budget(
    buffer_bytes: usize,
    concurrency: usize,
    envelope: &ResourceEnvelopeReport,
) -> Result<u64, ThreadBudgetError> {
    let requested = u64::try_from(buffer_bytes)
        .ok()
        .and_then(|bytes| bytes.checked_mul(u64::try_from(concurrency).ok()?))
        .ok_or(ThreadBudgetError::MemoryBudget {
            requested: u64::MAX,
            available: envelope.memory_headroom_bytes,
        })?;
    if requested > envelope.memory_headroom_bytes {
        return Err(ThreadBudgetError::MemoryBudget {
            requested,
            available: envelope.memory_headroom_bytes,
        });
    }
    Ok(requested)
}

fn detect_cpuset() -> Result<(usize, String), ThreadBudgetError> {
    let cgroup_path = Path::new("/sys/fs/cgroup/cpuset.cpus.effective");
    if let Ok(value) = fs::read_to_string(cgroup_path) {
        return Ok((parse_cpu_list(value.trim())?, cgroup_path.display().to_string()));
    }
    let status = fs::read_to_string("/proc/self/status").map_err(|error| ThreadBudgetError::CpuSet(error.to_string()))?;
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:"))
        .map(str::trim)
        .ok_or_else(|| ThreadBudgetError::CpuSet("Cpus_allowed_list is absent".to_owned()))?;
    Ok((parse_cpu_list(value)?, "/proc/self/status:Cpus_allowed_list".to_owned()))
}

fn parse_cpu_list(value: &str) -> Result<usize, ThreadBudgetError> {
    if value.is_empty() {
        return Err(ThreadBudgetError::CpuSet("empty CPU set".to_owned()));
    }
    let mut total = 0_usize;
    for component in value.split(',') {
        if let Some((start, end)) = component.split_once('-') {
            let start = start.parse::<usize>().map_err(|_| ThreadBudgetError::CpuSet(component.to_owned()))?;
            let end = end.parse::<usize>().map_err(|_| ThreadBudgetError::CpuSet(component.to_owned()))?;
            if end < start {
                return Err(ThreadBudgetError::CpuSet(component.to_owned()));
            }
            total = total.checked_add(end - start + 1).ok_or_else(|| ThreadBudgetError::CpuSet(component.to_owned()))?;
        } else {
            component.parse::<usize>().map_err(|_| ThreadBudgetError::CpuSet(component.to_owned()))?;
            total = total.checked_add(1).ok_or_else(|| ThreadBudgetError::CpuSet(component.to_owned()))?;
        }
    }
    Ok(total)
}

fn positive_env(name: &'static str) -> Result<usize, ThreadBudgetError> {
    let value = env::var(name).map_err(|_| ThreadBudgetError::InvalidEnvironment { name })?;
    let value = value.parse::<usize>().map_err(|_| ThreadBudgetError::InvalidEnvironment { name })?;
    if value == 0 {
        return Err(ThreadBudgetError::InvalidEnvironment { name });
    }
    Ok(value)
}

fn read_cgroup_u64(path: &str) -> Result<u64, ThreadBudgetError> {
    let value = fs::read_to_string(path)
        .map_err(|error| ThreadBudgetError::CgroupMemory(format!("{path}: {error}")))?;
    let value = value.trim();
    if value == "max" {
        return Err(ThreadBudgetError::CgroupMemory(format!(
            "{path} is unlimited; a Guaranteed-QoS memory limit is required"
        )));
    }
    value
        .parse::<u64>()
        .map_err(|_| ThreadBudgetError::CgroupMemory(format!("{path}: {value}")))
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_NODE_SATURATION_PERCENT, ResourceEnvelopeReport, ThreadBudget,
        parse_cpu_list, validate_buffer_budget, validate_thread_budget,
    };

    #[test]
    fn cpu_ranges_are_counted_exactly() {
        assert_eq!(parse_cpu_list("0-3,8,10-11"), Ok(7));
    }

    #[test]
    fn nested_pool_sum_cannot_exceed_cpuset() {
        let budget = ThreadBudget { rust_compute: 16, blocking_io: 4, openmp: 8, blas: 8, control: 1 };
        assert!(validate_thread_budget(&budget, 32, false).is_err());
    }

    #[test]
    fn supported_saturation_never_consumes_failure_headroom() {
        assert_eq!(MAX_NODE_SATURATION_PERCENT, 80);
    }

    #[test]
    fn multipart_buffers_cannot_consume_reserved_headroom() {
        let envelope = ResourceEnvelopeReport {
            cpuset_cores: 4,
            cpuset_source: "test".to_owned(),
            memory_limit_bytes: 1_000,
            memory_current_bytes: 600,
            usable_memory_bytes: 800,
            memory_headroom_bytes: 200,
            saturation_target_percent: 80,
            memory_saturated: false,
        };
        assert_eq!(validate_buffer_budget(50, 4, &envelope), Ok(200));
        assert!(validate_buffer_budget(51, 4, &envelope).is_err());
    }
}
