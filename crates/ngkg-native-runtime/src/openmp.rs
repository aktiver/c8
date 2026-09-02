//! Safe process boundary for the optional OpenMP predicate kernel.

use std::{
    env,
    io::{Read, Write},
    path::Path,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use crate::{LeafPredicate, NativeRuntimeError, ScanColumns};

const INPUT_MAGIC: &[u8; 8] = b"NGKGOMP1";
const OUTPUT_MAGIC: &[u8; 8] = b"NGKGOUT1";

pub(super) fn filter_batch(
    columns: &ScanColumns<'_>,
    predicate: &LeafPredicate,
) -> Result<Vec<bool>, NativeRuntimeError> {
    let executable = env::var("NGKG_OPENMP_FILTER_EXECUTABLE")
        .map_err(|_| NativeRuntimeError::OpenMpKernel)?;
    let executable = Path::new(&executable);
    let metadata = std::fs::symlink_metadata(executable).map_err(|_| NativeRuntimeError::OpenMpKernel)?;
    if !executable.is_absolute() || metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(NativeRuntimeError::OpenMpKernel);
    }
    let omp_threads = env::var("OMP_NUM_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .ok_or(NativeRuntimeError::OpenMpKernel)?;
    let timeout = env::var("NGKG_OPENMP_KERNEL_TIMEOUT_MS")
        .unwrap_or_else(|_| "30000".to_owned())
        .parse::<u64>()
        .ok()
        .filter(|value| (1..=120_000).contains(value))
        .map(Duration::from_millis)
        .ok_or(NativeRuntimeError::OpenMpKernel)?;
    let mut child = Command::new(executable)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .env_clear()
        .env("OMP_NUM_THREADS", omp_threads.to_string())
        .env("OMP_DYNAMIC", "FALSE")
        .env("OMP_MAX_ACTIVE_LEVELS", "1")
        .env("OMP_PROC_BIND", "close")
        .env("OMP_PLACES", "cores")
        .spawn()
        .map_err(|_| NativeRuntimeError::OpenMpKernel)?;
    let mut stdout = child.stdout.take().ok_or(NativeRuntimeError::OpenMpKernel)?;
    let expected_output_bytes = 16_usize
        .checked_add(columns.batch.num_rows())
        .ok_or(NativeRuntimeError::LimitExceeded)?;
    let reader = thread::spawn(move || -> Result<Vec<u8>, NativeRuntimeError> {
        let read_limit = u64::try_from(expected_output_bytes)
            .map_err(|_| NativeRuntimeError::LimitExceeded)?
            .checked_add(1)
            .ok_or(NativeRuntimeError::LimitExceeded)?;
        let mut output = Vec::with_capacity(expected_output_bytes);
        stdout
            .take(read_limit)
            .read_to_end(&mut output)
            .map_err(|_| NativeRuntimeError::OpenMpKernel)?;
        if output.len() != expected_output_bytes {
            return Err(NativeRuntimeError::OpenMpKernel);
        }
        Ok(output)
    });
    let mut input = child.stdin.take().ok_or(NativeRuntimeError::OpenMpKernel)?;
    input.write_all(INPUT_MAGIC).map_err(|_| NativeRuntimeError::OpenMpKernel)?;
    write_u64(&mut input, u64::try_from(columns.batch.num_rows()).map_err(|_| NativeRuntimeError::LimitExceeded)?)?;
    let flags = u64::from(predicate.subject_id.is_some())
        | (u64::from(predicate.predicate_id.is_some()) << 1)
        | (u64::from(predicate.object_id.is_some()) << 2)
        | (u64::from(predicate.graph_id.is_some()) << 3)
        | (u64::from(predicate.require_queryable) << 4);
    write_u64(&mut input, flags)?;
    for value in [
        predicate.subject_id.unwrap_or_default(),
        predicate.predicate_id.unwrap_or_default(),
        predicate.object_id.unwrap_or_default(),
        predicate.graph_id.unwrap_or_default(),
    ] {
        write_u64(&mut input, value)?;
    }
    write_u64(
        &mut input,
        u64::try_from(predicate.allowed_graph_ids.len()).map_err(|_| NativeRuntimeError::LimitExceeded)?,
    )?;
    for graph in &predicate.allowed_graph_ids {
        write_u64(&mut input, *graph)?;
    }
    for row in 0..columns.batch.num_rows() {
        for value in [
            columns.subject(row)?,
            columns.predicate(row)?,
            columns.object(row)?,
            columns.graph(row)?,
        ] {
            write_u64(&mut input, value)?;
        }
        input
            .write_all(&[u8::from(columns.queryable(row)?)])
            .map_err(|_| NativeRuntimeError::OpenMpKernel)?;
    }
    drop(input);
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().map_err(|_| NativeRuntimeError::OpenMpKernel)? {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            let _ = reader.join();
            return Err(NativeRuntimeError::OpenMpKernel);
        }
        thread::sleep(Duration::from_millis(5));
    };
    let output = reader
        .join()
        .map_err(|_| NativeRuntimeError::OpenMpKernel)??;
    if !status.success() {
        return Err(NativeRuntimeError::OpenMpKernel);
    }
    if &output[..8] != OUTPUT_MAGIC {
        return Err(NativeRuntimeError::OpenMpKernel);
    }
    let count = u64::from_le_bytes(
        output[8..16]
            .try_into()
            .map_err(|_| NativeRuntimeError::OpenMpKernel)?,
    );
    if usize::try_from(count).ok() != Some(columns.batch.num_rows())
        || output[16..].iter().any(|value| *value > 1)
    {
        return Err(NativeRuntimeError::OpenMpKernel);
    }
    Ok(output[16..].iter().map(|value| *value == 1).collect())
}

fn write_u64(output: &mut impl Write, value: u64) -> Result<(), NativeRuntimeError> {
    output
        .write_all(&value.to_le_bytes())
        .map_err(|_| NativeRuntimeError::OpenMpKernel)
}
