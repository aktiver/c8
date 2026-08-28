//! Exact partition kernels and completeness barriers for Phase 40.13.8.
//!
//! These functions operate on SPARQL JSON solution mappings, not SQL rows. Unbound variables are
//! absent, compatibility is defined only over variables bound on both sides, and bag duplicates
//! are preserved unless the algebra explicitly requests `DISTINCT`. Expression-bearing OPTIONAL,
//! aggregates, RDF-term ordering and subqueries stay in the qualified scalar evaluator; this
//! module provides their deterministic group/range ownership and complete-partition merge.

use std::{cmp::Ordering, collections::BTreeSet};

use ngkg_query_planner::DistributedAlgebraOperator;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::{ExecutionError, inner_join_sparql_json, parse_sparql_term, project_sparql_json};

/// Complete input for one native worker partition.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NativeAlgebraTask {
    pub operator: DistributedAlgebraOperator,
    pub left_rows: Vec<Value>,
    pub right_rows: Vec<Value>,
    pub projection: Vec<String>,
    pub offset: usize,
    pub length: Option<usize>,
    pub max_output_rows: usize,
}

/// Execute the closed set of standards-safe native partition operators.
///
/// Any scalar-oracle operator fails closed instead of being approximated. `REDUCED` preserves all
/// duplicates, which is one of the result multiplicities explicitly permitted by SPARQL.
pub fn execute_native_algebra_task(
    task: &NativeAlgebraTask,
) -> Result<Vec<Value>, ExecutionError> {
    if task.max_output_rows == 0 {
        return Err(ExecutionError::IntermediateRowLimit);
    }
    use DistributedAlgebraOperator::{
        Distinct, Join, Minus, Project, Reduced, Slice, Union, Values,
    };
    match task.operator {
        Join => inner_join_sparql_json(
            &task.left_rows,
            &task.right_rows,
            task.max_output_rows,
        ),
        Union => union_sparql_json(
            &task.left_rows,
            &task.right_rows,
            task.max_output_rows,
        ),
        Minus => minus_sparql_json(
            &task.left_rows,
            &task.right_rows,
            task.max_output_rows,
        ),
        Project => project_sparql_json(&task.left_rows, &task.projection).and_then(|rows| {
            if rows.len() > task.max_output_rows {
                Err(ExecutionError::IntermediateRowLimit)
            } else {
                Ok(rows)
            }
        }),
        Distinct => distinct_sparql_json(&task.left_rows, task.max_output_rows),
        Reduced | Values => {
            validate_rows(&task.left_rows)?;
            if task.left_rows.len() > task.max_output_rows {
                Err(ExecutionError::IntermediateRowLimit)
            } else {
                Ok(task.left_rows.clone())
            }
        }
        Slice => global_slice_sparql_json(
            std::slice::from_ref(&task.left_rows),
            task.offset,
            task.length,
            task.max_output_rows,
        ),
        _ => Err(ExecutionError::UnsafeNativeAlgebraOperator),
    }
}

/// Immutable identity shared by every output partition of one algebra stage.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AlgebraPartitionIdentity {
    pub query_sha256: String,
    pub plan_sha256: String,
    pub stage_id: String,
    pub stage_algebra_sha256: String,
    pub partition: u32,
    pub partition_count: u32,
}

/// Checksum-bound complete output from one worker-owned algebra partition.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AlgebraPartitionResult {
    pub identity: AlgebraPartitionIdentity,
    pub head: Vec<String>,
    pub rows: Vec<Value>,
    pub input_sha256: String,
    pub output_sha256: String,
    pub complete: bool,
    pub worker_id: String,
}

/// SPARQL bag union. Rows retain left-then-right order and all duplicates.
pub fn union_sparql_json(
    left: &[Value],
    right: &[Value],
    max_rows: usize,
) -> Result<Vec<Value>, ExecutionError> {
    validate_rows(left)?;
    validate_rows(right)?;
    let total = left
        .len()
        .checked_add(right.len())
        .filter(|total| *total <= max_rows)
        .ok_or(ExecutionError::IntermediateRowLimit)?;
    let mut output = Vec::with_capacity(total);
    output.extend_from_slice(left);
    output.extend_from_slice(right);
    Ok(output)
}

/// Exact expression-free SPARQL LeftJoin used for `OPTIONAL { ... }`.
///
/// Expression-bearing OPTIONAL remains on the scalar-oracle partition lane. This kernel preserves
/// each unmatched left solution once and emits every compatible merged right solution.
pub fn left_join_sparql_json(
    left: &[Value],
    right: &[Value],
    max_rows: usize,
) -> Result<Vec<Value>, ExecutionError> {
    validate_rows(left)?;
    validate_rows(right)?;
    let mut output = Vec::new();
    for left_row in left {
        let left_object = left_row
            .as_object()
            .ok_or(ExecutionError::InvalidSparqlBinding)?;
        let mut matched = false;
        for right_row in right {
            let right_object = right_row
                .as_object()
                .ok_or(ExecutionError::InvalidSparqlBinding)?;
            if !compatible(left_object, right_object) {
                continue;
            }
            reserve_row(&output, max_rows)?;
            output.push(Value::Object(merge(left_object, right_object)?));
            matched = true;
        }
        if !matched {
            reserve_row(&output, max_rows)?;
            output.push(left_row.clone());
        }
    }
    Ok(output)
}

/// Exact SPARQL MINUS.
///
/// A left solution is removed only when a compatible right solution shares at least one bound
/// variable. Disjoint domains therefore retain the left row, unlike a SQL anti-join shortcut.
pub fn minus_sparql_json(
    left: &[Value],
    right: &[Value],
    max_rows: usize,
) -> Result<Vec<Value>, ExecutionError> {
    validate_rows(left)?;
    validate_rows(right)?;
    let mut output = Vec::new();
    for left_row in left {
        let left_object = left_row
            .as_object()
            .ok_or(ExecutionError::InvalidSparqlBinding)?;
        let excluded: bool = right.iter().try_fold(false, |excluded, right_row| {
            if excluded {
                return Ok::<bool, ExecutionError>(true);
            }
            let right_object = right_row
                .as_object()
                .ok_or(ExecutionError::InvalidSparqlBinding)?;
            let shared = left_object
                .keys()
                .any(|key| right_object.contains_key(key));
            Ok(shared && compatible(left_object, right_object))
        })?;
        if !excluded {
            reserve_row(&output, max_rows)?;
            output.push(left_row.clone());
        }
    }
    Ok(output)
}

/// Exact `DISTINCT` over complete RDF-term solution mappings.
pub fn distinct_sparql_json(
    rows: &[Value],
    max_rows: usize,
) -> Result<Vec<Value>, ExecutionError> {
    validate_rows(rows)?;
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for row in rows {
        if seen.insert(canonical_binding_key(row)?) {
            reserve_row(&output, max_rows)?;
            output.push(row.clone());
        }
    }
    Ok(output)
}

/// Assign every complete SPARQL group to exactly one stable worker partition.
///
/// Missing group variables use an explicit unbound marker, so all unbound members of the group
/// are co-located. The scalar evaluator then computes the entire group and owns aggregate errors,
/// datatype promotion, `DISTINCT`, `GROUP_CONCAT`, `SAMPLE`, and empty-group behavior.
pub fn group_owned_partitions(
    rows: &[Value],
    group_variables: &[String],
    partition_count: u32,
    max_rows: usize,
) -> Result<Vec<Vec<Value>>, ExecutionError> {
    validate_rows(rows)?;
    if partition_count < 2 || group_variables.iter().any(|variable| variable.is_empty()) {
        return Err(ExecutionError::InvalidPartitionCount);
    }
    if rows.len() > max_rows {
        return Err(ExecutionError::IntermediateRowLimit);
    }
    let partition_len = usize::try_from(partition_count)
        .map_err(|_| ExecutionError::InvalidPartitionCount)?;
    let mut partitions = vec![Vec::new(); partition_len];
    for row in rows {
        let object = row
            .as_object()
            .ok_or(ExecutionError::InvalidSparqlBinding)?;
        let mut hash = Sha256::new();
        hash.update(b"ngkg-sparql-group-owner-v1\0");
        for variable in group_variables {
            hash_component(&mut hash, variable.as_bytes())?;
            match object.get(variable) {
                Some(term) => hash_component(&mut hash, &canonical_term(term)?)?,
                None => hash_component(&mut hash, b"unbound")?,
            }
        }
        let digest = hash.finalize();
        let owner = u64::from_be_bytes(
            digest[..8]
                .try_into()
                .map_err(|_| ExecutionError::InvalidPartitionCount)?,
        ) % u64::from(partition_count);
        let owner = usize::try_from(owner).map_err(|_| ExecutionError::InvalidPartitionCount)?;
        partitions[owner].push(row.clone());
    }
    Ok(partitions)
}

/// Apply global OFFSET/LIMIT only after all input partitions are complete and ordered.
pub fn global_slice_sparql_json(
    ordered_partitions: &[Vec<Value>],
    offset: usize,
    length: Option<usize>,
    max_rows: usize,
) -> Result<Vec<Value>, ExecutionError> {
    for partition in ordered_partitions {
        validate_rows(partition)?;
    }
    let wanted = length.unwrap_or(max_rows).min(max_rows);
    let mut skipped = 0_usize;
    let mut output = Vec::with_capacity(wanted);
    for row in ordered_partitions.iter().flatten() {
        if skipped < offset {
            skipped = skipped
                .checked_add(1)
                .ok_or(ExecutionError::IntermediateRowLimit)?;
            continue;
        }
        if output.len() == wanted {
            break;
        }
        output.push(row.clone());
    }
    Ok(output)
}

/// Deterministic k-way merge of worker-sorted ranges.
///
/// `compare` must be the pinned scalar evaluator's SPARQL ORDER BY comparator. This function
/// validates that every worker range is locally ordered and uses partition ordinal as the stable
/// tie breaker; it never invents an RDF-term ordering in native code.
pub fn merge_ordered_partitions_by<F>(
    partitions: &[Vec<Value>],
    max_rows: usize,
    compare: F,
) -> Result<Vec<Value>, ExecutionError>
where
    F: Fn(&Value, &Value) -> Ordering,
{
    for partition in partitions {
        validate_rows(partition)?;
        if partition
            .windows(2)
            .any(|pair| compare(&pair[0], &pair[1]) == Ordering::Greater)
        {
            return Err(ExecutionError::InvalidSparqlBinding);
        }
    }
    let total = partitions.iter().try_fold(0_usize, |total, partition| {
        total
            .checked_add(partition.len())
            .filter(|total| *total <= max_rows)
            .ok_or(ExecutionError::IntermediateRowLimit)
    })?;
    let mut cursors = vec![0_usize; partitions.len()];
    let mut output = Vec::with_capacity(total);
    while output.len() < total {
        let mut winner: Option<usize> = None;
        for (partition, cursor) in cursors.iter().copied().enumerate() {
            let Some(candidate) = partitions[partition].get(cursor) else {
                continue;
            };
            winner = match winner {
                None => Some(partition),
                Some(current) => {
                    let current_value = &partitions[current][cursors[current]];
                    if compare(candidate, current_value) == Ordering::Less {
                        Some(partition)
                    } else {
                        Some(current)
                    }
                }
            };
        }
        let winner = winner.ok_or(ExecutionError::InvalidSparqlBinding)?;
        output.push(partitions[winner][cursors[winner]].clone());
        cursors[winner] = cursors[winner]
            .checked_add(1)
            .ok_or(ExecutionError::IntermediateRowLimit)?;
    }
    Ok(output)
}

/// Verify and merge every partition before exposing a complete algebra stage.
pub fn complete_algebra_partition_set(
    mut results: Vec<AlgebraPartitionResult>,
    max_rows: usize,
) -> Result<Vec<Value>, ExecutionError> {
    let first = results.first().ok_or(ExecutionError::InvalidPartitionCount)?;
    validate_identity(&first.identity)?;
    let expected_count = first.identity.partition_count;
    let expected_query = first.identity.query_sha256.clone();
    let expected_plan = first.identity.plan_sha256.clone();
    let expected_stage = first.identity.stage_id.clone();
    let expected_algebra = first.identity.stage_algebra_sha256.clone();
    let expected_head = first.head.clone();
    if results.len()
        != usize::try_from(expected_count).map_err(|_| ExecutionError::InvalidPartitionCount)?
    {
        return Err(ExecutionError::InvalidPartitionCount);
    }
    results.sort_by_key(|result| result.identity.partition);
    let mut output = Vec::new();
    for (partition, result) in results.into_iter().enumerate() {
        validate_identity(&result.identity)?;
        if !result.complete
            || result.worker_id.is_empty()
            || result.identity.query_sha256 != expected_query
            || result.identity.plan_sha256 != expected_plan
            || result.identity.stage_id != expected_stage
            || result.identity.stage_algebra_sha256 != expected_algebra
            || usize::try_from(result.identity.partition).ok() != Some(partition)
            || result.identity.partition_count != expected_count
            || result.head != expected_head
            || !lower_hex_sha256(&result.input_sha256)
            || !lower_hex_sha256(&result.output_sha256)
            || relation_sha256(&result.head, &result.rows)? != result.output_sha256
        {
            return Err(ExecutionError::InvalidArrowMetadata(
                "algebra partition set is incomplete or checksum-inconsistent".to_owned(),
            ));
        }
        if output
            .len()
            .checked_add(result.rows.len())
            .is_none_or(|total| total > max_rows)
        {
            return Err(ExecutionError::IntermediateRowLimit);
        }
        output.extend(result.rows);
    }
    Ok(output)
}

fn validate_rows(rows: &[Value]) -> Result<(), ExecutionError> {
    for row in rows {
        let object = row
            .as_object()
            .ok_or(ExecutionError::InvalidSparqlBinding)?;
        for term in object.values() {
            parse_sparql_term(term)?;
        }
    }
    Ok(())
}

fn compatible(left: &Map<String, Value>, right: &Map<String, Value>) -> bool {
    left.iter().all(|(variable, left_term)| {
        right
            .get(variable)
            .is_none_or(|right_term| right_term == left_term)
    })
}

fn merge(
    left: &Map<String, Value>,
    right: &Map<String, Value>,
) -> Result<Map<String, Value>, ExecutionError> {
    let mut output = left.clone();
    for (variable, term) in right {
        if output.get(variable).is_some_and(|existing| existing != term) {
            return Err(ExecutionError::IncompatibleBinding(stable_variable_id(variable)));
        }
        output.entry(variable.clone()).or_insert_with(|| term.clone());
    }
    Ok(output)
}

fn reserve_row(output: &[Value], max_rows: usize) -> Result<(), ExecutionError> {
    if output.len() >= max_rows {
        Err(ExecutionError::IntermediateRowLimit)
    } else {
        Ok(())
    }
}

fn canonical_binding_key(row: &Value) -> Result<Vec<u8>, ExecutionError> {
    let object = row
        .as_object()
        .ok_or(ExecutionError::InvalidSparqlBinding)?;
    let mut encoded = Vec::new();
    let mut variables = object.keys().collect::<Vec<_>>();
    variables.sort_unstable();
    for variable in variables {
        append_component(&mut encoded, variable.as_bytes())?;
        append_component(&mut encoded, &canonical_term(&object[variable.as_str()])?)?;
    }
    Ok(encoded)
}

fn canonical_term(term: &Value) -> Result<Vec<u8>, ExecutionError> {
    parse_sparql_term(term)?;
    serde_json::to_vec(term).map_err(|_| ExecutionError::InvalidSparqlTerm("JSON".to_owned()))
}

fn append_component(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), ExecutionError> {
    let length = u64::try_from(bytes.len()).map_err(|_| ExecutionError::IntermediateRowLimit)?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn hash_component(hash: &mut Sha256, bytes: &[u8]) -> Result<(), ExecutionError> {
    let length = u64::try_from(bytes.len()).map_err(|_| ExecutionError::IntermediateRowLimit)?;
    hash.update(length.to_be_bytes());
    hash.update(bytes);
    Ok(())
}

fn relation_sha256(head: &[String], rows: &[Value]) -> Result<String, ExecutionError> {
    validate_rows(rows)?;
    let mut hash = Sha256::new();
    hash.update(b"ngkg-distributed-algebra-relation-v1\0");
    for variable in head {
        hash_component(&mut hash, variable.as_bytes())?;
    }
    for row in rows {
        hash_component(&mut hash, &canonical_binding_key(row)?)?;
    }
    let digest = hash.finalize();
    Ok(hex_encode(&digest))
}

fn validate_identity(identity: &AlgebraPartitionIdentity) -> Result<(), ExecutionError> {
    if !lower_hex_sha256(&identity.query_sha256)
        || !lower_hex_sha256(&identity.plan_sha256)
        || !lower_hex_sha256(&identity.stage_algebra_sha256)
        || identity.stage_id.is_empty()
        || identity.partition_count < 2
        || identity.partition >= identity.partition_count
    {
        return Err(ExecutionError::InvalidArrowMetadata(
            "algebra partition identity is invalid".to_owned(),
        ));
    }
    Ok(())
}

fn lower_hex_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn stable_variable_id(variable: &str) -> u16 {
    variable.bytes().fold(0_u16, |state, byte| {
        state.wrapping_mul(257).wrapping_add(u16::from(byte))
    })
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{
        distinct_sparql_json, group_owned_partitions, left_join_sparql_json,
        minus_sparql_json, union_sparql_json,
    };

    fn uri(value: &str) -> Value {
        json!({"type": "uri", "value": value})
    }

    #[test]
    fn optional_preserves_unmatched_left_rows_and_bag_duplicates() {
        let left = vec![json!({"s": uri("urn:a")}), json!({"s": uri("urn:b")})];
        let right = vec![
            json!({"s": uri("urn:a"), "o": uri("urn:x")}),
            json!({"s": uri("urn:a"), "o": uri("urn:x")}),
        ];
        let output = left_join_sparql_json(&left, &right, 10).unwrap_or_default();
        assert_eq!(output.len(), 3);
        assert_eq!(output[2], left[1]);
    }

    #[test]
    fn minus_with_disjoint_domains_keeps_the_left_solution() {
        let left = vec![json!({"s": uri("urn:a")})];
        let right = vec![json!({"o": uri("urn:a")})];
        assert_eq!(minus_sparql_json(&left, &right, 10).unwrap_or_default(), left);
    }

    #[test]
    fn union_and_distinct_have_separate_bag_semantics() {
        let row = json!({"s": uri("urn:a")});
        let bag = union_sparql_json(std::slice::from_ref(&row), std::slice::from_ref(&row), 10)
            .unwrap_or_default();
        assert_eq!(bag.len(), 2);
        assert_eq!(distinct_sparql_json(&bag, 10).unwrap_or_default(), vec![row]);
    }

    #[test]
    fn equal_group_keys_have_one_owner_even_when_unbound() {
        let rows = vec![json!({}), json!({}), json!({"g": uri("urn:g")})];
        let partitions = group_owned_partitions(&rows, &["g".to_owned()], 8, 10)
            .unwrap_or_default();
        assert_eq!(partitions.iter().map(Vec::len).sum::<usize>(), 3);
        assert!(partitions.iter().any(|partition| partition.len() == 2));
    }
}
