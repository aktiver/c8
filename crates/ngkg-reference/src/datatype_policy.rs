//! Phase 40.2 deterministic OWL Direct datatype policy and high-volume literal validation.

use std::{collections::BTreeMap, fs, path::Path, thread};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::rdf::{NormalizedFact, NormalizedObject};

const EMBEDDED_POLICY: &[u8] = include_bytes!("../../../policies/owl-direct-datatype-policy.json");
/// Every qualification, offline, and exact lane must accept this exact version identifier.
pub const OWL_DIRECT_DATATYPE_POLICY_ID: &str = "ngkg-owl2-direct-datatype-policy-v1";

/// One datatype IRI and the lexical-space validator applied to literals using it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SupportedDatatype {
    /// Absolute datatype IRI admitted by this policy.
    pub iri: String,
    /// Deterministic lexical-space validator identifier.
    pub lexical_space: String,
}

/// Deliberate lexical limits for validators that implement a documented subset.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatatypeLexicalLimits {
    /// Maximum decimal digits accepted by unbounded integer lexical forms.
    pub integer_digits_max: usize,
    /// Maximum year digits accepted by date/time lexical forms.
    pub date_time_year_digits_max: usize,
    /// Published XML Name validation mode.
    pub xml_name_validation: String,
}

/// Operator-shipped Phase 40.2 datatype policy.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatatypePolicy {
    /// Contract format version.
    pub format_version: u32,
    /// Stable operator policy identifier.
    pub policy_id: String,
    /// Required action for datatypes outside the map.
    pub unsupported_datatype_behavior: String,
    /// Required action for lexical forms outside the declared space.
    pub ill_typed_literal_behavior: String,
    /// Literal canonicalization behavior.
    pub canonicalization: String,
    /// Maximum UTF-8 bytes accepted for one reasoning-visible lexical form.
    pub max_lexical_bytes: usize,
    /// Additional bounded lexical-space limits.
    pub lexical_limits: DatatypeLexicalLimits,
    /// Strictly sorted supported datatype map.
    pub supported_datatypes: Vec<SupportedDatatype>,
}

/// Deterministic evidence emitted after scanning every reasoning-visible literal.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatatypeValidationSummary {
    /// Evidence format version.
    pub format_version: u32,
    /// Policy identifier applied to the snapshot.
    pub policy_id: String,
    /// SHA-256 of the exact policy bytes.
    pub policy_sha256: String,
    /// Number of facts visible to offline reasoning.
    pub reasoning_fact_count: u64,
    /// Number of reasoning-visible literal facts validated.
    pub literal_count: u64,
    /// Number of validated language-tagged literals.
    pub language_literal_count: u64,
    /// Number of deterministic validation lanes used.
    pub worker_count: usize,
    /// Per-datatype exact literal counts.
    pub datatype_counts: BTreeMap<String, u64>,
}

#[derive(Debug, Error)]
pub enum DatatypePolicyError {
    /// Policy artifact could not be read or written.
    #[error("datatype policy I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Policy artifact is not valid JSON for the runtime model.
    #[error("datatype policy JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    /// Policy-level deterministic invariants are invalid.
    #[error("datatype policy contract is invalid: {0}")]
    InvalidPolicy(String),
    /// A scoped validation lane terminated without returning evidence.
    #[error("datatype validation worker terminated unexpectedly")]
    WorkerFailure,
    /// One reasoning-visible literal violates the supported datatype policy.
    #[error(
        "reasoning literal at fact index {fact_index} with datatype {datatype_iri} is invalid: {detail}"
    )]
    InvalidLiteral {
        /// Original normalized fact index used for deterministic failure selection.
        fact_index: usize,
        /// Literal datatype IRI.
        datatype_iri: String,
        /// Bounded human-readable failure detail.
        detail: String,
    },
}

/// Write the exact repository-shipped policy bytes into the immutable snapshot and validate them.
pub fn write_embedded_policy(path: &Path) -> Result<(DatatypePolicy, String), DatatypePolicyError> {
    let policy: DatatypePolicy = serde_json::from_slice(EMBEDDED_POLICY)?;
    validate_policy_contract(&policy)?;
    fs::write(path, EMBEDDED_POLICY)?;
    let sha256 = hex::encode(Sha256::digest(EMBEDDED_POLICY));
    Ok((policy, sha256))
}

/// Load and validate a policy artifact independently from the embedded representation.
pub fn read_policy(path: &Path) -> Result<(DatatypePolicy, String), DatatypePolicyError> {
    let bytes = fs::read(path)?;
    let policy: DatatypePolicy = serde_json::from_slice(&bytes)?;
    validate_policy_contract(&policy)?;
    Ok((policy, hex::encode(Sha256::digest(&bytes))))
}

fn validate_policy_contract(policy: &DatatypePolicy) -> Result<(), DatatypePolicyError> {
    if policy.format_version != 1
        || policy.policy_id != OWL_DIRECT_DATATYPE_POLICY_ID
        || policy.unsupported_datatype_behavior != "reject_snapshot"
        || policy.ill_typed_literal_behavior != "reject_snapshot"
        || policy.canonicalization != "preserve_source_lexical_form"
        || policy.max_lexical_bytes == 0
        || policy.lexical_limits.integer_digits_max == 0
        || policy.lexical_limits.date_time_year_digits_max < 4
        || policy.lexical_limits.xml_name_validation != "ascii_subset"
        || policy.supported_datatypes.is_empty()
    {
        return Err(DatatypePolicyError::InvalidPolicy(
            "format, fail-closed behavior, lexical limits, or datatype map is invalid".to_owned(),
        ));
    }
    let mut previous: Option<&str> = None;
    for datatype in &policy.supported_datatypes {
        if datatype.iri.is_empty() || datatype.lexical_space.is_empty() {
            return Err(DatatypePolicyError::InvalidPolicy(
                "datatype IRI and lexicalSpace must be non-empty".to_owned(),
            ));
        }
        if previous.is_some_and(|value| value >= datatype.iri.as_str()) {
            return Err(DatatypePolicyError::InvalidPolicy(
                "supportedDatatypes must be strictly sorted by IRI and duplicate-free".to_owned(),
            ));
        }
        previous = Some(&datatype.iri);
    }
    Ok(())
}

/// Validate every reasoning-visible RDF literal with deterministic multi-core partitioning.
pub fn validate_reasoning_literals(
    facts: &[NormalizedFact],
    policy: &DatatypePolicy,
    policy_sha256: &str,
) -> Result<DatatypeValidationSummary, DatatypePolicyError> {
    validate_policy_contract(policy)?;
    let lookup = policy
        .supported_datatypes
        .iter()
        .map(|datatype| (datatype.iri.clone(), datatype.lexical_space.clone()))
        .collect::<BTreeMap<_, _>>();
    let available = thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let worker_count = available.min(32).min(facts.len().max(1));
    let chunk_size = facts.len().max(1).div_ceil(worker_count);

    let mut summaries = Vec::new();
    let mut failures = Vec::new();
    thread::scope(|scope| {
        let mut handles = Vec::new();
        for (chunk_id, chunk) in facts.chunks(chunk_size).enumerate() {
            let start = chunk_id * chunk_size;
            let lookup = &lookup;
            handles.push(scope.spawn(move || validate_chunk(start, chunk, policy, lookup)));
        }
        for handle in handles {
            match handle.join() {
                Ok(Ok(summary)) => summaries.push(summary),
                Ok(Err(failure)) => failures.push(failure),
                Err(_) => failures.push((usize::MAX, String::new(), "worker failure".to_owned())),
            }
        }
    });
    if let Some((index, datatype_iri, detail)) = failures.into_iter().min_by_key(|row| row.0) {
        if index == usize::MAX {
            return Err(DatatypePolicyError::WorkerFailure);
        }
        return Err(DatatypePolicyError::InvalidLiteral {
            fact_index: index,
            datatype_iri,
            detail,
        });
    }

    let mut reasoning_fact_count = 0_u64;
    let mut literal_count = 0_u64;
    let mut language_literal_count = 0_u64;
    let mut datatype_counts = BTreeMap::<String, u64>::new();
    for summary in summaries {
        reasoning_fact_count = reasoning_fact_count.saturating_add(summary.reasoning_fact_count);
        literal_count = literal_count.saturating_add(summary.literal_count);
        language_literal_count =
            language_literal_count.saturating_add(summary.language_literal_count);
        for (datatype, count) in summary.datatype_counts {
            datatype_counts
                .entry(datatype)
                .and_modify(|value| *value = value.saturating_add(count))
                .or_insert(count);
        }
    }
    Ok(DatatypeValidationSummary {
        format_version: 1,
        policy_id: policy.policy_id.clone(),
        policy_sha256: policy_sha256.to_owned(),
        reasoning_fact_count,
        literal_count,
        language_literal_count,
        worker_count,
        datatype_counts,
    })
}

#[derive(Default)]
struct ChunkSummary {
    reasoning_fact_count: u64,
    literal_count: u64,
    language_literal_count: u64,
    datatype_counts: BTreeMap<String, u64>,
}

fn validate_chunk(
    start: usize,
    facts: &[NormalizedFact],
    policy: &DatatypePolicy,
    lookup: &BTreeMap<String, String>,
) -> Result<ChunkSummary, (usize, String, String)> {
    let mut summary = ChunkSummary::default();
    for (offset, fact) in facts.iter().enumerate() {
        if !fact.participates_in_reasoning {
            continue;
        }
        summary.reasoning_fact_count = summary.reasoning_fact_count.saturating_add(1);
        let NormalizedObject::Literal {
            lexical_value,
            datatype_iri,
            language,
            ..
        } = &fact.object
        else {
            continue;
        };
        let Some(lexical_space) = lookup.get(datatype_iri) else {
            return Err((
                start + offset,
                datatype_iri.clone(),
                "datatype is not present in the operator-supported datatype map".to_owned(),
            ));
        };
        if let Err(detail) = validate_literal(
            lexical_space,
            lexical_value,
            language.as_deref(),
            &policy.lexical_limits,
            policy.max_lexical_bytes,
        ) {
            return Err((start + offset, datatype_iri.clone(), detail));
        }
        summary.literal_count = summary.literal_count.saturating_add(1);
        if language.is_some() {
            summary.language_literal_count = summary.language_literal_count.saturating_add(1);
        }
        summary
            .datatype_counts
            .entry(datatype_iri.clone())
            .and_modify(|value| *value = value.saturating_add(1))
            .or_insert(1);
    }
    Ok(summary)
}

fn validate_literal(
    lexical_space: &str,
    value: &str,
    language: Option<&str>,
    limits: &DatatypeLexicalLimits,
    max_lexical_bytes: usize,
) -> Result<(), String> {
    if value.len() > max_lexical_bytes {
        return Err(format!(
            "lexical value exceeds {max_lexical_bytes} UTF-8 bytes"
        ));
    }
    if lexical_space != "language_tagged_string" && language.is_some() {
        return Err("language tag is only legal with rdf:langString".to_owned());
    }
    match lexical_space {
        "language_tagged_string" => {
            let tag =
                language.ok_or_else(|| "rdf:langString requires a language tag".to_owned())?;
            if !valid_language_tag(tag) {
                return Err("language tag is outside the documented BCP47 subset".to_owned());
            }
            Ok(())
        }
        "string" => Ok(()),
        "normalized_string" => {
            if value.chars().any(|ch| matches!(ch, '\r' | '\n' | '\t')) {
                Err(
                    "normalizedString contains a prohibited carriage-return, line-feed, or tab"
                        .to_owned(),
                )
            } else {
                Ok(())
            }
        }
        "token" => validate_token(value),
        "language" => {
            if valid_language_tag(value) {
                Ok(())
            } else {
                Err("invalid xsd:language lexical form".to_owned())
            }
        }
        "name" => validate_name(value, true, true),
        "ncname" => validate_name(value, false, true),
        "nmtoken" => validate_name(value, true, false),
        "boolean" => match value {
            "true" | "false" | "1" | "0" => Ok(()),
            _ => Err("expected true, false, 1, or 0".to_owned()),
        },
        "decimal" => validate_decimal(value),
        "integer" => validate_integer(value, limits.integer_digits_max).map(|_| ()),
        "non_positive_integer" => {
            integer_sign_constraint(value, limits.integer_digits_max, false, true)
        }
        "negative_integer" => {
            integer_sign_constraint(value, limits.integer_digits_max, false, false)
        }
        "positive_integer" => {
            integer_sign_constraint(value, limits.integer_digits_max, true, false)
        }
        "non_negative_integer" => {
            integer_sign_constraint(value, limits.integer_digits_max, true, true)
        }
        "long" => validate_bounded_integer(
            value,
            limits.integer_digits_max,
            i64::MIN as i128,
            i64::MAX as i128,
        ),
        "int" => validate_bounded_integer(
            value,
            limits.integer_digits_max,
            i32::MIN as i128,
            i32::MAX as i128,
        ),
        "short" => validate_bounded_integer(
            value,
            limits.integer_digits_max,
            i16::MIN as i128,
            i16::MAX as i128,
        ),
        "byte" => validate_bounded_integer(
            value,
            limits.integer_digits_max,
            i8::MIN as i128,
            i8::MAX as i128,
        ),
        "unsigned_long" => {
            validate_bounded_integer(value, limits.integer_digits_max, 0, u64::MAX as i128)
        }
        "unsigned_int" => {
            validate_bounded_integer(value, limits.integer_digits_max, 0, u32::MAX as i128)
        }
        "unsigned_short" => {
            validate_bounded_integer(value, limits.integer_digits_max, 0, u16::MAX as i128)
        }
        "unsigned_byte" => {
            validate_bounded_integer(value, limits.integer_digits_max, 0, u8::MAX as i128)
        }
        "float" => validate_float(value, true),
        "double" => validate_float(value, false),
        "hex_binary" => {
            if value.len() % 2 == 0 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                Ok(())
            } else {
                Err("hexBinary must contain an even number of hexadecimal characters".to_owned())
            }
        }
        "any_uri" => validate_any_uri(value),
        "date_time" => validate_date_time(value, false, limits.date_time_year_digits_max),
        "date_time_stamp" => validate_date_time(value, true, limits.date_time_year_digits_max),
        "date" => validate_date(value, limits.date_time_year_digits_max),
        "time" => validate_time(value, false),
        other => Err(format!("unknown lexicalSpace {other}")),
    }
}

fn validate_token(value: &str) -> Result<(), String> {
    if value.chars().any(|ch| matches!(ch, '\r' | '\n' | '\t'))
        || value.starts_with(' ')
        || value.ends_with(' ')
        || value.contains("  ")
    {
        Err("token is not whitespace-collapsed".to_owned())
    } else {
        Ok(())
    }
}

fn valid_language_tag(value: &str) -> bool {
    let mut parts = value.split('-');
    let Some(first) = parts.next() else {
        return false;
    };
    if first.is_empty() || first.len() > 8 || !first.bytes().all(|byte| byte.is_ascii_alphabetic())
    {
        return false;
    }
    parts.all(|part| {
        !part.is_empty() && part.len() <= 8 && part.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })
}

fn validate_name(value: &str, allow_colon: bool, require_name_start: bool) -> Result<(), String> {
    if value.is_empty() || !value.is_ascii() {
        return Err(
            "XML name validation is intentionally limited to the documented ASCII subset"
                .to_owned(),
        );
    }
    let mut bytes = value.bytes();
    let first = bytes.next().ok_or_else(|| "name is empty".to_owned())?;
    let valid_start =
        first.is_ascii_alphabetic() || first == b'_' || (allow_colon && first == b':');
    if require_name_start && !valid_start {
        return Err("invalid ASCII XML name start character".to_owned());
    }
    let valid = |byte: u8| {
        byte.is_ascii_alphanumeric()
            || matches!(byte, b'_' | b'-' | b'.')
            || (allow_colon && byte == b':')
    };
    if (!require_name_start && !valid(first)) || !bytes.all(valid) {
        Err("invalid ASCII XML name character".to_owned())
    } else {
        Ok(())
    }
}

fn validate_decimal(value: &str) -> Result<(), String> {
    let unsigned = value
        .strip_prefix('+')
        .or_else(|| value.strip_prefix('-'))
        .unwrap_or(value);
    if unsigned.is_empty() || unsigned.contains('e') || unsigned.contains('E') {
        return Err("invalid decimal lexical form".to_owned());
    }
    let mut pieces = unsigned.split('.');
    let whole = pieces.next().unwrap_or_default();
    let fraction = pieces.next();
    if pieces.next().is_some() {
        return Err("decimal contains more than one decimal point".to_owned());
    }
    let digits = |text: &str| text.bytes().all(|byte| byte.is_ascii_digit());
    match fraction {
        None if !whole.is_empty() && digits(whole) => Ok(()),
        Some(frac) if digits(whole) && digits(frac) && (!whole.is_empty() || !frac.is_empty()) => {
            Ok(())
        }
        _ => Err("invalid decimal lexical form".to_owned()),
    }
}

fn validate_integer(value: &str, max_digits: usize) -> Result<(&str, bool), String> {
    let negative = value.starts_with('-');
    let unsigned = value
        .strip_prefix('+')
        .or_else(|| value.strip_prefix('-'))
        .unwrap_or(value);
    if unsigned.is_empty()
        || unsigned.len() > max_digits
        || !unsigned.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(format!(
            "integer must contain 1..={max_digits} decimal digits"
        ));
    }
    Ok((unsigned, negative))
}

fn integer_sign_constraint(
    value: &str,
    max_digits: usize,
    positive: bool,
    allow_zero: bool,
) -> Result<(), String> {
    let (digits, negative) = validate_integer(value, max_digits)?;
    let zero = digits.bytes().all(|byte| byte == b'0');
    let correct_sign = if zero {
        allow_zero
    } else if positive {
        !negative
    } else {
        negative
    };
    if correct_sign {
        Ok(())
    } else {
        Err("integer sign/zero constraint is violated".to_owned())
    }
}

fn validate_bounded_integer(
    value: &str,
    max_digits: usize,
    minimum: i128,
    maximum: i128,
) -> Result<(), String> {
    validate_integer(value, max_digits)?;
    let parsed = value
        .parse::<i128>()
        .map_err(|_| "integer exceeds the bounded datatype range".to_owned())?;
    if (minimum..=maximum).contains(&parsed) {
        Ok(())
    } else {
        Err("integer exceeds the bounded datatype range".to_owned())
    }
}

fn validate_float(value: &str, single_precision: bool) -> Result<(), String> {
    if matches!(value, "INF" | "-INF" | "NaN") {
        return Ok(());
    }
    if !valid_floating_lexical(value) {
        return Err("invalid XML Schema floating-point lexical form".to_owned());
    }
    if single_precision {
        value
            .parse::<f32>()
            .map(|_| ())
            .map_err(|_| "xsd:float is outside the implementation numeric range".to_owned())
    } else {
        value
            .parse::<f64>()
            .map(|_| ())
            .map_err(|_| "xsd:double is outside the implementation numeric range".to_owned())
    }
}

fn valid_floating_lexical(value: &str) -> bool {
    let unsigned = value
        .strip_prefix('+')
        .or_else(|| value.strip_prefix('-'))
        .unwrap_or(value);
    if unsigned.is_empty() || !unsigned.is_ascii() {
        return false;
    }
    let exponent_position = unsigned.find('e').or_else(|| unsigned.find('E'));
    let (mantissa, exponent) = exponent_position.map_or((unsigned, None), |position| {
        (&unsigned[..position], Some(&unsigned[position + 1..]))
    });
    if exponent.is_some_and(|part| {
        let digits = part
            .strip_prefix('+')
            .or_else(|| part.strip_prefix('-'))
            .unwrap_or(part);
        digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit())
    }) {
        return false;
    }
    let mut points = mantissa.split('.');
    let left = points.next().unwrap_or_default();
    let right = points.next();
    if points.next().is_some() {
        return false;
    }
    let digits = |text: &str| text.bytes().all(|byte| byte.is_ascii_digit());
    match right {
        None => !left.is_empty() && digits(left),
        Some(fraction) => {
            digits(left) && digits(fraction) && (!left.is_empty() || !fraction.is_empty())
        }
    }
}

fn validate_any_uri(value: &str) -> Result<(), String> {
    if value.chars().any(|ch| {
        ch.is_control()
            || ch.is_whitespace()
            || matches!(ch, '<' | '>' | '"' | '{' | '}' | '|' | '\\' | '^' | '`')
    }) {
        Err("anyURI contains an unescaped control, whitespace, or delimiter character".to_owned())
    } else {
        Ok(())
    }
}

fn split_timezone(value: &str) -> Result<(&str, Option<&str>), String> {
    if let Some(core) = value.strip_suffix('Z') {
        return Ok((core, Some("Z")));
    }
    if value.len() >= 6 {
        let position = value.len() - 6;
        let bytes = value.as_bytes();
        if matches!(bytes[position], b'+' | b'-') && bytes[position + 3] == b':' {
            let timezone = &value[position..];
            validate_timezone(timezone)?;
            return Ok((&value[..position], Some(timezone)));
        }
    }
    Ok((value, None))
}

fn validate_timezone(value: &str) -> Result<(), String> {
    if value == "Z" {
        return Ok(());
    }
    let bytes = value.as_bytes();
    if bytes.len() != 6 || !matches!(bytes[0], b'+' | b'-') || bytes[3] != b':' {
        return Err("invalid timezone offset".to_owned());
    }
    let hour = parse_two(&value[1..3])?;
    let minute = parse_two(&value[4..6])?;
    if hour > 14 || minute > 59 || (hour == 14 && minute != 0) {
        Err("timezone offset exceeds ±14:00".to_owned())
    } else {
        Ok(())
    }
}

fn validate_date_time(
    value: &str,
    timezone_required: bool,
    max_year_digits: usize,
) -> Result<(), String> {
    if !value.is_ascii() {
        return Err("dateTime lexical form must be ASCII".to_owned());
    }
    let (core, timezone) = split_timezone(value)?;
    if timezone_required && timezone.is_none() {
        return Err("dateTimeStamp requires an explicit timezone".to_owned());
    }
    let (date, time) = core
        .split_once('T')
        .ok_or_else(|| "dateTime requires a T separator".to_owned())?;
    validate_date_core(date, max_year_digits)?;
    validate_time_core(time)
}

fn validate_date(value: &str, max_year_digits: usize) -> Result<(), String> {
    if !value.is_ascii() {
        return Err("date lexical form must be ASCII".to_owned());
    }
    let (core, _) = split_timezone(value)?;
    validate_date_core(core, max_year_digits)
}

fn validate_time(value: &str, timezone_required: bool) -> Result<(), String> {
    if !value.is_ascii() {
        return Err("time lexical form must be ASCII".to_owned());
    }
    let (core, timezone) = split_timezone(value)?;
    if timezone_required && timezone.is_none() {
        return Err("time requires a timezone under this policy".to_owned());
    }
    validate_time_core(core)
}

fn validate_date_core(value: &str, max_year_digits: usize) -> Result<(), String> {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let mut pieces = unsigned.split('-');
    let year = pieces.next().unwrap_or_default();
    let month = pieces
        .next()
        .ok_or_else(|| "date is missing month".to_owned())?;
    let day = pieces
        .next()
        .ok_or_else(|| "date is missing day".to_owned())?;
    if pieces.next().is_some()
        || year.len() < 4
        || year.len() > max_year_digits
        || !year.bytes().all(|byte| byte.is_ascii_digit())
        || year.bytes().all(|byte| byte == b'0')
    {
        return Err("invalid year lexical form".to_owned());
    }
    let month = parse_two(month)?;
    let day = parse_two(day)?;
    if !(1..=12).contains(&month) {
        return Err("month is outside 01..12".to_owned());
    }
    let max_day = match month {
        2 if is_leap_year(year) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if day == 0 || day > max_day {
        Err("day is outside the calendar range".to_owned())
    } else {
        Ok(())
    }
}

fn validate_time_core(value: &str) -> Result<(), String> {
    let mut pieces = value.split(':');
    let hour = parse_two(pieces.next().unwrap_or_default())?;
    let minute = parse_two(
        pieces
            .next()
            .ok_or_else(|| "time is missing minutes".to_owned())?,
    )?;
    let seconds = pieces
        .next()
        .ok_or_else(|| "time is missing seconds".to_owned())?;
    if pieces.next().is_some() || minute > 59 {
        return Err("invalid time lexical form".to_owned());
    }
    let (whole_seconds, fraction) = seconds
        .split_once('.')
        .map_or((seconds, None), |(whole, frac)| (whole, Some(frac)));
    let second = parse_two(whole_seconds)?;
    if second > 59
        || fraction
            .is_some_and(|frac| frac.is_empty() || !frac.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err("seconds are outside 00..59 or fraction is invalid".to_owned());
    }
    if hour < 24 {
        return Ok(());
    }
    if hour == 24
        && minute == 0
        && second == 0
        && fraction.is_none_or(|frac| frac.bytes().all(|byte| byte == b'0'))
    {
        Ok(())
    } else {
        Err("hour 24 is only legal for 24:00:00".to_owned())
    }
}

fn parse_two(value: &str) -> Result<u8, String> {
    if value.len() != 2 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("expected exactly two decimal digits".to_owned());
    }
    value
        .parse::<u8>()
        .map_err(|_| "invalid two-digit number".to_owned())
}

fn is_leap_year(year: &str) -> bool {
    decimal_mod(year, 400) == 0 || (decimal_mod(year, 4) == 0 && decimal_mod(year, 100) != 0)
}

fn decimal_mod(value: &str, modulus: u32) -> u32 {
    value.bytes().fold(0_u32, |accumulator, byte| {
        (accumulator * 10 + u32::from(byte - b'0')) % modulus
    })
}

#[cfg(test)]
mod phase40_2_tests {
    use super::{DatatypeLexicalLimits, validate_literal};

    fn limits() -> DatatypeLexicalLimits {
        DatatypeLexicalLimits {
            integer_digits_max: 4096,
            date_time_year_digits_max: 18,
            xml_name_validation: "ascii_subset".to_owned(),
        }
    }

    #[test]
    fn rejects_ill_typed_integer_and_accepts_bounded_integer() {
        assert!(validate_literal("integer", "not-an-integer", None, &limits(), 1024).is_err());
        assert!(validate_literal("int", "2147483647", None, &limits(), 1024).is_ok());
        assert!(validate_literal("int", "2147483648", None, &limits(), 1024).is_err());
    }

    #[test]
    fn validates_language_and_datetime_stamp() {
        assert!(
            validate_literal(
                "language_tagged_string",
                "hello",
                Some("en-US"),
                &limits(),
                1024
            )
            .is_ok()
        );
        assert!(
            validate_literal("language_tagged_string", "hello", None, &limits(), 1024).is_err()
        );
        assert!(
            validate_literal(
                "date_time_stamp",
                "2026-08-24T16:20:00-05:00",
                None,
                &limits(),
                1024
            )
            .is_ok()
        );
        assert!(
            validate_literal(
                "date_time_stamp",
                "2026-08-24T16:20:00",
                None,
                &limits(),
                1024
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_oversized_lexical_forms() {
        assert!(validate_literal("string", "abcdef", None, &limits(), 5).is_err());
    }
}
