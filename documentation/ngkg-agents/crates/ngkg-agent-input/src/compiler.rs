use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const CHUNK_DOMAIN: &[u8] = b"ngkg-prompt-chunk-v1\0";
const REQUIREMENT_DOMAIN: &[u8] = b"ngkg-prompt-requirement-v1\0";
const PART_COMPILE_DOMAIN: &[u8] = b"ngkg-prompt-compiled-part-v1\0";

#[derive(Clone, Copy, Debug)]
pub struct CompileLimits {
    pub target_chunk_bytes: usize,
    pub maximum_chunk_bytes: usize,
    pub maximum_chunks: usize,
    pub maximum_requirements: usize,
}

impl Default for CompileLimits {
    fn default() -> Self {
        Self {
            target_chunk_bytes: 16_384,
            maximum_chunk_bytes: 65_536,
            maximum_chunks: 100_000,
            maximum_requirements: 100_000,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RequirementKind {
    Instruction,
    Prohibition,
    AcceptanceCriterion,
    RequiredOutput,
    Identifier,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptChunk {
    pub chunk_id: String,
    pub ordinal: u32,
    pub byte_start: u64,
    pub byte_end: u64,
    pub heading_path: Vec<String>,
    pub text_sha256: String,
    pub text: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptRequirement {
    pub requirement_id: String,
    pub kind: RequirementKind,
    pub mandatory: bool,
    pub source_chunk_id: String,
    pub byte_start: u64,
    pub byte_end: u64,
    pub normalized_text: String,
    pub text_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompiledPart {
    pub part_ordinal: u32,
    pub source_sha256: String,
    pub chunks: Vec<PromptChunk>,
    pub requirements: Vec<PromptRequirement>,
    pub compiled_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompiledContext {
    pub selected_chunk_ids: Vec<String>,
    pub requirement_ids: Vec<String>,
    pub encoded_bytes: usize,
    pub context_sha256: String,
}

#[derive(Debug, Error)]
pub enum CompileError {
    #[error("source is not valid UTF-8")]
    InvalidUtf8,
    #[error("compiler limit exceeded")]
    Limit,
    #[error("source offset overflow")]
    Overflow,
}

/// Compile one immutable part. Parallel extraction is followed by a canonical
/// ordinal sort, making the root independent of Rayon scheduling and core count.
pub fn compile_part(
    input_id: Uuid,
    part_ordinal: u32,
    source: &[u8],
    limits: CompileLimits,
) -> Result<CompiledPart, CompileError> {
    if limits.target_chunk_bytes == 0
        || limits.maximum_chunk_bytes < limits.target_chunk_bytes
        || limits.maximum_chunks == 0
    {
        return Err(CompileError::Limit);
    }
    let source_sha256 = sha256(source);
    let Ok(text) = std::str::from_utf8(source) else {
        let compiled_sha256 = compiled_root(part_ordinal, &source_sha256, &[], &[]);
        return Ok(CompiledPart {
            part_ordinal,
            source_sha256,
            chunks: Vec::new(),
            requirements: Vec::new(),
            compiled_sha256,
        });
    };
    let spans = structural_spans(text, limits)?;
    let mut chunks = spans
        .par_iter()
        .enumerate()
        .map(|(ordinal, span)| {
            let value = &text[span.start..span.end];
            let text_sha256 = sha256(value.as_bytes());
            let chunk_id = stable_id(
                CHUNK_DOMAIN,
                input_id,
                part_ordinal,
                span.start,
                span.end,
                &text_sha256,
            );
            Ok(PromptChunk {
                chunk_id,
                ordinal: u32::try_from(ordinal).map_err(|_| CompileError::Overflow)?,
                byte_start: u64::try_from(span.start).map_err(|_| CompileError::Overflow)?,
                byte_end: u64::try_from(span.end).map_err(|_| CompileError::Overflow)?,
                heading_path: span.heading_path.clone(),
                text_sha256,
                text: value.to_owned(),
            })
        })
        .collect::<Result<Vec<_>, CompileError>>()?;
    chunks.sort_by_key(|chunk| chunk.ordinal);
    let mut requirements = chunks
        .par_iter()
        .flat_map(|chunk| extract_requirements(input_id, part_ordinal, chunk))
        .collect::<Vec<_>>();
    requirements
        .sort_by(|a, b| (a.byte_start, &a.requirement_id).cmp(&(b.byte_start, &b.requirement_id)));
    requirements.dedup_by(|a, b| a.requirement_id == b.requirement_id);
    if requirements.len() > limits.maximum_requirements {
        return Err(CompileError::Limit);
    }
    let compiled_sha256 = compiled_root(part_ordinal, &source_sha256, &chunks, &requirements);
    Ok(CompiledPart {
        part_ordinal,
        source_sha256,
        chunks,
        requirements,
        compiled_sha256,
    })
}

/// Deterministic extractive reduction. Every mandatory requirement's source
/// chunk is included before optional structural context. No model summary can
/// replace or weaken a source instruction.
pub fn reduce_context(
    parts: &[CompiledPart],
    budget_bytes: usize,
) -> Result<CompiledContext, CompileError> {
    if budget_bytes == 0 {
        return Err(CompileError::Limit);
    }
    let mut chunks = parts
        .iter()
        .flat_map(|part| {
            part.chunks
                .iter()
                .map(move |chunk| (part.part_ordinal, chunk))
        })
        .collect::<Vec<_>>();
    chunks.sort_by_key(|(part, chunk)| (*part, chunk.ordinal));
    let mut requirements = parts
        .iter()
        .flat_map(|part| {
            part.requirements
                .iter()
                .map(move |requirement| (part.part_ordinal, requirement))
        })
        .collect::<Vec<_>>();
    requirements.sort_by(|(part_a, a), (part_b, b)| {
        (*part_a, a.byte_start, &a.requirement_id).cmp(&(*part_b, b.byte_start, &b.requirement_id))
    });
    let mandatory = requirements
        .iter()
        .filter(|(_, r)| r.mandatory)
        .map(|(_, r)| r.source_chunk_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut selected = Vec::new();
    let mut bytes = 0usize;
    for (_, chunk) in chunks
        .iter()
        .filter(|(_, c)| mandatory.contains(c.chunk_id.as_str()))
        .chain(
            chunks
                .iter()
                .filter(|(_, c)| !mandatory.contains(c.chunk_id.as_str())),
        )
    {
        if selected.iter().any(|id| id == &chunk.chunk_id) {
            continue;
        }
        let next = bytes
            .checked_add(chunk.text.len())
            .ok_or(CompileError::Overflow)?;
        if next > budget_bytes {
            if mandatory.contains(chunk.chunk_id.as_str()) {
                return Err(CompileError::Limit);
            }
            continue;
        }
        selected.push(chunk.chunk_id.clone());
        bytes = next;
    }
    let requirement_ids = requirements
        .into_iter()
        .map(|(_, r)| r.requirement_id.clone())
        .collect::<Vec<_>>();
    let mut d = Sha256::new();
    d.update(b"ngkg-prompt-context-root-v1\0");
    d.update(
        u64::try_from(budget_bytes)
            .map_err(|_| CompileError::Overflow)?
            .to_be_bytes(),
    );
    for id in &selected {
        d.update(id.as_bytes());
    }
    for id in &requirement_ids {
        d.update(id.as_bytes());
    }
    Ok(CompiledContext {
        selected_chunk_ids: selected,
        requirement_ids,
        encoded_bytes: bytes,
        context_sha256: hex::encode(d.finalize()),
    })
}

#[derive(Clone)]
struct Span {
    start: usize,
    end: usize,
    heading_path: Vec<String>,
}

fn structural_spans(text: &str, limits: CompileLimits) -> Result<Vec<Span>, CompileError> {
    let mut spans = Vec::new();
    let mut start = 0usize;
    let mut headings = Vec::<String>::new();
    let mut current_headings = Vec::<String>::new();
    for line in text.split_inclusive('\n') {
        let end = start
            .checked_add(line.len())
            .ok_or(CompileError::Overflow)?;
        let trimmed = line.trim();
        if let Some(level) = heading_level(trimmed) {
            headings.truncate(level.saturating_sub(1));
            headings.push(trimmed[level..].trim().to_owned());
            current_headings.clone_from(&headings);
        }
        let last_start = spans.last().map_or(0, |span: &Span| span.end);
        let accumulated = end.saturating_sub(last_start);
        let boundary = trimmed.is_empty() || heading_level(trimmed).is_some();
        if (boundary && accumulated >= limits.target_chunk_bytes)
            || accumulated >= limits.maximum_chunk_bytes
        {
            if end > last_start {
                spans.push(Span {
                    start: last_start,
                    end,
                    heading_path: current_headings.clone(),
                });
            }
            if spans.len() > limits.maximum_chunks {
                return Err(CompileError::Limit);
            }
        }
        start = end;
    }
    let last = spans.last().map_or(0, |span| span.end);
    if last < text.len() {
        spans.push(Span {
            start: last,
            end: text.len(),
            heading_path: current_headings,
        });
    }
    if spans.is_empty() && !text.is_empty() {
        spans.push(Span {
            start: 0,
            end: text.len(),
            heading_path: Vec::new(),
        });
    }
    if spans.len() > limits.maximum_chunks {
        return Err(CompileError::Limit);
    }
    Ok(spans)
}

fn heading_level(line: &str) -> Option<usize> {
    let count = line.bytes().take_while(|byte| *byte == b'#').count();
    (count > 0 && count <= 6 && line.as_bytes().get(count) == Some(&b' ')).then_some(count)
}

fn extract_requirements(
    input_id: Uuid,
    part_ordinal: u32,
    chunk: &PromptChunk,
) -> Vec<PromptRequirement> {
    let mut offset = usize::try_from(chunk.byte_start).unwrap_or(usize::MAX);
    let mut result = Vec::new();
    for line in chunk.text.split_inclusive('\n') {
        let trimmed = line.trim();
        let upper = trimmed.to_ascii_uppercase();
        let kind = if contains_any(
            &upper,
            &["MUST NOT", "SHALL NOT", "DO NOT", "NEVER", "PROHIBITED"],
        ) {
            Some(RequirementKind::Prohibition)
        } else if contains_any(
            &upper,
            &["ACCEPTANCE", "PASS WHEN", "QUALIFICATION", "VALIDATE THAT"],
        ) {
            Some(RequirementKind::AcceptanceCriterion)
        } else if contains_any(&upper, &["RETURN ", "OUTPUT ", "PRODUCE ", "DELIVER "]) {
            Some(RequirementKind::RequiredOutput)
        } else if contains_any(
            &upper,
            &["MUST ", "SHALL ", "REQUIRED", "NEED TO", "SHOULD "],
        ) {
            Some(RequirementKind::Instruction)
        } else if trimmed.starts_with("REQ-") || trimmed.starts_with("ID:") {
            Some(RequirementKind::Identifier)
        } else {
            None
        };
        let leading = line.len().saturating_sub(line.trim_start().len());
        let start = offset.saturating_add(leading);
        let end = start.saturating_add(trimmed.len());
        offset = offset.saturating_add(line.len());
        let Some(kind) = kind else { continue };
        let normalized_text = trimmed.split_whitespace().collect::<Vec<_>>().join(" ");
        let text_sha256 = sha256(normalized_text.as_bytes());
        result.push(PromptRequirement {
            requirement_id: stable_id(
                REQUIREMENT_DOMAIN,
                input_id,
                part_ordinal,
                start,
                end,
                &text_sha256,
            ),
            kind,
            mandatory: !matches!(kind, RequirementKind::Identifier),
            source_chunk_id: chunk.chunk_id.clone(),
            byte_start: u64::try_from(start).unwrap_or(u64::MAX),
            byte_end: u64::try_from(end).unwrap_or(u64::MAX),
            normalized_text,
            text_sha256,
        });
    }
    result
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}
fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn stable_id(
    domain: &[u8],
    input_id: Uuid,
    part: u32,
    start: usize,
    end: usize,
    text_sha256: &str,
) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(input_id.as_bytes());
    digest.update(part.to_be_bytes());
    digest.update(u64::try_from(start).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(u64::try_from(end).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(text_sha256.as_bytes());
    format!(
        "{}-{}",
        if domain == CHUNK_DOMAIN { "CHK" } else { "REQ" },
        hex::encode(digest.finalize())
    )
}
fn compiled_root(
    part: u32,
    source: &str,
    chunks: &[PromptChunk],
    requirements: &[PromptRequirement],
) -> String {
    let mut digest = Sha256::new();
    digest.update(PART_COMPILE_DOMAIN);
    digest.update(part.to_be_bytes());
    digest.update(source.as_bytes());
    for chunk in chunks {
        digest.update(chunk.chunk_id.as_bytes());
        digest.update(chunk.text_sha256.as_bytes());
    }
    for requirement in requirements {
        digest.update(requirement.requirement_id.as_bytes());
        digest.update(requirement.text_sha256.as_bytes());
    }
    hex::encode(digest.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deterministic_across_thread_pools() -> Result<(), Box<dyn std::error::Error>> {
        let source = b"# Scope\nThe service MUST preserve evidence.\n\nIt MUST NOT infer false from absence.\n";
        let input = Uuid::from_u128(1);
        let one = rayon::ThreadPoolBuilder::new()
            .num_threads(1)
            .build()?
            .install(|| compile_part(input, 0, source, CompileLimits::default()))?;
        let four = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()?
            .install(|| compile_part(input, 0, source, CompileLimits::default()))?;
        assert_eq!(one, four);
        assert_eq!(one.requirements.len(), 2);
        let reduced = reduce_context(std::slice::from_ref(&one), source.len())?;
        assert_eq!(reduced.requirement_ids.len(), 2);
        assert_eq!(reduced.selected_chunk_ids.len(), one.chunks.len());
        Ok(())
    }
}
