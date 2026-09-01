//! Resumable, content-addressed agent input and deterministic context compiler.
//!
//! Exact source bytes remain authoritative in object storage. Derived chunks,
//! requirements, summaries and indexes can always be discarded and rebuilt.

#![allow(missing_docs)]

mod compiler;
mod repository;
mod storage;

pub use compiler::{
    CompileLimits, CompiledContext, CompiledPart, PromptChunk, PromptRequirement, RequirementKind,
    compile_part, reduce_context,
};
pub use repository::{
    ClaimedShard, CreateInput, InputManifest, InputPart, InputRepository, InputStatus,
    RepositoryError, RequirementRecord,
};
pub use storage::{InputObjectStore, ObjectStoreConfiguration, StorageError};
