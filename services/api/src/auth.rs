use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::Path,
};

use axum::http::HeaderMap;
use ngkg_dataset::valid_authorization_label;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

const TOKEN_FILE_FORMAT_VERSION: u32 = 1;
const MAX_TOKEN_FILE_BYTES: u64 = 1_048_576;
const MAX_PRINCIPAL_ID_BYTES: usize = 256;
const QUERY_EXECUTE_SCOPE: &str = "queries:execute";
const VALID_SCOPES: [&str; 13] = [
    "datasets:write",
    "sources:write",
    "ingestions:create",
    "jobs:read",
    "jobs:cancel",
    "snapshots:read",
    "snapshots:publish",
    "storage:read",
    "storage:write",
    "storage:restore",
    QUERY_EXECUTE_SCOPE,
    "query-logs:read",
    "query-logs:read:text",
];

#[derive(Clone, Debug)]
pub(crate) struct Identity {
    pub(crate) tenant_id: Uuid,
    pub(crate) principal_id: String,
    scopes: BTreeSet<String>,
    pub(crate) graph_authorization_labels: BTreeSet<String>,
}

#[derive(Clone)]
pub(crate) struct TokenAuthorizer {
    identities: HashMap<[u8; 32], Identity>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TokenFile {
    format_version: u32,
    tokens: Vec<TokenEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TokenEntry {
    token_sha256: String,
    tenant_id: Uuid,
    principal_id: String,
    scopes: BTreeSet<String>,
    #[serde(default)]
    graph_authorization_labels: BTreeSet<String>,
}

impl TokenAuthorizer {
    pub(crate) fn load(path: &Path, expected_sha256: &str) -> Result<Self, String> {
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| format!("cannot inspect authentication token file: {error}"))?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > MAX_TOKEN_FILE_BYTES
        {
            return Err(
                "authentication token file must be a non-empty regular file no larger than 1 MiB"
                    .to_owned(),
            );
        }
        let bytes = fs::read(path)
            .map_err(|error| format!("cannot read authentication token file: {error}"))?;
        let expected = decode_sha256(expected_sha256)
            .map_err(|_| "authentication token file SHA-256 must be 64 lowercase hexadecimal characters".to_owned())?;
        let observed: [u8; 32] = Sha256::digest(&bytes).into();
        if observed != expected {
            return Err("authentication token file checksum does not match its deployment".to_owned());
        }
        let config: TokenFile = serde_json::from_slice(&bytes)
            .map_err(|error| format!("authentication token file is invalid: {error}"))?;
        if config.format_version != TOKEN_FILE_FORMAT_VERSION || config.tokens.is_empty() {
            return Err("authentication token file must be non-empty formatVersion 1".to_owned());
        }
        let mut identities = HashMap::new();
        for token in config.tokens {
            validate_entry(&token)?;
            let hash = decode_sha256(&token.token_sha256)?;
            let identity = Identity {
                tenant_id: token.tenant_id,
                principal_id: token.principal_id,
                scopes: token.scopes,
                graph_authorization_labels: token.graph_authorization_labels,
            };
            if identities.insert(hash, identity).is_some() {
                return Err("authentication token hashes must be unique".to_owned());
            }
        }
        Ok(Self { identities })
    }

    pub(crate) fn authorize(
        &self,
        headers: &HeaderMap,
        required_scope: &str,
    ) -> Result<Identity, AuthError> {
        let value = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or(AuthError::Unauthenticated)?;
        let token = value
            .strip_prefix("Bearer ")
            .filter(|token| !token.is_empty())
            .ok_or(AuthError::Unauthenticated)?;
        let hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let identity = self
            .identities
            .get(&hash)
            .ok_or(AuthError::Unauthenticated)?;
        if !identity.scopes.contains(required_scope) {
            return Err(AuthError::Forbidden);
        }
        Ok(identity.clone())
    }
}

fn validate_entry(token: &TokenEntry) -> Result<(), String> {
    if token.tenant_id.is_nil() {
        return Err("authentication tenantId must be a non-nil UUID".to_owned());
    }
    if token.principal_id.is_empty()
        || token.principal_id.len() > MAX_PRINCIPAL_ID_BYTES
        || token.scopes.is_empty()
    {
        return Err(
            "authentication identities require a 1..256 byte principal and at least one scope"
                .to_owned(),
        );
    }
    if token
        .scopes
        .iter()
        .any(|scope| !VALID_SCOPES.contains(&scope.as_str()))
    {
        return Err("authentication identity contains an unknown scope".to_owned());
    }
    if token
        .graph_authorization_labels
        .iter()
        .any(|label| !valid_authorization_label(label))
    {
        return Err("authentication identity contains an invalid graph label".to_owned());
    }
    if token.scopes.contains(QUERY_EXECUTE_SCOPE)
        && token.graph_authorization_labels.is_empty()
    {
        return Err(
            "queries:execute identities require at least one graphAuthorizationLabel".to_owned(),
        );
    }
    Ok(())
}

pub(crate) enum AuthError {
    Unauthenticated,
    Forbidden,
}

fn decode_sha256(value: &str) -> Result<[u8; 32], String> {
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err("tokenSha256 must be 64 lowercase hexadecimal characters".to_owned());
    }
    hex::decode(value)
        .map_err(|error| error.to_string())?
        .try_into()
        .map_err(|_| "tokenSha256 has the wrong decoded length".to_owned())
}

#[cfg(test)]
mod tests {
    use super::decode_sha256;

    #[test]
    fn token_hash_contract_is_lowercase_and_fixed_width() {
        assert!(decode_sha256(&"a".repeat(64)).is_ok());
        assert!(decode_sha256(&"A".repeat(64)).is_err());
        assert!(decode_sha256("abc").is_err());
    }
}
