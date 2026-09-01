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
const QUERY_LOG_READ_SCOPE: &str = "query-logs:read";
const QUERY_LOG_TEXT_SCOPE: &str = "query-logs:read:text";
const VALID_SCOPES: [&str; 15] = [
    "datasets:write",
    "sources:write",
    "ingestions:create",
    "imports:create",
    "imports:read",
    "jobs:read",
    "jobs:cancel",
    "snapshots:read",
    "snapshots:publish",
    "storage:read",
    "storage:write",
    "storage:restore",
    QUERY_EXECUTE_SCOPE,
    QUERY_LOG_READ_SCOPE,
    QUERY_LOG_TEXT_SCOPE,
];

#[derive(Clone, Debug)]
pub(crate) struct Identity {
    pub(crate) tenant_id: Uuid,
    pub(crate) principal_id: String,
    pub(crate) graph_authorization_labels: BTreeSet<String>,
    scopes: BTreeSet<String>,
}

impl Identity {
    pub(crate) fn can_read_all_query_logs(&self) -> bool {
        self.scopes.contains(QUERY_LOG_READ_SCOPE)
    }

    pub(crate) fn can_read_query_text(&self, principal_id: &str) -> bool {
        self.principal_id == principal_id || self.scopes.contains(QUERY_LOG_TEXT_SCOPE)
    }
}

#[derive(Clone)]
pub(crate) struct TokenAuthorizer {
    identities: HashMap<[u8; 32], TokenIdentity>,
}

#[derive(Clone)]
struct TokenIdentity {
    identity: Identity,
    query_access: bool,
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
            .map_err(|error| format!("cannot inspect token file: {error}"))?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() == 0
            || metadata.len() > MAX_TOKEN_FILE_BYTES
        {
            return Err(
                "token file must be a non-empty regular file no larger than 1 MiB".to_owned(),
            );
        }
        let bytes = fs::read(path).map_err(|error| format!("cannot read token file: {error}"))?;
        let expected = decode_sha256(expected_sha256)
            .map_err(|_| "token file SHA-256 must be 64 lowercase hex characters".to_owned())?;
        let observed: [u8; 32] = Sha256::digest(&bytes).into();
        if observed != expected {
            return Err("token file checksum does not match its deployment".to_owned());
        }
        let file: TokenFile = serde_json::from_slice(&bytes)
            .map_err(|error| format!("token file is invalid: {error}"))?;
        if file.format_version != TOKEN_FILE_FORMAT_VERSION || file.tokens.is_empty() {
            return Err("token file must be non-empty formatVersion 1".to_owned());
        }
        let mut identities = HashMap::new();
        for entry in file.tokens {
            validate_entry(&entry)?;
            let hash = decode_sha256(&entry.token_sha256)?;
            let query_access = entry.scopes.contains(QUERY_EXECUTE_SCOPE);
            let value = TokenIdentity {
                identity: Identity {
                    tenant_id: entry.tenant_id,
                    principal_id: entry.principal_id,
                    graph_authorization_labels: entry.graph_authorization_labels,
                    scopes: entry.scopes,
                },
                query_access,
            };
            if identities.insert(hash, value).is_some() {
                return Err("token hashes must be unique".to_owned());
            }
        }
        Ok(Self { identities })
    }

    pub(crate) fn authorize(&self, headers: &HeaderMap) -> Result<Identity, AuthError> {
        let token = bearer(headers)?;
        let hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let value = self
            .identities
            .get(&hash)
            .ok_or(AuthError::Unauthenticated)?;
        if !value.query_access {
            return Err(AuthError::Forbidden);
        }
        Ok(value.identity.clone())
    }

    /// Query users may inspect their own executions; enterprise auditors with
    /// `query-logs:read` may inspect the complete tenant-scoped ledger.
    pub(crate) fn authorize_query_logs(&self, headers: &HeaderMap) -> Result<Identity, AuthError> {
        let token = bearer(headers)?;
        let hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let value = self
            .identities
            .get(&hash)
            .ok_or(AuthError::Unauthenticated)?;
        if !value.query_access && !value.identity.can_read_all_query_logs() {
            return Err(AuthError::Forbidden);
        }
        Ok(value.identity.clone())
    }

    pub(crate) fn query_tenant_ids(&self) -> BTreeSet<Uuid> {
        self.identities
            .values()
            .filter(|value| value.query_access)
            .map(|value| value.identity.tenant_id)
            .collect()
    }
}

fn validate_entry(entry: &TokenEntry) -> Result<(), String> {
    if entry.tenant_id.is_nil() {
        return Err("token tenantId must be a non-nil UUID".to_owned());
    }
    if entry.principal_id.is_empty() || entry.principal_id.len() > MAX_PRINCIPAL_ID_BYTES {
        return Err("token principalId must contain 1..256 bytes".to_owned());
    }
    if entry.scopes.is_empty()
        || entry
            .scopes
            .iter()
            .any(|scope| !VALID_SCOPES.contains(&scope.as_str()))
    {
        return Err("token scopes must be a non-empty set of supported scopes".to_owned());
    }
    if entry
        .graph_authorization_labels
        .iter()
        .any(|label| !valid_authorization_label(label))
    {
        return Err("token contains an invalid graph authorization label".to_owned());
    }
    if entry.scopes.contains(QUERY_EXECUTE_SCOPE) && entry.graph_authorization_labels.is_empty() {
        return Err(
            "queries:execute tokens require at least one graphAuthorizationLabel".to_owned(),
        );
    }
    Ok(())
}

pub(crate) fn bearer(headers: &HeaderMap) -> Result<&str, AuthError> {
    headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .ok_or(AuthError::Unauthenticated)
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
        return Err("tokenSha256 must be lowercase SHA-256".to_owned());
    }
    hex::decode(value)
        .map_err(|error| error.to_string())?
        .try_into()
        .map_err(|_| "tokenSha256 has the wrong decoded length".to_owned())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::{TokenAuthorizer, decode_sha256};

    #[test]
    fn token_hash_is_exact_lowercase_sha256() {
        assert!(decode_sha256(&"a".repeat(64)).is_ok());
        assert!(decode_sha256(&"A".repeat(64)).is_err());
        assert!(decode_sha256("abc").is_err());
    }

    #[test]
    fn authorizer_is_bound_to_checksum_and_graph_labels() -> Result<(), String> {
        let path = std::env::temp_dir().join(format!("ngkg-auth-{}.json", Uuid::new_v4()));
        let bytes = br#"{"formatVersion":1,"tokens":[{"tokenSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tenantId":"00000000-0000-0000-0000-000000000001","principalId":"principal","scopes":["queries:execute"],"graphAuthorizationLabels":["domain:hdfs"]}]}"#;
        fs::write(&path, bytes).map_err(|error| error.to_string())?;
        let checksum = hex::encode(Sha256::digest(bytes));
        let valid = TokenAuthorizer::load(&path, &checksum);
        let invalid_checksum = TokenAuthorizer::load(&path, &"0".repeat(64));
        fs::remove_file(path).map_err(|error| error.to_string())?;
        assert!(valid.is_ok());
        assert!(invalid_checksum.is_err());
        Ok(())
    }

    #[test]
    fn query_token_without_graph_labels_fails_closed() -> Result<(), String> {
        let path = std::env::temp_dir().join(format!("ngkg-auth-{}.json", Uuid::new_v4()));
        let bytes = br#"{"formatVersion":1,"tokens":[{"tokenSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tenantId":"00000000-0000-0000-0000-000000000001","principalId":"principal","scopes":["queries:execute"]}]}"#;
        fs::write(&path, bytes).map_err(|error| error.to_string())?;
        let checksum = hex::encode(Sha256::digest(bytes));
        let result = TokenAuthorizer::load(&path, &checksum);
        fs::remove_file(path).map_err(|error| error.to_string())?;
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn query_log_auditor_does_not_require_graph_access() -> Result<(), String> {
        let path = std::env::temp_dir().join(format!("ngkg-auth-{}.json", Uuid::new_v4()));
        let bytes = br#"{"formatVersion":1,"tokens":[{"tokenSha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","tenantId":"00000000-0000-0000-0000-000000000001","principalId":"auditor","scopes":["query-logs:read"]}]}"#;
        fs::write(&path, bytes).map_err(|error| error.to_string())?;
        let checksum = hex::encode(Sha256::digest(bytes));
        let result = TokenAuthorizer::load(&path, &checksum);
        fs::remove_file(path).map_err(|error| error.to_string())?;
        assert!(result.is_ok());
        Ok(())
    }
}
