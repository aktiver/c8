//! NGKG 1.0 opaque-token compatibility verifier.

use std::{
    collections::{BTreeSet, HashMap},
    path::PathBuf,
    sync::Arc,
};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    AuthError, AuthenticatedRequest, Identity, bearer_header, decode_sha256, safe_regular_file,
};

const MAXIMUM_TOKEN_FILE_BYTES: u64 = 1_048_576;

/// Checksum-bound token-file configuration.
#[derive(Clone, Debug)]
pub struct OpaqueConfiguration {
    /// Mounted JSON token file.
    pub token_file: PathBuf,
    /// Exact lowercase SHA-256 of the file.
    pub token_file_sha256: String,
}

/// Loaded immutable compatibility-token map.
#[derive(Clone)]
pub struct OpaqueAuthorizer {
    identities: Arc<HashMap<[u8; 32], Identity>>,
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

impl OpaqueAuthorizer {
    pub(crate) fn load(configuration: &OpaqueConfiguration) -> Result<Self, AuthError> {
        let bytes = safe_regular_file(&configuration.token_file, MAXIMUM_TOKEN_FILE_BYTES)?;
        let expected = decode_sha256(&configuration.token_file_sha256)?;
        let observed: [u8; 32] = Sha256::digest(&bytes).into();
        if observed != expected {
            return Err(AuthError::Configuration("token file checksum mismatch"));
        }
        let file: TokenFile = serde_json::from_slice(&bytes)?;
        if file.format_version != 1 || file.tokens.is_empty() {
            return Err(AuthError::Configuration("unsupported or empty token file"));
        }
        let mut identities = HashMap::new();
        for entry in file.tokens {
            if entry.tenant_id.is_nil()
                || entry.principal_id.is_empty()
                || entry.principal_id.len() > 256
                || !entry.scopes.contains("queries:execute")
                || entry.graph_authorization_labels.is_empty()
            {
                return Err(AuthError::Configuration("opaque identity is invalid"));
            }
            let hash = decode_sha256(&entry.token_sha256)?;
            let identity = Identity {
                tenant_id: entry.tenant_id,
                subject: entry.principal_id,
                actor: Some("ngkg-mcp-gateway".to_owned()),
                scopes: entry.scopes,
                graph_authorization_labels: entry.graph_authorization_labels,
                policy_version_sha256: expected,
                issuer: None,
                audiences: BTreeSet::new(),
                issued_at: None,
                not_before: None,
                expires_at: None,
                jti: None,
                agent_execution_id: None,
                client_id: None,
            };
            if identities.insert(hash, identity).is_some() {
                return Err(AuthError::Configuration("duplicate opaque token hash"));
            }
        }
        Ok(Self {
            identities: Arc::new(identities),
        })
    }

    pub(crate) fn authenticate(&self, token: &str) -> Result<AuthenticatedRequest, AuthError> {
        let hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let identity = self
            .identities
            .get(&hash)
            .cloned()
            .ok_or(AuthError::Unauthorized)?;
        Ok(AuthenticatedRequest {
            identity,
            upstream_authorization: bearer_header(token)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_hash_lookup_never_accepts_raw_hash_text() {
        let token = "not-a-real-secret";
        let hash: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        let identity = Identity {
            tenant_id: Uuid::from_u128(1),
            subject: "subject".to_owned(),
            actor: Some("ngkg-mcp-gateway".to_owned()),
            scopes: BTreeSet::from(["queries:execute".to_owned()]),
            graph_authorization_labels: BTreeSet::from(["tenant".to_owned()]),
            policy_version_sha256: [7; 32],
            issuer: None,
            audiences: BTreeSet::new(),
            issued_at: None,
            not_before: None,
            expires_at: None,
            jti: None,
            agent_execution_id: None,
            client_id: None,
        };
        let authorizer = OpaqueAuthorizer {
            identities: Arc::new(HashMap::from([(hash, identity)])),
        };
        assert!(authorizer.authenticate(token).is_ok());
        assert!(authorizer.authenticate(&hex::encode(hash)).is_err());
    }
}
