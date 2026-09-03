//! Shared NGKG authentication boundary.
//!
//! The crate deliberately supports two explicit trust modes. Opaque mode keeps
//! the NGKG 1.0 checksum-bound token-file contract. Delegation mode verifies a
//! short-lived asymmetric JWT and can exchange an external bearer for a
//! narrower, NGKG-audience delegation. The modes never fall back to each other.

mod delegation;
mod exchange;
mod opaque;

use std::{collections::BTreeSet, path::Path, sync::Arc};

use http::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use delegation::{DelegationConfiguration, DelegationVerifier};
pub use exchange::{ExchangeAuthentication, TokenExchangeConfiguration};
pub use opaque::OpaqueConfiguration;

/// Maximum accepted bearer size, including compact JWTs.
pub const MAXIMUM_BEARER_BYTES: usize = 16_384;

/// An immutable identity produced only by a configured trusted verifier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Identity {
    /// Authoritative tenant; never sourced from a tool argument or request body.
    pub tenant_id: Uuid,
    /// Original user or workload subject.
    pub subject: String,
    /// Delegating gateway/client actor, when different from the subject.
    pub actor: Option<String>,
    /// Authorized scopes.
    pub scopes: BTreeSet<String>,
    /// Trusted graph-policy labels.
    pub graph_authorization_labels: BTreeSet<String>,
    /// Immutable policy version checksum.
    pub policy_version_sha256: [u8; 32],
    /// Token issuer in delegation mode.
    pub issuer: Option<String>,
    /// Exact token audiences in delegation mode.
    pub audiences: BTreeSet<String>,
    /// Issued-at epoch seconds.
    pub issued_at: Option<u64>,
    /// Not-before epoch seconds.
    pub not_before: Option<u64>,
    /// Expiry epoch seconds.
    pub expires_at: Option<u64>,
    /// Unique token identifier in delegation mode.
    pub jti: Option<String>,
    /// Managed agent execution binding, when present.
    pub agent_execution_id: Option<Uuid>,
    /// OAuth client identity, when present.
    pub client_id: Option<String>,
}

/// Authenticated request context and the bearer authorized for NGKG upstream.
#[derive(Clone, Debug)]
pub struct AuthenticatedRequest {
    /// Verified identity.
    pub identity: Identity,
    /// Internal bearer. Callers must never log or persist it.
    pub upstream_authorization: HeaderValue,
}

/// Explicit authentication configuration. Variants are mutually exclusive.
#[derive(Clone, Debug)]
pub enum AuthenticationConfiguration {
    /// Checksum-bound NGKG 1.0 opaque compatibility tokens.
    Opaque(OpaqueConfiguration),
    /// Direct internal delegation JWT verification.
    Delegation(Box<DelegationConfiguration>),
    /// External OAuth bearer exchange followed by internal JWT verification.
    DelegationExchange {
        /// Internal delegation verifier configuration.
        delegation: Box<DelegationConfiguration>,
        /// Narrowing token-exchange configuration.
        exchange: Box<TokenExchangeConfiguration>,
    },
}

/// Runtime authenticator selected exactly once at startup.
#[derive(Clone)]
pub enum Authenticator {
    /// Opaque compatibility verifier.
    Opaque(opaque::OpaqueAuthorizer),
    /// Direct internal delegation verifier.
    Delegation(Arc<DelegationVerifier>),
    /// External exchange plus internal delegation verification.
    DelegationExchange(Arc<exchange::TokenExchangeAuthenticator>),
}

impl Authenticator {
    /// Construct and prime the selected verifier. Delegation startup fails when
    /// no valid JWKS can be obtained; readiness never reports a false success.
    pub async fn build(configuration: AuthenticationConfiguration) -> Result<Self, AuthError> {
        match configuration {
            AuthenticationConfiguration::Opaque(configuration) => Ok(Self::Opaque(
                opaque::OpaqueAuthorizer::load(&configuration)?,
            )),
            AuthenticationConfiguration::Delegation(configuration) => Ok(Self::Delegation(
                Arc::new(DelegationVerifier::new(*configuration).await?),
            )),
            AuthenticationConfiguration::DelegationExchange {
                delegation,
                exchange,
            } => {
                let verifier = Arc::new(DelegationVerifier::new(*delegation).await?);
                Ok(Self::DelegationExchange(Arc::new(
                    exchange::TokenExchangeAuthenticator::new(*exchange, verifier)?,
                )))
            }
        }
    }

    /// Authenticate a request. No variant attempts a different mode on failure.
    pub async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedRequest, AuthError> {
        let token = bearer(headers)?;
        match self {
            Self::Opaque(authorizer) => authorizer.authenticate(token),
            Self::Delegation(verifier) => {
                let identity = verifier.verify(token).await?;
                Ok(AuthenticatedRequest {
                    identity,
                    upstream_authorization: bearer_header(token)?,
                })
            }
            Self::DelegationExchange(authenticator) => authenticator.authenticate(token).await,
        }
    }

    /// Verify that the currently selected verifier remains usable.
    pub async fn ready(&self) -> Result<(), AuthError> {
        match self {
            Self::Opaque(_) => Ok(()),
            Self::Delegation(verifier) => verifier.ready().await,
            Self::DelegationExchange(authenticator) => authenticator.ready().await,
        }
    }
}

/// OAuth protected-resource metadata served by the gateway.
#[derive(Clone, Debug, Serialize)]
pub struct ProtectedResourceMetadata {
    /// Canonical MCP resource URI.
    pub resource: String,
    /// Exact authorization servers accepted by this resource.
    pub authorization_servers: Vec<String>,
    /// Bearer transport methods.
    pub bearer_methods_supported: Vec<String>,
    /// Scopes used by the read-only gateway.
    pub scopes_supported: Vec<String>,
}

/// Authentication error. Responses intentionally avoid token and policy detail.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// Bearer is absent, malformed, too large, expired, or unauthorized.
    #[error("valid bearer authentication is required")]
    Unauthorized,
    /// Operator configuration is invalid.
    #[error("authentication configuration is invalid: {0}")]
    Configuration(&'static str),
    /// A required identity or exchange dependency is unavailable.
    #[error("authentication dependency is unavailable")]
    Unavailable,
    /// I/O error during checksum-bound configuration loading.
    #[error("authentication configuration I/O failed")]
    Io(#[from] std::io::Error),
    /// JSON configuration error.
    #[error("authentication configuration JSON failed")]
    Json(#[from] serde_json::Error),
}

pub(crate) fn bearer(headers: &HeaderMap) -> Result<&str, AuthError> {
    headers
        .get(http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() <= MAXIMUM_BEARER_BYTES + 7)
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty() && value.len() <= MAXIMUM_BEARER_BYTES)
        .ok_or(AuthError::Unauthorized)
}

pub(crate) fn bearer_header(token: &str) -> Result<HeaderValue, AuthError> {
    if token.is_empty() || token.len() > MAXIMUM_BEARER_BYTES {
        return Err(AuthError::Unauthorized);
    }
    HeaderValue::from_str(&format!("Bearer {token}")).map_err(|_| AuthError::Unauthorized)
}

pub(crate) fn decode_sha256(value: &str) -> Result<[u8; 32], AuthError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(AuthError::Configuration("SHA-256 must be lowercase hex"));
    }
    hex::decode(value)
        .map_err(|_| AuthError::Configuration("SHA-256 is malformed"))?
        .try_into()
        .map_err(|_| AuthError::Configuration("SHA-256 length is invalid"))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub(crate) struct SecretFile {
    pub(crate) value: String,
}

pub(crate) fn safe_regular_file(path: &Path, maximum: u64) -> Result<Vec<u8>, AuthError> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > maximum
    {
        return Err(AuthError::Configuration("secret file safety check failed"));
    }
    Ok(std::fs::read(path)?)
}
