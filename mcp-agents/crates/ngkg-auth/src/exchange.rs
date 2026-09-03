//! RFC 8693-style external-to-internal token narrowing.

use std::{collections::BTreeSet, net::IpAddr, path::PathBuf, sync::Arc, time::Duration};

use futures_util::StreamExt;
use reqwest::{Client, StatusCode, header};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use url::Url;

use crate::{
    AuthError, AuthenticatedRequest, DelegationVerifier, SecretFile, bearer_header, decode_sha256,
    safe_regular_file,
};

const MAXIMUM_EXCHANGE_RESPONSE_BYTES: usize = 65_536;
const MAXIMUM_CLIENT_SECRET_FILE_BYTES: u64 = 16_384;

/// Exchange-client authentication. Workload identity/mTLS is preferred.
#[derive(Clone, Debug)]
pub enum ExchangeAuthentication {
    /// The exact exchange endpoint authenticates the workload through mTLS,
    /// service mesh, or provider workload identity.
    WorkloadIdentity,
    /// Compatibility client secret loaded from a checksum-bound mounted file.
    ClientSecretFile {
        /// OAuth client identifier.
        client_id: String,
        /// Mounted JSON file containing `{ "value": "..." }`.
        path: PathBuf,
        /// Exact file SHA-256.
        sha256: String,
    },
}

/// External bearer exchange configuration.
#[derive(Clone, Debug)]
pub struct TokenExchangeConfiguration {
    /// Exact HTTPS token endpoint; discovery and redirects are prohibited.
    pub endpoint: Url,
    /// Exact internal audience requested from the exchange service.
    pub audience: String,
    /// Maximum scopes the gateway may request or receive.
    pub requested_scopes: BTreeSet<String>,
    /// Exchange client authentication.
    pub authentication: ExchangeAuthentication,
    /// Connect timeout.
    pub connect_timeout: Duration,
    /// Whole-request timeout.
    pub request_timeout: Duration,
    /// Allow HTTPS loopback only in explicit test deployments.
    pub allow_loopback: bool,
}

/// Exchanges an external token and verifies the result as an internal token.
pub struct TokenExchangeAuthenticator {
    configuration: TokenExchangeConfiguration,
    client: Client,
    verifier: Arc<DelegationVerifier>,
    client_secret: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExchangeResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    issued_token_type: Option<String>,
}

impl TokenExchangeAuthenticator {
    /// Construct a bounded exchange client.
    pub fn new(
        configuration: TokenExchangeConfiguration,
        verifier: Arc<DelegationVerifier>,
    ) -> Result<Self, AuthError> {
        validate_configuration(&configuration)?;
        let client_secret = match &configuration.authentication {
            ExchangeAuthentication::WorkloadIdentity => None,
            ExchangeAuthentication::ClientSecretFile { path, sha256, .. } => {
                let bytes = safe_regular_file(path, MAXIMUM_CLIENT_SECRET_FILE_BYTES)?;
                let expected = decode_sha256(sha256)?;
                let observed: [u8; 32] = Sha256::digest(&bytes).into();
                if expected != observed {
                    return Err(AuthError::Configuration("client secret checksum mismatch"));
                }
                let file: SecretFile = serde_json::from_slice(&bytes)?;
                if file.value.is_empty() || file.value.len() > 8_192 {
                    return Err(AuthError::Configuration("client secret is outside bounds"));
                }
                Some(file.value)
            }
        };
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(configuration.connect_timeout)
            .timeout(configuration.request_timeout)
            .build()
            .map_err(|_| AuthError::Configuration("exchange HTTP client is invalid"))?;
        Ok(Self {
            configuration,
            client,
            verifier,
            client_secret,
        })
    }

    /// Exchange the external token and verify the narrower internal result.
    pub async fn authenticate(
        &self,
        external_token: &str,
    ) -> Result<AuthenticatedRequest, AuthError> {
        let scope = self
            .configuration
            .requested_scopes
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(" ");
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair(
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            )
            .append_pair("subject_token", external_token)
            .append_pair(
                "subject_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            )
            .append_pair(
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:jwt",
            )
            .append_pair("audience", &self.configuration.audience)
            .append_pair("scope", &scope)
            .finish();
        let mut request = self
            .client
            .post(self.configuration.endpoint.clone())
            .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .header(header::ACCEPT, "application/json")
            .body(body);
        if let (ExchangeAuthentication::ClientSecretFile { client_id, .. }, Some(secret)) = (
            &self.configuration.authentication,
            self.client_secret.as_deref(),
        ) {
            request = request.basic_auth(client_id, Some(secret));
        }
        let response = request.send().await.map_err(|_| AuthError::Unavailable)?;
        if response.status() != StatusCode::OK {
            return Err(AuthError::Unauthorized);
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| AuthError::Unavailable)?;
            if bytes.len().saturating_add(chunk.len()) > MAXIMUM_EXCHANGE_RESPONSE_BYTES {
                return Err(AuthError::Unavailable);
            }
            bytes.extend_from_slice(&chunk);
        }
        let exchanged: ExchangeResponse =
            serde_json::from_slice(&bytes).map_err(|_| AuthError::Unavailable)?;
        if !exchanged.token_type.eq_ignore_ascii_case("Bearer")
            || exchanged.access_token.is_empty()
            || exchanged.expires_in == 0
            || exchanged.expires_in > 600
            || exchanged.issued_token_type.as_deref().is_some_and(|value| {
                value != "urn:ietf:params:oauth:token-type:jwt"
                    && value != "urn:ietf:params:oauth:token-type:access_token"
            })
        {
            return Err(AuthError::Unauthorized);
        }
        let response_scopes = exchanged
            .scope
            .split_ascii_whitespace()
            .map(ToOwned::to_owned)
            .collect::<BTreeSet<_>>();
        if !response_scopes.is_empty()
            && !response_scopes.is_subset(&self.configuration.requested_scopes)
        {
            return Err(AuthError::Unauthorized);
        }
        let identity = self.verifier.verify(&exchanged.access_token).await?;
        if !identity
            .scopes
            .is_subset(&self.configuration.requested_scopes)
            || !identity.audiences.contains(&self.configuration.audience)
        {
            return Err(AuthError::Unauthorized);
        }
        Ok(AuthenticatedRequest {
            identity,
            upstream_authorization: bearer_header(&exchanged.access_token)?,
        })
    }

    /// Readiness is bound to the internal JWKS verifier.
    pub async fn ready(&self) -> Result<(), AuthError> {
        self.verifier.ready().await
    }
}

fn validate_configuration(configuration: &TokenExchangeConfiguration) -> Result<(), AuthError> {
    if configuration.audience.is_empty()
        || configuration.requested_scopes.is_empty()
        || !configuration.requested_scopes.contains("queries:execute")
    {
        return Err(AuthError::Configuration("exchange authority is invalid"));
    }
    let endpoint = &configuration.endpoint;
    if endpoint.scheme() != "https"
        || endpoint.username() != ""
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || endpoint.host_str().is_none()
    {
        return Err(AuthError::Configuration(
            "exchange endpoint must be exact HTTPS",
        ));
    }
    if let Some(host) = endpoint.host_str() {
        let loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback());
        if loopback && !configuration.allow_loopback {
            return Err(AuthError::Configuration("exchange loopback is disabled"));
        }
    }
    if let ExchangeAuthentication::ClientSecretFile { client_id, .. } =
        &configuration.authentication
        && (client_id.is_empty() || client_id.len() > 256)
    {
        return Err(AuthError::Configuration("exchange client ID is invalid"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exchange_must_be_scope_narrowing_and_https() -> Result<(), url::ParseError> {
        let invalid = TokenExchangeConfiguration {
            endpoint: Url::parse("http://identity.example.test/token")?,
            audience: "ngkg".to_owned(),
            requested_scopes: BTreeSet::from(["admin:*".to_owned()]),
            authentication: ExchangeAuthentication::WorkloadIdentity,
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            allow_loopback: false,
        };
        assert!(validate_configuration(&invalid).is_err());
        Ok(())
    }
}
