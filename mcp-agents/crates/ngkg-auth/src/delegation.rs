//! Short-lived asymmetric NGKG delegation-token verification.

use std::{
    collections::{BTreeSet, HashSet},
    net::IpAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures_util::StreamExt;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{Jwk, JwkSet},
};
use reqwest::{Client, StatusCode, header};
use serde::Deserialize;
use tokio::sync::RwLock;
use url::Url;
use uuid::Uuid;

use crate::{AuthError, Identity, decode_sha256};

const MAXIMUM_JWKS_BYTES: usize = 1_048_576;
const MAXIMUM_JWKS_KEYS: usize = 64;

/// Strict internal delegation verifier configuration.
#[derive(Clone, Debug)]
pub struct DelegationConfiguration {
    /// Exact token issuer.
    pub issuer: String,
    /// Exact internal NGKG audience.
    pub audience: String,
    /// Operator-configured HTTPS JWKS location.
    pub jwks_url: Url,
    /// Permitted asymmetric JOSE algorithms.
    pub allowed_algorithms: BTreeSet<String>,
    /// Required JOSE `typ` value, normally `at+jwt`.
    pub required_typ: String,
    /// Maximum delegation lifetime.
    pub maximum_token_lifetime: Duration,
    /// Clock skew accepted for time claims.
    pub clock_skew: Duration,
    /// Configured key refresh interval.
    pub jwks_cache_ttl: Duration,
    /// Bounded last-known-good interval after refresh failure.
    pub jwks_last_known_good_grace: Duration,
    /// JWKS connect timeout.
    pub connect_timeout: Duration,
    /// JWKS request timeout.
    pub request_timeout: Duration,
    /// Allow HTTPS loopback only in explicit test deployments.
    pub allow_loopback: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct DelegationClaims {
    iss: String,
    #[serde(deserialize_with = "one_or_many")]
    aud: BTreeSet<String>,
    sub: String,
    exp: u64,
    nbf: u64,
    iat: u64,
    jti: String,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(rename = "tokenUse")]
    token_use: String,
    ngkg: NgkgClaims,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct NgkgClaims {
    tenant_id: Uuid,
    actor: Option<String>,
    scopes: BTreeSet<String>,
    graph_authorization_labels: BTreeSet<String>,
    policy_version_sha256: String,
    agent_execution_id: Option<Uuid>,
}

#[derive(Debug)]
struct KeyState {
    jwks: Arc<JwkSet>,
    etag: Option<String>,
    refreshed_epoch_seconds: u64,
    fresh_until_epoch_seconds: u64,
    usable_until_epoch_seconds: u64,
}

/// Thread-safe verifier with bounded, rotation-aware JWKS state.
pub struct DelegationVerifier {
    configuration: DelegationConfiguration,
    client: Client,
    keys: RwLock<KeyState>,
}

impl DelegationVerifier {
    /// Build the verifier and synchronously establish the first trusted key set.
    pub async fn new(configuration: DelegationConfiguration) -> Result<Self, AuthError> {
        validate_configuration(&configuration)?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(configuration.connect_timeout)
            .timeout(configuration.request_timeout)
            .build()
            .map_err(|_| AuthError::Configuration("JWKS HTTP client is invalid"))?;
        let now = epoch_seconds()?;
        let initial = fetch_jwks(&client, &configuration, None, now)
            .await?
            .ok_or(AuthError::Unavailable)?;
        Ok(Self {
            configuration,
            client,
            keys: RwLock::new(initial),
        })
    }

    /// Verify a bearer and produce the immutable internal identity.
    pub async fn verify(&self, token: &str) -> Result<Identity, AuthError> {
        let now = epoch_seconds()?;
        let header = decode_header(token).map_err(|_| AuthError::Unauthorized)?;
        let kid = header.kid.as_deref().ok_or(AuthError::Unauthorized)?;
        if header.typ.as_deref() != Some(self.configuration.required_typ.as_str())
            || !algorithm_allowed(header.alg, &self.configuration.allowed_algorithms)
        {
            return Err(AuthError::Unauthorized);
        }

        self.refresh_if_needed(kid, now).await?;
        let keys = self.keys.read().await;
        if now > keys.usable_until_epoch_seconds {
            return Err(AuthError::Unavailable);
        }
        let jwk = find_unique_key(&keys.jwks, kid)?;
        ensure_jwk_compatible(jwk, header.alg)?;
        let decoding_key = DecodingKey::from_jwk(jwk).map_err(|_| AuthError::Unauthorized)?;
        let mut validation = Validation::new(header.alg);
        validation.set_audience(&[self.configuration.audience.as_str()]);
        validation.set_issuer(&[self.configuration.issuer.as_str()]);
        validation.set_required_spec_claims(&["exp", "nbf", "iat", "iss", "aud", "sub", "jti"]);
        validation.validate_nbf = true;
        validation.leeway = self.configuration.clock_skew.as_secs();
        let data = decode::<DelegationClaims>(token, &decoding_key, &validation)
            .map_err(|_| AuthError::Unauthorized)?;
        identity_from_claims(data.claims, &self.configuration, now)
    }

    /// Readiness fails after the bounded last-known-good interval.
    pub async fn ready(&self) -> Result<(), AuthError> {
        let now = epoch_seconds()?;
        let state = self.keys.read().await;
        if now <= state.usable_until_epoch_seconds && !state.jwks.keys.is_empty() {
            Ok(())
        } else {
            Err(AuthError::Unavailable)
        }
    }

    async fn refresh_if_needed(&self, kid: &str, now: u64) -> Result<(), AuthError> {
        {
            let state = self.keys.read().await;
            if now <= state.fresh_until_epoch_seconds && find_unique_key(&state.jwks, kid).is_ok() {
                return Ok(());
            }
        }
        let mut state = self.keys.write().await;
        if now <= state.fresh_until_epoch_seconds && find_unique_key(&state.jwks, kid).is_ok() {
            return Ok(());
        }
        match fetch_jwks(
            &self.client,
            &self.configuration,
            state.etag.as_deref(),
            now,
        )
        .await
        {
            Ok(Some(replacement)) => {
                *state = replacement;
            }
            Ok(None) => {
                state.refreshed_epoch_seconds = now;
                state.fresh_until_epoch_seconds = now
                    .checked_add(self.configuration.jwks_cache_ttl.as_secs())
                    .ok_or(AuthError::Unavailable)?;
                state.usable_until_epoch_seconds = state
                    .fresh_until_epoch_seconds
                    .checked_add(self.configuration.jwks_last_known_good_grace.as_secs())
                    .ok_or(AuthError::Unavailable)?;
            }
            Err(error) if now <= state.usable_until_epoch_seconds => {
                tracing::warn!(
                    error = %error,
                    last_refresh_epoch_seconds = state.refreshed_epoch_seconds,
                    "JWKS refresh failed; bounded last-known-good keys retained"
                );
            }
            Err(_) => return Err(AuthError::Unavailable),
        }
        find_unique_key(&state.jwks, kid).map(|_| ())
    }
}

async fn fetch_jwks(
    client: &Client,
    configuration: &DelegationConfiguration,
    etag: Option<&str>,
    now: u64,
) -> Result<Option<KeyState>, AuthError> {
    let mut request = client.get(configuration.jwks_url.clone());
    if let Some(value) = etag {
        request = request.header(header::IF_NONE_MATCH, value);
    }
    let response = request.send().await.map_err(|_| AuthError::Unavailable)?;
    if response.status() == StatusCode::NOT_MODIFIED {
        return Ok(None);
    }
    if response.status() != StatusCode::OK {
        return Err(AuthError::Unavailable);
    }
    let response_etag = response
        .headers()
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .filter(|value| value.len() <= 256)
        .map(ToOwned::to_owned);
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| AuthError::Unavailable)?;
        if bytes.len().saturating_add(chunk.len()) > MAXIMUM_JWKS_BYTES {
            return Err(AuthError::Unavailable);
        }
        bytes.extend_from_slice(&chunk);
    }
    let jwks: JwkSet = serde_json::from_slice(&bytes).map_err(|_| AuthError::Unavailable)?;
    validate_jwks(&jwks, &configuration.allowed_algorithms)?;
    let fresh_until_epoch_seconds = now
        .checked_add(configuration.jwks_cache_ttl.as_secs())
        .ok_or(AuthError::Unavailable)?;
    let usable_until_epoch_seconds = fresh_until_epoch_seconds
        .checked_add(configuration.jwks_last_known_good_grace.as_secs())
        .ok_or(AuthError::Unavailable)?;
    Ok(Some(KeyState {
        jwks: Arc::new(jwks),
        etag: response_etag,
        refreshed_epoch_seconds: now,
        fresh_until_epoch_seconds,
        usable_until_epoch_seconds,
    }))
}

fn identity_from_claims(
    claims: DelegationClaims,
    configuration: &DelegationConfiguration,
    now: u64,
) -> Result<Identity, AuthError> {
    if claims.iss != configuration.issuer
        || !claims.aud.contains(&configuration.audience)
        || claims.sub.is_empty()
        || claims.sub.len() > 256
        || claims.jti.is_empty()
        || claims.jti.len() > 256
        || claims.token_use != "ngkg-delegation"
        || claims.ngkg.tenant_id.is_nil()
        || claims.ngkg.scopes.is_empty()
        || !claims.ngkg.scopes.contains("queries:execute")
        || claims.ngkg.graph_authorization_labels.is_empty()
        || claims
            .ngkg
            .actor
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 256)
        || claims
            .client_id
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 256)
        || claims.iat > now.saturating_add(configuration.clock_skew.as_secs())
        || claims.nbf > claims.exp
        || claims.iat > claims.exp
        || claims.exp.saturating_sub(claims.iat) > configuration.maximum_token_lifetime.as_secs()
    {
        return Err(AuthError::Unauthorized);
    }
    let policy_version_sha256 =
        decode_sha256(&claims.ngkg.policy_version_sha256).map_err(|_| AuthError::Unauthorized)?;
    Ok(Identity {
        tenant_id: claims.ngkg.tenant_id,
        subject: claims.sub,
        actor: claims.ngkg.actor,
        scopes: claims.ngkg.scopes,
        graph_authorization_labels: claims.ngkg.graph_authorization_labels,
        policy_version_sha256,
        issuer: Some(claims.iss),
        audiences: claims.aud,
        issued_at: Some(claims.iat),
        not_before: Some(claims.nbf),
        expires_at: Some(claims.exp),
        jti: Some(claims.jti),
        agent_execution_id: claims.ngkg.agent_execution_id,
        client_id: claims.client_id,
    })
}

fn validate_configuration(configuration: &DelegationConfiguration) -> Result<(), AuthError> {
    if configuration.issuer.is_empty()
        || configuration.audience.is_empty()
        || configuration.required_typ.is_empty()
        || configuration.maximum_token_lifetime.is_zero()
        || configuration.jwks_cache_ttl.is_zero()
        || configuration.allowed_algorithms.is_empty()
    {
        return Err(AuthError::Configuration("delegation bounds are invalid"));
    }
    let url = &configuration.jwks_url;
    if url.scheme() != "https"
        || url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.host_str().is_none()
    {
        return Err(AuthError::Configuration(
            "JWKS URL must be an exact HTTPS URL",
        ));
    }
    if let Some(host) = url.host_str() {
        let loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback());
        if loopback && !configuration.allow_loopback {
            return Err(AuthError::Configuration("JWKS loopback is disabled"));
        }
    }
    for algorithm in &configuration.allowed_algorithms {
        if !matches!(
            algorithm.as_str(),
            "RS256" | "RS384" | "RS512" | "PS256" | "PS384" | "PS512" | "ES256" | "ES384" | "EdDSA"
        ) {
            return Err(AuthError::Configuration(
                "delegation algorithm is not permitted",
            ));
        }
    }
    Ok(())
}

fn validate_jwks(jwks: &JwkSet, allowed: &BTreeSet<String>) -> Result<(), AuthError> {
    if jwks.keys.is_empty() || jwks.keys.len() > MAXIMUM_JWKS_KEYS {
        return Err(AuthError::Unavailable);
    }
    let mut kids = HashSet::new();
    for key in &jwks.keys {
        let kid = key.common.key_id.as_deref().ok_or(AuthError::Unavailable)?;
        if kid.is_empty() || kid.len() > 256 || !kids.insert(kid) {
            return Err(AuthError::Unavailable);
        }
        if let Some(algorithm) = key.common.key_algorithm.as_ref()
            && !allowed.contains(&format!("{algorithm:?}"))
        {
            return Err(AuthError::Unavailable);
        }
    }
    Ok(())
}

fn find_unique_key<'a>(jwks: &'a JwkSet, kid: &str) -> Result<&'a Jwk, AuthError> {
    let mut matches = jwks
        .keys
        .iter()
        .filter(|key| key.common.key_id.as_deref() == Some(kid));
    let key = matches.next().ok_or(AuthError::Unauthorized)?;
    if matches.next().is_some() {
        return Err(AuthError::Unauthorized);
    }
    Ok(key)
}

fn ensure_jwk_compatible(jwk: &Jwk, algorithm: Algorithm) -> Result<(), AuthError> {
    if let Some(use_) = jwk.common.public_key_use.as_ref()
        && format!("{use_:?}") != "Signature"
    {
        return Err(AuthError::Unauthorized);
    }
    if let Some(key_algorithm) = jwk.common.key_algorithm.as_ref()
        && format!("{key_algorithm:?}") != format!("{algorithm:?}")
    {
        return Err(AuthError::Unauthorized);
    }
    Ok(())
}

fn algorithm_allowed(algorithm: Algorithm, allowed: &BTreeSet<String>) -> bool {
    allowed.contains(&format!("{algorithm:?}"))
}

fn epoch_seconds() -> Result<u64, AuthError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs())
        .map_err(|_| AuthError::Unavailable)
}

fn one_or_many<'de, D>(deserializer: D) -> Result<BTreeSet<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        One(String),
        Many(BTreeSet<String>),
    }
    Ok(match Value::deserialize(deserializer)? {
        Value::One(value) => BTreeSet::from([value]),
        Value::Many(values) => values,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configuration() -> Result<DelegationConfiguration, AuthError> {
        Ok(DelegationConfiguration {
            issuer: "https://identity.example.test/".to_owned(),
            audience: "https://ngkg.example.test/".to_owned(),
            jwks_url: Url::parse("https://identity.example.test/jwks.json")
                .map_err(|_| AuthError::Configuration("test URL is invalid"))?,
            allowed_algorithms: BTreeSet::from(["RS256".to_owned()]),
            required_typ: "at+jwt".to_owned(),
            maximum_token_lifetime: Duration::from_mins(5),
            clock_skew: Duration::from_secs(30),
            jwks_cache_ttl: Duration::from_mins(5),
            jwks_last_known_good_grace: Duration::from_mins(5),
            connect_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(5),
            allow_loopback: false,
        })
    }

    #[test]
    fn symmetric_and_none_algorithms_are_never_configurable() -> Result<(), AuthError> {
        for algorithm in ["none", "HS256", "HS384", "HS512"] {
            let mut value = configuration()?;
            value.allowed_algorithms = BTreeSet::from([algorithm.to_owned()]);
            assert!(validate_configuration(&value).is_err());
        }
        Ok(())
    }

    #[test]
    fn jwt_lifetime_and_ngkg_namespace_are_fail_closed() -> Result<(), AuthError> {
        let configuration = configuration()?;
        let claims = DelegationClaims {
            iss: configuration.issuer.clone(),
            aud: BTreeSet::from([configuration.audience.clone()]),
            sub: "user".to_owned(),
            exp: 1_000,
            nbf: 1,
            iat: 1,
            jti: "jti".to_owned(),
            client_id: Some("client".to_owned()),
            token_use: "ngkg-delegation".to_owned(),
            ngkg: NgkgClaims {
                tenant_id: Uuid::from_u128(1),
                actor: Some("gateway".to_owned()),
                scopes: BTreeSet::from(["queries:execute".to_owned()]),
                graph_authorization_labels: BTreeSet::from(["tenant".to_owned()]),
                policy_version_sha256: "a".repeat(64),
                agent_execution_id: None,
            },
        };
        assert!(identity_from_claims(claims, &configuration, 100).is_err());
        Ok(())
    }
}
