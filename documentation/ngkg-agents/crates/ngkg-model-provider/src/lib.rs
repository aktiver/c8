//! Bounded model-provider adapters. Provider output is an untrusted proposal;
//! this crate never assigns semantic authority or issues answer certificates.

#![allow(missing_docs)]

use http::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Client, redirect::Policy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use thiserror::Error;
use tokio::sync::Semaphore;
use url::Url;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ProviderProtocol {
    OpenAiCompatible,
    AnthropicMessages,
}

#[derive(Clone, Debug)]
pub struct ProviderConfiguration {
    pub name: String,
    pub protocol: ProviderProtocol,
    pub endpoint: Url,
    pub allowed_models: Vec<String>,
    pub credential_file: Option<PathBuf>,
    pub credential_file_sha256: Option<String>,
    pub maximum_request_bytes: usize,
    pub maximum_response_bytes: usize,
    pub maximum_in_flight: usize,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub allow_http: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ProviderFileConfiguration {
    name: String,
    protocol: ProviderProtocol,
    endpoint: Url,
    allowed_models: Vec<String>,
    credential_file: Option<PathBuf>,
    credential_file_sha256: Option<String>,
    maximum_request_bytes: usize,
    maximum_response_bytes: usize,
    maximum_in_flight: usize,
    connect_timeout_milliseconds: u64,
    request_timeout_milliseconds: u64,
    allow_http: bool,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerationRequest {
    pub model: String,
    pub system: String,
    pub context: String,
    pub maximum_output_tokens: u32,
    pub temperature_milli: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClaimProposal {
    pub canonical_ntriple: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ModelProposal {
    pub claims: Vec<ClaimProposal>,
}

#[derive(Clone, Debug)]
pub struct GenerationOutcome {
    pub proposal: ModelProposal,
    pub request_sha256: [u8; 32],
    pub response_sha256: [u8; 32],
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
}

#[derive(Clone)]
pub struct ProviderRegistry {
    providers: Arc<BTreeMap<String, ProviderClient>>,
    waiting: Arc<AtomicU64>,
}

#[derive(Clone)]
struct ProviderClient {
    configuration: ProviderConfiguration,
    http: Client,
    lanes: Arc<Semaphore>,
}

impl ProviderRegistry {
    /// Build a registry from one immutable, checksum-bound mounted file.
    pub fn from_checksum_bound_file(
        path: PathBuf,
        expected_sha256: &str,
    ) -> Result<Self, ProviderError> {
        // Kubernetes projected Secrets use atomic symlinks. Following the link
        // is safe here because exact file bytes are independently pinned.
        let metadata = fs::metadata(&path)?;
        if !metadata.file_type().is_file() || metadata.len() > 1_048_576 {
            return Err(ProviderError::Configuration(
                "provider file is not a bounded regular file",
            ));
        }
        let bytes = fs::read(path)?;
        if expected_sha256.len() != 64 || hex::encode(Sha256::digest(&bytes)) != expected_sha256 {
            return Err(ProviderError::Credential);
        }
        let records: Vec<ProviderFileConfiguration> = serde_json::from_slice(&bytes)?;
        let configurations = records
            .into_iter()
            .map(|record| ProviderConfiguration {
                name: record.name,
                protocol: record.protocol,
                endpoint: record.endpoint,
                allowed_models: record.allowed_models,
                credential_file: record.credential_file,
                credential_file_sha256: record.credential_file_sha256,
                maximum_request_bytes: record.maximum_request_bytes,
                maximum_response_bytes: record.maximum_response_bytes,
                maximum_in_flight: record.maximum_in_flight,
                connect_timeout: Duration::from_millis(record.connect_timeout_milliseconds),
                request_timeout: Duration::from_millis(record.request_timeout_milliseconds),
                allow_http: record.allow_http,
            })
            .collect();
        Self::build(configurations)
    }

    pub fn build(configurations: Vec<ProviderConfiguration>) -> Result<Self, ProviderError> {
        if configurations.is_empty() {
            return Err(ProviderError::Configuration(
                "at least one provider is required",
            ));
        }
        let mut providers = BTreeMap::new();
        for configuration in configurations {
            validate_configuration(&configuration)?;
            let mut headers = HeaderMap::new();
            if let Some(path) = &configuration.credential_file {
                let metadata = fs::metadata(path)?;
                if !metadata.is_file() || metadata.len() > 16_384 {
                    return Err(ProviderError::Credential);
                }
                let bytes = fs::read(path)?;
                let expected = configuration.credential_file_sha256.as_deref().ok_or(
                    ProviderError::Configuration("credential checksum is required"),
                )?;
                if hex::encode(Sha256::digest(&bytes)) != expected {
                    return Err(ProviderError::Credential);
                }
                let secret = std::str::from_utf8(&bytes)
                    .map_err(|_| ProviderError::Credential)?
                    .trim();
                if secret.is_empty() || secret.len() > 16_384 {
                    return Err(ProviderError::Credential);
                }
                match configuration.protocol {
                    ProviderProtocol::OpenAiCompatible => {
                        headers.insert(
                            http::header::AUTHORIZATION,
                            HeaderValue::from_str(&format!("Bearer {secret}"))
                                .map_err(|_| ProviderError::Credential)?,
                        );
                    }
                    ProviderProtocol::AnthropicMessages => {
                        headers.insert(
                            HeaderName::from_static("x-api-key"),
                            HeaderValue::from_str(secret).map_err(|_| ProviderError::Credential)?,
                        );
                        headers.insert(
                            HeaderName::from_static("anthropic-version"),
                            HeaderValue::from_static("2023-06-01"),
                        );
                    }
                }
            }
            let http = Client::builder()
                .connect_timeout(configuration.connect_timeout)
                .timeout(configuration.request_timeout)
                .redirect(Policy::none())
                .default_headers(headers.clone())
                .https_only(!configuration.allow_http)
                .build()?;
            let lanes = Arc::new(Semaphore::new(configuration.maximum_in_flight));
            if providers
                .insert(
                    configuration.name.clone(),
                    ProviderClient {
                        configuration,
                        http,
                        lanes,
                    },
                )
                .is_some()
            {
                return Err(ProviderError::Configuration("duplicate provider name"));
            }
        }
        Ok(Self {
            providers: Arc::new(providers),
            waiting: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn generate(
        &self,
        provider: &str,
        request: &GenerationRequest,
    ) -> Result<GenerationOutcome, ProviderError> {
        let client = self
            .providers
            .get(provider)
            .ok_or(ProviderError::NotAllowed)?;
        if !client
            .configuration
            .allowed_models
            .iter()
            .any(|model| model == &request.model)
        {
            return Err(ProviderError::NotAllowed);
        }
        let body = build_body(client.configuration.protocol, request)?;
        if body.len() > client.configuration.maximum_request_bytes {
            return Err(ProviderError::Limit);
        }
        let request_sha256 = Sha256::digest(&body).into();
        self.waiting.fetch_add(1, Ordering::Relaxed);
        let admitted = tokio::time::timeout(
            Duration::from_secs(1),
            Arc::clone(&client.lanes).acquire_owned(),
        )
        .await;
        self.waiting.fetch_sub(1, Ordering::Relaxed);
        let _permit = admitted
            .map_err(|_| ProviderError::Admission)?
            .map_err(|_| ProviderError::Admission)?;
        let response = client
            .http
            .post(client.configuration.endpoint.clone())
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(ProviderError::Upstream);
        }
        if response.content_length().is_some_and(|length| {
            length > u64::try_from(client.configuration.maximum_response_bytes).unwrap_or(u64::MAX)
        }) {
            return Err(ProviderError::Limit);
        }
        let bytes = bounded_body(response, client.configuration.maximum_response_bytes).await?;
        let response_sha256 = Sha256::digest(&bytes).into();
        let (content, input_tokens, output_tokens) =
            extract_content(client.configuration.protocol, &bytes)?;
        let proposal: ModelProposal = serde_json::from_str(&content)?;
        if proposal.claims.is_empty() || proposal.claims.len() > 10_000 {
            return Err(ProviderError::Limit);
        }
        Ok(GenerationOutcome {
            proposal,
            request_sha256,
            response_sha256,
            input_tokens,
            output_tokens,
        })
    }

    /// Hash the exact wire request before admission so failed calls are auditable.
    pub fn request_sha256(
        &self,
        provider: &str,
        request: &GenerationRequest,
    ) -> Result<[u8; 32], ProviderError> {
        let client = self
            .providers
            .get(provider)
            .ok_or(ProviderError::NotAllowed)?;
        if !client
            .configuration
            .allowed_models
            .iter()
            .any(|model| model == &request.model)
        {
            return Err(ProviderError::NotAllowed);
        }
        let body = build_body(client.configuration.protocol, request)?;
        if body.len() > client.configuration.maximum_request_bytes {
            return Err(ProviderError::Limit);
        }
        Ok(Sha256::digest(body).into())
    }

    #[must_use]
    pub fn waiting_requests(&self) -> u64 {
        self.waiting.load(Ordering::Relaxed)
    }
}

fn build_body(
    protocol: ProviderProtocol,
    request: &GenerationRequest,
) -> Result<Vec<u8>, ProviderError> {
    let schema_instruction = "Return only closed JSON: {\"claims\":[{\"canonicalNtriple\":\"<absolute-subject-IRI> <absolute-predicate-IRI> <object> .\"}]}. Do not return prose, SPARQL, blank nodes, SERVICE, variables, or claims absent from the supplied certified context.";
    let value = match protocol {
        ProviderProtocol::OpenAiCompatible => {
            serde_json::json!({"model":request.model,"temperature":f64::from(request.temperature_milli)/1000.0,"max_tokens":request.maximum_output_tokens,"messages":[{"role":"system","content":format!("{}\n{}",request.system,schema_instruction)},{"role":"user","content":request.context}],"response_format":{"type":"json_object"}})
        }
        ProviderProtocol::AnthropicMessages => {
            serde_json::json!({"model":request.model,"temperature":f64::from(request.temperature_milli)/1000.0,"max_tokens":request.maximum_output_tokens,"system":format!("{}\n{}",request.system,schema_instruction),"messages":[{"role":"user","content":request.context}]})
        }
    };
    Ok(serde_json::to_vec(&value)?)
}

fn extract_content(
    protocol: ProviderProtocol,
    bytes: &[u8],
) -> Result<(String, Option<i64>, Option<i64>), ProviderError> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    match protocol {
        ProviderProtocol::OpenAiCompatible => Ok((
            value
                .pointer("/choices/0/message/content")
                .and_then(|v| v.as_str())
                .ok_or(ProviderError::Wire)?
                .to_owned(),
            value
                .pointer("/usage/prompt_tokens")
                .and_then(serde_json::Value::as_i64),
            value
                .pointer("/usage/completion_tokens")
                .and_then(serde_json::Value::as_i64),
        )),
        ProviderProtocol::AnthropicMessages => Ok((
            value
                .pointer("/content/0/text")
                .and_then(|v| v.as_str())
                .ok_or(ProviderError::Wire)?
                .to_owned(),
            value
                .pointer("/usage/input_tokens")
                .and_then(serde_json::Value::as_i64),
            value
                .pointer("/usage/output_tokens")
                .and_then(serde_json::Value::as_i64),
        )),
    }
}

async fn bounded_body(
    response: reqwest::Response,
    maximum: usize,
) -> Result<bytes::Bytes, ProviderError> {
    use futures_util::StreamExt;
    let mut stream = response.bytes_stream();
    let mut body = bytes::BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body
            .len()
            .checked_add(chunk.len())
            .is_none_or(|length| length > maximum)
        {
            return Err(ProviderError::Limit);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body.freeze())
}

fn validate_configuration(c: &ProviderConfiguration) -> Result<(), ProviderError> {
    if c.name.is_empty()
        || c.allowed_models.is_empty()
        || c.maximum_request_bytes == 0
        || c.maximum_response_bytes == 0
        || c.maximum_in_flight == 0
    {
        return Err(ProviderError::Configuration("provider limits are invalid"));
    }
    if !matches!(c.endpoint.scheme(), "https" | "http")
        || c.endpoint.scheme() != "https" && !c.allow_http
    {
        return Err(ProviderError::Configuration(
            "provider endpoint requires HTTPS",
        ));
    }
    if c.endpoint.scheme() == "http"
        && c.endpoint.host_str().is_none_or(|host| {
            host.contains('.') && !matches!(host, "localhost" | "127.0.0.1" | "::1")
        })
    {
        return Err(ProviderError::Configuration(
            "HTTP is limited to cluster-local single-label services or loopback",
        ));
    }
    if c.endpoint.username() != ""
        || c.endpoint.password().is_some()
        || c.endpoint.query().is_some()
        || c.endpoint.fragment().is_some()
    {
        return Err(ProviderError::Configuration(
            "provider URL contains forbidden components",
        ));
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum ProviderError {
    #[error("provider configuration is invalid: {0}")]
    Configuration(&'static str),
    #[error("provider or model is not allowed")]
    NotAllowed,
    #[error("provider credential is invalid")]
    Credential,
    #[error("provider limit exceeded")]
    Limit,
    #[error("provider admission timed out")]
    Admission,
    #[error("provider rejected the request")]
    Upstream,
    #[error("provider response is invalid")]
    Wire,
    #[error("provider I/O failed")]
    Io(#[from] std::io::Error),
    #[error("provider HTTP failed")]
    Http(#[from] reqwest::Error),
    #[error("provider JSON failed")]
    Json(#[from] serde_json::Error),
}
