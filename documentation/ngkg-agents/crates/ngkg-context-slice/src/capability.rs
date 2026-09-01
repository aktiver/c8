#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{fs, path::Path, time::Duration};

use jsonwebtoken::{
    Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{SliceError, valid_hash};

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CapabilityRequest {
    pub audience: String,
    pub range_start: u64,
    pub range_end_exclusive: u64,
    pub ttl_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CapabilityClaims {
    pub iss: String,
    pub sub: String,
    pub aud: String,
    pub exp: u64,
    pub nbf: u64,
    pub iat: u64,
    pub jti: String,
    pub tenant_id: Uuid,
    pub slice_id: Uuid,
    pub manifest_sha256: String,
    pub range_start: u64,
    pub range_end_exclusive: u64,
    pub policy_version_sha256: String,
}

#[derive(Clone)]
pub struct CapabilityIssuer {
    issuer: String,
    key: Vec<u8>,
    maximum_ttl: Duration,
    clock_skew: Duration,
}

impl CapabilityIssuer {
    pub fn load(
        issuer: String,
        key_file: &Path,
        expected_sha256: &str,
        maximum_ttl: Duration,
        clock_skew: Duration,
    ) -> Result<Self, SliceError> {
        if issuer.is_empty() || !valid_hash(expected_sha256) || maximum_ttl.is_zero() {
            return Err(SliceError::Configuration("capability signer"));
        }
        let metadata = fs::symlink_metadata(key_file)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || !(32..=4096).contains(&metadata.len())
        {
            return Err(SliceError::Configuration("capability key file safety"));
        }
        #[cfg(unix)]
        if metadata.permissions().mode() & 0o027 != 0 {
            return Err(SliceError::Configuration("capability key file permissions"));
        }
        let key = fs::read(key_file)?;
        if crate::sha256(&key) != expected_sha256 {
            return Err(SliceError::Checksum);
        }
        Ok(Self {
            issuer,
            key,
            maximum_ttl,
            clock_skew,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn issue(
        &self,
        tenant_id: Uuid,
        slice_id: Uuid,
        subject: &str,
        manifest_sha256: &str,
        policy_version_sha256: &str,
        request: &CapabilityRequest,
        total_bytes: u64,
        now: u64,
    ) -> Result<(String, CapabilityClaims), SliceError> {
        if subject.is_empty()
            || subject.len() > 256
            || request.audience.is_empty()
            || request.audience.len() > 256
            || request.range_start >= request.range_end_exclusive
            || request.range_end_exclusive > total_bytes
            || request.ttl_seconds == 0
            || request.ttl_seconds > self.maximum_ttl.as_secs()
            || !valid_hash(manifest_sha256)
            || !valid_hash(policy_version_sha256)
        {
            return Err(SliceError::Invalid("capability bounds"));
        }
        let claims = CapabilityClaims {
            iss: self.issuer.clone(),
            sub: subject.to_owned(),
            aud: request.audience.clone(),
            exp: now.saturating_add(request.ttl_seconds),
            nbf: now,
            iat: now,
            jti: Uuid::new_v4().to_string(),
            tenant_id,
            slice_id,
            manifest_sha256: manifest_sha256.to_owned(),
            range_start: request.range_start,
            range_end_exclusive: request.range_end_exclusive,
            policy_version_sha256: policy_version_sha256.to_owned(),
        };
        let mut header = Header::new(Algorithm::HS256);
        header.typ = Some("ngkg-context-capability+jwt".to_owned());
        let token = encode(&header, &claims, &EncodingKey::from_secret(&self.key))?;
        Ok((token, claims))
    }

    pub fn verify(
        &self,
        token: &str,
        audience: &str,
        now: u64,
    ) -> Result<CapabilityClaims, SliceError> {
        if token.len() > 16_384 || audience.is_empty() {
            return Err(SliceError::Unauthorized);
        }
        let header = decode_header(token).map_err(|_| SliceError::Unauthorized)?;
        if header.alg != Algorithm::HS256
            || header.typ.as_deref() != Some("ngkg-context-capability+jwt")
        {
            return Err(SliceError::Unauthorized);
        }
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&[self.issuer.as_str()]);
        validation.set_audience(&[audience]);
        validation.leeway = self.clock_skew.as_secs();
        validation.set_required_spec_claims(&["exp", "nbf", "iat", "iss", "sub", "aud", "jti"]);
        validation.validate_nbf = true;
        let data =
            decode::<CapabilityClaims>(token, &DecodingKey::from_secret(&self.key), &validation)
                .map_err(|_| SliceError::Unauthorized)?;
        let claims = data.claims;
        if claims.iat > now.saturating_add(self.clock_skew.as_secs())
            || claims.nbf > claims.iat
            || claims.exp <= claims.iat
            || claims.exp.saturating_sub(claims.iat) > self.maximum_ttl.as_secs()
            || claims.range_start >= claims.range_end_exclusive
            || !valid_hash(&claims.manifest_sha256)
            || !valid_hash(&claims.policy_version_sha256)
        {
            return Err(SliceError::Unauthorized);
        }
        Ok(claims)
    }

    pub fn token_sha256(token: &str) -> Vec<u8> {
        Sha256::digest(token.as_bytes()).to_vec()
    }
}
