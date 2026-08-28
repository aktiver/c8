use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::Arc,
};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use uuid::Uuid;

use super::AdmissionClass;

const MAX_POLICY_BYTES: u64 = 1_048_576;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TenantAdmissionPolicyFile {
    format_version: u32,
    tenants: Vec<TenantPolicyEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TenantPolicyEntry {
    tenant_id: Uuid,
    query: ClassPolicy,
    fragment: ClassPolicy,
    shuffle: ClassPolicy,
    locator: ClassPolicy,
    hydration: ClassPolicy,
    fragment_worker_max_in_flight: usize,
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ClassPolicy {
    max_in_flight: usize,
    max_pending: usize,
}

pub(crate) struct TenantAdmissionRegistry {
    policies: BTreeMap<Uuid, Arc<TenantAdmissionLanes>>,
    policy_sha256: String,
}

pub(crate) struct TenantAdmissionLanes {
    execution: [Arc<Semaphore>; 5],
    pending: [Arc<Semaphore>; 5],
    fragment_worker: Arc<Semaphore>,
}

impl TenantAdmissionRegistry {
    pub(crate) fn load(
        path: &Path,
        expected_sha256: &str,
        max_tenants: usize,
        authorized_tenants: &BTreeSet<Uuid>,
        global_execution_limits: [usize; 5],
        global_pending_limits: [usize; 5],
        global_fragment_worker_limit: usize,
    ) -> Result<Self, String> {
        let metadata = fs::metadata(path)
            .map_err(|error| format!("cannot inspect tenant admission policy: {error}"))?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_POLICY_BYTES {
            return Err(
                "tenant admission policy must be a non-empty regular file no larger than 1 MiB"
                    .to_owned(),
            );
        }
        let bytes = fs::read(path)
            .map_err(|error| format!("cannot read tenant admission policy: {error}"))?;
        Self::from_bytes(
            &bytes,
            expected_sha256,
            max_tenants,
            authorized_tenants,
            global_execution_limits,
            global_pending_limits,
            global_fragment_worker_limit,
        )
    }

    pub(crate) fn from_bytes(
        bytes: &[u8],
        expected_sha256: &str,
        max_tenants: usize,
        authorized_tenants: &BTreeSet<Uuid>,
        global_execution_limits: [usize; 5],
        global_pending_limits: [usize; 5],
        global_fragment_worker_limit: usize,
    ) -> Result<Self, String> {
        if bytes.is_empty()
            || u64::try_from(bytes.len())
                .ok()
                .is_none_or(|length| length > MAX_POLICY_BYTES)
        {
            return Err("tenant admission policy must contain 1 byte through 1 MiB".to_owned());
        }
        validate_sha256(expected_sha256)?;
        let observed_sha256 = hex::encode(Sha256::digest(bytes));
        if observed_sha256 != expected_sha256 {
            return Err(
                "tenant admission policy checksum does not match its deployment".to_owned(),
            );
        }
        if max_tenants == 0 || authorized_tenants.is_empty() {
            return Err(
                "tenant admission bounds and authorized tenant set must be non-empty".to_owned(),
            );
        }
        let file: TenantAdmissionPolicyFile = serde_json::from_slice(bytes)
            .map_err(|error| format!("tenant admission policy is invalid: {error}"))?;
        if file.format_version != 1 || file.tenants.is_empty() {
            return Err("tenant admission policy must be non-empty formatVersion 1".to_owned());
        }
        if file.tenants.len() > max_tenants {
            return Err("tenant admission policy exceeds the configured tenant ceiling".to_owned());
        }
        let multi_tenant = authorized_tenants.len() > 1;

        let mut policies = BTreeMap::new();
        for entry in file.tenants {
            let classes = [
                entry.query,
                entry.fragment,
                entry.shuffle,
                entry.locator,
                entry.hydration,
            ];
            for (index, class) in classes.iter().enumerate() {
                if class.max_in_flight == 0 || class.max_pending == 0 {
                    return Err("tenant execution and pending limits must be positive".to_owned());
                }
                if class.max_in_flight > global_execution_limits[index]
                    || class.max_pending > global_pending_limits[index]
                    || class.max_in_flight > Semaphore::MAX_PERMITS
                    || class.max_pending > Semaphore::MAX_PERMITS
                {
                    return Err(
                        "tenant class limits must not exceed their global or semaphore ceilings"
                            .to_owned(),
                    );
                }
                if multi_tenant
                    && (class.max_in_flight >= global_execution_limits[index]
                        || class.max_pending >= global_pending_limits[index])
                {
                    return Err(
                        "a multi-tenant class must leave at least one global execution and pending lane for a peer tenant"
                            .to_owned(),
                    );
                }
            }
            if entry.fragment_worker_max_in_flight == 0
                || entry.fragment_worker_max_in_flight > global_fragment_worker_limit
                || entry.fragment_worker_max_in_flight > Semaphore::MAX_PERMITS
                || (multi_tenant
                    && entry.fragment_worker_max_in_flight >= global_fragment_worker_limit)
                || classes[AdmissionClass::Fragment.index()].max_in_flight
                    > entry.fragment_worker_max_in_flight
                || classes[AdmissionClass::Shuffle.index()].max_in_flight
                    > entry.fragment_worker_max_in_flight
            {
                return Err(
                    "tenant fragment-worker limit is invalid for its fragment and shuffle lanes"
                        .to_owned(),
                );
            }
            let lanes = TenantAdmissionLanes {
                execution: std::array::from_fn(|index| {
                    Arc::new(Semaphore::new(classes[index].max_in_flight))
                }),
                pending: std::array::from_fn(|index| {
                    Arc::new(Semaphore::new(classes[index].max_pending))
                }),
                fragment_worker: Arc::new(Semaphore::new(entry.fragment_worker_max_in_flight)),
            };
            if policies.insert(entry.tenant_id, Arc::new(lanes)).is_some() {
                return Err("tenant admission policy contains a duplicate tenantId".to_owned());
            }
        }
        let policy_tenants = policies.keys().copied().collect::<BTreeSet<_>>();
        if policy_tenants != *authorized_tenants {
            return Err(
                "tenant admission policy must cover exactly the tenants with query access"
                    .to_owned(),
            );
        }
        Ok(Self {
            policies,
            policy_sha256: observed_sha256,
        })
    }

    pub(crate) fn lanes(&self, tenant_id: Uuid) -> Option<Arc<TenantAdmissionLanes>> {
        self.policies.get(&tenant_id).map(Arc::clone)
    }

    pub(crate) fn tenant_count(&self) -> usize {
        self.policies.len()
    }

    pub(crate) fn policy_sha256(&self) -> &str {
        &self.policy_sha256
    }
}

impl TenantAdmissionLanes {
    pub(crate) fn execution(&self, class: AdmissionClass) -> Arc<Semaphore> {
        Arc::clone(&self.execution[class.index()])
    }

    pub(crate) fn pending(&self, class: AdmissionClass) -> Arc<Semaphore> {
        Arc::clone(&self.pending[class.index()])
    }

    pub(crate) fn fragment_worker(&self) -> Arc<Semaphore> {
        Arc::clone(&self.fragment_worker)
    }
}

fn validate_sha256(value: &str) -> Result<(), String> {
    if value.len() != 64
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
    {
        return Err(
            "tenant admission policy SHA-256 must be 64 lowercase hex characters".to_owned(),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use sha2::{Digest, Sha256};
    use uuid::Uuid;

    use super::TenantAdmissionRegistry;

    fn policy(tenant: Uuid, query_limit: usize) -> Vec<u8> {
        format!(
            r#"{{"formatVersion":1,"tenants":[{{"tenantId":"{tenant}","query":{{"maxInFlight":{query_limit},"maxPending":1}},"fragment":{{"maxInFlight":1,"maxPending":1}},"shuffle":{{"maxInFlight":1,"maxPending":1}},"locator":{{"maxInFlight":1,"maxPending":1}},"hydration":{{"maxInFlight":1,"maxPending":1}},"fragmentWorkerMaxInFlight":1}}]}}"#
        )
        .into_bytes()
    }

    #[test]
    fn policy_is_checksum_bound_and_covers_authorized_tenants_exactly() {
        let tenant = Uuid::from_u128(1);
        let bytes = policy(tenant, 1);
        let checksum = hex::encode(Sha256::digest(&bytes));
        let tenants = BTreeSet::from([tenant]);
        assert!(
            TenantAdmissionRegistry::from_bytes(&bytes, &checksum, 1, &tenants, [2; 5], [2; 5], 2,)
                .is_ok()
        );
        assert!(
            TenantAdmissionRegistry::from_bytes(
                &bytes,
                &"0".repeat(64),
                1,
                &tenants,
                [2; 5],
                [2; 5],
                2,
            )
            .is_err()
        );
        assert!(
            TenantAdmissionRegistry::from_bytes(
                &bytes,
                &checksum,
                1,
                &BTreeSet::from([tenant, Uuid::from_u128(2)]),
                [2; 5],
                [2; 5],
                2,
            )
            .is_err()
        );
    }

    #[test]
    fn tenant_limits_cannot_exceed_global_envelopes() {
        let tenant = Uuid::from_u128(1);
        let bytes = policy(tenant, 2);
        let checksum = hex::encode(Sha256::digest(&bytes));
        assert!(
            TenantAdmissionRegistry::from_bytes(
                &bytes,
                &checksum,
                1,
                &BTreeSet::from([tenant]),
                [1; 5],
                [1; 5],
                1,
            )
            .is_err()
        );
    }

    #[test]
    fn multi_tenant_policy_must_reserve_a_peer_lane() {
        let tenant_a = Uuid::from_u128(1);
        let tenant_b = Uuid::from_u128(2);
        let entry = |tenant: Uuid| {
            format!(
                r#"{{"tenantId":"{tenant}","query":{{"maxInFlight":2,"maxPending":2}},"fragment":{{"maxInFlight":1,"maxPending":1}},"shuffle":{{"maxInFlight":1,"maxPending":1}},"locator":{{"maxInFlight":1,"maxPending":1}},"hydration":{{"maxInFlight":1,"maxPending":1}},"fragmentWorkerMaxInFlight":1}}"#
            )
        };
        let bytes = format!(
            r#"{{"formatVersion":1,"tenants":[{},{}]}}"#,
            entry(tenant_a),
            entry(tenant_b)
        )
        .into_bytes();
        let checksum = hex::encode(Sha256::digest(&bytes));
        assert!(
            TenantAdmissionRegistry::from_bytes(
                &bytes,
                &checksum,
                2,
                &BTreeSet::from([tenant_a, tenant_b]),
                [2, 2, 2, 2, 2],
                [2, 2, 2, 2, 2],
                2,
            )
            .is_err()
        );
    }
}
