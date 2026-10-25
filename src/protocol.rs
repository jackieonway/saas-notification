use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum MemberType {
    Employee,
    Customer,
}

impl MemberType {
    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "EMPLOYEE" => Some(Self::Employee),
            "CUSTOMER" => Some(Self::Customer),
            _ => None,
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Employee => "EMPLOYEE",
            Self::Customer => "CUSTOMER",
        }
    }
}

/// Routing identity is always supplied by the platform, never by a socket client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Identity {
    pub(crate) user_id: Uuid,
    pub(crate) tenant_id: Uuid,
    pub(crate) organization_id: Uuid,
    pub(crate) membership_id: Uuid,
    pub(crate) member_type: MemberType,
    pub(crate) identity_version: i64,
}

#[derive(Clone, Debug)]
pub(crate) struct Context {
    pub(crate) identity: Identity,
    pub(crate) session_id: Uuid,
    pub(crate) account_version: i64,
    pub(crate) expires_at: DateTime<Utc>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ContextError {
    Invalid,
    Expired,
}

impl Context {
    pub(crate) fn parse(value: &Value, now: DateTime<Utc>) -> Result<Self, ContextError> {
        let object = value.as_object().ok_or(ContextError::Invalid)?;
        if object.len() != 11
            || object.get("proofVersion").and_then(Value::as_str) != Some("1")
            || object.get("contractVersion").and_then(Value::as_str) != Some("1.0")
        {
            return Err(ContextError::Invalid);
        }
        let id = |field| {
            object
                .get(field)
                .and_then(Value::as_str)
                .and_then(canonical_uuid)
                .ok_or(ContextError::Invalid)
        };
        let expires_at = object
            .get("expiresAt")
            .and_then(Value::as_str)
            .and_then(|text| DateTime::parse_from_rfc3339(text).ok())
            .map(|value| value.with_timezone(&Utc))
            .ok_or(ContextError::Invalid)?;
        let context = Self {
            identity: Identity {
                user_id: id("userId")?,
                tenant_id: id("tenantId")?,
                organization_id: id("organizationId")?,
                membership_id: id("membershipId")?,
                member_type: object
                    .get("memberType")
                    .and_then(Value::as_str)
                    .and_then(MemberType::parse)
                    .ok_or(ContextError::Invalid)?,
                identity_version: object
                    .get("identityVersion")
                    .and_then(Value::as_i64)
                    .filter(|version| *version >= 1)
                    .ok_or(ContextError::Invalid)?,
            },
            session_id: id("sessionId")?,
            account_version: object
                .get("accountVersion")
                .and_then(Value::as_i64)
                .filter(|version| *version >= 0)
                .ok_or(ContextError::Invalid)?,
            expires_at,
        };
        if context.expires_at <= now {
            return Err(ContextError::Expired);
        }
        Ok(context)
    }

    pub(crate) fn same_binding(&self, previous: &Self) -> bool {
        self.identity == previous.identity
            && self.session_id == previous.session_id
            && self.account_version == previous.account_version
    }

    pub(crate) fn public(&self) -> Value {
        json!({
            "contractVersion": "1.0", "userId": self.identity.user_id,
            "tenantId": self.identity.tenant_id, "organizationId": self.identity.organization_id,
            "expiresAt": self.expires_at.to_rfc3339_opts(SecondsFormat::AutoSi, true),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Hint {
    pub(crate) id: Uuid,
    pub(crate) identity: Identity,
}

impl Hint {
    pub(crate) fn parse(raw: &str) -> Option<Self> {
        if raw.len() > 4096 {
            return None;
        }
        let value: Value = serde_json::from_str(raw).ok()?;
        let object = value.as_object()?;
        if object.len() != 8 || object.get("hintVersion").and_then(Value::as_str) != Some("1") {
            return None;
        }
        let id = |field| object.get(field)?.as_str().and_then(canonical_uuid);
        Some(Self {
            id: id("id")?,
            identity: Identity {
                user_id: id("userId")?,
                tenant_id: id("tenantId")?,
                organization_id: id("organizationId")?,
                membership_id: id("membershipId")?,
                member_type: object
                    .get("memberType")?
                    .as_str()
                    .and_then(MemberType::parse)?,
                identity_version: object
                    .get("identityVersion")?
                    .as_i64()
                    .filter(|version| *version >= 1)?,
            },
        })
    }
}

/// Only this allowlisted DTO crosses the realtime boundary; persistence metadata
/// such as source_kind, is_read, and session identifiers cannot be serialized.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Notification {
    pub(crate) contract_version: &'static str,
    pub(crate) id: Uuid,
    pub(crate) user_id: Uuid,
    pub(crate) tenant_id: Uuid,
    pub(crate) organization_id: Uuid,
    pub(crate) application_id: Option<Uuid>,
    pub(crate) publication_id: Option<Uuid>,
    #[serde(rename = "type")]
    pub(crate) kind: String,
    pub(crate) title: String,
    pub(crate) content: String,
    pub(crate) link: String,
    pub(crate) created_at: String,
}

/// Internal ownership proof is kept outside the public Notification 1 DTO.
#[derive(Clone, Debug)]
pub(crate) struct StoredDelivery {
    pub(crate) recipient: Identity,
    pub(crate) notification: Notification,
}

pub(crate) fn authentication_token(value: &Value) -> Option<String> {
    let object = value.as_object()?;
    if object.len() != 1 {
        return None;
    }
    let token = object.get("token")?.as_str()?.trim();
    if token.is_empty()
        || token.len() > 4096
        || token.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return None;
    }
    Some(token.to_owned())
}

pub(crate) fn canonical_uuid(value: &str) -> Option<Uuid> {
    crate::identifiers::parse(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "01962345-6789-7abc-8def-0123456789ab";

    #[test]
    fn hints_require_the_exact_versioned_recipient_proof() {
        let valid = json!({"hintVersion":"1","id": ID,"userId": ID,"tenantId": ID,
            "organizationId": ID,"membershipId":ID,"memberType":"EMPLOYEE","identityVersion":1});
        assert!(Hint::parse(&valid.to_string()).is_some());
        for (field, value) in [
            ("hintVersion", json!("2")),
            (
                "membershipId",
                json!("11111111-2222-4333-8444-555555555555"),
            ),
            ("memberType", json!("UNKNOWN")),
            ("identityVersion", json!(0)),
            ("identityVersion", json!("1")),
            ("identityVersion", json!(1.0)),
            ("identityVersion", Value::Null),
            ("identityVersion", json!(-1)),
            ("identityVersion", json!(u64::MAX)),
        ] {
            let mut wrong = valid.clone();
            wrong[field] = value;
            assert!(Hint::parse(&wrong.to_string()).is_none());
        }
        for field in ["id", "userId", "tenantId", "organizationId", "membershipId"] {
            for invalid in [
                "0196234567897abc8def0123456789ab",     // Noncanonical form.
                "11111111-2222-4333-8444-555555555555", // Legacy v4.
                "00000000-0000-0000-0000-000000000000", // Nil.
                "01962345-6789-7abc-cdef-0123456789ab", // Invalid variant.
            ] {
                let mut wrong = valid.clone();
                wrong[field] = json!(invalid);
                assert!(
                    Hint::parse(&wrong.to_string()).is_none(),
                    "{field}: {invalid}"
                );
            }
        }
        let mut extra = valid.clone();
        extra["payload"] = json!("not allowed");
        assert!(Hint::parse(&extra.to_string()).is_none());
        let mut maximum = valid.clone();
        maximum["identityVersion"] = json!(i64::MAX);
        assert_eq!(
            Hint::parse(&maximum.to_string())
                .unwrap()
                .identity
                .identity_version,
            i64::MAX
        );
        for field in [
            "hintVersion",
            "membershipId",
            "memberType",
            "identityVersion",
        ] {
            let mut missing = valid.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(Hint::parse(&missing.to_string()).is_none());
        }
        for value in [
            Value::Null,
            json!([ID, ID, ID, ID]),
            json!(1),
            json!({"id":ID}),
            json!({"id":ID,"userId":ID,"tenantId":ID,"organizationId":ID}),
            json!({"id":ID,"userId":ID,"tenantId":ID,"organizationId":ID,"payload":"not allowed"}),
            json!({"id":"0196234567897abc8def0123456789ab","userId":ID,"tenantId":ID,"organizationId":ID}),
            json!({"id":"11111111-2222-4333-8444-555555555555","userId":ID,"tenantId":ID,"organizationId":ID}),
            json!({"id":ID,"userId":"00000000-0000-0000-0000-000000000000","tenantId":ID,"organizationId":ID}),
            json!({"id":ID,"userId":ID,"tenantId":ID,"organizationId":"01962345-6789-7abc-cdef-0123456789ab"}),
        ] {
            assert!(Hint::parse(&value.to_string()).is_none());
        }
        assert!(Hint::parse(&" ".repeat(4097)).is_none());
        assert!(canonical_uuid("{01962345-6789-7abc-8def-0123456789ab}").is_none());
    }

    #[test]
    fn authentication_is_one_bounded_token_field_without_identity_claims() {
        assert_eq!(
            authentication_token(&json!({"token":"  opaque.value  "})).as_deref(),
            Some("opaque.value")
        );
        for value in [
            Value::Null,
            json!(["token"]),
            json!("token"),
            json!({"token":""}),
            json!({"token":"a\nb"}),
            json!({"token":"a\u{7f}b"}),
            json!({"token":"a b"}),
            json!({"token":"valid","userId":ID}),
            json!({"token":"x".repeat(4097)}),
        ] {
            assert!(authentication_token(&value).is_none());
        }
    }

    #[test]
    fn platform_context_checks_contract_identity_expiry_and_hides_session() {
        let now = DateTime::from_timestamp(1_900_000_000, 0).unwrap();
        let mut value = proof();
        let context = Context::parse(&value, now).unwrap();
        assert_eq!(context.public().as_object().unwrap().len(), 5);
        assert!(context.public().get("sessionId").is_none());
        for field in [
            "userId",
            "tenantId",
            "organizationId",
            "sessionId",
            "membershipId",
        ] {
            let mut legacy = value.clone();
            legacy[field] = json!("11111111-2222-4333-8444-555555555555");
            assert_eq!(
                Context::parse(&legacy, now).err(),
                Some(ContextError::Invalid)
            );
        }
        value["expiresAt"] = json!(now.to_rfc3339());
        assert_eq!(
            Context::parse(&value, now).err(),
            Some(ContextError::Expired)
        );
        value["expiresAt"] = json!("invalid");
        assert_eq!(
            Context::parse(&value, now).err(),
            Some(ContextError::Invalid)
        );
        assert!(
            Context::parse(&json!(["1.0", ID, ID, ID, ID, "2031-01-01T00:00:00Z"]), now).is_err()
        );
    }

    fn proof() -> Value {
        json!({"proofVersion":"1","contractVersion":"1.0","userId":ID,"tenantId":ID,
            "organizationId":ID,"sessionId":ID,"expiresAt":"2031-01-01T00:00:00Z",
            "membershipId":ID,"memberType":"EMPLOYEE","identityVersion":1,"accountVersion":0})
    }

    #[test]
    fn authority_proof_is_strict_and_public_ack_keeps_its_original_fields() {
        let now = DateTime::from_timestamp(1_900_000_000, 0).unwrap();
        for (field, value) in [
            ("proofVersion", json!("2")),
            ("memberType", json!("MACHINE")),
            ("identityVersion", json!(0)),
            ("accountVersion", json!(-1)),
            ("identityVersion", json!("1")),
            ("accountVersion", json!("0")),
            ("identityVersion", json!(1.5)),
            ("accountVersion", json!(0.0)),
            ("identityVersion", Value::Null),
            ("accountVersion", Value::Null),
            ("identityVersion", json!(u64::MAX)),
            ("accountVersion", json!(u64::MAX)),
        ] {
            let mut wrong = proof();
            wrong[field] = value;
            assert_eq!(
                Context::parse(&wrong, now).err(),
                Some(ContextError::Invalid)
            );
        }
        for field in [
            "proofVersion",
            "membershipId",
            "memberType",
            "identityVersion",
            "accountVersion",
        ] {
            let mut missing = proof();
            missing.as_object_mut().unwrap().remove(field);
            assert!(Context::parse(&missing, now).is_err());
        }
        let mut extra = proof();
        extra["unrecognized"] = json!(true);
        assert!(Context::parse(&extra, now).is_err());
        let context = Context::parse(&proof(), now).unwrap();
        let mut maximum = proof();
        maximum["identityVersion"] = json!(i64::MAX);
        maximum["accountVersion"] = json!(i64::MAX);
        let maximum = Context::parse(&maximum, now).unwrap();
        assert_eq!(maximum.identity.identity_version, i64::MAX);
        assert_eq!(maximum.account_version, i64::MAX);
        let keys = context
            .public()
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(
            keys,
            [
                "contractVersion",
                "userId",
                "tenantId",
                "organizationId",
                "expiresAt"
            ]
            .map(str::to_owned)
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn an_existing_connection_cannot_replace_any_member_or_account_generation() {
        let now = DateTime::from_timestamp(1_900_000_000, 0).unwrap();
        let original = Context::parse(&proof(), now).unwrap();
        for (field, value) in [
            ("userId", json!(Uuid::now_v7())),
            ("tenantId", json!(Uuid::now_v7())),
            ("organizationId", json!(Uuid::now_v7())),
            ("sessionId", json!(Uuid::now_v7())),
            ("membershipId", json!(Uuid::now_v7())),
            ("memberType", json!("CUSTOMER")),
            ("identityVersion", json!(2)),
            ("accountVersion", json!(1)),
        ] {
            let mut changed = proof();
            changed[field] = value;
            assert!(!Context::parse(&changed, now)
                .unwrap()
                .same_binding(&original));
        }
        let mut expiry_only = proof();
        expiry_only["expiresAt"] = json!("2032-01-01T00:00:00Z");
        assert!(Context::parse(&expiry_only, now)
            .unwrap()
            .same_binding(&original));
    }
}
