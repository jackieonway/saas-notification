use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

/// Routing identity is always supplied by the platform, never by a socket client.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct Identity {
    pub(crate) user_id: Uuid,
    pub(crate) tenant_id: Uuid,
    pub(crate) organization_id: Uuid,
}

#[derive(Clone, Debug)]
pub(crate) struct Context {
    pub(crate) identity: Identity,
    pub(crate) session_id: Uuid,
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
        if object.get("contractVersion").and_then(Value::as_str) != Some("1.0") {
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
            },
            session_id: id("sessionId")?,
            expires_at,
        };
        if context.expires_at <= now {
            return Err(ContextError::Expired);
        }
        Ok(context)
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
        if object.len() != 4 {
            return None;
        }
        let id = |field| object.get(field)?.as_str().and_then(canonical_uuid);
        Some(Self {
            id: id("id")?,
            identity: Identity {
                user_id: id("userId")?,
                tenant_id: id("tenantId")?,
                organization_id: id("organizationId")?,
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

impl Notification {
    pub(crate) fn identity(&self) -> Identity {
        Identity {
            user_id: self.user_id,
            tenant_id: self.tenant_id,
            organization_id: self.organization_id,
        }
    }
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
    if value.len() != 36
        || !value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
    {
        return None;
    }
    Uuid::parse_str(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "11111111-2222-4333-8444-555555555555";

    #[test]
    fn hints_admit_exactly_four_canonical_uuid_fields() {
        let valid = json!({"id": ID,"userId": ID,"tenantId": ID,"organizationId": ID});
        assert!(Hint::parse(&valid.to_string()).is_some());
        for value in [
            Value::Null,
            json!([ID, ID, ID, ID]),
            json!(1),
            json!({"id":ID}),
            json!({"id":ID,"userId":ID,"tenantId":ID,"organizationId":ID,"payload":"not allowed"}),
            json!({"id":"11111111222243338444555555555555","userId":ID,"tenantId":ID,"organizationId":ID}),
        ] {
            assert!(Hint::parse(&value.to_string()).is_none());
        }
        assert!(Hint::parse(&" ".repeat(4097)).is_none());
        assert!(canonical_uuid("{11111111-2222-4333-8444-555555555555}").is_none());
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
        let mut value = json!({"contractVersion":"1.0","userId":ID,"tenantId":ID,
            "organizationId":ID,"sessionId":ID,"expiresAt":"2031-01-01T00:00:00Z"});
        let context = Context::parse(&value, now).unwrap();
        assert_eq!(context.public().as_object().unwrap().len(), 5);
        assert!(context.public().get("sessionId").is_none());
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
}
