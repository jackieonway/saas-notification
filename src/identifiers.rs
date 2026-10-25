//! UUID policy for notification identities and request correlation.
use uuid::{Uuid, Variant};

pub(crate) fn new_id() -> Uuid {
    Uuid::now_v7()
}

pub(crate) fn valid(id: Uuid) -> bool {
    id.get_version_num() == 7 && id.get_variant() == Variant::RFC4122
}

pub(crate) fn parse(value: &str) -> Option<Uuid> {
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
    Uuid::parse_str(value).ok().filter(|id| valid(*id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_ids_reject_legacy_versions_nil_and_non_rfc_variants() {
        assert!(parse("01962345-6789-7abc-8def-0123456789ab").is_some());
        assert!(parse("01962345-6789-7ABC-BDEF-0123456789AB").is_some());
        for invalid in [
            "11111111-2222-4333-8444-555555555555", // Explicit legacy v4 negative.
            "00000000-0000-0000-0000-000000000000", // Nil is never an identity.
            "01962345-6789-7abc-cdef-0123456789ab",
            "0196234567897abc8def0123456789ab",
            "{01962345-6789-7abc-8def-0123456789ab}",
        ] {
            assert!(parse(invalid).is_none(), "accepted invalid typed UUID");
        }
    }

    #[test]
    fn generated_request_ids_remain_distinct_under_concurrent_creation() {
        let workers: Vec<_> = (0..4)
            .map(|_| std::thread::spawn(|| (0..256).map(|_| new_id()).collect::<Vec<_>>()))
            .collect();
        let ids: std::collections::HashSet<_> = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(ids.len(), 1024);
        assert!(ids.into_iter().all(valid));
    }
}
