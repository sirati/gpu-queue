//! One estimate policy for every submission path. Preserve explicit -1 on the
//! wire so that opting out remains distinguishable from an omitted estimate.
pub const WARNING: &str = "GPU duration estimate omitted; supply --expected-secs N (expected_runtime_secs in APIs), or -1 to explicitly declare no estimate and suppress this warning";

pub fn validate(value: Option<i64>) -> Result<(), String> {
    match value {
        Some(n) if n == 0 || n < -1 => {
            Err("expected runtime must be positive seconds or -1 for no estimate".into())
        }
        _ => Ok(()),
    }
}

pub fn seconds(value: Option<i64>) -> Option<u64> {
    value.filter(|n| *n > 0).map(|n| n as u64)
}

pub fn warning(value: Option<i64>) -> Option<&'static str> {
    value.is_none().then_some(WARNING)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omission_warns_but_explicit_unknown_is_silent_without_a_duration() {
        assert_eq!(warning(None), Some(WARNING));
        assert_eq!(warning(Some(-1)), None);
        assert_eq!(warning(Some(60)), None);
        assert_eq!(seconds(None), None);
        assert_eq!(seconds(Some(-1)), None);
        assert_eq!(seconds(Some(60)), Some(60));
    }

    #[test]
    fn only_positive_seconds_or_explicit_unknown_are_valid() {
        for n in [None, Some(-1), Some(1), Some(i64::MAX)] {
            assert!(validate(n).is_ok());
        }
        for n in [0, -2, i64::MIN] {
            assert!(validate(Some(n)).is_err());
        }
    }
}
