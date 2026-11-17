//! An authority's outage: which authority did not answer, the operation it did not answer, and why. An outage
//! stops work to be resumed later; it is never a verdict.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outage {
    /// The authority that did not answer, such as the compile service.
    pub source: String,
    /// What was asked of it: its wire operation.
    pub operation: String,
    /// The original reason, as the caller observed it.
    pub reason: String,
}
impl Outage {
    pub fn new(source: &str, operation: &str, reason: impl Into<String>) -> Self {
        Self {
            source: source.to_owned(),
            operation: operation.to_owned(),
            reason: reason.into(),
        }
    }
}
impl std::fmt::Display for Outage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.reason)
    }
}
