//! The one place a record's figures are checked: the record as written against its derivation, field for field.
use serde_json::{Number, Value};

/// A figure a derivation could not know, at its JSON pointer in the record, and why.
#[derive(Clone, Debug, PartialEq)]
pub struct Unknown {
    pub path: &'static str,
    pub reason: &'static str,
}
impl Unknown {
    /// Shows the figure in a derived record as unknown, with why.
    pub fn mark(&self, record: &mut Value) {
        record
            .pointer_mut(self.path)
            .into_iter()
            .for_each(|figure| *figure = serde_json::json!({ "unknown": self.reason }));
    }
}

/// Whether the record as written is its derivation: the same fields with the same values, numbers compared by value,
/// but for the figures the derivation could not know.
pub fn agree(written: &Value, derived: &Value, unknown: &[Unknown]) -> Result<(), String> {
    difference(Some(written), Some(derived), String::new(), unknown).map_or(Ok(()), Err)
}

fn difference(written: Option<&Value>, derived: Option<&Value>, path: String, unknown: &[Unknown]) -> Option<String> {
    let known = !unknown.iter().any(|figure| figure.path == path);
    match (known, written, derived) {
        (false, _, _) => None,
        (true, Some(Value::Object(written)), Some(Value::Object(derived))) => {
            let keys: std::collections::BTreeSet<&String> = written.keys().chain(derived.keys()).collect();
            keys.into_iter()
                .find_map(|key| difference(written.get(key), derived.get(key), format!("{path}/{key}"), unknown))
        }
        (true, Some(Value::Array(written)), Some(Value::Array(derived))) => match written.len() == derived.len() {
            false => Some(format!("the record's {path} holds {} entries, its derivation {}", written.len(), derived.len())),
            true => written
                .iter()
                .zip(derived)
                .enumerate()
                .find_map(|(index, (written, derived))| {
                    difference(Some(written), Some(derived), format!("{path}/{index}"), unknown)
                }),
        },
        (true, Some(Value::Number(written)), Some(Value::Number(derived))) => {
            (!same_number(written, derived)).then(|| format!("the record's {path} is {written}, its derivation {derived}"))
        }
        (true, written, derived) => (written != derived).then(|| {
            let show = |value: Option<&Value>| value.map_or("absent".to_owned(), Value::to_string);
            format!("the record's {path} is {}, its derivation {}", show(written), show(derived))
        }),
    }
}

/// Two numbers of equal value: exact for integers, and by value where either is written as a float.
fn same_number(written: &Number, derived: &Number) -> bool {
    match (written.as_u64(), derived.as_u64(), written.as_i64(), derived.as_i64()) {
        (Some(written), Some(derived), _, _) => written == derived,
        (_, _, Some(written), Some(derived)) => written == derived,
        _ => written.as_f64() == derived.as_f64(),
    }
}
