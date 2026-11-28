//! A job's environment, each of its strings held once by the whole service: jobs of one campaign repeat the same
//! variables, which are most of a retained group's bytes.
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

/// The variables a job sets for its compiler, by name; it reads, writes and compares as the map it holds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Environment(BTreeMap<Arc<str>, Arc<str>>);

impl Environment {
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(name, value)| (&**name, &**value))
    }
    pub fn to_map(&self) -> BTreeMap<String, String> {
        self.iter().map(|(name, value)| (name.to_owned(), value.to_owned())).collect()
    }
}

impl From<BTreeMap<String, String>> for Environment {
    fn from(variables: BTreeMap<String, String>) -> Self {
        let mut pool = POOL.lock().unwrap_or_else(PoisonError::into_inner);
        Self(variables.into_iter().map(|(name, value)| (pool.intern(name), pool.intern(value))).collect())
    }
}

impl Serialize for Environment {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.iter())
    }
}

impl<'de> Deserialize<'de> for Environment {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        BTreeMap::<String, String>::deserialize(deserializer).map(Self::from)
    }
}

/// Below this many strings the pool is never swept.
const UNSWEPT: usize = 4096;

/// Every string an environment holds, once. Once it holds twice the larger of `UNSWEPT` and what its last sweep
/// kept, the strings no environment holds any more leave it.
struct Pool {
    strings: HashSet<Arc<str>>,
    kept: usize,
}

static POOL: LazyLock<Mutex<Pool>> = LazyLock::new(|| Mutex::new(Pool { strings: HashSet::new(), kept: 0 }));

impl Pool {
    fn intern(&mut self, string: String) -> Arc<str> {
        if let Some(held) = self.strings.get(string.as_str()) {
            return Arc::clone(held);
        }
        if self.strings.len() >= 2 * self.kept.max(UNSWEPT) {
            // Only the pool can hand out a string it alone holds, and it is locked, so none is taken meanwhile.
            self.strings.retain(|held| Arc::strong_count(held) > 1);
            self.kept = self.strings.len();
        }
        let held: Arc<str> = string.into();
        self.strings.insert(Arc::clone(&held));
        held
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variables(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|&(name, value)| (name.to_owned(), value.to_owned())).collect()
    }

    /// Q-E1: an environment writes, reads and compares exactly as the map it was made from.
    #[test]
    fn an_environment_round_trips_as_its_map() {
        let map = variables(&[("PATH", "/bin:/usr/bin"), ("A", ""), ("Z", "\"quoted\" \u{e9}")]);
        let environment = Environment::from(map.clone());
        let written = serde_json::to_string(&environment).unwrap();
        assert_eq!(written, serde_json::to_string(&map).unwrap(), "(a) byte-equal to the map's");
        let read: Environment = serde_json::from_str(&written).unwrap();
        assert_eq!(read, environment, "(b) read back equal");
        assert_eq!(read.to_map(), map, "(c) the map it holds");
        assert_ne!(read, Environment::from(variables(&[("PATH", "/bin")])), "(d) another map differs");
        assert_eq!(serde_json::to_string(&Environment::default()).unwrap(), "{}");
    }

    /// Q-E2: two environments with a string in common hold it once.
    #[test]
    fn repeated_strings_are_held_once() {
        let value = "/nix/store/q-e2-shared-value/bin".repeat(64);
        let first = Environment::from(variables(&[("PATH", &value), ("ONE", "1")]));
        let second: Environment = serde_json::from_str(&serde_json::to_string(&variables(&[("PATH", &value)])).unwrap()).unwrap();
        let held = |environment: &Environment| Arc::as_ptr(environment.0.get("PATH").unwrap());
        assert_eq!(held(&first), held(&second), "one allocation for both");
    }

    /// Q-E3: a sweep drops the strings no environment holds and keeps those one does.
    #[test]
    fn a_sweep_keeps_only_held_strings() {
        let mut pool = Pool { strings: HashSet::new(), kept: 0 };
        let kept = pool.intern("q-e3-kept".to_owned());
        for index in 1..2 * UNSWEPT {
            drop(pool.intern(format!("q-e3-dropped-{index}")));
        }
        assert_eq!(pool.strings.len(), 2 * UNSWEPT, "(a) no sweep below twice UNSWEPT");
        let fresh = pool.intern("q-e3-fresh".to_owned());
        assert_eq!(pool.strings.len(), 2, "(b) the sweep left the held string and the new one");
        assert_eq!(pool.kept, 1, "(c) the next sweep waits for twice the larger of UNSWEPT and what this one kept");
        assert!(Arc::ptr_eq(&pool.intern("q-e3-kept".to_owned()), &kept), "a held string is still shared");
        assert!(Arc::ptr_eq(&pool.intern("q-e3-fresh".to_owned()), &fresh));
    }
}
