//! Resume tokens: proof that gpu-switch gave a job its place in the line.
//! The secret lives apart from the saved state, so a client can re-register
//! its place even when gpu-switch could not keep its state.

use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::protocol::{JobSpec, Placement};
use crate::switch::state::Side;

pub struct Secret([u8; 32]);

impl Secret {
    /// The secret in `dir`, created on first use; without a directory a
    /// fresh one that only this process honours.
    pub fn load_or_create(dir: Option<&Path>) -> Result<Secret, String> {
        let Some(dir) = dir else {
            return Ok(Secret(random()?));
        };
        let path = dir.join("secret");
        let mut bytes = [0u8; 32];
        if let Ok(mut f) = std::fs::File::open(&path) {
            if f.read_exact(&mut bytes).is_ok() {
                return Ok(Secret(bytes));
            }
        }
        let bytes = random()?;
        std::fs::create_dir_all(dir).map_err(|e| format!("creating {}: {e}", dir.display()))?;
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&path)
            .map_err(|e| format!("writing {}: {e}", path.display()))?;
        f.write_all(&bytes)
            .map_err(|e| format!("writing {}: {e}", path.display()))?;
        Ok(Secret(bytes))
    }

    pub fn token(&self, claim: &Claim) -> String {
        self.mac(claim)
            .finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    pub fn proves(&self, claim: &Claim, token: &str) -> bool {
        let Some(bytes) = hex(token) else {
            return false;
        };
        self.mac(claim).verify_slice(&bytes).is_ok()
    }

    fn mac(&self, claim: &Claim) -> Hmac<Sha256> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).expect("any key length");
        mac.update(claim.canonical().as_bytes());
        mac
    }
}

/// What a token vouches for.
pub struct Claim<'a> {
    pub side: Side,
    pub spec: &'a JobSpec,
    pub group: Option<&'a str>,
    pub key: Placement,
}

impl Claim<'_> {
    fn canonical(&self) -> String {
        format!(
            "{:?}|{}|{}|{}|{}|{}|{}|{}",
            self.side,
            self.spec.mode.as_str(),
            self.spec.vram,
            self.spec.priority,
            self.spec.cooperative,
            self.group.unwrap_or(""),
            self.key.class,
            self.key.ns
        )
    }
}

fn random() -> Result<[u8; 32], String> {
    let mut bytes = [0u8; 32];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut bytes))
        .map_err(|e| format!("reading /dev/urandom: {e}"))?;
    Ok(bytes)
}

fn hex(text: &str) -> Option<Vec<u8>> {
    (text.len() % 2 == 0)
        .then(|| {
            (0..text.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
                .collect()
        })
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Category, Mode};

    #[test]
    fn a_token_proves_exactly_its_claim() {
        let secret = Secret([7; 32]);
        let spec = JobSpec {
            mode: Mode::Shared,
            vram: 1 << 30,
            category: Category::Other,
            cooperative: false,
            priority: 0,
            max_runtime_secs: None,
        };
        let key = Placement { class: 1, ns: 42 };
        let claim = Claim {
            side: Side::Container,
            spec: &spec,
            group: Some("job"),
            key,
        };
        let token = secret.token(&claim);
        assert!(secret.proves(&claim, &token));
        let earlier = Claim {
            key: Placement { class: 1, ns: 41 },
            ..claim
        };
        assert!(!secret.proves(&earlier, &token));
        let host = Claim {
            side: Side::Host(1000),
            key,
            spec: &spec,
            group: Some("job"),
        };
        assert!(!secret.proves(&host, &token));
        assert!(!Secret([8; 32]).proves(&host, &token));
        assert!(!secret.proves(&host, "zz"));
    }
}
