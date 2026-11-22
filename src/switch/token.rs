//! Resume tokens: proof that gpu-switch gave a job its place in the line.
//! The secret lives apart from the saved state, so a client can re-register
//! its place even when gpu-switch could not keep its state. A secret that
//! cannot be read refuses the start, so a failed read never replaces it.

use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::protocol::{JobSpec, Placement};
use crate::saved_file::{self, Optional};
use crate::switch::state::Side;

pub struct Secret([u8; 32]);

impl Secret {
    /// The secret in `dir`, created when none is saved there; without a directory a fresh one that only this process
    /// honours. A secret that cannot be read refuses the start, and the saved one stays.
    pub fn load_or_create(dir: Option<&Path>) -> Result<Secret, String> {
        let Some(dir) = dir else {
            return Ok(Secret(random()?));
        };
        let path = dir.join("secret");
        match saved_file::read(&path) {
            Optional::Present(bytes) => match bytes.first_chunk::<32>() {
                Some(key) => Ok(Secret(*key)),
                None => {
                    eprintln!(
                        "gpu-switch: {} holds {} bytes, fewer than a secret's 32; a new secret replaces it",
                        path.display(),
                        bytes.len()
                    );
                    create(dir, &path)
                }
            },
            Optional::Absent => create(dir, &path),
            Optional::Failed(error) => Err(error),
        }
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
    /// A named share is appended only when present, so tokens issued before shares existed still prove.
    fn canonical(&self) -> String {
        let mut canonical = format!(
            "{:?}|{}|{}|{}|{}|{}|{}|{}",
            self.side,
            self.spec.mode.as_str(),
            self.spec.vram,
            self.spec.priority,
            self.spec.cooperative,
            self.group.unwrap_or(""),
            self.key.class,
            self.key.ns
        );
        if let Some(share) = &self.spec.share {
            canonical.push_str(&format!("|share={share}"));
        }
        canonical
    }
}

/// A new secret, written to `path` in `dir`.
fn create(dir: &Path, path: &Path) -> Result<Secret, String> {
    let bytes = random()?;
    std::fs::create_dir_all(dir).map_err(|e| format!("creating {}: {e}", dir.display()))?;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
        .map_err(|e| format!("writing {}: {e}", path.display()))?;
    f.write_all(&bytes)
        .map_err(|e| format!("writing {}: {e}", path.display()))?;
    Ok(Secret(bytes))
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
    use crate::protocol::{Category, Mode, WatchdogPolicy};
    use crate::saved_file::WriteOnly;
    use crate::switch::control::tests::{spec, StateDir};
    use std::os::unix::fs::PermissionsExt;

    /// The claim the secret's tests prove: an entry of `Side::Host(1000)` with no group and the key `{1, 42}`.
    fn claim(spec: &JobSpec) -> Claim<'_> {
        Claim { side: Side::Host(1000), spec, group: None, key: Placement { class: 1, ns: 42 } }
    }

    #[test]
    fn a_secret_that_cannot_be_read_refuses_the_start_and_stays() {
        let shared = spec(None);
        let claim = claim(&shared);
        let dir = StateDir::new();
        let secret = dir.path().join("secret");
        std::fs::write(&secret, [7u8; 32]).unwrap();
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
        let fault = WriteOnly::at(&secret);
        assert_eq!(
            Secret::load_or_create(Some(dir.path())).err(),
            Some(format!("{}: Permission denied (os error 13)", secret.display())),
            "1. a secret that cannot be read refuses the start"
        );
        fault.lift();
        assert_eq!(std::fs::read(&secret).unwrap(), [7u8; 32], "2. the saved secret stays");
        let loaded = Secret::load_or_create(Some(dir.path())).expect("3. the lifted fault lets the secret load");
        assert!(loaded.proves(&claim, &Secret([7; 32]).token(&claim)), "3. it is the saved secret");
        let other = StateDir::new();
        let directory = other.path().join("secret");
        std::fs::create_dir(&directory).unwrap();
        assert_eq!(
            Secret::load_or_create(Some(other.path())).err(),
            Some(format!("{}: Is a directory (os error 21)", directory.display())),
            "4. a directory at the secret's path refuses the start"
        );
    }

    #[test]
    fn an_absent_secret_is_created_a_short_one_replaced_and_a_long_one_read() {
        let shared = spec(None);
        let claim = claim(&shared);
        let load = |dir: &StateDir| Secret::load_or_create(Some(dir.path()));
        let length = |dir: &StateDir| std::fs::metadata(dir.path().join("secret")).unwrap().len();
        let absent = StateDir::new();
        let first = load(&absent).expect("1 (a). an absent secret is created");
        let mode = std::fs::metadata(absent.path().join("secret")).unwrap().permissions().mode() & 0o777;
        assert_eq!((length(&absent), mode), (32, 0o600), "1 (a). the created secret has 32 bytes and mode 0600");
        assert!(load(&absent).unwrap().proves(&claim, &first.token(&claim)), "1 (a). a second load reads it");
        let short = StateDir::new();
        std::fs::write(short.path().join("secret"), [5u8; 5]).unwrap();
        let replaced = load(&short).expect("2 (b). a short secret is replaced");
        assert_eq!(length(&short), 32, "2 (b). the replacing secret has 32 bytes");
        assert!(load(&short).unwrap().proves(&claim, &replaced.token(&claim)), "2 (b). a second load reads it");
        let long = StateDir::new();
        std::fs::write(long.path().join("secret"), [[9u8; 32].as_slice(), &[1u8; 8]].concat()).unwrap();
        let read = load(&long).expect("3 (c). a long secret is read");
        assert!(read.proves(&claim, &Secret([9; 32]).token(&claim)), "3 (c). its first 32 bytes are the secret");
        assert_eq!(length(&long), 40, "3 (c). the file keeps its 40 bytes");
    }

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
            expected_runtime_secs: None,
            watchdog: WatchdogPolicy::Standard,
            share: None,
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

    #[test]
    fn a_token_issued_before_shares_still_proves_and_a_share_is_part_of_the_claim() {
        let secret = Secret([7; 32]);
        let spec = JobSpec {
            mode: Mode::Exclusive,
            vram: 0,
            category: Category::Other,
            cooperative: false,
            priority: 3,
            max_runtime_secs: None,
            expected_runtime_secs: None,
            watchdog: WatchdogPolicy::Standard,
            share: None,
        };
        let key = Placement { class: 1, ns: 42 };
        // The canonical form of gpu-switch 6d54736, which knew no shares.
        let mut earlier = Hmac::<Sha256>::new_from_slice(&[7; 32]).unwrap();
        earlier.update(b"Host(1000)|exclusive|0|3|false||1|42");
        let issued: String = earlier.finalize().into_bytes().iter().map(|b| format!("{b:02x}")).collect();
        let claim = Claim { side: Side::Host(1000), spec: &spec, group: None, key };
        assert!(secret.proves(&claim, &issued));
        let shared = JobSpec { share: Some("kernel-a".into()), ..spec.clone() };
        let named = Claim { side: Side::Host(1000), spec: &shared, group: None, key };
        assert!(!secret.proves(&named, &issued));
        assert!(secret.proves(&named, &secret.token(&named)));
    }
}
