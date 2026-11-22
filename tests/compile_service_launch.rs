//! The disposable compile service's own guarantee: it runs as the test's child only where the capacity it reads there
//! admits the test's request after its headroom. A missing user service manager alone never selects that.
mod compile_service;

use compile_service::{Launch, Uncontained};

const GIB: u64 = 1 << 30;
const REQUEST: u64 = 12 * GIB;
const HEADROOM: u64 = GIB;

fn select(unit: Option<&str>, reading: Result<(u64, u64), String>) -> Result<Launch, Uncontained> {
    Launch::select(unit.map(str::to_owned), reading, REQUEST, HEADROOM)
}

fn uncontained(reading: Result<(u64, u64), String>) -> Result<Launch, Uncontained> {
    Err(Uncontained { request: REQUEST, headroom: HEADROOM, reading })
}

#[test]
fn a_user_service_manager_gives_the_service_a_scope_of_its_own_whatever_the_capacity_where_the_test_runs() {
    for reading in [Ok((GIB, 8 * GIB)), Ok((64 * GIB, 64 * GIB)), Err("no MemTotal".to_owned())] {
        assert_eq!(select(Some("gate.scope"), reading), Ok(Launch::Scoped("gate.scope".into())));
    }
}

#[test]
fn without_a_user_service_manager_the_service_is_the_tests_child_where_its_capacity_admits_the_request() {
    // The capacity decides, as at registration; little available memory only delays a grant.
    for reading in [Ok((GIB, REQUEST + HEADROOM)), Ok((64 * GIB, 64 * GIB))] {
        assert_eq!(select(None, reading), Ok(Launch::Child));
    }
}

#[test]
fn without_a_user_service_manager_a_capacity_short_of_the_request_after_headroom_is_a_named_prerequisite() {
    // An 8 GiB ceiling without a user bus; one byte short; the request without its headroom; no reading.
    let readings = [Ok((GIB, 8 * GIB)), Ok((64 * GIB, REQUEST + HEADROOM - 1)), Ok((64 * GIB, REQUEST)), Err("no MemTotal".to_owned())];
    for reading in readings {
        let launch = select(None, reading.clone());
        assert_eq!(launch, uncontained(reading));
        let named = launch.unwrap_err().to_string();
        assert!(named.starts_with("prerequisite unavailable, containment or capacity:"), "{named}");
    }
}
