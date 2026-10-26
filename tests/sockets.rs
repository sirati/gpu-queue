//! Who reaches what: gpu-switch's arbiter socket, the proxy's per-listener
//! policy, and ninfer-relay in front of a socket that comes and goes.

mod common;

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::*;
use serde_json::Value;

fn ask(sock: &std::path::Path, line: &str) -> Value {
    let mut s = UnixStream::connect(sock).unwrap();
    s.write_all(format!("{line}\n").as_bytes()).unwrap();
    let mut reply = String::new();
    BufReader::new(s).read_line(&mut reply).unwrap();
    serde_json::from_str(&reply).unwrap()
}

#[test]
fn the_arbiter_link_has_its_own_socket() {
    let h = Harness::start("arb-sock", Opts::default());
    for sock in [&h.cont_sock, &h.host_sock] {
        let r = ask(sock, r#"{"op":"arbiter_attach"}"#);
        assert_eq!(r["ok"], false, "{r}");
    }
    let r = ask(&h.arb_sock, r#"{"op":"status"}"#);
    assert_eq!(r["ok"], false, "{r}");
    let r = ask(&h.arb_sock, r#"{"op":"serve","on":true}"#);
    assert_eq!(r["ok"], false, "{r}");
    assert!(h.log("switch.log").contains("arbiter attached"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_container_listener_serves_messages_only() {
    let h = Harness::start("cont-listener", Opts::default());
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("SERVING");
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(&h.proxy_cont)
        .unwrap()
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o666, "the configured mode");

    for stream in [false, true] {
        let (status, _, body) = request_unix(
            &h.proxy_cont,
            "POST",
            "/v1/messages?beta=true",
            Some(message(stream)),
        )
        .await;
        assert_eq!(status, 200);
        assert!(text(body).await.contains("Hello"), "stream {stream}");
    }

    // Messages, count_tokens and models; nothing else, the mock's own
    // endpoints included, which the whole-API listener serves.
    let (status, _, _) = request(h.proxy, "GET", "/mock/refused", &[], None).await;
    assert_eq!(status, 200);
    let (status, _, _) = request_unix(
        &h.proxy_cont,
        "POST",
        "/v1/messages/count_tokens",
        Some(asking("S", "count")),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _, _) = request_unix(&h.proxy_cont, "GET", "/v1/models", None).await;
    assert_eq!(status, 200);
    for (method, path) in [
        ("GET", "/mock/refused"),
        ("POST", "/v1/chat/completions"),
        ("POST", "/v1/responses"),
        ("GET", "/health"),
        ("GET", "/metrics"),
    ] {
        let body = (method == "POST").then(|| asking("S", "x"));
        let (status, _, _) = request_unix(&h.proxy_cont, method, path, body).await;
        assert_eq!(status, 404, "{method} {path}");
    }
}

fn relay(h: &Harness, socket: &std::path::Path) -> (Child, std::net::SocketAddr) {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let log = std::fs::File::create(h.dir.join("relay.log")).unwrap();
    let child = Command::new(RELAY)
        .args(["--listen", &addr.to_string(), "--socket"])
        .arg(socket)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .spawn()
        .unwrap();
    wait_until("relay listening", Duration::from_secs(10), || {
        std::net::TcpStream::connect(addr).is_ok()
    });
    (child, addr)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_relay_holds_requests_while_the_socket_is_away() {
    let mut h = Harness::start("relay", Opts::default());
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("SERVING");
    // The socket is not there yet: the request waits for it.
    let later = h.dir.join("later.sock");
    let (mut child, addr) = relay(&h, &later);
    let pending = tokio::spawn(async move {
        let (status, _, body) =
            request(addr, "POST", "/v1/messages", &[], Some(message(false))).await;
        (status, text(body).await)
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(!pending.is_finished());
    std::os::unix::fs::symlink(&h.proxy_cont, &later).unwrap();
    let (status, body) = tokio::time::timeout(Duration::from_secs(10), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, 200, "{body}");
    assert!(body.contains(REPLY), "{body}");

    // The arbiter goes away and comes back: the relay's port stays, and
    // requests sent meanwhile are answered, also one from a client that shut
    // its side down after sending (HTTP/1.0 over socat).
    h.stop_arbiter();
    let pending = tokio::spawn(async move {
        let (status, _, body) =
            request(addr, "POST", "/v1/messages", &[], Some(message(true))).await;
        (status, text(body).await)
    });
    let half_closed = std::thread::spawn(move || {
        use std::io::Read;
        let body = message(false).to_string();
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        write!(
            s,
            "POST /v1/messages HTTP/1.0\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        s.shutdown(std::net::Shutdown::Write).unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).unwrap();
        out
    });
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert!(!pending.is_finished());
    assert!(!half_closed.is_finished());
    h.start_arbiter();
    h.wait_word("SERVING");
    let (status, body) = tokio::time::timeout(Duration::from_secs(20), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        events(&body).last().map(String::as_str),
        Some("message_stop"),
        "{body}"
    );
    let raw = half_closed.join().unwrap();
    assert!(
        raw.starts_with("HTTP/1.0 200") && raw.contains(REPLY),
        "{raw}"
    );
    assert!(h.log("relay.log").contains("holding the connection"));
    let _ = child.kill();
    let _ = child.wait();
}
