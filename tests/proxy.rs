//! The holding proxy, driven through gpu-switch's start/stop.

mod common;

use std::time::{Duration, Instant};

use common::*;
use serde_json::Value;

const KA: &str = ": keep-alive\n\n";

fn serving(name: &str, opts: Opts) -> Harness {
    let h = Harness::start(name, opts);
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("SERVING");
    h
}

fn delay(ms: u64) -> Opts {
    Opts {
        mock_args: vec!["--health-delay-ms".into(), ms.to_string()],
        ..Opts::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passthrough_while_serving() {
    let h = serving("pass", Opts::default());
    let (status, headers, body) = request(
        h.proxy,
        "POST",
        "/v1/messages?beta=true",
        &[("x-mock-echo", "abc"), ("anthropic-version", "2023-06-01")],
        Some(message(false)),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(headers["x-mock-echo"], "abc");
    assert_eq!(headers["x-mock-path"], "/v1/messages?beta=true");
    let v: Value = serde_json::from_str(&text(body).await).unwrap();
    assert_eq!(v["content"][0]["text"], REPLY);

    let (status, headers, body) =
        request(h.proxy, "POST", "/v1/messages", &[], Some(message(true))).await;
    assert_eq!(status, 200);
    assert!(headers["content-type"]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
    let sse = text(body).await;
    assert!(
        !sse.contains("keep-alive"),
        "direct passthrough adds nothing: {sse}"
    );
    let ev = events(&sse);
    assert_eq!(ev.first().unwrap(), "message_start");
    assert_eq!(ev.last().unwrap(), "message_stop");

    let (status, _, body) = request(
        h.proxy,
        "POST",
        "/v1/messages/count_tokens",
        &[],
        Some(message(false)),
    )
    .await;
    assert_eq!(status, 200);
    assert!(text(body).await.contains("input_tokens"));
    let (status, headers, body) = request(h.proxy, "GET", "/v1/models", &[], None).await;
    assert_eq!(status, 200);
    assert!(headers.get("x-gpu-arbiter-cache").is_none());
    assert!(text(body).await.contains("ninfer-mock"));
    let (status, _, _) = request(h.proxy, "GET", "/nope", &[], None).await;
    assert_eq!(status, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_held_during_apply_now_restart() {
    let h = serving("hold-stream", delay(1500));
    let job = h.submit(Who::Host, EXCL, &["true"]);
    h.run_ok(Who::Host, &["set-host-cache", "4096", "--apply-now"]);
    wait_until("restart", Duration::from_secs(10), || h.word() != "SERVING");
    let sent = Instant::now();
    let (status, headers, body) =
        request(h.proxy, "POST", "/v1/messages", &[], Some(message(true))).await;
    assert_eq!(status, 200);
    assert!(
        sent.elapsed() < Duration::from_millis(500),
        "200 goes out at once"
    );
    assert_eq!(headers["content-type"], "text/event-stream");
    let chunks = chunks(body).await;
    let all: String = chunks.iter().map(|(_, c)| c.as_str()).collect();
    let rest = all.trim_start_matches(KA);
    let keepalives = (all.len() - rest.len()) / KA.len();
    assert!(keepalives >= 3, "keep-alives while held: {all:?}");
    assert!(rest.starts_with("event: message_start\n"), "{rest:?}");
    assert_eq!(all.matches("event: message_start").count(), 1);
    assert_eq!(events(rest).last().unwrap(), "message_stop");
    h.wait_word("SERVING");
    let s = h.status(Who::Host);
    assert_eq!(s["host_cache_current_mib"], 4096);
    assert_eq!(h.starts().len(), 2);
    assert_eq!(h.starts()[1].last().unwrap(), "4096");
    // The restart did not let the queue in.
    assert_eq!(h.job(Who::Host, job)["state"], "queued");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_streaming_held_and_models_cached() {
    let h = serving("hold-plain", delay(1500));
    let (_, _, body) = request(h.proxy, "GET", "/v1/models", &[], None).await;
    let models = text(body).await;
    h.run_ok(Who::Host, &["set-host-cache", "1024", "--apply-now"]);
    wait_until("restart", Duration::from_secs(10), || h.word() != "SERVING");
    let t = Instant::now();
    let (status, headers, body) = request(h.proxy, "GET", "/v1/models", &[], None).await;
    assert_eq!(status, 200);
    assert_eq!(headers["x-gpu-arbiter-cache"], "hit");
    assert_eq!(text(body).await, models);
    assert!(t.elapsed() < Duration::from_millis(500));
    let addr = h.proxy;
    let count = tokio::spawn(async move {
        let (s, _, b) = request(
            addr,
            "POST",
            "/v1/messages/count_tokens",
            &[],
            Some(message(false)),
        )
        .await;
        (s, text(b).await)
    });
    let plain = tokio::spawn(async move {
        let (s, _, b) = request(addr, "POST", "/v1/messages", &[], Some(message(false))).await;
        (s, text(b).await, Instant::now())
    });
    let (cs, cbody) = count.await.unwrap();
    let (ps, pbody, done) = plain.await.unwrap();
    assert_eq!(cs, 200);
    assert!(cbody.contains("input_tokens"));
    assert_eq!(ps, 200);
    assert!(pbody.contains(REPLY));
    assert!(
        done - t >= Duration::from_millis(500),
        "held until NInfer was healthy"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_while_off_until_serve_on() {
    let h = Harness::start("off-hold", Opts::default());
    h.wait_word("OFF");
    let addr = h.proxy;
    let stream = tokio::spawn(async move {
        let (s, _, b) = request(addr, "POST", "/v1/messages", &[], Some(message(true))).await;
        (s, text(b).await)
    });
    let plain = tokio::spawn(async move {
        let (s, _, b) = request(addr, "POST", "/v1/messages", &[], Some(message(false))).await;
        (s, text(b).await)
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(h.word(), "OFF");
    assert!(h.starts().is_empty(), "requests do not start NInfer");
    let out = h.run_ok(Who::Host, &["serve", "on"]);
    assert!(
        out.starts_with("serving on: NInfer queues for the GPU now\n"),
        "{out}"
    );
    let (ss, sse) = stream.await.unwrap();
    let (ps, body) = plain.await.unwrap();
    assert_eq!(ss, 200);
    assert!(sse.starts_with(KA));
    assert!(sse
        .trim_start_matches(KA)
        .starts_with("event: message_start\n"));
    assert_eq!(ps, 200);
    assert!(body.contains(REPLY));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upstream_error_after_200_becomes_error_event() {
    let h = Harness::start(
        "upstream-err",
        Opts {
            mock_args: vec![
                "--health-delay-ms".into(),
                "1000".into(),
                "--error-status".into(),
                "529".into(),
            ],
            ..Opts::default()
        },
    );
    h.run_ok(Who::Host, &["serve", "on"]);
    let (status, _, body) =
        request(h.proxy, "POST", "/v1/messages", &[], Some(message(true))).await;
    assert_eq!(status, 200);
    let sse = text(body).await;
    assert!(sse.starts_with(KA), "{sse:?}");
    assert!(!sse.contains("message_start"), "never fabricated: {sse:?}");
    let rest = sse.trim_start_matches(KA);
    assert!(rest.starts_with("event: error\ndata: "), "{rest:?}");
    let data: Value =
        serde_json::from_str(rest.lines().nth(1).unwrap().strip_prefix("data: ").unwrap()).unwrap();
    assert_eq!(data["error"]["type"], "overloaded_error");
    h.wait_word("SERVING");
    let (status, _, _) = request(h.proxy, "POST", "/v1/messages", &[], Some(message(true))).await;
    assert_eq!(status, 529, "while serving, passed through as HTTP");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ninfer_crash_holds_and_retries() {
    let h = serving("crash", delay(500));
    let pid = h.ninfer_pid().expect("NInfer runs");
    signal(pid, libc::SIGKILL);
    let addr = h.proxy;
    let plain = tokio::spawn(async move {
        let (s, _, b) = request(addr, "POST", "/v1/messages", &[], Some(message(false))).await;
        (s, text(b).await)
    });
    let stream = tokio::spawn(async move {
        let (s, _, b) = request(addr, "POST", "/v1/messages", &[], Some(message(true))).await;
        (s, text(b).await)
    });
    let (ps, pbody) = plain.await.unwrap();
    let (ss, sbody) = stream.await.unwrap();
    assert_eq!(ps, 200);
    assert!(pbody.contains(REPLY));
    assert_eq!(ss, 200);
    assert_eq!(sbody.matches("event: message_start").count(), 1);
    h.wait_word("SERVING");
    assert_eq!(h.starts().len(), 2);
    let cont = h.status(Who::Cont);
    assert!(
        cont["ninfer_error"]
            .as_str()
            .unwrap()
            .contains("exited unexpectedly"),
        "{cont}"
    );
    assert!(
        h.status(Who::Host)["ninfer_error"].is_null(),
        "error texts stay on the container side"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn switch_drain_waits_for_in_flight() {
    let h = serving(
        "drain",
        Opts {
            mock_args: vec!["--token-delay-ms".into(), "300".into()],
            ..Opts::default()
        },
    );
    let addr = h.proxy;
    let stream = tokio::spawn(async move {
        let (s, _, b) = request(addr, "POST", "/v1/messages", &[], Some(message(true))).await;
        (s, text(b).await, now())
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let id = h.submit(Who::Cont, EXCL, &["true"]);
    h.run_ok(Who::Cont, &["switch-gpu"]);
    wait_until("draining", Duration::from_secs(5), || {
        h.word() == "DRAINING"
    });
    let (status, sse, finished) = stream.await.unwrap();
    assert_eq!(status, 200);
    assert_eq!(
        events(&sse).last().unwrap(),
        "message_stop",
        "the turn finishes"
    );
    assert_eq!(h.wait_job(Who::Cont, id), 0);
    assert!(h.job(Who::Cont, id)["started_at"].as_f64().unwrap() >= finished - 0.05);
    h.wait_word("SERVING");
    assert_eq!(h.starts().len(), 2, "NInfer returns after the linger");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn switch_drain_timeout_aborts_in_flight() {
    let h = serving(
        "drain-timeout",
        Opts {
            mock_args: vec!["--token-delay-ms".into(), "2000".into()],
            drain_timeout: Some(0.5),
            ..Opts::default()
        },
    );
    let addr = h.proxy;
    let stream = tokio::spawn(async move {
        let (s, _, b) = request(addr, "POST", "/v1/messages", &[], Some(message(true))).await;
        (s, text(b).await)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let id = h.submit(Who::Host, EXCL, &["true"]);
    h.run_ok(Who::Host, &["switch-gpu"]);
    let (status, sse) = stream.await.unwrap();
    assert_eq!(status, 200);
    let ev = events(&sse);
    assert_eq!(ev.first().unwrap(), "message_start");
    assert_eq!(ev.last().unwrap(), "error", "{sse}");
    assert!(sse.contains("drain timeout"));
    assert_eq!(h.wait_job(Who::Host, id), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_of_gpu_switch_keeps_ninfer_serving() {
    let mut h = serving("restart", Opts::default());
    let pid = h.ninfer_pid().expect("NInfer runs");
    h.stop_switch();
    let addr = h.proxy;
    let (status, _, body) = request(addr, "POST", "/v1/messages", &[], Some(message(false))).await;
    assert_eq!(status, 200, "served while gpu-switch is away");
    assert!(text(body).await.contains(REPLY));
    h.start_switch();
    wait_until("reattached", Duration::from_secs(10), || {
        h.log("switch.log").matches("arbiter attached").count() >= 2
    });
    assert_eq!(h.word(), "SERVING");
    assert!(alive(pid), "the same NInfer");
    assert_eq!(h.starts().len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn link_loss_beyond_the_grace_stops_ninfer_and_requests_stay_held() {
    let mut h = serving(
        "link",
        Opts {
            arbiter_config: serde_json::json!({"link_grace_secs": 0.5}),
            ..Opts::default()
        },
    );
    let pid = h.ninfer_pid().expect("NInfer runs");
    h.stop_switch();
    wait_until(
        "NInfer stopped by the fail-safe",
        Duration::from_secs(5),
        || !alive(pid),
    );
    let addr = h.proxy;
    let plain = tokio::spawn(async move {
        let (s, _, b) = request(addr, "POST", "/v1/messages", &[], Some(message(false))).await;
        (s, text(b).await)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!plain.is_finished(), "held without NInfer");
    h.start_switch();
    wait_until("reattached", Duration::from_secs(10), || {
        h.log("switch.log").matches("arbiter attached").count() >= 2
    });
    let (status, body) = plain.await.unwrap();
    assert_eq!(
        status, 200,
        "serve on was kept: NInfer starts again by itself"
    );
    assert!(body.contains(REPLY));
    assert_eq!(h.starts().len(), 2);
}

#[test]
fn waiting_arbiter_until_attached() {
    let mut h = Harness::start(
        "noarb",
        Opts {
            arbiter: false,
            ..Opts::default()
        },
    );
    h.run_ok(Who::Host, &["serve", "on"]);
    h.wait_word("WAITING_ARBITER");
    h.start_arbiter();
    h.wait_word("SERVING");
}
