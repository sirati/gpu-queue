//! Holding proxy in front of NInfer. Requests are forwarded only while the
//! arbiter is SERVING; otherwise they wait (streaming messages behind SSE
//! keep-alives). A failed upstream exchange that has not produced response
//! bytes yet is retried after NInfer is back.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use tokio::net::{TcpListener, UnixListener};

use crate::arbiter::config::{secs, Listener};
use crate::arbiter::state::{CachedResponse, InFlight, Shared};
use crate::http::{self, BoxError, FrameTx, ProxyBody};

const RETRY_DELAY: Duration = Duration::from_millis(250);
const KEEPALIVE: &[u8] = b": keep-alive\n\n";
const ERROR_BODY_LIMIT: usize = 64 * 1024;
const MODELS_BODY_LIMIT: usize = 4 * 1024 * 1024;

/// What one listener lets through.
#[derive(Debug, Default)]
pub struct Policy {
    pub messages_only: bool,
}

impl Policy {
    pub fn of(l: &Listener) -> Policy {
        Policy {
            messages_only: l.messages_only,
        }
    }
}

/// The whole API on a TCP listener.
pub async fn serve(shared: Arc<Shared>, listener: TcpListener) {
    let policy = Arc::new(Policy::default());
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                eprintln!("gpu-arbiter: proxy accept: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        connection(shared.clone(), policy.clone(), stream);
    }
}

pub async fn serve_unix(shared: Arc<Shared>, listener: UnixListener, policy: Policy) {
    let policy = Arc::new(policy);
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(e) => {
                eprintln!("gpu-arbiter: proxy accept: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        connection(shared.clone(), policy.clone(), stream);
    }
}

fn connection<S>(shared: Arc<Shared>, policy: Arc<Policy>, stream: S)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let svc = service_fn(move |req| {
            let shared = shared.clone();
            let policy = policy.clone();
            async move { Ok::<_, Infallible>(handle(shared, &policy, req).await) }
        });
        // A client that has sent its request and shut its side down still
        // gets the answer, however long the request is held (ninfer-relay
        // passes such a shutdown on).
        let _ = hyper::server::conn::http1::Builder::new()
            .half_close(true)
            .serve_connection(TokioIo::new(stream), svc)
            .await;
    });
}

/// The endpoints a `messages_only` listener serves.
fn messages_endpoint(method: &Method, path: &str) -> bool {
    matches!(
        (method, path),
        (&Method::POST, "/v1/messages")
            | (&Method::POST, "/v1/messages/count_tokens")
            | (&Method::GET, "/v1/models")
    )
}

struct Forward {
    method: Method,
    path_and_query: String,
    headers: HeaderMap,
    body: Bytes,
}

impl Forward {
    async fn send(&self, shared: &Shared) -> Result<Response<Incoming>, BoxError> {
        http::send_upstream(
            shared.config.ninfer.port,
            self.method.clone(),
            &self.path_and_query,
            &self.headers,
            self.body.clone(),
        )
        .await
    }

    /// `None` if the drain timeout aborted the exchange first.
    async fn send_unless_aborted(
        &self,
        shared: &Shared,
        guard: &InFlight,
    ) -> Option<Result<Response<Incoming>, BoxError>> {
        tokio::select! {
            r = self.send(shared) => Some(r),
            _ = guard.aborted() => None,
        }
    }
}

async fn handle(
    shared: Arc<Shared>,
    policy: &Policy,
    req: Request<Incoming>,
) -> Response<ProxyBody> {
    let (parts, body) = req.into_parts();
    if policy.messages_only && !messages_endpoint(&parts.method, parts.uri.path()) {
        return http::json_response(
            StatusCode::NOT_FOUND,
            &http::anthropic_error_body(
                "not_found_error",
                &format!(
                    "gpu-arbiter: {} {} is not served on this socket",
                    parts.method,
                    parts.uri.path()
                ),
            ),
        );
    }
    let body = match body.collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => {
            return http::json_response(
                StatusCode::BAD_REQUEST,
                &http::anthropic_error_body(
                    "invalid_request_error",
                    &format!("gpu-arbiter: reading request body: {e}"),
                ),
            )
        }
    };
    let path = parts.uri.path().to_string();
    let fwd = Forward {
        method: parts.method,
        path_and_query: parts
            .uri
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/")
            .into(),
        headers: parts.headers,
        body,
    };
    if fwd.method == Method::GET && path == "/v1/models" {
        return models(shared, fwd).await;
    }
    if fwd.method == Method::POST && path == "/v1/messages" && wants_stream(&fwd.body) {
        return stream(shared, fwd).await;
    }
    held(shared, fwd).await
}

fn wants_stream(body: &[u8]) -> bool {
    #[derive(Deserialize)]
    struct Probe {
        stream: Option<serde_json::Value>,
    }
    matches!(
        serde_json::from_slice::<Probe>(body),
        Ok(Probe {
            stream: Some(serde_json::Value::Bool(true))
        })
    )
}

fn upstream_failed(what: &str, e: &dyn std::fmt::Display) {
    eprintln!("gpu-arbiter: upstream {what}: {e}; holding and retrying");
}

/// Wait for SERVING, forward, retry until a response arrives.
async fn held(shared: Arc<Shared>, fwd: Forward) -> Response<ProxyBody> {
    loop {
        let guard = shared.enter().await;
        match fwd.send_unless_aborted(&shared, &guard).await {
            Some(Ok(resp)) => return passthrough(resp, guard),
            Some(Err(e)) => upstream_failed(&fwd.path_and_query, &e),
            None => {}
        }
        drop(guard);
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

async fn models(shared: Arc<Shared>, fwd: Forward) -> Response<ProxyBody> {
    if let Some(guard) = shared.try_enter() {
        if let Some(Ok(resp)) = fwd.send_unless_aborted(&shared, &guard).await {
            let (parts, body) = resp.into_parts();
            let body = match http::collect_limited(body, MODELS_BODY_LIMIT).await {
                Ok(b) => b,
                Err(e) => {
                    return http::json_response(
                        StatusCode::BAD_GATEWAY,
                        &http::anthropic_error_body(
                            "api_error",
                            &format!("gpu-arbiter: reading /v1/models: {e}"),
                        ),
                    )
                }
            };
            let headers = http::end_to_end(&parts.headers);
            if parts.status == StatusCode::OK {
                *shared
                    .models_cache
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(CachedResponse {
                    status: parts.status,
                    headers: headers.clone(),
                    body: body.clone(),
                });
            }
            let mut out = Response::new(http::full(body));
            *out.status_mut() = parts.status;
            *out.headers_mut() = headers;
            return out;
        }
    }
    let cached = shared
        .models_cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .map(|c| {
            let mut out = Response::new(http::full(c.body.clone()));
            *out.status_mut() = c.status;
            *out.headers_mut() = c.headers.clone();
            out.headers_mut()
                .insert("x-gpu-arbiter-cache", "hit".parse().expect("static"));
            out
        });
    match cached {
        Some(r) => r,
        None => held(shared, fwd).await,
    }
}

fn is_sse(headers: &HeaderMap) -> bool {
    headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim_start().starts_with("text/event-stream"))
}

/// Upstream response to the client, body streamed through a pump that keeps
/// the request counted as in flight until the body ends.
fn passthrough(resp: Response<Incoming>, guard: InFlight) -> Response<ProxyBody> {
    let (mut parts, body) = resp.into_parts();
    let sse = is_sse(&parts.headers);
    parts.headers = http::end_to_end(&parts.headers);
    let hint = if sse {
        SizeHint::default()
    } else {
        body.size_hint()
    };
    let (tx, out) = http::channel_body(hint);
    tokio::spawn(pump(body, tx, guard, sse));
    Response::from_parts(parts, out)
}

async fn pump(mut body: Incoming, tx: FrameTx, guard: InFlight, sse: bool) {
    let aborted = guard.aborted();
    tokio::pin!(aborted);
    let failure = loop {
        tokio::select! {
            frame = body.frame() => match frame {
                Some(Ok(frame)) => {
                    if tx.send(Ok(frame)).await.is_err() {
                        return;
                    }
                }
                Some(Err(e)) => break format!("upstream stream failed: {e}"),
                None => return,
            },
            _ = &mut aborted => break "request aborted by gpu-arbiter after the GPU switch drain timeout".to_string(),
            _ = tx.closed() => return,
        }
    };
    eprintln!("gpu-arbiter: response cut short: {failure}");
    let item = if sse {
        Ok(Frame::data(http::sse_error_event("api_error", &failure)))
    } else {
        Err(failure.into())
    };
    let _ = tx.send(item).await;
}

async fn stream(shared: Arc<Shared>, fwd: Forward) -> Response<ProxyBody> {
    if let Some(guard) = shared.try_enter() {
        match fwd.send_unless_aborted(&shared, &guard).await {
            Some(Ok(resp)) => return passthrough(resp, guard),
            Some(Err(e)) => upstream_failed(&fwd.path_and_query, &e),
            None => {}
        }
    }
    let (tx, body) = http::channel_body(SizeHint::default());
    tokio::spawn(held_stream(shared, fwd, tx));
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(body)
        .expect("static response")
}

async fn keepalive(tx: &FrameTx) -> Result<(), ()> {
    tx.send(Ok(Frame::data(Bytes::from_static(KEEPALIVE))))
        .await
        .map_err(|_| ())
}

/// The 200 is already out: keep the client alive with SSE comments until an
/// upstream exchange succeeds, then pipe it or turn an HTTP error into an
/// in-stream error event.
async fn held_stream(shared: Arc<Shared>, fwd: Forward, tx: FrameTx) {
    if keepalive(&tx).await.is_err() {
        return;
    }
    let period = secs(shared.config.proxy.keepalive_interval_secs);
    let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let guard = {
            let enter = shared.enter();
            tokio::pin!(enter);
            loop {
                tokio::select! {
                    g = &mut enter => break g,
                    _ = tick.tick() => if keepalive(&tx).await.is_err() { return },
                    _ = tx.closed() => return,
                }
            }
        };
        let result = {
            let send = fwd.send_unless_aborted(&shared, &guard);
            tokio::pin!(send);
            loop {
                tokio::select! {
                    r = &mut send => break r,
                    _ = tick.tick() => if keepalive(&tx).await.is_err() { return },
                    _ = tx.closed() => return,
                }
            }
        };
        match result {
            Some(Ok(resp)) if resp.status() == StatusCode::OK => {
                pump(resp.into_body(), tx, guard, true).await;
                return;
            }
            Some(Ok(resp)) => {
                let status = resp.status();
                let body = tokio::time::timeout(
                    Duration::from_secs(10),
                    http::collect_limited(resp.into_body(), ERROR_BODY_LIMIT),
                )
                .await
                .ok()
                .and_then(|r| r.ok())
                .unwrap_or_default();
                drop(guard);
                let (kind, message) = upstream_error(status, &body);
                let _ = tx
                    .send(Ok(Frame::data(http::sse_error_event(&kind, &message))))
                    .await;
                return;
            }
            Some(Err(e)) => upstream_failed(&fwd.path_and_query, &e),
            None => {}
        }
        drop(guard);
        let pause = tokio::time::sleep(RETRY_DELAY);
        tokio::pin!(pause);
        loop {
            tokio::select! {
                _ = &mut pause => break,
                _ = tick.tick() => if keepalive(&tx).await.is_err() { return },
                _ = tx.closed() => return,
            }
        }
    }
}

/// Error type and message for an in-stream error event, preferring what an
/// Anthropic-shaped upstream error body says.
fn upstream_error(status: StatusCode, body: &[u8]) -> (String, String) {
    let parsed: Option<serde_json::Value> = serde_json::from_slice(body).ok();
    let err = parsed.as_ref().and_then(|v| v.get("error"));
    let kind = err
        .and_then(|e| e.get("type"))
        .and_then(|t| t.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| http::error_type_for_status(status).to_string());
    let detail = err
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| {
            let text = String::from_utf8_lossy(body);
            text.chars().take(500).collect()
        });
    let prefix = match status.as_u16() {
        408 | 504 => "upstream timeout",
        _ => "upstream error",
    };
    (
        kind,
        format!("{prefix} (HTTP {}): {detail}", status.as_u16()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_only() {
        assert!(messages_endpoint(&Method::POST, "/v1/messages"));
        assert!(messages_endpoint(&Method::GET, "/v1/models"));
        assert!(!messages_endpoint(&Method::POST, "/v1/chat/completions"));
        assert!(!messages_endpoint(&Method::POST, "/v1/responses"));
        assert!(!messages_endpoint(&Method::GET, "/metrics"));
    }

    #[test]
    fn stream_probe() {
        assert!(wants_stream(br#"{"model":"x","stream":true}"#));
        assert!(!wants_stream(br#"{"model":"x","stream":false}"#));
        assert!(!wants_stream(br#"{"model":"x"}"#));
        assert!(!wants_stream(b"not json"));
    }

    #[test]
    fn error_mapping() {
        let (k, m) = upstream_error(StatusCode::from_u16(529).unwrap(), b"busy");
        assert_eq!(k, "overloaded_error");
        assert!(m.contains("529") && m.contains("busy"));
        let (k, m) = upstream_error(StatusCode::GATEWAY_TIMEOUT, b"");
        assert_eq!(k, "api_error");
        assert!(m.starts_with("upstream timeout"));
        let (k, _) = upstream_error(StatusCode::UNPROCESSABLE_ENTITY, b"");
        assert_eq!(k, "invalid_request_error");
        let body = br#"{"type":"error","error":{"type":"rate_limit_error","message":"slow down"}}"#;
        let (k, m) = upstream_error(StatusCode::TOO_MANY_REQUESTS, body);
        assert_eq!(k, "rate_limit_error");
        assert!(m.ends_with("slow down"));
    }
}
