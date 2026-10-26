use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::header::{HeaderMap, HeaderName, HeaderValue};
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;
pub type ProxyBody = BoxBody<Bytes, BoxError>;
pub type FrameTx = mpsc::Sender<Result<Frame<Bytes>, BoxError>>;

/// Response body fed by a pump task.
pub struct ChannelBody {
    rx: mpsc::Receiver<Result<Frame<Bytes>, BoxError>>,
    hint: SizeHint,
}

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, BoxError>>> {
        self.get_mut().rx.poll_recv(cx)
    }

    fn size_hint(&self) -> SizeHint {
        self.hint
    }
}

pub fn channel_body(hint: SizeHint) -> (FrameTx, ProxyBody) {
    let (tx, rx) = mpsc::channel(16);
    (tx, ChannelBody { rx, hint }.boxed())
}

pub fn full(body: impl Into<Bytes>) -> ProxyBody {
    Full::new(body.into())
        .map_err(|never| match never {})
        .boxed()
}

pub fn json_response(status: StatusCode, value: &serde_json::Value) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(full(value.to_string()))
        .expect("static response")
}

pub fn anthropic_error_body(kind: &str, message: &str) -> serde_json::Value {
    serde_json::json!({"type": "error", "error": {"type": kind, "message": message}})
}

pub fn sse_error_event(kind: &str, message: &str) -> Bytes {
    Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        anthropic_error_body(kind, message)
    ))
}

/// Anthropic error type for an upstream HTTP status.
pub fn error_type_for_status(status: StatusCode) -> &'static str {
    match status.as_u16() {
        401 => "authentication_error",
        403 => "permission_error",
        404 => "not_found_error",
        413 => "request_too_large",
        429 => "rate_limit_error",
        529 => "overloaded_error",
        408 | 504 => "api_error",
        400..=499 => "invalid_request_error",
        _ => "api_error",
    }
}

/// RFC 9110 hop-by-hop headers plus the ones each leg recomputes.
const DROPPED: [&str; 10] = [
    "connection",
    "keep-alive",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "proxy-authenticate",
    "proxy-authorization",
    "host",
    "content-length",
];

pub fn end_to_end(inbound: &HeaderMap) -> HeaderMap {
    let named: Vec<String> = inbound
        .get_all("connection")
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().to_ascii_lowercase())
        .filter(|t| !t.is_empty())
        .collect();
    let mut out = HeaderMap::with_capacity(inbound.len());
    for (name, value) in inbound.iter() {
        let n = name.as_str();
        if DROPPED.contains(&n) || named.iter().any(|t| t == n) {
            continue;
        }
        out.append(HeaderName::from(name), value.clone());
    }
    out
}

/// One request on a fresh HTTP/1.1 connection to 127.0.0.1:`port`.
pub async fn send_upstream(
    port: u16,
    method: hyper::Method,
    path_and_query: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<Response<Incoming>, BoxError> {
    let stream = TcpStream::connect(("127.0.0.1", port)).await?;
    stream.set_nodelay(true)?;
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream)).await?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut req = Request::builder()
        .method(method)
        .uri(path_and_query)
        .body(Full::new(body))?;
    *req.headers_mut() = end_to_end(headers);
    req.headers_mut()
        .insert("host", HeaderValue::from_str(&format!("127.0.0.1:{port}"))?);
    Ok(sender.send_request(req).await?)
}

pub async fn collect_limited(body: Incoming, limit: usize) -> Result<Bytes, BoxError> {
    let bytes = http_body_util::Limited::new(body, limit)
        .collect()
        .await?
        .to_bytes();
    Ok(bytes)
}
