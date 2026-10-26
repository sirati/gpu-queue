//! A loopback port in front of gpu-arbiter's unix socket, for clients that
//! only speak TCP (Claude Code, CLIProxyAPI). The port stays up whatever the
//! other end does: while the socket cannot be reached (the NInfer container
//! starting or restarting) each connection is held, its request read ahead,
//! and handed over once the socket answers. While NInfer itself is stopped
//! the arbiter holds the request, as it always has.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};

const RETRY: Duration = Duration::from_millis(250);
/// Request bytes read ahead while the socket is away.
const AHEAD_LIMIT: usize = 64 << 20;

pub struct Relay {
    pub listen: SocketAddr,
    pub socket: PathBuf,
    /// How long a connection waits for the socket before it is dropped.
    pub hold: Duration,
}

pub async fn run(relay: Relay) -> Result<(), String> {
    if !relay.listen.ip().is_loopback() {
        return Err(format!("{} is not a loopback address", relay.listen));
    }
    let l = TcpListener::bind(relay.listen)
        .await
        .map_err(|e| format!("bind {}: {e}", relay.listen))?;
    eprintln!(
        "ninfer-relay: {} -> {}",
        relay.listen,
        relay.socket.display()
    );
    let relay = std::sync::Arc::new(relay);
    loop {
        let (tcp, _) = match l.accept().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("ninfer-relay: accept: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let _ = tcp.set_nodelay(true);
        let relay = relay.clone();
        tokio::spawn(async move { connection(&relay, tcp).await });
    }
}

async fn connection(relay: &Relay, mut tcp: TcpStream) {
    let deadline = Instant::now() + relay.hold;
    let mut ahead = Vec::new();
    let mut eof = false;
    let mut logged = false;
    let mut unix = loop {
        match UnixStream::connect(&relay.socket).await {
            Ok(u) => break u,
            Err(e) => {
                if !logged {
                    eprintln!(
                        "ninfer-relay: {}: {e}; holding the connection",
                        relay.socket.display()
                    );
                    logged = true;
                }
            }
        }
        if Instant::now() >= deadline {
            eprintln!(
                "ninfer-relay: socket still away after {:?}, dropping a connection",
                relay.hold
            );
            return;
        }
        let room = !eof && ahead.len() < AHEAD_LIMIT;
        tokio::select! {
            _ = tokio::time::sleep(RETRY) => {}
            r = tcp.read_buf(&mut ahead), if room => match r {
                Ok(0) => eof = true,
                Ok(_) => {}
                Err(_) => return,
            },
        }
    };
    if logged {
        eprintln!("ninfer-relay: socket back, handing over a held connection");
    }
    if unix.write_all(&ahead).await.is_err() {
        return;
    }
    if eof {
        // The client has sent all it will: pass that on, then the answer back.
        let _ = unix.shutdown().await;
        let (mut ur, _uw) = unix.split();
        let (_tr, mut tw) = tcp.split();
        let _ = tokio::io::copy(&mut ur, &mut tw).await;
        let _ = tw.shutdown().await;
        return;
    }
    let _ = tokio::io::copy_bidirectional(&mut tcp, &mut unix).await;
}
