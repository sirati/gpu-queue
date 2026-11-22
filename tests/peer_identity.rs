//! A connection's sender is the socket's peer, read through the descriptor `SO_PEERPIDFD` gives. The parent runs this
//! executable again as each child that connects to it.
use gpu_arbiter::procs::{Peer, Process, Unread};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::os::linux::net::SocketAddrExt;
use std::os::unix::net::{SocketAddr, UnixListener, UnixStream};
use std::process::{Child, Command, Stdio};

/// The abstract name a child connects to, in its environment.
const CONNECT: &str = "PEER_CONNECT";

fn main() {
    match std::env::var(CONNECT) {
        Ok(name) => child(&name),
        Err(_) => parent(),
    }
}

/// Connects, says ready, and holds the connection until its standard input ends.
fn child(name: &str) {
    let mut stream = UnixStream::connect_addr(&SocketAddr::from_abstract_name(name.as_bytes()).unwrap()).unwrap();
    stream.write_all(b"ready\n").unwrap();
    std::io::stdin().read_to_end(&mut Vec::new()).unwrap();
}

/// A child connected to `listener`, and the parent's end of its connection once the child said ready.
fn connected(listener: &UnixListener, name: &str) -> (Child, UnixStream) {
    let child = Command::new(std::env::current_exe().unwrap()).env(CONNECT, name).stdin(Stdio::piped()).spawn().unwrap();
    let (stream, _) = listener.accept().unwrap();
    let mut line = String::new();
    BufReader::new(&stream).read_line(&mut line).unwrap();
    assert_eq!(line, "ready\n");
    (child, stream)
}

/// The pid `SO_PEERCRED` gives for the socket's peer.
fn credentials(stream: &UnixStream) -> u32 {
    // SAFETY: getsockopt into a caller-owned ucred of the length it is given.
    let mut credentials: libc::ucred = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut credentials as *mut libc::ucred).cast(),
            &mut length,
        )
    };
    assert_eq!(rc, 0, "SO_PEERCRED: {}", std::io::Error::last_os_error());
    credentials.pid as u32
}

/// Sets this process's soft descriptor limit; returns the one it replaced.
fn soft_limit(soft: u64) -> u64 {
    // SAFETY: getrlimit and setrlimit with a caller-owned rlimit.
    let mut limit: libc::rlimit = unsafe { std::mem::zeroed() };
    assert_eq!(unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) }, 0);
    let kept = limit.rlim_cur;
    limit.rlim_cur = soft;
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
    kept
}

/// Ends a child's connection and reaps it.
fn ended(mut child: Child) {
    drop(child.stdin.take());
    child.wait().unwrap();
}

fn parent() {
    let name = format!("gpu-queue-peer-identity-{}", std::process::id());
    let listener = UnixListener::bind_addr(&SocketAddr::from_abstract_name(name.as_bytes()).unwrap()).unwrap();

    let (first, stream) = connected(&listener, &name);
    let peer = Peer::of(stream.as_fd());
    let birth = gpu_arbiter::procs::started(first.id()).unwrap();
    assert_eq!(peer.process(), Ok(Process { pid: first.id(), birth }), "(a) the sender, read through its descriptor");
    assert_eq!(credentials(&stream), first.id(), "(a) the descriptor's pid is SO_PEERCRED's");
    println!("(a) a connected child is read as itself");

    ended(first);
    assert_eq!(peer.process(), Err(Unread::Gone), "(b) a reaped sender is gone");
    assert_eq!(Peer::of(stream.as_fd()).process(), Err(Unread::Gone), "(b) and is gone at acquisition");
    println!("(b) a reaped child is gone");

    let (second, other) = connected(&listener, &name);
    let lowest = std::fs::File::open("/dev/null").unwrap().as_raw_fd() as u64;
    let kept = soft_limit(lowest);
    let exhausted = Peer::of(other.as_fd());
    soft_limit(kept);
    let failed = Err(Unread::Failed("SO_PEERPIDFD: Too many open files (os error 24)".into()));
    assert_eq!(exhausted.process(), failed, "(c) an exhausted descriptor table is a failed acquisition");
    assert!(Peer::of(other.as_fd()).process().is_ok(), "(c) with the limit restored, the sender is read");
    println!("(c) descriptor exhaustion is a failed read, not a gone sender");
    ended(second);
}
