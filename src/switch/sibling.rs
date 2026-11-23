//! Test-only (R-d; DECISION-W-GQ-9 ruling 2): the line between the test and one child it starts, a sibling process in a
//! tree of its own. The child is owned by `roles::Role`, which kills and reaps it; this module adds only its line, one
//! JSON line per acknowledgement or command, in each direction over a Unix stream the `Sibling` binds. Every wait is
//! an event wait; the run's scope is the hang guard. The test writes each acknowledgement's name to stderr as it
//! arrives, so the last one names the seam of a run that never ended.

use std::ffi::OsStr;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};

use crate::roles::Role;
use crate::switch::control::tests::StateDir;

/// The environment entry through which a child finds its line.
const LINE: &str = "GQ_SIBLING_LINE";

/// A child's end of its line to the test.
#[derive(Clone)]
pub(crate) struct Link(Arc<UnixStream>);

impl Link {
    /// The line to the test, at the path its `Sibling` set in this process's environment, opened by naming `role`.
    pub(crate) fn connect(role: &str) -> (Link, Inbox) {
        let path = std::env::var_os(LINE).expect("a sibling's environment names its line");
        let stream = UnixStream::connect(path).expect("the test's socket");
        let link = Link(Arc::new(stream.try_clone().expect("a second handle")));
        link.send(json!({ "role": role }));
        (link, Inbox(BufReader::new(stream)))
    }

    /// One line, written with blocking `std` I/O: it polls no Tokio resource.
    pub(crate) fn send(&self, value: Value) {
        let mut line = serde_json::to_vec(&value).expect("serializable");
        line.push(b'\n');
        (&*self.0).write_all(&line).expect("the link to the test");
    }
}

/// The lines a child reads from the test.
pub(crate) struct Inbox(BufReader<UnixStream>);

impl Inbox {
    pub(crate) fn next(&mut self) -> Value {
        let mut line = String::new();
        let read = self.0.read_line(&mut line).expect("the link");
        assert!(read > 0, "the link closed");
        serde_json::from_str(&line).expect("a JSON line")
    }
}

/// A child's line as the test reads it: every acknowledgement is kept and its name written to stderr.
struct Peer {
    name: &'static str,
    link: UnixStream,
    inbox: Inbox,
    heard: Vec<Value>,
}

impl Peer {
    /// The child's connection, whose first line names its role.
    fn accept(listener: &UnixListener, name: &'static str) -> Peer {
        let (stream, _) = listener.accept().expect("a role connects");
        let mut inbox = Inbox(BufReader::new(stream.try_clone().unwrap()));
        assert_eq!(inbox.next()["role"], name);
        Peer { name, link: stream, inbox, heard: Vec::new() }
    }

    fn next(&mut self) -> Value {
        let ack = self.inbox.next();
        let _ = writeln!(std::io::stderr(), "sibling {}: acknowledged {}", self.name, ack["ack"]);
        self.heard.push(ack.clone());
        ack
    }

    /// The next acknowledgement named `ack`.
    fn expect(&mut self, ack: &str) -> Value {
        loop {
            let next = self.next();
            if next["ack"] == ack {
                return next;
            }
        }
    }

    fn send(&self, command: Value) {
        let mut line = serde_json::to_vec(&command).unwrap();
        line.push(b'\n');
        (&self.link).write_all(&line).expect("the link to a role");
    }
}

/// One child of the test and its line, in drop order: the child is killed and reaped first, then its line closes,
/// then its directory goes.
pub(crate) struct Sibling {
    role: Role,
    peer: Peer,
    _dir: StateDir,
}

impl Sibling {
    /// The role test `role`, started through `Role::start` with `environment` and its line.
    pub(crate) fn start(role: &'static str, environment: &[(&str, &OsStr)]) -> Sibling {
        Sibling::with_line(role, environment, |environment| Role::start(role, environment))
    }

    /// As `start`, through `Role::logged`, with the role's output in `log`.
    pub(crate) fn logged(role: &'static str, environment: &[(&str, &OsStr)], log: &Path) -> Sibling {
        Sibling::with_line(role, environment, |environment| Role::logged(role, environment, log))
    }

    /// Binds the line in a directory of its own, adds its path to `environment`, starts the child, and accepts the
    /// child's connection.
    fn with_line(role: &'static str, environment: &[(&str, &OsStr)], started: impl FnOnce(&[(&str, &OsStr)]) -> Role) -> Sibling {
        let dir = StateDir::new();
        let line = dir.path().join("line.sock");
        let listener = UnixListener::bind(&line).expect("the sibling's line");
        let mut environment = environment.to_vec();
        environment.push((LINE, line.as_os_str()));
        let role_process = started(&environment);
        let peer = Peer::accept(&listener, role);
        Sibling { role: role_process, peer, _dir: dir }
    }

    /// Writes `line` to the child.
    pub(crate) fn send(&self, line: Value) {
        self.peer.send(line);
    }

    /// The child's next line, with no deadline.
    pub(crate) fn next(&mut self) -> Value {
        self.peer.next()
    }

    /// The child's next acknowledgement named `ack`, with no deadline.
    pub(crate) fn expect(&mut self, ack: &str) -> Value {
        self.peer.expect(ack)
    }

    /// Every line the child sent, in order.
    pub(crate) fn heard(&self) -> &[Value] {
        &self.peer.heard
    }

    /// Takes the lines heard so far.
    pub(crate) fn take_heard(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.peer.heard)
    }

    pub(crate) fn pid(&self) -> u32 {
        self.role.pid()
    }

    pub(crate) fn status(&mut self) -> std::process::ExitStatus {
        self.role.status()
    }

    pub(crate) fn running(&mut self) -> bool {
        self.role.running()
    }

    pub(crate) fn wait(self) -> std::process::ExitStatus {
        self.role.wait()
    }
}
