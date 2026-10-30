//! Separate-process handover. The predecessor retains waitpid ownership only.
use super::{Config, Message};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

const HANDOFF_PROTOCOL: u64 = 2;
// Bump when a build must supersede every earlier one. Shells activate their own
// pinned build, so without this order a stale snapshot downgrades the service.
const SERVICE_GENERATION: u64 = 9;

pub fn capabilities() -> Value {
    json!({"handoff_protocol":HANDOFF_PROTOCOL,"host_memory_protocol":1})
}

pub fn service_generation() -> u64 {
    SERVICE_GENERATION
}

async fn query(path: &Path, flag: &str) -> Result<Option<Vec<u8>>, String> {
    let mut command = Command::new(path);
    command
        .arg(flag)
        .stdin(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let output = tokio::time::timeout(std::time::Duration::from_secs(2), command.output())
        .await
        .map_err(|_| "successor capability preflight timed out")?
        .map_err(|e| e.to_string())?;
    Ok(output.status.success().then_some(output.stdout))
}

pub async fn validate_successor(path: &Path) -> Result<(), String> {
    if !path.is_absolute() || !path.is_file() {
        return Err("successor must be an absolute compiler executable".into());
    }
    let advertised = query(path, "--handoff-capabilities")
        .await?
        .ok_or("successor cannot advertise the required native handover capabilities")?;
    let reply: Value = serde_json::from_slice(&advertised).map_err(|e| e.to_string())?;
    if reply != capabilities() {
        return Err("successor cannot retain the current native handover capabilities".into());
    }
    // Builds older than the generation query reject the unknown option.
    let generation = query(path, "--service-generation")
        .await?
        .and_then(|out| String::from_utf8(out).ok()?.trim().parse::<u64>().ok())
        .unwrap_or(0);
    if generation < SERVICE_GENERATION {
        return Err(format!(
            "successor belongs to compiler service generation {generation}; generation {SERVICE_GENERATION} retains authority"
        ));
    }
    Ok(())
}

pub fn same_code(left: &Path, right: &Path) -> Result<bool, String> {
    use std::io::Read;
    let mut left = std::fs::File::open(left).map_err(|e| e.to_string())?;
    let mut right = std::fs::File::open(right).map_err(|e| e.to_string())?;
    if left.metadata().map_err(|e| e.to_string())?.len()
        != right.metadata().map_err(|e| e.to_string())?.len()
    {
        return Ok(false);
    }
    let mut a = [0u8; 65536];
    let mut b = [0u8; 65536];
    loop {
        let count = left.read(&mut a).map_err(|e| e.to_string())?;
        if count == 0 {
            return Ok(true);
        }
        right
            .read_exact(&mut b[..count])
            .map_err(|e| e.to_string())?;
        if a[..count] != b[..count] {
            return Ok(false);
        }
    }
}

fn send_descriptor(socket: i32, descriptor: i32) -> Result<(), String> {
    let mut byte = [1u8];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let mut control = [0usize; 8];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = unsafe { libc::CMSG_SPACE(std::mem::size_of::<i32>() as u32) } as usize;
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&msg);
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(std::mem::size_of::<i32>() as u32) as usize;
        std::ptr::write_unaligned(libc::CMSG_DATA(header).cast::<i32>(), descriptor);
        if libc::sendmsg(socket, &msg, libc::MSG_NOSIGNAL) != 1 {
            return Err(std::io::Error::last_os_error().to_string());
        }
    }
    Ok(())
}
fn receive_descriptor(socket: i32) -> Result<OwnedFd, String> {
    let mut byte = [0u8];
    let mut iov = libc::iovec {
        iov_base: byte.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let mut control = [0usize; 8];
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen = std::mem::size_of_val(&control);
    unsafe {
        if libc::recvmsg(socket, &mut msg, libc::MSG_CMSG_CLOEXEC) != 1
            || msg.msg_flags & libc::MSG_CTRUNC != 0
        {
            return Err("compiler handover descriptor receive failed".into());
        }
        let header = libc::CMSG_FIRSTHDR(&msg);
        if header.is_null()
            || (*header).cmsg_level != libc::SOL_SOCKET
            || (*header).cmsg_type != libc::SCM_RIGHTS
            || (*header).cmsg_len != libc::CMSG_LEN(std::mem::size_of::<i32>() as u32) as usize
        {
            return Err("compiler handover requires one listener descriptor".into());
        }
        Ok(OwnedFd::from_raw_fd(std::ptr::read_unaligned(
            libc::CMSG_DATA(header).cast::<i32>(),
        )))
    }
}
async fn send(stream: &mut UnixStream, value: Value) -> Result<(), String> {
    let mut bytes = serde_json::to_vec(&value).map_err(|e| e.to_string())?;
    bytes.push(b'\n');
    stream.write_all(&bytes).await.map_err(|e| e.to_string())
}
async fn receive(reader: &mut BufReader<UnixStream>) -> Result<Value, String> {
    let mut line = String::new();
    if reader
        .read_line(&mut line)
        .await
        .map_err(|e| e.to_string())?
        == 0
    {
        return Err("compiler handover peer disappeared".into());
    }
    serde_json::from_str(&line).map_err(|e| e.to_string())
}

pub async fn inherited(
    config: &Config,
) -> Result<Option<(UnixListener, UnixStream, BTreeSet<String>)>, String> {
    let Some(raw) = std::env::var_os("COMPILE_QUEUE_HANDOFF_FD") else {
        return Ok(None);
    };
    std::env::remove_var("COMPILE_QUEUE_HANDOFF_FD");
    let fd = raw
        .to_string_lossy()
        .parse::<i32>()
        .map_err(|e| e.to_string())?;
    if fd < 3 {
        return Err("invalid handover channel".into());
    }
    let socket = unsafe { std::os::unix::net::UnixStream::from_raw_fd(fd) };
    // The listener crosses the private channel with SCM_RIGHTS, never via a
    // filesystem alias or a shared lock. CLOEXEC prevents leaks into compilers.
    unsafe {
        libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
    }
    let listener = std::os::unix::net::UnixListener::from(receive_descriptor(socket.as_raw_fd())?);
    use std::os::linux::net::SocketAddrExt;
    if listener
        .local_addr()
        .map_err(|e| e.to_string())?
        .as_abstract_name()
        != super::endpoint(&config.state)?.as_abstract_name()
    {
        return Err("handover listener is for a different compiler state".into());
    }
    listener.set_nonblocking(true).map_err(|e| e.to_string())?;
    socket.set_nonblocking(true).map_err(|e| e.to_string())?;
    let mut reader = BufReader::new(UnixStream::from_std(socket).map_err(|e| e.to_string())?);
    let manifest = receive(&mut reader).await?;
    let ids = serde_json::from_value(manifest["reaping"].clone()).map_err(|e| e.to_string())?;
    Ok(Some((
        UnixListener::from_std(listener).map_err(|e| e.to_string())?,
        reader.into_inner(),
        ids,
    )))
}

pub async fn ready(mut stream: UnixStream, sender: mpsc::Sender<Message>) -> Result<(), String> {
    send(
        &mut stream,
        json!({"ready":std::process::id(),"handoff_protocol":HANDOFF_PROTOCOL,"host_memory_protocol":1}),
    )
    .await?;
    let mut reader = BufReader::new(stream);
    if receive(&mut reader).await?["activate"] != true {
        return Err("handover activation missing".into());
    }
    tokio::spawn(async move {
        while let Ok(value) = receive(&mut reader).await {
            if let (Some(id), Some(code)) = (value["job_id"].as_str(), value["returncode"].as_i64())
            {
                if sender
                    .send(Message::Exit(id.to_owned(), code as i32))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
        let _ = sender.send(Message::ReaperGone).await;
    });
    Ok(())
}

pub async fn successor(
    path: &Path,
    config: &Config,
    listener: &UnixListener,
    ids: &BTreeSet<String>,
) -> Result<(Child, UnixStream), String> {
    if !path.is_absolute() || !path.is_file() {
        return Err("successor must be an absolute compiler executable".into());
    }
    let (parent, child) = std::os::unix::net::UnixStream::pair().map_err(|e| e.to_string())?;
    let fd = child.as_raw_fd();
    let mut command = Command::new(path);
    command
        .args([
            "--state-dir",
            config.state.to_str().ok_or("invalid state path")?,
            "--minimum-ram",
            &config.minimum_ram.to_string(),
            "--average-ram",
            &config.average_ram.to_string(),
        ])
        .env("COMPILE_QUEUE_HANDOFF_FD", fd.to_string())
        .kill_on_drop(false);
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(fd, libc::F_SETFD, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut process = command.spawn().map_err(|e| e.to_string())?;
    drop(child);
    let setup = async {
        send_descriptor(parent.as_raw_fd(), listener.as_raw_fd())?;
        parent.set_nonblocking(true).map_err(|e| e.to_string())?;
        let mut stream = UnixStream::from_std(parent).map_err(|e| e.to_string())?;
        send(&mut stream, json!({"reaping":ids})).await?;
        let mut reader = BufReader::new(stream);
        let reply = receive(&mut reader).await?;
        if reply["ready"].as_u64() != process.id().map(u64::from) {
            return Err("successor identity differs".into());
        }
        if reply["handoff_protocol"].as_u64() != Some(HANDOFF_PROTOCOL) {
            return Err(
                "successor lacks current compiler handover protocol; original authority retained"
                    .into(),
            );
        }
        if reply["host_memory_protocol"] != 1 {
            return Err("successor cannot retain native host memory claims".into());
        }
        Ok(reader.into_inner())
    };
    match tokio::time::timeout(std::time::Duration::from_secs(30), setup).await {
        Ok(Ok(stream)) => Ok((process, stream)),
        result => {
            let _ = process.kill().await;
            Err(match result {
                Ok(Err(e)) => e,
                _ => "successor readiness timed out".into(),
            })
        }
    }
}

pub async fn activate(stream: &mut UnixStream, pid: u32) -> Result<(), String> {
    super::notify(&format!(
        "MAINPID={pid}\nSTATUS=Compiler successor owns scheduling"
    ));
    send(stream, json!({"activate":true})).await
}

pub async fn reap_only(
    mut stream: UnixStream,
    children: Vec<(String, Child)>,
    mut inherited: BTreeSet<String>,
    mut messages: mpsc::Receiver<Message>,
    state: std::path::PathBuf,
) -> Result<(), String> {
    let (exits, mut receive_exits) = mpsc::channel(1024);
    for (id, mut child) in children {
        let tx = exits.clone();
        let state = state.clone();
        let pid = child.id();
        tokio::spawn(async move {
            if let Ok(status) = child.wait().await {
                use std::os::unix::process::ExitStatusExt;
                let code = status
                    .code()
                    .unwrap_or_else(|| -status.signal().unwrap_or(0));
                let result = json!({"job_id":id,"pid":pid,"returncode":code});
                if super::store::write(
                    &state.join("reaper-exits").join(format!("{id}.json")),
                    &result,
                )
                .await
                .is_ok()
                {
                    let _ = tx.send((id, code)).await;
                }
            }
        });
    }
    drop(exits);
    let mut local = true;
    while local || !inherited.is_empty() {
        tokio::select! {
            exit=receive_exits.recv(), if local => match exit {
                Some((id,code))=>{let _=send(&mut stream,json!({"job_id":id,"returncode":code})).await;},
                None=>local=false,
            },
            message=messages.recv()=>match message {
                Some(Message::Exit(id,code))=>{inherited.remove(&id);let _=send(&mut stream,json!({"job_id":id,"returncode":code})).await;},
                Some(Message::ReaperGone)|None=>inherited.clear(),
                _=>{},
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn package_path_changes_do_not_change_identical_executable_content() {
        let root = std::env::temp_dir().join(format!(
            "compiler-code-identity-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir(&root).unwrap();
        let a = root.join("first");
        let b = root.join("second");
        std::fs::write(&a, b"identical compiler image").unwrap();
        std::fs::copy(&a, &b).unwrap();
        assert!(same_code(&a, &b).unwrap());
        std::fs::write(&b, b"different compiler image").unwrap();
        assert!(!same_code(&a, &b).unwrap());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn successors_from_an_older_service_generation_cannot_take_authority() {
        let root = std::env::temp_dir().join(format!(
            "compiler-service-generation-{}-{}",
            std::process::id(),
            crate::protocol::now_unix().to_bits()
        ));
        std::fs::create_dir(&root).unwrap();
        // A child writes each fixture so no descriptor of this process, inherited
        // by a concurrently spawned test command, can leave it busy to execute.
        let successor = |name: &str, generation: &str| {
            let path = root.join(name);
            let script = format!(
                "#!/bin/sh\ncase \"$1\" in\n--handoff-capabilities) echo '{}';;\n{generation}\n*) exit 2;;\nesac\n",
                capabilities()
            );
            let written = std::process::Command::new("/bin/sh")
                .args(["-c", "printf %s \"$1\" > \"$2\" && chmod 755 \"$2\"", "sh"])
                .arg(script)
                .arg(&path)
                .status()
                .unwrap();
            assert!(written.success());
            path
        };
        let reports = |generation: u64| format!("--service-generation) echo {generation};;");
        let current = successor("current", &reports(SERVICE_GENERATION));
        let newer = successor("newer", &reports(SERVICE_GENERATION + 1));
        let older = successor("older", &reports(SERVICE_GENERATION - 1));
        let unversioned = successor("unversioned", "");
        assert_eq!(validate_successor(&current).await, Ok(()));
        assert_eq!(validate_successor(&newer).await, Ok(()));
        assert!(validate_successor(&older)
            .await
            .unwrap_err()
            .contains(&format!("generation {}", SERVICE_GENERATION - 1)));
        assert!(validate_successor(&unversioned)
            .await
            .unwrap_err()
            .contains("generation 0"));
        std::fs::remove_dir_all(root).unwrap();
    }
}
