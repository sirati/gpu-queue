//! Native host-memory claims. No Python admission state or process wrappers.
use super::client::Client;
use serde_json::{json, Value};
use std::io::{BufRead, Write};
use std::os::fd::AsRawFd;
use std::time::Duration;

pub struct HostMemoryClaim {
    client: Client,
    id: String,
    bytes: u64,
    label: String,
    owner: u32,
    attempted: bool,
    checked: bool,
    granted: bool,
    released: bool,
}
impl HostMemoryClaim {
    pub fn new(client: Client, bytes: u64, label: String) -> Result<Self, String> {
        if bytes == 0 {
            return Err("host memory claim must be positive bytes".into());
        }
        if std::env::var_os("COMPILE_QUEUE_JOB").is_some() {
            return Err(
                "compilations cannot acquire preparation RAM or wait for another admission".into(),
            );
        }
        let id = std::fs::read_to_string("/proc/sys/kernel/random/uuid")
            .map_err(|e| e.to_string())?
            .trim()
            .to_owned();
        Ok(Self {
            client,
            id,
            bytes,
            label: label.chars().take(200).collect(),
            owner: std::process::id(),
            attempted: false,
            checked: false,
            granted: false,
            released: false,
        })
    }
    pub fn memory_bytes(&self) -> u64 {
        self.bytes
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    fn check(&self) -> Result<(), String> {
        if self.owner != std::process::id() {
            return Err("only the original process may operate its host memory claim".into());
        }
        if self.released {
            return Err("host memory claim was already released".into());
        }
        Ok(())
    }
    /// Returns once the daemon grants the claim. Call before allocating
    /// preparation inputs; the daemon answers, the claim never polls.
    pub async fn acquire(&mut self) -> Result<(), String> {
        self.check()?;
        if !self.checked {
            let health = self.client.health().await?;
            if health["host_memory"]["protocol"] != 1 {
                return Err(
                    "native daemon does not support host memory admission; use the updated service"
                        .into(),
                );
            }
            self.checked = true;
        }
        self.attempted = true;
        let status = self.client.wait(json!({"op":"host_memory_wait","claim_id":self.id,"memory_bytes":self.bytes,"label":self.label})).await?;
        if status["state"] != "granted" {
            return Err(status["error"]
                .as_str()
                .unwrap_or("host memory admission failed")
                .into());
        }
        self.granted = true;
        Ok(())
    }
    pub async fn track_worker(&self, pid: u32) -> Result<(), String> {
        self.check()?;
        if !self.granted {
            return Err("acquire host memory before registering workers".into());
        }
        self.client
            .request(json!({"op":"host_memory_track","claim_id":self.id,"worker_pid":pid}))
            .await?;
        Ok(())
    }
    pub async fn release(&mut self) -> Result<(), String> {
        if self.released {
            return Ok(());
        }
        self.check()?;
        if self.attempted {
            self.client
                .request(json!({"op":"host_memory_release","claim_id":self.id}))
                .await?;
        }
        self.released = true;
        Ok(())
    }
    // Drop cannot borrow a caller's Tokio runtime. This bounded native RPC marks
    // a dropped claim retiring; the actor still waits for registered workers.
    fn abandon(&self) -> Result<(), String> {
        let mut stream =
            std::os::unix::net::UnixStream::connect_addr(&super::endpoint(self.client.state())?)
                .map_err(|e| e.to_string())?;
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .map_err(|e| e.to_string())?;
        stream
            .set_write_timeout(Some(Duration::from_secs(2)))
            .map_err(|e| e.to_string())?;
        let mut peer: libc::ucred = unsafe { std::mem::zeroed() };
        let mut size = std::mem::size_of_val(&peer) as libc::socklen_t;
        if unsafe {
            libc::getsockopt(
                stream.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                (&mut peer as *mut libc::ucred).cast(),
                &mut size,
            )
        } != 0
            || peer.uid != unsafe { libc::getuid() }
        {
            return Err("host memory endpoint belongs to another user".into());
        }
        let mut bytes = serde_json::to_vec(
            &json!({"protocol":1,"op":"host_memory_abandon","claim_id":self.id}),
        )
        .map_err(|e| e.to_string())?;
        bytes.push(b'\n');
        stream.write_all(&bytes).map_err(|e| e.to_string())?;
        let mut line = String::new();
        std::io::BufReader::new(stream)
            .read_line(&mut line)
            .map_err(|e| e.to_string())?;
        let reply: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
        if reply["ok"] != true {
            return Err(reply["error"]
                .as_str()
                .unwrap_or("host memory cleanup failed")
                .into());
        }
        Ok(())
    }
}
impl Drop for HostMemoryClaim {
    fn drop(&mut self) {
        if self.owner == std::process::id() && self.attempted && !self.released {
            if let Err(error) = self.abandon() {
                eprintln!(
                    "host memory claim {} remains bound to its process lifetime: {error}",
                    self.id
                );
            }
        }
    }
}
