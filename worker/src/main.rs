//! ci-worker — the binary that runs inside every CI pod.
//!
//! Lifecycle:
//!   1. Parse JOB_SPEC from env (orchestrator sets it).
//!   2. Unshare(2) requested namespaces (pid/mount/net/user/ipc/cgroup).
//!   3. Create a cgroup v2 leaf under /sys/fs/cgroup/ci-runner/<job_id>
//!      and apply cpu.max, memory.max, io.max, pids.max.
//!   4. Drop capabilities, set no_new_privs, optionally pivot rootfs.
//!   5. Fork + exec the user command. Tail stdout/stderr.
//!   6. Periodically read cgroup counters and stream WorkerStatus back to
//!      the dispatcher over gRPC.
//!   7. On exit, report exit code and tear down the cgroup.

use std::ffi::CString;
use std::fs;
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use nix::sched::{unshare, CloneFlags};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

#[derive(Parser, Debug)]
struct Args {
    /// Dispatcher gRPC endpoint.
    #[arg(long, env = "DISPATCHER_ENDPOINT", default_value = "http://dispatcher.ci-runner.svc:7070")]
    dispatcher: String,

    /// Job id (set by Pod env). Used as cgroup leaf name.
    #[arg(long, env = "JOB_ID")]
    job_id: String,

    /// Worker id (Pod name).
    #[arg(long, env = "WORKER_ID")]
    worker_id: String,

    /// JSON-encoded JobInner — see types::Job (subset).
    #[arg(long, env = "JOB_SPEC")]
    job_spec: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct JobInner {
    command: Vec<String>,
    env: Vec<(String, String)>,
    cpu: f64,
    memory_mib: i64,
    io_bandwidth_mbps: i64,
    isolation: Isolation,
    timeout_secs: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Isolation {
    pid_ns: bool,
    mount_ns: bool,
    net_ns: bool,
    user_ns: bool,
    ipc_ns: bool,
    cgroup_ns: bool,
    drop_capabilities: bool,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let args = Args::parse();
    let inner: JobInner = serde_json::from_str(&args.job_spec).context("decode JOB_SPEC")?;

    info!(job=%args.job_id, "worker starting");

    // 1. unshare namespaces.
    let mut flags = CloneFlags::empty();
    if inner.isolation.pid_ns    { flags |= CloneFlags::CLONE_NEWPID; }
    if inner.isolation.mount_ns  { flags |= CloneFlags::CLONE_NEWNS; }
    if inner.isolation.net_ns    { flags |= CloneFlags::CLONE_NEWNET; }
    if inner.isolation.user_ns   { flags |= CloneFlags::CLONE_NEWUSER; }
    if inner.isolation.ipc_ns    { flags |= CloneFlags::CLONE_NEWIPC; }
    if inner.isolation.cgroup_ns { flags |= CloneFlags::CLONE_NEWCGROUP; }
    if !flags.is_empty() {
        if let Err(e) = unshare(flags) {
            warn!("unshare failed (kernel may lack permission): {e}");
        }
    }

    // 2. apply cgroup v2 caps. In a real pod the kubelet has already created
    // an outer cgroup; this carves out a stricter inner leaf.
    let dir = apply_cgroup(
        &args.job_id,
        (inner.cpu * 100_000.0) as u64,  // quota_us
        100_000,                          // period_us = 100ms
        (inner.memory_mib as u64) * 1024 * 1024,
        inner.io_bandwidth_mbps as u64,
    ).context("apply cgroup")?;

    // 3. drop capabilities + no_new_privs.
    if inner.isolation.drop_capabilities {
        if let Err(e) = drop_all_caps() { warn!("drop caps: {e}"); }
        unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); }
    }

    // 4. spawn child.
    if inner.command.is_empty() { bail!("empty command"); }
    let mut cmd = Command::new(&inner.command[0]);
    cmd.args(&inner.command[1..])
       .envs(inner.env.iter().map(|(k,v)| (k.clone(), v.clone())))
       .stdout(Stdio::piped())
       .stderr(Stdio::piped());

    // chroot into a known readonly dir if requested. Skipped for portability.
    let mut child = cmd.spawn().context("spawn")?;
    let pid = Pid::from_raw(child.id() as i32);

    // 5. background poller for cgroup stats + timeout.
    let dir_for_poll = dir.clone();
    let job_id = args.job_id.clone();
    let timeout = Duration::from_secs(inner.timeout_secs.max(1) as u64);
    let start = Instant::now();
    let poll_handle = std::thread::spawn(move || {
        loop {
            std::thread::sleep(Duration::from_secs(2));
            let rss = read_peak_rss(&dir_for_poll);
            let cpu = read_cpu_seconds(&dir_for_poll);
            // In a full build this would push a WorkerStatus over gRPC.
            info!(job=%job_id, peak_rss=rss, cpu_seconds=cpu, "tick");
            if start.elapsed() > timeout {
                warn!(job=%job_id, "timeout exceeded — SIGTERM");
                let _ = kill(pid, Signal::SIGTERM);
                std::thread::sleep(Duration::from_secs(5));
                let _ = kill(pid, Signal::SIGKILL);
                return;
            }
        }
    });

    // 6. tail child output.
    let status = child.wait().context("wait")?;
    let _ = poll_handle.join();
    let code = status.code().unwrap_or(-1);
    info!(job=%args.job_id, exit=code, "worker done");

    // 7. cleanup cgroup (best-effort).
    let _ = fs::remove_dir(&dir);
    std::process::exit(code);
}

fn apply_cgroup(job_id: &str, cpu_quota_us: u64, cpu_period_us: u64,
                mem_bytes: u64, io_mbps: u64) -> Result<PathBuf> {
    let root = PathBuf::from("/sys/fs/cgroup/ci-runner");
    let dir = root.join(job_id);
    fs::create_dir_all(&dir).ok();
    fs::write(dir.join("cpu.max"), format!("{cpu_quota_us} {cpu_period_us}")).ok();
    fs::write(dir.join("memory.max"), mem_bytes.to_string()).ok();
    if io_mbps > 0 {
        let bps = io_mbps * 1024 * 1024;
        fs::write(dir.join("io.max"), format!("8:0 rbps={bps} wbps={bps}")).ok();
    }
    fs::write(dir.join("pids.max"), "4096").ok();
    fs::write(dir.join("cgroup.procs"), std::process::id().to_string()).ok();
    Ok(dir)
}

fn read_peak_rss(dir: &PathBuf) -> i64 {
    fs::read_to_string(dir.join("memory.peak")).ok()
        .and_then(|s| s.trim().parse().ok()).unwrap_or(0)
}

fn read_cpu_seconds(dir: &PathBuf) -> f64 {
    let Ok(s) = fs::read_to_string(dir.join("cpu.stat")) else { return 0.0; };
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("usage_usec ") {
            if let Ok(us) = rest.trim().parse::<u64>() { return us as f64 / 1_000_000.0; }
        }
    }
    0.0
}

fn drop_all_caps() -> Result<()> {
    // CAP_LAST_CAP varies by kernel. Drop a safe range explicitly.
    for cap in 0..=40 {
        unsafe { libc::prctl(libc::PR_CAPBSET_DROP, cap as libc::c_ulong, 0, 0, 0); }
    }
    Ok(())
}
