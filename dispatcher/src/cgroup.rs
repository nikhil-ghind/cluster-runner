//! cgroup v2 + namespace helpers used by the in-pod worker. The worker
//! binary embeds this module to apply hard resource caps before exec'ing
//! the user command. Kept here in the dispatcher crate so the schemas
//! and accounting live next to the scheduler logic.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use nix::sched::{unshare, CloneFlags};
use nix::sys::stat::Mode;
use nix::unistd::{getpid, mkdir};

/// Apply cgroup v2 caps for the calling process by creating
/// /sys/fs/cgroup/ci-runner/<job_id> and writing cpu.max / memory.max / io.max.
pub fn apply_cgroup(job_id: &str, cpu_quota_us: u64, cpu_period_us: u64,
                    mem_bytes: u64, io_bandwidth_mbps: u64) -> Result<PathBuf> {
    let root = Path::new("/sys/fs/cgroup/ci-runner");
    if !root.exists() {
        mkdir(root, Mode::S_IRWXU | Mode::S_IRGRP | Mode::S_IXGRP)
            .with_context(|| "create ci-runner cgroup root")?;
        // Enable required controllers on the parent so children inherit them.
        let _ = fs::write(root.join("cgroup.subtree_control"), "+cpu +memory +io +pids");
    }
    let dir = root.join(job_id);
    if !dir.exists() { fs::create_dir_all(&dir)?; }

    fs::write(dir.join("cpu.max"), format!("{cpu_quota_us} {cpu_period_us}"))
        .context("write cpu.max")?;
    fs::write(dir.join("memory.max"), mem_bytes.to_string())
        .context("write memory.max")?;
    if io_bandwidth_mbps > 0 {
        let bps = io_bandwidth_mbps * 1024 * 1024;
        // 8:0 is sda by default; in a real deploy this would be discovered.
        let _ = fs::write(dir.join("io.max"), format!("8:0 rbps={bps} wbps={bps}"));
    }
    // Hard-cap pids to limit fork bombs.
    let _ = fs::write(dir.join("pids.max"), "4096");

    // Move the current process into the cgroup.
    fs::write(dir.join("cgroup.procs"), getpid().as_raw().to_string())
        .context("attach to cgroup")?;
    Ok(dir)
}

/// Unshare the requested namespaces. Must be called *before* exec.
pub fn unshare_namespaces(pid: bool, mnt: bool, net: bool, user: bool, ipc: bool, cgroup: bool) -> Result<()> {
    let mut flags = CloneFlags::empty();
    if pid    { flags |= CloneFlags::CLONE_NEWPID; }
    if mnt    { flags |= CloneFlags::CLONE_NEWNS; }
    if net    { flags |= CloneFlags::CLONE_NEWNET; }
    if user   { flags |= CloneFlags::CLONE_NEWUSER; }
    if ipc    { flags |= CloneFlags::CLONE_NEWIPC; }
    if cgroup { flags |= CloneFlags::CLONE_NEWCGROUP; }
    if !flags.is_empty() {
        unshare(flags).context("unshare(2) namespaces")?;
    }
    Ok(())
}

/// Read peak memory usage from memory.peak (cgroup v2).
pub fn read_peak_rss(dir: &Path) -> i64 {
    fs::read_to_string(dir.join("memory.peak"))
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .unwrap_or(0)
}

/// Read total cpu time from cpu.stat.
pub fn read_cpu_seconds(dir: &Path) -> f64 {
    let Ok(stat) = fs::read_to_string(dir.join("cpu.stat")) else { return 0.0; };
    for line in stat.lines() {
        if let Some(rest) = line.strip_prefix("usage_usec ") {
            if let Ok(us) = rest.trim().parse::<u64>() {
                return us as f64 / 1_000_000.0;
            }
        }
    }
    0.0
}
