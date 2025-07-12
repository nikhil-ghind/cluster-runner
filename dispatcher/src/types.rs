//! Shared dispatcher domain types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Phase {
    Pending,
    Scheduled,
    Running,
    Succeeded,
    Failed,
    Canceled,
    TimedOut,
}

impl Phase {
    pub fn terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled | Self::TimedOut)
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Scheduled => "SCHEDULED",
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Canceled => "CANCELED",
            Self::TimedOut => "TIMED_OUT",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub name: String,
    pub image: String,
    pub command: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cpu: f64,
    pub memory_mib: i64,
    pub gpu: i32,
    pub gpu_class: String,
    pub ephemeral_storage_mib: i64,
    pub priority: i32,
    pub retries_remaining: i32,
    pub timeout_secs: i32,
    pub isolation: IsolationProfile,
    pub placement: PlacementHint,
    pub labels: std::collections::BTreeMap<String, String>,
    pub git_sha: String,
    pub artifact_url: String,
    pub created_at: DateTime<Utc>,
    pub phase: Phase,
    pub node: Option<String>,
    pub pod: Option<String>,
    pub exit_code: Option<i32>,
    pub usage: ResourceUsage,
}

impl Job {
    pub fn new_id() -> String {
        uuid::Uuid::new_v4().to_string()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IsolationProfile {
    pub pid_ns: bool,
    pub mount_ns: bool,
    pub net_ns: bool,
    pub user_ns: bool,
    pub ipc_ns: bool,
    pub cgroup_ns: bool,
    pub seccomp_profile: String,
    pub apparmor_profile: String,
    pub readonly_rootfs: bool,
    pub drop_capabilities: bool,
}

impl IsolationProfile {
    pub fn hardened() -> Self {
        Self {
            pid_ns: true,
            mount_ns: true,
            net_ns: true,
            user_ns: true,
            ipc_ns: true,
            cgroup_ns: true,
            seccomp_profile: "runtime/default".into(),
            apparmor_profile: "runtime/default".into(),
            readonly_rootfs: true,
            drop_capabilities: true,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlacementHint {
    pub preferred_cloud: String,
    pub node_selectors: Vec<String>,
    pub priority_class: String,
    pub spot_ok: bool,
    pub zone_spread: i32,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub cpu_seconds: f64,
    pub peak_rss_bytes: i64,
    pub rx_bytes: i64,
    pub tx_bytes: i64,
    pub read_bytes: i64,
    pub write_bytes: i64,
}
