//! gRPC service implementations.

use std::pin::Pin;
use std::sync::Arc;

use chrono::Utc;
use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

use crate::pb;
use crate::pb::dispatcher_server::Dispatcher;
use crate::pb::worker_callback_server::WorkerCallback;
use crate::state::SharedState;
use crate::types::{IsolationProfile, Job, Phase, PlacementHint, ResourceUsage};

pub struct DispatcherSvc {
    state: Arc<SharedState>,
}

impl DispatcherSvc {
    pub fn new(state: Arc<SharedState>) -> Self { Self { state } }
}

#[tonic::async_trait]
impl Dispatcher for DispatcherSvc {
    async fn submit_job(&self, req: Request<pb::JobSpec>) -> Result<Response<pb::JobHandle>, Status> {
        let job = into_job(req.into_inner())?;
        let id = self.state.submit(job);
        Ok(Response::new(pb::JobHandle { job_id: id, queue: "default".into() }))
    }

    async fn submit_matrix(&self, req: Request<pb::MatrixSpec>) -> Result<Response<pb::MatrixHandle>, Status> {
        let spec = req.into_inner();
        let base = spec.base.ok_or_else(|| Status::invalid_argument("base required"))?;
        let combos = expand_axes(&spec.axes);
        if let (true, n) = (spec.max_parallel > 0, spec.max_parallel) {
            info!(matrix=%spec.name, jobs=combos.len(), max_parallel=n, "expanded matrix");
        }
        let mut ids = Vec::with_capacity(combos.len());
        for combo in combos {
            let mut job = into_job(base.clone())?;
            job.name = format!("{}::{}", spec.name, combo.iter().map(|(k,v)| format!("{k}={v}")).collect::<Vec<_>>().join(","));
            for (k, v) in &combo {
                job.env.push((format!("MATRIX_{}", k.to_uppercase()), v.clone()));
                job.labels.insert(k.clone(), v.clone());
            }
            let id = self.state.submit(job);
            ids.push(id);
        }
        Ok(Response::new(pb::MatrixHandle {
            matrix_id: uuid::Uuid::new_v4().to_string(),
            job_ids: ids,
        }))
    }

    type StreamEventsStream = Pin<Box<dyn Stream<Item = Result<pb::JobEvent, Status>> + Send>>;

    async fn stream_events(&self, req: Request<pb::WatchRequest>) -> Result<Response<Self::StreamEventsStream>, Status> {
        let filter = req.into_inner().filter_label;
        let mut rx = self.state.events.subscribe();
        let (out, out_rx) = mpsc::channel::<Result<pb::JobEvent, Status>>(256);
        let st = self.state.clone();
        tokio::spawn(async move {
            while let Ok(evt) = rx.recv().await {
                if !filter.is_empty() {
                    if let Some(j) = st.jobs.get(&evt.job_id) {
                        if !j.labels.values().any(|v| v == &filter) { continue; }
                    }
                }
                let usage = st.jobs.get(&evt.job_id).map(|j| j.usage.clone()).unwrap_or_default();
                let _ = out.send(Ok(pb::JobEvent {
                    job_id: evt.job_id,
                    phase: evt.phase.as_str().into(),
                    exit_code: evt.exit_code,
                    node: evt.node,
                    pod: evt.pod,
                    ts_unix_ms: evt.ts_ms,
                    message: evt.message,
                    usage: Some(pb::ResourceUsage {
                        cpu_seconds: usage.cpu_seconds,
                        peak_rss_bytes: usage.peak_rss_bytes,
                        rx_bytes: usage.rx_bytes,
                        tx_bytes: usage.tx_bytes,
                        read_bytes: usage.read_bytes,
                        write_bytes: usage.write_bytes,
                    }),
                })).await;
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(out_rx))))
    }

    async fn cancel_job(&self, req: Request<pb::JobId>) -> Result<Response<pb::Ack>, Status> {
        let id = req.into_inner().id;
        if let Some(mut j) = self.state.jobs.get_mut(&id) {
            j.phase = Phase::Canceled;
        }
        self.state.emit(&id, Phase::Canceled, -1, "", "", "canceled by user");
        Ok(Response::new(pb::Ack { ok: true, message: "canceled".into() }))
    }

    async fn snapshot(&self, _: Request<pb::SnapshotRequest>) -> Result<Response<pb::ClusterSnapshot>, Status> {
        let total = self.state.jobs.len() as i32;
        let mut running = 0; let mut pending = 0;
        for kv in self.state.jobs.iter() {
            match kv.phase {
                Phase::Running => running += 1,
                Phase::Pending | Phase::Scheduled => pending += 1,
                _ => {}
            }
        }
        Ok(Response::new(pb::ClusterSnapshot {
            total_jobs: total,
            running_jobs: running,
            pending_jobs: pending,
            nodes: vec![],
            ts_unix_ms: Utc::now().timestamp_millis(),
        }))
    }
}

pub struct WorkerCallbackSvc { state: Arc<SharedState> }

impl WorkerCallbackSvc {
    pub fn new(state: Arc<SharedState>) -> Self { Self { state } }
}

#[tonic::async_trait]
impl WorkerCallback for WorkerCallbackSvc {
    async fn report(&self, req: Request<Streaming<pb::WorkerStatus>>) -> Result<Response<pb::Ack>, Status> {
        let mut s = req.into_inner();
        while let Some(status) = s.message().await? {
            let phase = match status.phase.as_str() {
                "RUNNING" => Phase::Running,
                "SUCCEEDED" => Phase::Succeeded,
                "FAILED" => Phase::Failed,
                "TIMED_OUT" => Phase::TimedOut,
                _ => continue,
            };
            self.state.mark(&status.job_id, phase, Some(status.exit_code), None, None, "worker report");
            if let Some(mut j) = self.state.jobs.get_mut(&status.job_id) {
                if let Some(u) = status.usage {
                    j.usage = ResourceUsage {
                        cpu_seconds: u.cpu_seconds,
                        peak_rss_bytes: u.peak_rss_bytes,
                        rx_bytes: u.rx_bytes,
                        tx_bytes: u.tx_bytes,
                        read_bytes: u.read_bytes,
                        write_bytes: u.write_bytes,
                    };
                }
            }
        }
        Ok(Response::new(pb::Ack { ok: true, message: "ok".into() }))
    }
}

fn into_job(s: pb::JobSpec) -> Result<Job, Status> {
    let res = s.resources.unwrap_or_default();
    let iso = s.isolation.unwrap_or_default();
    let pl  = s.placement.unwrap_or_default();
    let mut labels = std::collections::BTreeMap::new();
    for (k,v) in s.labels { labels.insert(k, v); }
    let priority = match pl.priority_class.as_str() {
        "critical" => 4, "high" => 3, "normal" => 2, "low" => 1, _ => 2,
    };
    if res.cpu <= 0.0 || res.memory_mib <= 0 {
        warn!("job {} has zero resource request; using defaults", s.name);
    }
    Ok(Job {
        id: Job::new_id(),
        name: s.name,
        image: s.image,
        command: s.command,
        env: s.env.into_iter().map(|e| (e.name, e.value)).collect(),
        cpu: if res.cpu > 0.0 { res.cpu } else { 1.0 },
        memory_mib: if res.memory_mib > 0 { res.memory_mib } else { 512 },
        gpu: res.gpu,
        gpu_class: res.gpu_class,
        ephemeral_storage_mib: res.ephemeral_storage_mib,
        priority,
        retries_remaining: s.retries.max(0),
        timeout_secs: if s.timeout_secs > 0 { s.timeout_secs } else { 3600 },
        isolation: IsolationProfile {
            pid_ns: iso.pid_namespace,
            mount_ns: iso.mount_namespace,
            net_ns:  iso.net_namespace,
            user_ns: iso.user_namespace,
            ipc_ns:  iso.ipc_namespace,
            cgroup_ns: iso.cgroup_namespace,
            seccomp_profile: iso.seccomp_profile,
            apparmor_profile: iso.apparmor_profile,
            readonly_rootfs: iso.readonly_rootfs,
            drop_capabilities: iso.drop_capabilities,
        },
        placement: PlacementHint {
            preferred_cloud: pl.preferred_cloud,
            node_selectors: pl.node_selectors,
            priority_class: pl.priority_class,
            spot_ok: pl.spot_ok,
            zone_spread: pl.zone_spread,
        },
        labels,
        git_sha: s.git_sha,
        artifact_url: s.artifact_url,
        created_at: chrono::Utc::now(),
        phase: Phase::Pending,
        node: None,
        pod: None,
        exit_code: None,
        usage: ResourceUsage::default(),
    })
}

fn expand_axes(axes: &[pb::MatrixAxis]) -> Vec<Vec<(String, String)>> {
    let mut out: Vec<Vec<(String, String)>> = vec![vec![]];
    for axis in axes {
        let mut next = Vec::with_capacity(out.len() * axis.values.len());
        for prefix in &out {
            for v in &axis.values {
                let mut p = prefix.clone();
                p.push((axis.name.clone(), v.clone()));
                next.push(p);
            }
        }
        out = next;
    }
    out
}
