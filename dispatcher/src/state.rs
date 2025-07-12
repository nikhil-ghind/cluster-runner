//! Shared in-memory dispatcher state.

use std::sync::Arc;

use chrono::Utc;
use dashmap::DashMap;
use tokio::sync::broadcast;

use crate::cluster::Topology;
use crate::metrics::Registry;
use crate::queue::PriorityQueue;
use crate::types::{Job, Phase};

pub struct SharedState {
    pub jobs: DashMap<String, Job>,
    pub queue: PriorityQueue,
    pub events: broadcast::Sender<JobEventOwned>,
    pub metrics: Registry,
    pub topology: Topology,
}

#[derive(Debug, Clone)]
pub struct JobEventOwned {
    pub job_id: String,
    pub phase: Phase,
    pub exit_code: i32,
    pub node: String,
    pub pod: String,
    pub ts_ms: i64,
    pub message: String,
}

impl SharedState {
    pub fn new(metrics: Registry, topology: Topology) -> Self {
        let (tx, _) = broadcast::channel(8192);
        Self {
            jobs: DashMap::new(),
            queue: PriorityQueue::new(),
            events: tx,
            metrics,
            topology,
        }
    }

    pub fn submit(self: &Arc<Self>, mut job: Job) -> String {
        let id = job.id.clone();
        job.created_at = Utc::now();
        job.phase = Phase::Pending;
        self.metrics.jobs_submitted.inc();
        self.metrics.queue_depth.inc();
        self.queue.enqueue(job.clone());
        self.jobs.insert(id.clone(), job);
        self.emit(&id, Phase::Pending, 0, "", "", "queued");
        id
    }

    pub fn emit(&self, id: &str, phase: Phase, exit: i32, node: &str, pod: &str, msg: &str) {
        let evt = JobEventOwned {
            job_id: id.into(),
            phase,
            exit_code: exit,
            node: node.into(),
            pod: pod.into(),
            ts_ms: Utc::now().timestamp_millis(),
            message: msg.into(),
        };
        let _ = self.events.send(evt);
    }

    pub fn mark(&self, id: &str, phase: Phase, exit: Option<i32>, node: Option<&str>, pod: Option<&str>, msg: &str) {
        if let Some(mut j) = self.jobs.get_mut(id) {
            j.phase = phase;
            if let Some(c) = exit { j.exit_code = Some(c); }
            if let Some(n) = node { j.node = Some(n.into()); }
            if let Some(p) = pod  { j.pod  = Some(p.into()); }
            match phase {
                Phase::Succeeded => self.metrics.jobs_succeeded.inc(),
                Phase::Failed | Phase::TimedOut => self.metrics.jobs_failed.inc(),
                Phase::Canceled => self.metrics.jobs_canceled.inc(),
                _ => {}
            }
            self.emit(id, phase, exit.unwrap_or(-1), node.unwrap_or(""), pod.unwrap_or(""), msg);
        }
    }
}
