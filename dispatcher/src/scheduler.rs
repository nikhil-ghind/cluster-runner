//! The scheduler pulls jobs from the priority queue and creates Pods.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tracing::{error, info, warn};

use crate::cluster::KubeClient;
use crate::state::SharedState;
use crate::types::Phase;

pub struct Scheduler {
    state: Arc<SharedState>,
    kube: KubeClient,
    concurrency: Arc<Semaphore>,
    workers: usize,
}

impl Scheduler {
    pub fn new(state: Arc<SharedState>, kube: KubeClient, max: usize, workers: usize) -> Self {
        Self { state, kube, concurrency: Arc::new(Semaphore::new(max)), workers }
    }

    pub fn spawn(self) {
        for w in 0..self.workers {
            let state = self.state.clone();
            let kube  = self.kube.clone();
            let sem   = self.concurrency.clone();
            tokio::spawn(async move { worker(w, state, kube, sem).await });
        }
    }
}

async fn worker(idx: usize, state: Arc<SharedState>, kube: KubeClient, sem: Arc<Semaphore>) {
    info!(worker=idx, "scheduler worker online");
    loop {
        let Some(job) = state.queue.dequeue() else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        state.metrics.queue_depth.dec();

        let permit = match sem.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => { warn!("semaphore closed"); return; }
        };

        let pod = kube.pod_for(&job);
        match kube.submit_pod(&pod).await {
            Ok(()) => {
                state.mark(&job.id, Phase::Scheduled, None, None,
                           pod.metadata.name.as_deref(), "pod created");
                info!(worker=idx, job=%job.id, "scheduled");
                // Permit is released when the pod-watch loop marks the job terminal.
                spawn_releaser(state.clone(), job.id.clone(), permit);
            }
            Err(e) => {
                error!(worker=idx, job=%job.id, error=?e, "pod submit failed");
                if job.retries_remaining > 0 {
                    let mut requeued = job.clone();
                    requeued.retries_remaining -= 1;
                    state.queue.enqueue(requeued);
                    state.metrics.queue_depth.inc();
                } else {
                    state.mark(&job.id, Phase::Failed, Some(127), None, None, "pod submit failed");
                }
            }
        }
    }
}

fn spawn_releaser(state: Arc<SharedState>, id: String, permit: tokio::sync::OwnedSemaphorePermit) {
    tokio::spawn(async move {
        let mut sub = state.events.subscribe();
        while let Ok(evt) = sub.recv().await {
            if evt.job_id == id && evt.phase.terminal() {
                drop(permit);
                return;
            }
        }
    });
}
