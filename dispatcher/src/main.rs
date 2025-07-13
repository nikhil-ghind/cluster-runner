//! cluster-dispatcher — Rust gRPC job dispatcher for the HPC CI runner.
//!
//! Responsibilities:
//!   * Accept job submissions from the orchestrator (Python service).
//!   * Translate JobSpecs into Kubernetes Pods with proper cgroup/namespace
//!     isolation, GPU resource requests, and node affinity for hybrid cloud.
//!   * Stream job lifecycle events to subscribers (orchestrator + UI).
//!   * Enforce concurrency and admission control via priority-aware queues.

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tonic::transport::Server;
use tracing::info;

mod cgroup;
mod cluster;
mod metrics;
mod queue;
mod scheduler;
mod server;
mod state;
mod types;

pub mod pb {
    tonic::include_proto!("dispatcher.v1");
}

#[derive(Parser, Debug)]
#[command(name = "dispatcher", version)]
struct Args {
    /// Listen address for the gRPC dispatcher service.
    #[arg(long, default_value = "0.0.0.0:7070")]
    listen: String,

    /// Prometheus metrics endpoint.
    #[arg(long, default_value = "0.0.0.0:9095")]
    metrics: String,

    /// Path to kubeconfig (uses in-cluster config if missing).
    #[arg(long)]
    kubeconfig: Option<String>,

    /// Default namespace used for spawned worker pods.
    #[arg(long, default_value = "ci-runner")]
    namespace: String,

    /// Maximum total in-flight jobs across the cluster.
    #[arg(long, default_value_t = 2048)]
    max_concurrency: usize,

    /// Number of scheduler reconcile workers.
    #[arg(long, default_value_t = 8)]
    scheduler_workers: usize,

    /// Path to YAML cluster topology (clouds, zones, capacities).
    #[arg(long, default_value = "/etc/dispatcher/topology.yaml")]
    topology: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let args = Args::parse();
    info!(?args, "dispatcher starting");

    let metrics = metrics::Registry::new();
    let topology = cluster::Topology::load(&args.topology).unwrap_or_default();
    let kube_client = cluster::KubeClient::connect(args.kubeconfig.as_deref(), &args.namespace).await?;

    let state = Arc::new(state::SharedState::new(metrics.clone(), topology));
    let scheduler = scheduler::Scheduler::new(
        state.clone(),
        kube_client.clone(),
        args.max_concurrency,
        args.scheduler_workers,
    );
    scheduler.spawn();

    // Prometheus exporter — runs on its own port.
    metrics.spawn_exporter(&args.metrics).await?;

    // Kubernetes pod watcher — translates pod events into JobEvents.
    cluster::watch_pods(kube_client.clone(), state.clone()).await?;

    let dispatcher = server::DispatcherSvc::new(state.clone());
    let worker_cb = server::WorkerCallbackSvc::new(state.clone());

    info!(addr = %args.listen, "gRPC listening");

    Server::builder()
        .add_service(pb::dispatcher_server::DispatcherServer::new(dispatcher))
        .add_service(pb::worker_callback_server::WorkerCallbackServer::new(worker_cb))
        .serve(args.listen.parse()?)
        .await?;

    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .json()
        .init();
}
