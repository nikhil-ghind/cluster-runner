//! Prometheus exporter — exposes queue depth and job counters on a
//! separate HTTP port so the gRPC service doesn't need to multiplex.

use std::convert::Infallible;
use std::net::SocketAddr;

use anyhow::Result;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use prometheus::{Encoder, IntCounter, IntGauge, Registry as PromRegistry, TextEncoder};
use tracing::info;

#[derive(Clone)]
pub struct Registry {
    inner: PromRegistry,
    pub queue_depth: IntGauge,
    pub jobs_submitted: IntCounter,
    pub jobs_succeeded: IntCounter,
    pub jobs_failed: IntCounter,
    pub jobs_canceled: IntCounter,
}

impl Registry {
    pub fn new() -> Self {
        let inner = PromRegistry::new();
        let queue_depth = IntGauge::new("dispatcher_queue_depth", "queued jobs").unwrap();
        let jobs_submitted = IntCounter::new("dispatcher_jobs_submitted_total", "submitted").unwrap();
        let jobs_succeeded = IntCounter::new("dispatcher_jobs_succeeded_total", "succeeded").unwrap();
        let jobs_failed = IntCounter::new("dispatcher_jobs_failed_total", "failed").unwrap();
        let jobs_canceled = IntCounter::new("dispatcher_jobs_canceled_total", "canceled").unwrap();
        for m in [&queue_depth as &dyn prometheus::core::Collector] {
            inner.register(Box::new(m.clone())).ok();
        }
        inner.register(Box::new(queue_depth.clone())).ok();
        inner.register(Box::new(jobs_submitted.clone())).ok();
        inner.register(Box::new(jobs_succeeded.clone())).ok();
        inner.register(Box::new(jobs_failed.clone())).ok();
        inner.register(Box::new(jobs_canceled.clone())).ok();
        Self { inner, queue_depth, jobs_submitted, jobs_succeeded, jobs_failed, jobs_canceled }
    }

    pub async fn spawn_exporter(&self, addr: &str) -> Result<()> {
        let reg = self.inner.clone();
        let addr: SocketAddr = addr.parse()?;
        tokio::spawn(async move {
            let mk = make_service_fn(move |_| {
                let reg = reg.clone();
                async move {
                    Ok::<_, Infallible>(service_fn(move |_req: Request<Body>| {
                        let reg = reg.clone();
                        async move {
                            let metric_families = reg.gather();
                            let encoder = TextEncoder::new();
                            let mut buf = Vec::new();
                            encoder.encode(&metric_families, &mut buf).unwrap();
                            Ok::<_, Infallible>(Response::new(Body::from(buf)))
                        }
                    }))
                }
            });
            info!(%addr, "prometheus exporter listening");
            Server::bind(&addr).serve(mk).await.ok();
        });
        Ok(())
    }
}
