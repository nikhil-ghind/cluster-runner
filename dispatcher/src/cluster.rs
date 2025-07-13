//! Kubernetes integration: client, topology, and pod-watch translation.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use futures::StreamExt;
use k8s_openapi::api::core::v1::{
    Capabilities, Container, EnvVar, Pod, PodSecurityContext, PodSpec, ResourceRequirements,
    SecurityContext, Toleration,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, DeleteParams, PostParams};
use kube::runtime::watcher;
use kube::Client;
use serde::Deserialize;
use tracing::{info, warn};

use crate::state::SharedState;
use crate::types::{Job, Phase};

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Topology {
    pub clouds: Vec<CloudTopology>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct CloudTopology {
    pub name: String,        // "onprem"|"aws"|"gcp"|"azure"
    pub zones: Vec<String>,
    pub node_selector: BTreeMap<String, String>,
    pub spot_supported: bool,
    pub gpu_classes: Vec<String>,
}

impl Topology {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("read topology {path}"))?;
        Ok(serde_yaml::from_str(&raw)?)
    }
}

#[derive(Clone)]
pub struct KubeClient {
    pub client: Client,
    pub namespace: String,
}

impl KubeClient {
    pub async fn connect(_kubeconfig: Option<&str>, namespace: &str) -> Result<Self> {
        // kube-rs picks up in-cluster or $KUBECONFIG automatically.
        let client = Client::try_default().await
            .context("connect to kube apiserver")?;
        Ok(Self { client, namespace: namespace.into() })
    }

    pub fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    /// Translate an internal Job into a Pod spec with hardened security.
    pub fn pod_for(&self, job: &Job) -> Pod {
        let mut requests = BTreeMap::new();
        let mut limits = BTreeMap::new();
        requests.insert("cpu".into(), Quantity(format!("{}", job.cpu)));
        requests.insert("memory".into(), Quantity(format!("{}Mi", job.memory_mib)));
        if job.ephemeral_storage_mib > 0 {
            requests.insert("ephemeral-storage".into(), Quantity(format!("{}Mi", job.ephemeral_storage_mib)));
        }
        if job.gpu > 0 {
            limits.insert("nvidia.com/gpu".into(), Quantity(job.gpu.to_string()));
        }
        limits.extend(requests.clone());

        let env: Vec<EnvVar> = job.env.iter().map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            value_from: None,
        }).collect();

        let mut labels = BTreeMap::new();
        labels.insert("ci-runner/job-id".into(), job.id.clone());
        labels.insert("ci-runner/git-sha".into(), job.git_sha.clone());
        for (k, v) in &job.labels { labels.insert(format!("ci-runner/{k}"), v.clone()); }

        let security_context = SecurityContext {
            allow_privilege_escalation: Some(false),
            read_only_root_filesystem: Some(job.isolation.readonly_rootfs),
            run_as_non_root: Some(true),
            capabilities: if job.isolation.drop_capabilities {
                Some(Capabilities { add: None, drop: Some(vec!["ALL".into()]) })
            } else { None },
            ..Default::default()
        };

        let mut node_selector = BTreeMap::new();
        if !job.placement.preferred_cloud.is_empty() {
            node_selector.insert("ci-runner/cloud".into(), job.placement.preferred_cloud.clone());
        }
        for sel in &job.placement.node_selectors {
            if let Some((k, v)) = sel.split_once('=') {
                node_selector.insert(k.into(), v.into());
            }
        }

        let tolerations = if job.placement.spot_ok {
            Some(vec![Toleration {
                key: Some("cloud.google.com/gke-spot".into()),
                operator: Some("Equal".into()),
                value: Some("true".into()),
                effect: Some("NoSchedule".into()),
                toleration_seconds: None,
            }])
        } else { None };

        Pod {
            metadata: ObjectMeta {
                name: Some(format!("job-{}", &job.id[..8])),
                namespace: Some(self.namespace.clone()),
                labels: Some(labels),
                ..Default::default()
            },
            spec: Some(PodSpec {
                restart_policy: Some("Never".into()),
                automount_service_account_token: Some(false),
                node_selector: if node_selector.is_empty() { None } else { Some(node_selector) },
                tolerations,
                priority_class_name: if job.placement.priority_class.is_empty() { None }
                    else { Some(job.placement.priority_class.clone()) },
                active_deadline_seconds: Some(job.timeout_secs as i64),
                security_context: Some(PodSecurityContext {
                    run_as_non_root: Some(true),
                    fs_group: Some(2000),
                    ..Default::default()
                }),
                containers: vec![Container {
                    name: "runner".into(),
                    image: Some(job.image.clone()),
                    command: if job.command.is_empty() { None } else { Some(job.command.clone()) },
                    env: Some(env),
                    resources: Some(ResourceRequirements {
                        requests: Some(requests),
                        limits: Some(limits),
                        claims: None,
                    }),
                    security_context: Some(security_context),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    pub async fn submit_pod(&self, pod: &Pod) -> Result<()> {
        let api = self.pods();
        api.create(&PostParams::default(), pod).await?;
        Ok(())
    }

    pub async fn delete_pod(&self, name: &str) -> Result<()> {
        let api = self.pods();
        api.delete(name, &DeleteParams::default()).await.ok();
        Ok(())
    }
}

pub async fn watch_pods(kube: KubeClient, state: Arc<SharedState>) -> Result<()> {
    let api = kube.pods();
    tokio::spawn(async move {
        let mut stream = watcher(api, watcher::Config::default().labels("ci-runner/job-id"))
            .boxed();
        while let Some(event) = stream.next().await {
            match event {
                Ok(watcher::Event::Applied(pod)) | Ok(watcher::Event::Restarted(_) ) => {
                    if let Ok(watcher::Event::Applied(pod)) = event { translate(&state, &pod); }
                }
                Ok(_) => {}
                Err(e) => warn!("watcher error: {e:?}"),
            }
        }
    });
    Ok(())
}

fn translate(state: &Arc<SharedState>, pod: &Pod) {
    let Some(labels) = pod.metadata.labels.as_ref() else { return; };
    let Some(job_id) = labels.get("ci-runner/job-id") else { return; };
    let Some(status) = pod.status.as_ref() else { return; };
    let pod_name = pod.metadata.name.clone().unwrap_or_default();
    let node = pod.spec.as_ref().and_then(|s| s.node_name.clone()).unwrap_or_default();
    let (phase, exit) = match status.phase.as_deref() {
        Some("Pending") => (Phase::Scheduled, None),
        Some("Running") => (Phase::Running, None),
        Some("Succeeded") => (Phase::Succeeded, Some(0)),
        Some("Failed") => (Phase::Failed, Some(1)),
        _ => return,
    };
    state.mark(job_id, phase, exit, Some(&node), Some(&pod_name), "pod update");
    info!(job=%job_id, phase=phase.as_str(), pod=%pod_name, "lifecycle");
}
