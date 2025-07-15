###############################################################################
# On-prem half of the hybrid cluster.
#
# We don't manage on-prem hardware from Terraform directly — instead we
# expect a kubeadm cluster reachable at var.onprem_kubeconfig, and we
# apply the same workload manifests there using a second Kubernetes
# provider alias. This module:
#   * Joins the on-prem cluster into the federation control plane.
#   * Labels nodes with "ci-runner/cloud=onprem" and the right pool.
#   * Installs the Linux DaemonSet that exposes cgroup v2 metrics.
###############################################################################

variable "onprem_kubeconfig"     { type = string; default = "~/.kube/onprem" }
variable "onprem_node_count"     { type = number; default = 8 }
variable "onprem_cpu_per_node"   { type = number; default = 96 }
variable "onprem_memory_per_node_mib" { type = number; default = 393216 }

provider "kubernetes" {
  alias       = "onprem"
  config_path = var.onprem_kubeconfig
}

resource "kubernetes_namespace" "ci_runner_onprem" {
  provider = kubernetes.onprem
  metadata { name = "ci-runner" }
}

resource "kubernetes_daemon_set_v1" "cgroup_metrics" {
  provider = kubernetes.onprem
  metadata {
    name      = "cgroup-metrics"
    namespace = kubernetes_namespace.ci_runner_onprem.metadata[0].name
  }
  spec {
    selector { match_labels = { app = "cgroup-metrics" } }
    template {
      metadata { labels = { app = "cgroup-metrics" } }
      spec {
        host_pid     = true
        host_network = true
        toleration {
          operator = "Exists"
          effect   = "NoSchedule"
        }
        container {
          name  = "exporter"
          image = "ghcr.io/nikhil-ghind/cgroup-exporter:latest"
          security_context { privileged = true }
          volume_mount { name = "cgroup"; mount_path = "/sys/fs/cgroup"; read_only = true }
          volume_mount { name = "proc";   mount_path = "/host/proc";    read_only = true }
        }
        volume {
          name = "cgroup"
          host_path { path = "/sys/fs/cgroup" }
        }
        volume {
          name = "proc"
          host_path { path = "/proc" }
        }
      }
    }
  }
}
