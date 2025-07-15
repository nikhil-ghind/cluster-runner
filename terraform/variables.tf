# Auxiliary tunables.

variable "dispatcher_image" {
  type        = string
  default     = "ghcr.io/nikhil-ghind/cluster-dispatcher:latest"
  description = "Container image for the Rust dispatcher."
}

variable "orchestrator_image" {
  type        = string
  default     = "ghcr.io/nikhil-ghind/cluster-orchestrator:latest"
  description = "Container image for the Python orchestrator."
}

variable "worker_image" {
  type        = string
  default     = "ghcr.io/nikhil-ghind/ci-worker:latest"
  description = "Worker pod image (cgroup/namespace driver)."
}

variable "enable_federation" {
  type        = bool
  default     = true
  description = "If true, deploy Karmada onto the AWS half for fed scheduling."
}
