###############################################################################
# Hybrid-cloud HPC CI cluster — Terraform root module.
#
# Provisions three node pools:
#   1. on-prem-control: orchestrator + dispatcher (CPU only)
#   2. cloud-cpu:       burstable CPU runners on spot instances
#   3. cloud-gpu:       A100/H100 GPU runners (on-demand)
#
# The pools share one Kubernetes control plane via cluster federation
# (Karmada in production; this module wires the AWS half).
###############################################################################

terraform {
  required_version = ">= 1.7.0"
  required_providers {
    aws        = { source = "hashicorp/aws",        version = "~> 5.40" }
    kubernetes = { source = "hashicorp/kubernetes", version = "~> 2.27" }
    helm       = { source = "hashicorp/helm",       version = "~> 2.13" }
    random     = { source = "hashicorp/random",     version = "~> 3.6"  }
  }
}

variable "cluster_name"  { type = string;  default = "ci-runner" }
variable "region"        { type = string;  default = "us-west-2" }
variable "k8s_version"   { type = string;  default = "1.29"      }
variable "cpu_min"       { type = number;  default = 0   }
variable "cpu_max"       { type = number;  default = 200 }
variable "gpu_min"       { type = number;  default = 0   }
variable "gpu_max"       { type = number;  default = 24  }
variable "tags"          { type = map(string); default = { project = "ci-runner" } }

provider "aws" {
  region = var.region
  default_tags { tags = var.tags }
}

data "aws_availability_zones" "available" { state = "available" }

###############################################################################
# Networking
###############################################################################
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.7"
  name    = "${var.cluster_name}-vpc"
  cidr    = "10.40.0.0/16"
  azs             = slice(data.aws_availability_zones.available.names, 0, 3)
  private_subnets = ["10.40.0.0/19",  "10.40.32.0/19", "10.40.64.0/19"]
  public_subnets  = ["10.40.96.0/22", "10.40.100.0/22","10.40.104.0/22"]
  enable_nat_gateway   = true
  single_nat_gateway   = false
  enable_dns_hostnames = true
}

###############################################################################
# EKS cluster — control plane
###############################################################################
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.8"
  cluster_name    = var.cluster_name
  cluster_version = var.k8s_version
  vpc_id          = module.vpc.vpc_id
  subnet_ids      = module.vpc.private_subnets
  enable_irsa     = true
  cluster_endpoint_public_access = true

  cluster_addons = {
    coredns                = { most_recent = true }
    kube-proxy             = { most_recent = true }
    vpc-cni                = { most_recent = true }
    aws-ebs-csi-driver     = { most_recent = true }
  }

  eks_managed_node_groups = {
    cpu_spot = {
      desired_size = 4
      min_size     = var.cpu_min
      max_size     = var.cpu_max
      capacity_type = "SPOT"
      instance_types = ["c6i.4xlarge", "c6a.4xlarge", "m6i.4xlarge"]
      labels = { "ci-runner/cloud" = "aws", "ci-runner/pool" = "cpu" }
      taints = [{ key = "ci-runner/pool", value = "cpu", effect = "NO_SCHEDULE" }]
    }
    gpu_a100 = {
      desired_size = 0
      min_size     = var.gpu_min
      max_size     = var.gpu_max
      capacity_type = "ON_DEMAND"
      instance_types = ["p4d.24xlarge"]
      ami_type       = "AL2_x86_64_GPU"
      labels = { "ci-runner/cloud" = "aws", "ci-runner/pool" = "gpu", "ci-runner/gpu-class" = "a100" }
      taints = [{ key = "nvidia.com/gpu", value = "true", effect = "NO_SCHEDULE" }]
    }
  }
}

###############################################################################
# Kubernetes provider wired to the new cluster.
###############################################################################
provider "kubernetes" {
  host                   = module.eks.cluster_endpoint
  cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", var.cluster_name]
  }
}

provider "helm" {
  kubernetes {
    host                   = module.eks.cluster_endpoint
    cluster_ca_certificate = base64decode(module.eks.cluster_certificate_authority_data)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--cluster-name", var.cluster_name]
    }
  }
}

resource "kubernetes_namespace" "ci_runner" {
  metadata { name = "ci-runner" }
}

###############################################################################
# NVIDIA GPU operator (driver + device plugin) for the GPU pool.
###############################################################################
resource "helm_release" "nvidia_gpu_operator" {
  name             = "gpu-operator"
  repository       = "https://helm.ngc.nvidia.com/nvidia"
  chart            = "gpu-operator"
  namespace        = "gpu-operator"
  create_namespace = true
  version          = "v24.3.0"
  values = [yamlencode({
    driver = { enabled = true }
    toolkit = { enabled = true }
    devicePlugin = { enabled = true }
    nodeSelector = { "ci-runner/pool" = "gpu" }
  })]
}

###############################################################################
# Outputs
###############################################################################
output "cluster_endpoint" { value = module.eks.cluster_endpoint }
output "cluster_name"     { value = module.eks.cluster_name }
output "namespace"        { value = kubernetes_namespace.ci_runner.metadata[0].name }
