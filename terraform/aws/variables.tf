variable "region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "node_count" {
  description = "Total nodes to deploy (1 primary + N-1 secondaries). Minimum 1."
  type        = number
  default     = 2
  validation {
    condition     = var.node_count >= 1
    error_message = "node_count must be at least 1."
  }
}

variable "instance_type" {
  description = "EC2 instance type for the primary node"
  type        = string
  default     = "m7i.2xlarge"
  # Sizing guide:
  #   m7i.xlarge   (4 vCPU,  16GB)  — dev / small datasets
  #   m7i.2xlarge  (8 vCPU,  32GB)  — small prod
  #   m7i.4xlarge  (16 vCPU, 64GB)  — medium prod
  #   m7i.8xlarge  (32 vCPU, 128GB) — large prod
}

variable "worker_instance_type" {
  description = "EC2 instance type for secondary (Trino worker) nodes. Memory-optimized recommended."
  type        = string
  default     = "r7i.4xlarge"
  # Sizing guide:
  #   r7i.2xlarge  (8 vCPU,  64GB)  — small prod, light analytics
  #   r7i.4xlarge  (16 vCPU, 128GB) — medium prod, recommended default
  #   r7i.8xlarge  (32 vCPU, 256GB) — large prod, heavy analytics
}

variable "root_volume_gb" {
  description = "Root EBS volume size in GB per node"
  type        = number
  default     = 100
}

variable "ram_budget_gb" {
  description = <<-EOT
    RAM (GB) to allocate to Provisa services on each node.
    0 = use all available RAM on the instance.
    Determines Trino worker count on each node: ≥96GB→4, ≥48GB→2, ≥24GB→1, <24GB→0.
  EOT
  type        = number
  default     = 0
}

variable "vpc_cidr" {
  description = "CIDR block for the new VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "appimage_s3_bucket" {
  description = "S3 bucket containing the Provisa AppImage"
  type        = string
}

variable "appimage_s3_key" {
  description = "S3 object key of the Provisa AppImage"
  type        = string
  default     = "releases/Provisa.AppImage"
}

variable "key_pair" {
  description = "EC2 key pair name for SSH access. Leave blank to disable SSH."
  type        = string
  default     = ""
}

variable "admin_cidr" {
  description = "CIDR allowed SSH access (e.g. '203.0.113.0/24'). Leave blank to disable SSH."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Additional tags applied to all resources"
  type        = map(string)
  default     = {}
}

# ── Deployment choices (parity with the desktop installer wizard, REQ-972..979) ─
variable "federation_engine" {
  description = "Federation engine PROVISA_ENGINE: 'trino' (bundled cluster, default), 'duckdb', or 'sqlalchemy' (external engine — set engine_url)."
  type        = string
  default     = "trino"
  validation {
    condition     = contains(["trino", "duckdb", "sqlalchemy"], var.federation_engine)
    error_message = "federation_engine must be one of: trino, duckdb, sqlalchemy."
  }
}

variable "engine_url" {
  description = "External engine DSN when federation_engine=sqlalchemy (e.g. postgresql+psycopg://user:pass@host:5432/db)."
  type        = string
  default     = ""
}

variable "materialize_url" {
  description = "Optional external materialization-store DSN."
  type        = string
  default     = ""
}

variable "obs_mode" {
  description = "Observability: 'none' (built-in only), 'docker' (bundled Grafana/Prometheus), or 'collector' (export OTLP to otlp_endpoint)."
  type        = string
  default     = "none"
  validation {
    condition     = contains(["none", "docker", "collector"], var.obs_mode)
    error_message = "obs_mode must be one of: none, docker, collector."
  }
}

variable "otlp_endpoint" {
  description = "OTLP collector endpoint when obs_mode=collector (e.g. http://otel-gateway:4317)."
  type        = string
  default     = ""
}

variable "install_demo" {
  description = "Install the demo dataset and open the guided tour."
  type        = bool
  default     = false
}
