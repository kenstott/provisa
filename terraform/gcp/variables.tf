variable "project" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region to deploy into"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for VM placement"
  type        = string
  default     = "us-central1-a"
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

variable "machine_type" {
  description = "GCP machine type for the primary node"
  type        = string
  default     = "n2-standard-8"
  # Sizing guide:
  #   n2-standard-4  (4 vCPU,  16 GB) — dev / small datasets
  #   n2-standard-8  (8 vCPU,  32 GB) — small prod
  #   n2-standard-16 (16 vCPU, 64 GB) — medium prod
  #   n2-standard-32 (32 vCPU,128 GB) — large prod
}

variable "worker_machine_type" {
  description = "GCP machine type for secondary (Trino worker) nodes. Memory-optimized recommended."
  type        = string
  default     = "n2-highmem-16"
  # Sizing guide:
  #   n2-highmem-8  (8 vCPU,  64 GB)  — small prod, light analytics
  #   n2-highmem-16 (16 vCPU, 128 GB) — medium prod, recommended default
  #   n2-highmem-32 (32 vCPU, 256 GB) — large prod, heavy analytics
}

variable "disk_gb" {
  description = "Boot disk size in GB per node"
  type        = number
  default     = 100
}

variable "ram_budget_gb" {
  description = <<-EOT
    RAM (GB) to allocate to Provisa services on each node.
    0 = use all available RAM on the instance.
    Determines Trino worker count: ≥96GB→4, ≥48GB→2, ≥24GB→1, <24GB→0.
  EOT
  type        = number
  default     = 0
}

variable "network_cidr" {
  description = "CIDR block for the new subnet"
  type        = string
  default     = "10.0.0.0/16"
}

variable "gcs_bucket" {
  description = "GCS bucket containing the Provisa AppImage"
  type        = string
}

variable "gcs_object" {
  description = "Object path within the GCS bucket"
  type        = string
  default     = "releases/Provisa.AppImage"
}

variable "ssh_public_key" {
  description = "SSH public key for admin access (format: 'user:ssh-rsa ...'). Leave blank to disable SSH."
  type        = string
  default     = ""
}

variable "admin_cidr" {
  description = "CIDR allowed SSH access. Leave blank to disable SSH."
  type        = string
  default     = ""
}

variable "labels" {
  description = "Additional labels applied to all resources"
  type        = map(string)
  default     = {}
}
