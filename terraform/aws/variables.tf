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
  description = "EC2 instance type for all nodes"
  type        = string
  default     = "m7i.2xlarge"
  # Sizing guide:
  #   m7i.xlarge   (4 vCPU,  16GB)  — dev / small datasets, 0 Trino workers
  #   m7i.2xlarge  (8 vCPU,  32GB)  — small prod, 1 Trino worker
  #   m7i.4xlarge  (16 vCPU, 64GB)  — medium prod, 2 Trino workers
  #   m7i.8xlarge  (32 vCPU, 128GB) — large prod, 4 Trino workers
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
    Determines Trino worker count: ≥96GB→4, ≥48GB→2, ≥24GB→1, <24GB→0.
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
