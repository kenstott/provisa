variable "location" {
  description = "Azure region to deploy into"
  type        = string
  default     = "eastus"
}

variable "resource_group_name" {
  description = "Name for the Azure resource group"
  type        = string
  default     = "provisa"
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

variable "vm_size" {
  description = "Azure VM size for all nodes"
  type        = string
  default     = "Standard_D8s_v3"
  # Sizing guide:
  #   Standard_D4s_v3  (4 vCPU,  16 GB) — dev / small datasets, 0 Trino workers
  #   Standard_D8s_v3  (8 vCPU,  32 GB) — small prod, 1 Trino worker
  #   Standard_D16s_v3 (16 vCPU, 64 GB) — medium prod, 2 Trino workers
  #   Standard_D32s_v3 (32 vCPU,128 GB) — large prod, 4 Trino workers
}

variable "os_disk_gb" {
  description = "OS disk size in GB per node"
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

variable "vnet_cidr" {
  description = "CIDR block for the new VNet"
  type        = string
  default     = "10.0.0.0/16"
}

variable "storage_account_name" {
  description = "Azure Storage Account containing the Provisa AppImage"
  type        = string
}

variable "storage_container" {
  description = "Blob container name within the storage account"
  type        = string
  default     = "releases"
}

variable "appimage_blob" {
  description = "Blob name of the Provisa AppImage"
  type        = string
  default     = "Provisa.AppImage"
}

variable "admin_username" {
  description = "Admin username for SSH access"
  type        = string
  default     = "provisa"
}

variable "ssh_public_key" {
  description = "SSH public key for admin access. Leave blank to disable SSH."
  type        = string
  default     = ""
}

variable "admin_cidr" {
  description = "CIDR allowed SSH access. Leave blank to disable SSH."
  type        = string
  default     = ""
}

variable "tags" {
  description = "Additional tags applied to all resources"
  type        = map(string)
  default     = {}
}
