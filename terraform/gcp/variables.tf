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

variable "provisa_version" {
  description = <<-EOT
    Provisa release version (e.g. v0.1.0-alpha.271). Must match the AppImage build.
    The node stages the matching core-images zip (provisa-core-images-amd64-<version>.zip)
    from the same GCS directory as gcs_object, and exports PROVISA_VERSION so first-launch
    finds it locally (airgap path) instead of downloading from the GitHub release.
  EOT
  type        = string
}

variable "github_repo" {
  description = "GitHub repo (owner/name) to pull release assets from when stage_from_github=true."
  type        = string
  default     = "kenstott/provisa"
}

variable "stage_from_github" {
  description = <<-EOT
    When true, Terraform downloads the AppImage + core-images zip for provisa_version
    from the GitHub release and uploads them to gcs_bucket before the VMs boot.
    Requires authenticated `gh` and `gcloud`/`gsutil` on the machine running Terraform.
    Set false to manage the bucket objects yourself.
  EOT
  type        = bool
  default     = true
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

# ── Auth (parity with the desktop installer wizard, REQ-972..979) ──────────────
variable "auth_provider" {
  description = "Identity provider PROVISA_IDP: 'none' (unsecured), 'firebase', 'basic', 'keycloak', 'oauth', or 'oidc'."
  type        = string
  default     = "none"
  validation {
    condition     = contains(["none", "firebase", "basic", "keycloak", "oauth", "oidc"], var.auth_provider)
    error_message = "auth_provider must be one of: none, firebase, basic, keycloak, oauth, oidc."
  }
}

variable "firebase_project_id" {
  description = "Firebase project ID when auth_provider=firebase."
  type        = string
  default     = ""
}

variable "firebase_service_account_key" {
  description = "Firebase service-account JSON (or blank to use ADC on the node) when auth_provider=firebase."
  type        = string
  default     = ""
  sensitive   = true
}

variable "labels" {
  description = "Additional labels applied to all resources"
  type        = map(string)
  default     = {}
}

# ── Protocol surfaces (each gates a firewall rule + NetLB + container listener) ──
# API (8000), Arrow Flight (8815), and the UI (3000) are always exposed. The
# following are opt-in wire protocols Provisa can serve over the same federated
# catalog; enabling one publishes its port on the provisa container, opens the
# firewall, and adds a passthrough NetLB forwarding rule. Ports are fixed to each
# protocol's client-expected default (psql 5439, Neo4j Bolt 7687, MCP 8009,
# gRPC 50051). Default on for a fully exercisable test cluster.
variable "enable_pgwire" {
  description = "Expose the Postgres wire protocol (port 5439) — DBeaver/psql over the federated catalog."
  type        = bool
  default     = true
}

variable "enable_bolt" {
  description = "Expose the Neo4j Bolt protocol (port 7687) — Neo4j Browser/Bloom, Cypher over the graph."
  type        = bool
  default     = true
}

variable "enable_mcp" {
  description = "Expose the MCP server (port 8009, REQ-1008) for agent/tool access."
  type        = bool
  default     = true
}

variable "enable_grpc" {
  description = "Expose the gRPC API (port 50051). Only serves once a proto schema is registered."
  type        = bool
  default     = true
}

variable "mcp_role" {
  description = "Role the MCP server runs queries as when enable_mcp=true."
  type        = string
  default     = "admin"
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
  description = "Install the demo dataset and open the guided tour. A complete, fully functional deployment — nothing is limited; re-apply with false to reconfigure with your own sources later."
  type        = bool
  default     = false
}
