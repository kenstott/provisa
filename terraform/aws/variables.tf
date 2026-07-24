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

variable "provisa_version" {
  description = <<-EOT
    Provisa release version (e.g. v0.1.0-alpha.271). Must match the AppImage build.
    The node stages the matching core-images zip (provisa-core-images-amd64-<version>.zip)
    from the same S3 directory as appimage_s3_key, and exports PROVISA_VERSION so first-launch
    finds it locally (airgap path) instead of downloading from the GitHub release.
  EOT
  type        = string
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

variable "tls_cert_pem" {
  description = <<-EOT
    PEM-encoded TLS certificate served by every Provisa listener (API, UI, pgwire,
    bolt, Flight, gRPC, MCP). Supply a wildcard *.provisa.dev cert so cloud.provisa.dev
    and every {org}.provisa.dev terminate on one cert (REQ-1239). Full chain (leaf +
    issuers) recommended. When set together with tls_key_pem, first-launch adopts it via
    PROVISA_TLS_CERT and skips self-signed generation. Generate with
    scripts/issue-wildcard-cert.sh. Leave blank to fall back to a self-signed dev cert.
  EOT
  type        = string
  default     = ""
}

variable "tls_key_pem" {
  description = "PEM-encoded private key matching tls_cert_pem. Required when tls_cert_pem is set."
  type        = string
  default     = ""
  sensitive   = true
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

variable "tags" {
  description = "Additional tags applied to all resources"
  type        = map(string)
  default     = {}
}

# ── Protocol surfaces (each gates a node SG rule + NLB listener + container listener) ──
# API (8000), Arrow Flight (8815), and the UI (3000) are always exposed. The
# following are opt-in wire protocols Provisa can serve over the same federated
# catalog; enabling one publishes its port on the provisa container, opens the
# node SG, and adds an NLB target group + listener on the shared endpoint. Ports
# are fixed to each protocol's client-expected default (psql 5439, Neo4j Bolt 7687,
# MCP 8009, gRPC 50051). Default on for a fully exercisable test cluster.
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
