# ── terraform/gcp-saas ─────────────────────────────────────────────────────────
# Multi-tenant, on-demand SaaS deployment of Provisa. Distinct from the enterprise
# module (terraform/gcp), which is a fixed single-tenant N-node cluster with postgres
# in the coordinator's compose stack. This module offloads the control plane to
# Cloud SQL and autoscales Trino workers in a Spot MIG. See
# docs/deployment/gcp-saas-infra-plan.md. Multitenancy is forced on (not a variable).

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
  description = "GCP zone for the coordinator VM"
  type        = string
  default     = "us-central1-a"
}

variable "provisa_version" {
  description = <<-EOT
    Provisa release version (e.g. v0.1.0-alpha.289). Must match a published GitHub
    release. The coordinator and each worker curl the matching AppImage, core-images
    zip and trino-plugins tarball directly from that release at boot.
  EOT
  type        = string
}

variable "github_repo" {
  description = "Public GitHub repo (owner/name) the VMs curl release assets from at boot."
  type        = string
  default     = "kenstott/provisa"
}

variable "disk_gb" {
  description = "Boot disk size in GB per VM (coordinator and workers)."
  type        = number
  default     = 100
}

variable "network_cidr" {
  description = "CIDR block for the SaaS subnet."
  type        = string
  default     = "10.10.0.0/16"
}

# ── Coordinator (planner + TCP listeners + stateful singletons) ─────────────────
variable "coordinator_machine_type" {
  description = <<-EOT
    Machine type for the Trino coordinator. With the DB offloaded to Cloud SQL and
    node-scheduler.include-coordinator=false, the coordinator only plans + coordinates
    + streams results, so e2-standard-4 (4 vCPU, 16 GB) is the SaaS baseline: E2's
    ~10-25% lower per-core throughput only shows up as planning latency under
    concurrent load, and the box idle-stops via the front door anyway. Move back to
    n2-standard-4 (the enterprise baseline) when paying concurrency exists.
  EOT
  type        = string
  default     = "e2-standard-4"
}

# ── Front door (always-free e2-micro: wake-on-hit + idle-stop) ──────────────────
variable "front_door_machine_type" {
  description = <<-EOT
    Machine type for the front-door proxy VM. e2-micro in us-central1/us-west1/
    us-east1 rides GCP's always-free tier (1 per billing account), making the
    wake-on-hit front effectively $0.
  EOT
  type        = string
  default     = "e2-micro"
}

variable "idle_stop_minutes" {
  description = <<-EOT
    Minutes of zero traffic across every protocol port before the front door stops
    the coordinator. The next hit on any port starts it again (~2 min boot); HTTPS
    surfaces get a waking page, raw TCP clients are held until the listener is up.
  EOT
  type        = number
  default     = 60
}

variable "front_door_boot_grace_minutes" {
  description = "Minimum minutes after a wake before the idle reaper may stop the coordinator."
  type        = number
  default     = 10
}

variable "front_door_status_port" {
  description = <<-EOT
    HTTPS port the front door serves its authenticated wake/verify API on
    (GET /status = coordinator state + per-port reachability; POST /wake = start).
    Bearer token in the front_door_status_token output. Served by the front door
    itself, so it answers even while the coordinator is stopped.
  EOT
  type        = number
  default     = 9443
}

# ── Data plane (autoscaled Trino workers) ───────────────────────────────────────
variable "worker_machine_type" {
  description = "Machine type for Trino worker VMs (the autoscaled MIG). Memory-optimized recommended."
  type        = string
  default     = "n2-highmem-8"
}

variable "worker_min_nodes" {
  description = <<-EOT
    Minimum worker count. 0 = pure scale-to-zero (customer eats first-query cold
    start; cheapest idle). ≥1 = a warm worker pool (paid always-warm upsell).
  EOT
  type        = number
  default     = 0
  validation {
    condition     = var.worker_min_nodes >= 0
    error_message = "worker_min_nodes must be >= 0."
  }
}

variable "worker_max_nodes" {
  description = "Maximum worker count the autoscaler may scale the MIG up to."
  type        = number
  default     = 4
  validation {
    condition     = var.worker_max_nodes >= 1
    error_message = "worker_max_nodes must be >= 1."
  }
}

variable "worker_use_spot" {
  description = <<-EOT
    Run workers as Spot (preemptible) VMs — the metered, scale-to-zero data plane.
    Set false only for a guaranteed-warm worker SLA (an always-on paid add-on cannot
    ride a preemptible instance).
  EOT
  type        = bool
  default     = true
}

variable "autoscale_cpu_target" {
  description = "Target average CPU utilization (0.0–1.0) that drives worker MIG autoscaling."
  type        = number
  default     = 0.6
  validation {
    condition     = var.autoscale_cpu_target > 0 && var.autoscale_cpu_target <= 1
    error_message = "autoscale_cpu_target must be in (0, 1]."
  }
}

# ── Cloud SQL (managed control plane) ───────────────────────────────────────────
variable "cloudsql_tier" {
  description = <<-EOT
    Cloud SQL machine tier for the control-plane Postgres. db-f1-micro is the
    always-warm baseline (~$9/mo); scale up for many-tenant connection fan-out.
  EOT
  type        = string
  default     = "db-f1-micro"
}

variable "cloudsql_ha" {
  description = "Regional (high-availability) Cloud SQL. true = REGIONAL failover; false = ZONAL (cheaper, single-zone)."
  type        = bool
  default     = false
}

variable "cloudsql_max_connections" {
  description = <<-EOT
    Postgres max_connections for the control-plane instance. db-f1-micro's memory-derived
    default is 25, which the control plane + audit plane + per-org tenant handles exhaust at
    a couple of orgs. Raise this alongside the tier when tenant count grows.
  EOT
  type        = number
  default     = 100
}

variable "cloudsql_disk_gb" {
  description = "Cloud SQL data disk size in GB."
  type        = number
  default     = 20
}

# ── TLS / auth (parity with the enterprise module) ──────────────────────────────
variable "tls_cert_pem" {
  description = <<-EOT
    PEM-encoded wildcard *.provisa.dev TLS certificate served by every listener so
    every {org}.provisa.dev terminates on one cert (REQ-1239, subdomain-per-org).
    When set with tls_key_pem, first-launch adopts it and skips self-signed generation.
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

variable "auth_provider" {
  description = "Identity provider PROVISA_IDP: 'none', 'firebase', 'basic', 'keycloak', 'oauth', or 'oidc'. SaaS onboarding expects 'firebase'."
  type        = string
  default     = "firebase"
  validation {
    condition     = contains(["none", "firebase", "basic", "keycloak", "oauth", "oidc"], var.auth_provider)
    error_message = "auth_provider must be one of: none, firebase, basic, keycloak, oauth, oidc."
  }
}

variable "azure_tenant_id" {
  description = "Restrict Firebase Microsoft (Azure AD/Entra) sign-in to this tenant directory ID. Empty = 'common'."
  type        = string
  default     = ""
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

variable "firebase_web_api_key" {
  description = "Firebase web app apiKey (VITE_FIREBASE_API_KEY) when auth_provider=firebase."
  type        = string
  default     = ""
}

variable "firebase_web_auth_domain" {
  description = "Firebase web app authDomain (VITE_FIREBASE_AUTH_DOMAIN), e.g. <project>.firebaseapp.com."
  type        = string
  default     = ""
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
  description = "Additional labels applied to all resources."
  type        = map(string)
  default     = {}
}

# ── Protocol surfaces (opt-in wire protocols; same contract as enterprise) ───────
variable "enable_pgwire" {
  description = "Expose the Postgres wire protocol (port 5439)."
  type        = bool
  default     = true
}

variable "enable_bolt" {
  description = "Expose the Neo4j Bolt protocol (port 7687)."
  type        = bool
  default     = true
}

variable "enable_mcp" {
  description = "Expose the MCP server (port 8009)."
  type        = bool
  default     = true
}

variable "enable_grpc" {
  description = "Expose the gRPC API (port 50051)."
  type        = bool
  default     = true
}

variable "mcp_role" {
  description = "Role the MCP server runs queries as when enable_mcp=true."
  type        = string
  default     = "admin"
}

# ── Deployment choices (parity with the enterprise module) ──────────────────────
variable "obs_mode" {
  description = "Observability: 'none', 'docker' (bundled Grafana/Prometheus), or 'collector' (export OTLP to otlp_endpoint)."
  type        = string
  default     = "none"
  validation {
    condition     = contains(["none", "docker", "collector"], var.obs_mode)
    error_message = "obs_mode must be one of: none, docker, collector."
  }
}

variable "otlp_endpoint" {
  description = "OTLP collector endpoint when obs_mode=collector."
  type        = string
  default     = ""
}

variable "install_demo" {
  description = "Seed the demo dataset into the platform default org at first launch."
  type        = bool
  default     = false
}

# REQ-1330: outbound transactional mail (org invites). SaaS-only; delivered through
# the EmailSender port's Resend adapter, sending as mail_from_address with SPF/DKIM
# on its domain. Inbound MX for the domain stays with the operator's mailbox host.
variable "email_api_key" {
  description = "Resend API key for outbound invite mail. Blank leaves delivery refusing with a named error; invites still work as links."
  type        = string
  default     = ""
  sensitive   = true
}

variable "mail_from_address" {
  description = "Sender for outbound invite mail."
  type        = string
  default     = "invites@provisa.dev"
}

variable "mail_base_url" {
  description = "Public origin of the UI used in invite redemption links (e.g. https://cloud.provisa.dev)."
  type        = string
  default     = ""
}

# ── Public DNS (see dns.tf) ────────────────────────────────────────────────────
# The subdomain-per-org model needs `cloud.<zone>` and `*.<zone>` pointing at the shared
# front-door IP. Blank leaves DNS entirely alone, for a domain hosted somewhere other than
# Cloudflare or managed by hand.

variable "dns_zone" {
  description = "Cloudflare zone to manage the control-plane and org-wildcard A records in (e.g. provisa.dev). Blank = terraform manages no DNS."
  type        = string
  default     = ""
}

variable "cloudflare_api_token" {
  description = "Cloudflare API token scoped Zone.DNS:Edit on dns_zone. Required only when dns_zone is set."
  type        = string
  default     = ""
  sensitive   = true
}

variable "dns_ttl" {
  description = "TTL for the managed A records. Short by design: the shared IP is the only thing every protocol resolves through, so a re-IP must propagate fast."
  type        = number
  default     = 300
}
