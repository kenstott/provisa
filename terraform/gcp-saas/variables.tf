# ── terraform/gcp-saas ─────────────────────────────────────────────────────────
# Multi-tenant, on-demand SaaS deployment of Provisa. Distinct from the enterprise
# module (terraform/gcp), which is a fixed single-tenant N-node cluster with postgres
# in the coordinator's compose stack. This module offloads the control plane to
# Cloud SQL and runs every federation engine on a GKE cluster (gke.tf) instead of on
# VMs -- there is no Trino on the control-plane VM and no worker MIG. See
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
  description = "GCP zone for the control-plane VM and the (zonal) engine cluster"
  type        = string
  default     = "us-central1-a"
}

variable "provisa_version" {
  description = <<-EOT
    Provisa release version (e.g. v0.1.0-alpha.289). Must match a published GitHub
    release. The control-plane VM curls the matching AppImage, core-images zip and
    trino-plugins tarball directly from that release at boot.
  EOT
  type        = string
}

variable "github_repo" {
  description = "Public GitHub repo (owner/name) the VMs curl release assets from at boot."
  type        = string
  default     = "kenstott/provisa"
}

variable "disk_gb" {
  description = "Boot disk size in GB for the control-plane VM."
  type        = number
  default     = 100
}

variable "network_cidr" {
  description = "CIDR block for the SaaS subnet."
  type        = string
  default     = "10.10.0.0/16"
}

variable "object_store_disk_gb" {
  description = "Size of the coordinator's object-store data disk, which holds the OTel Iceberg warehouse."
  type        = number
  default     = 100
}

variable "coordinator_internal_ip" {
  description = <<-DESC
    Fixed internal address for the coordinator, inside network_cidr. The engine pods read the
    OTel Iceberg tables from this node's MinIO, so the endpoint they are handed must be stable
    across a stop/start and must be known before the instance is created — its own startup
    script carries the endpoint, and an instance cannot reference its own assigned network_ip.
  DESC
  type        = string
  default     = "10.10.0.4"
}

# ── Engine cluster (REQ-1447) ───────────────────────────────────────────────────
# The pod and service ranges are ALIAS ranges on the node subnet, not a separate
# network. Pods therefore hold routable VPC addresses, which is what lets the
# control-plane VM dial a shard's pod directly from outside the cluster.
variable "pods_cidr" {
  description = "Secondary range on the node subnet for GKE pod IPs."
  type        = string
  default     = "10.20.0.0/14"
}

variable "services_cidr" {
  description = "Secondary range on the node subnet for GKE service (ClusterIP) addresses."
  type        = string
  default     = "10.24.0.0/20"
}

variable "engine_release_channel" {
  description = "GKE release channel for the engine cluster."
  type        = string
  default     = "REGULAR"

  validation {
    condition     = contains(["RAPID", "REGULAR", "STABLE"], var.engine_release_channel)
    error_message = "engine_release_channel must be RAPID, REGULAR or STABLE."
  }
}

variable "engine_cluster_mode" {
  description = <<-EOT
    Which engine-cluster topology to build: "autopilot" or "standard" (REQ-1465).

    autopilot — no node pools at all. Billing is per pod REQUEST, so a shard at zero
    replicas costs nothing, and GKE's own system Deployments are Google's problem
    rather than a node we keep alive. This is the launch shape: with nobody querying,
    the shared lane bills $0.

    standard — a small always-on Spot system pool carries GKE's system Deployments,
    and each shard gets its own pool autoscaling 0..1, tainted so nothing but that
    shard's pod can hold it up. Running is ~6% cheaper per shard-hour than Autopilot,
    against a fixed ~$11/mo for the system pool: the crossover is about 460 shard-hours
    a month (~15 hours a day). Switch after launch, once real duty cycle is known.

    Switching REPLACES the cluster — enable_autopilot is immutable, and so is
    dns_config. Both modes are applied and exercised the same way; the control plane
    itself changes only PROVISA_ENGINE_CLUSTER_MODE, which decides whether a shard pod
    carries a nodeSelector and toleration.
  EOT
  type        = string
  default     = "autopilot"

  validation {
    condition     = contains(["autopilot", "standard"], var.engine_cluster_mode)
    error_message = "engine_cluster_mode must be autopilot or standard."
  }
}

variable "engine_system_pool_machine_type" {
  description = <<-EOT
    standard mode only: the machine carrying GKE's system Deployments (kube-dns,
    metrics-server, konnectivity-agent, event-exporter, kube-state-metrics,
    gmp-operator). They are Deployments, not DaemonSets, so without a pool of their
    own they schedule onto a shard pool and keep it — and its bill — alive around the
    clock, which is exactly what made the first Standard build unable to rest at zero
    (REQ-1464). Spot, because a system pod rescheduling on preemption costs nothing
    that matters.
  EOT
  type        = string
  default     = "e2-small"
}

variable "shared_shard_machine_type" {
  description = <<-EOT
    standard mode only: the machine a shared-lane shard's pod lands on. Must fit the
    pod's requests (PROVISA_ENGINE_CPU / PROVISA_ENGINE_MEMORY_GIB, 6 vCPU / 24 GiB)
    with room for GKE's per-node agents.
  EOT
  type        = string
  default     = "e2-highmem-8"
}

variable "shared_shards" {
  description = <<-EOT
    standard mode only: the shared-lane shards to build pools for. Each gets a pool
    autoscaling 0..1, tainted and labelled provisa.dev/shard=<shard>, which is what the
    control plane's pod selector matches. Autopilot needs no equivalent — a shard there
    is only a Deployment.
  EOT
  type        = list(string)
  default     = ["shared_1"]
}

variable "engine_image" {
  description = <<-EOT
    Trino image every engine pod runs (docker/trino-engine.Dockerfile: Trino plus
    the Provisa connector plugins and the OpenTelemetry agent). One image serves
    both lanes — shared and Pro differ by placement and size, never by image
    (REQ-1447).
  EOT
  type        = string
}

variable "zaychik_image" {
  description = <<-EOT
    Arrow Flight SQL proxy image (zaychik/Dockerfile), run as a sidecar in every
    engine pod. Flight is a protocol the engine speaks, not a service of its own, so
    the proxy shares the coordinator's pod, address and lifetime (REQ-045, REQ-1448).
  EOT
  type        = string
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

# ── Data plane ──────────────────────────────────────────────────────────────────
# There is no worker MIG. Query execution is the cluster's (REQ-1447): a shard is a
# pod Autopilot sizes a node for, and capacity is added by shards, never by VMs
# joining the control plane's Trino. The autoscaled MIG, its instance template and
# its regional autoscaler are gone with the GCE engine tier.

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

# REQ-125: break-glass superuser. The IdP owns every ordinary login on this deployment, so
# without this account there is no credential that reaches the API when the IdP is unreachable
# or a token cannot be minted — and no way for the deploy script to prove the engine path.
# Blank leaves the account unconfigured, which is the enterprise default.
variable "superuser_username" {
  description = "Break-glass superuser name (REQ-125). Blank leaves the account off."
  type        = string
  default     = ""
}

variable "superuser_password" {
  description = "Break-glass superuser password (REQ-125). Blank leaves the account off."
  type        = string
  default     = ""
  sensitive   = true
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
  description = "Public origin of the UI used in invite redemption links. Blank derives https://cloud.<dns_zone>, the record dns.tf creates; set it only when the UI is reached at some other origin."
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
