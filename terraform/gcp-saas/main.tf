terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
    # dns.tf — the control-plane and org-wildcard A records. v5 renamed cloudflare_record to
    # cloudflare_dns_record and changed its schema, so the major is pinned, not left to float.
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

# ── Networking (reused verbatim from the enterprise module) ─────────────────────

resource "google_compute_network" "main" {
  name                    = "provisa-saas-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "nodes" {
  name          = "provisa-saas-nodes"
  region        = var.region
  network       = google_compute_network.main.id
  ip_cidr_range = var.network_cidr
}

# ── Firewall (reused verbatim) ──────────────────────────────────────────────────

resource "google_compute_firewall" "protocols" {
  name    = "provisa-saas-allow-protocols"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = local.protocol_ports
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["provisa-saas-node"]
}

resource "google_compute_firewall" "intra_cluster" {
  name    = "provisa-saas-allow-intra-cluster"
  network = google_compute_network.main.name

  allow {
    protocol = "all"
  }

  source_tags = ["provisa-saas-node"]
  target_tags = ["provisa-saas-node"]
}

resource "google_compute_firewall" "ssh" {
  count   = var.ssh_public_key != "" && var.admin_cidr != "" ? 1 : 0
  name    = "provisa-saas-allow-ssh"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = [var.admin_cidr]
  target_tags   = ["provisa-saas-node"]
}

# ── Service account — GCS/ADC access for the node ───────────────────────────────

resource "google_service_account" "provisa" {
  account_id   = "provisa-saas-node"
  display_name = "Provisa SaaS Node"
}

# ── Private Service Access — required for Cloud SQL private IP on this VPC ────────

resource "google_compute_global_address" "psa" {
  name          = "provisa-saas-psa"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.main.id
}

resource "google_service_networking_connection" "psa" {
  network                 = google_compute_network.main.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.psa.name]
}

# ── Cloud SQL — managed, always-warm control plane (Postgres) ────────────────────
# Holds the multitenancy metadata (PLATFORM_DATABASE_URL / TENANT_DATABASE_URL).
# Private IP only; reachable from the coordinator over the PSA-peered VPC.

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "google_sql_database_instance" "main" {
  name                = "provisa-saas-control"
  region              = var.region
  database_version    = "POSTGRES_16"
  deletion_protection = false

  depends_on = [google_service_networking_connection.psa]

  settings {
    tier              = var.cloudsql_tier
    availability_type = var.cloudsql_ha ? "REGIONAL" : "ZONAL"
    disk_size         = var.cloudsql_disk_gb
    disk_autoresize   = true

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.main.id
    }

    # REQ-1316: db-f1-micro derives max_connections from its 0.6 GB of memory, giving 25 — of which
    # Cloud SQL reserves 3 for cloudsqladmin plus the superuser reserve. The control plane, the audit
    # plane and every org's tenant handle all connect to THIS one instance, so a deployment with a
    # couple of orgs and normal pool sizing runs the server out of slots and every query fails with
    # "remaining connection slots are reserved for roles with privileges of pg_use_reserved_connections".
    # The pool-per-org multiplication is fixed separately (one shared tenant engine); this is the
    # server-side headroom that keeps a second org from being fatal.
    database_flags {
      name  = "max_connections"
      value = var.cloudsql_max_connections
    }

    user_labels = local.all_labels
  }
}

resource "google_sql_database" "provisa" {
  name     = "provisa"
  instance = google_sql_database_instance.main.name
}

resource "google_sql_user" "provisa" {
  name     = "provisa"
  instance = google_sql_database_instance.main.name
  password = random_password.db.result
}

# ── Locals ──────────────────────────────────────────────────────────────────────

locals {
  all_labels = merge(var.labels, { project = "provisa", deployment = "saas" })

  images_zip      = "provisa-core-images-amd64-${var.provisa_version}.zip"
  plugins_tarball = "provisa-trino-plugins-${var.provisa_version}.tar.gz"

  # ── Protocol surface (identical contract to enterprise) ──────────────────────
  protocols = {
    api    = { port = 8000, enabled = true, probe = "https", path = "/health", env = null }
    ui     = { port = 443, enabled = true, probe = "tcp", path = null, env = null }
    flight = { port = 8815, enabled = true, probe = "tcp", path = null, env = "FLIGHT_PORT" }
    pgwire = { port = 5439, enabled = var.enable_pgwire, probe = "tcp", path = null, env = "PROVISA_PGWIRE_PORT" }
    bolt   = { port = 7687, enabled = var.enable_bolt, probe = "tcp", path = null, env = "PROVISA_BOLT_PORT" }
    mcp    = { port = 8009, enabled = var.enable_mcp, probe = "tcp", path = null, env = "PROVISA_MCP_PORT" }
    grpc   = { port = 50051, enabled = var.enable_grpc, probe = "tcp", path = null, env = "GRPC_PORT" }
  }
  enabled_protocols = { for k, v in local.protocols : k => v if v.enabled }
  protocol_ports    = [for k, v in local.enabled_protocols : tostring(v.port)]
  protocol_exports = join("\n    ", concat(
    [for k, v in local.enabled_protocols : "export ${v.env}=${v.port}" if contains(["pgwire", "bolt", "mcp", "grpc"], k)],
    var.enable_mcp ? ["export PROVISA_MCP_HOST=0.0.0.0", "export PROVISA_MCP_ROLE=${var.mcp_role}"] : []
  ))

  # Common boot prefix (installs Docker + gcloud, pulls the release assets, exports
  # the deployment env). Role-specific env (external DB for the coordinator, primary
  # IP for workers) and the AppImage launch line are appended per resource below.
  # Reproduced from the enterprise module with the SaaS delta: PROVISA_MULTITENANCY
  # is forced true (this module IS the multitenant control plane).
  base_startup = <<-SHELL
    #!/bin/bash
    set -euo pipefail
    cat > /etc/apt/apt.conf.d/99provisa-resilient <<'APTCONF'
    Acquire::http::Timeout "30";
    Acquire::https::Timeout "30";
    Acquire::Retries "3";
    Acquire::http::Pipeline-Depth "0";
    APTCONF
    apt-get update -qq
    apt-get install -y -qq apt-transport-https ca-certificates gnupg curl fuse unzip
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --batch --yes --dearmor -o /usr/share/keyrings/cloud.google.gpg
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list
    apt-get update -qq
    apt-get install -y -qq google-cloud-cli
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --batch --yes --dearmor -o /usr/share/keyrings/docker.gpg
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list
    apt-get update -qq
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin
    systemctl enable --now docker
    gh_base="https://github.com/${var.github_repo}/releases/download/${var.provisa_version}"
    curl -fSL --retry 5 --retry-all-errors "$gh_base/Provisa-${var.provisa_version}-linux-x86_64.AppImage" -o /opt/Provisa.AppImage
    chmod +x /opt/Provisa.AppImage
    curl -fSL --retry 5 --retry-all-errors "$gh_base/${local.images_zip}" -o /opt/${local.images_zip}
    curl -fSL --retry 5 --retry-all-errors "$gh_base/${local.plugins_tarball}" -o /opt/${local.plugins_tarball}
    export HOME=/root
    export PROVISA_VERSION="${var.provisa_version}"
    export PROVISA_ENGINE="trino"
    export PROVISA_ENGINE_URL=""
    export PROVISA_MATERIALIZE_URL=""
    export PROVISA_OBS_MODE="${var.obs_mode}"
    export PROVISA_OTLP_ENDPOINT="${var.otlp_endpoint}"
    export PROVISA_INSTALL_DEMO="${var.install_demo ? "y" : "n"}"
    export PROVISA_DOCKER_MODE="system"
    export PROVISA_IDP="${var.auth_provider}"
    # SaaS control plane: multitenant onboarding is always on (first user = platform
    # superadmin; later users self-create orgs / redeem invites). REQ-1266.
    export PROVISA_MULTITENANCY="true"
    # REQ-1330: SaaS is the only deployment mode that sends mail, and it sends through
    # the transactional-provider adapter — never SMTP into a mailbox host or a local relay.
    export PROVISA_MAIL_PROVIDER="resend"
    export PROVISA_EMAIL_API_KEY='${var.email_api_key}'
    export PROVISA_MAIL_FROM="${var.mail_from_address}"
    export PROVISA_MAIL_BASE_URL="${var.mail_base_url}"
    export FIREBASE_PROJECT_ID="${var.firebase_project_id}"
    export FIREBASE_SERVICE_ACCOUNT_KEY='${var.firebase_service_account_key}'
    export VITE_FIREBASE_API_KEY="${var.firebase_web_api_key}"
    export VITE_FIREBASE_AUTH_DOMAIN="${var.firebase_web_auth_domain}"
    export VITE_FIREBASE_PROJECT_ID="${var.firebase_project_id}"
    export VITE_AZURE_TENANT="${var.azure_tenant_id}"
    ${local.protocol_exports}
    export UI_PORT=${local.protocols.ui.port}
    %{if var.tls_cert_pem != "" && var.tls_key_pem != ""}
    mkdir -p /etc/provisa/tls
    printf '%s' '${base64encode(var.tls_cert_pem)}' | base64 -d > /etc/provisa/tls/node.crt
    printf '%s' '${base64encode(var.tls_key_pem)}' | base64 -d > /etc/provisa/tls/node.key
    chmod 600 /etc/provisa/tls/node.key
    export PROVISA_TLS_CERT=/etc/provisa/tls/node.crt
    export PROVISA_TLS_KEY=/etc/provisa/tls/node.key
    %{endif}
    cd /opt
  SHELL

  # External control-plane DB env (Cloud SQL). docker-compose.app.yml interpolates
  # PG_* / PLATFORM_DATABASE_URL / TENANT_DATABASE_URL from CONFIG_DB_*, and
  # first-launch persists these into the systemd EnvironmentFile (allowlist in
  # packaging/linux/first-launch.sh). PROVISA_EXTERNAL_CONTROL_DB flags the mode.
  external_db_exports = <<-SHELL
    export PROVISA_EXTERNAL_CONTROL_DB=1
    export CONFIG_DB_HOST="${google_sql_database_instance.main.private_ip_address}"
    export CONFIG_DB_PORT=5432
    export CONFIG_DB_NAME="${google_sql_database.provisa.name}"
    export CONFIG_DB_USER="${google_sql_user.provisa.name}"
    export CONFIG_DB_PASSWORD='${random_password.db.result}'
  SHELL

  metadata_ssh = var.ssh_public_key != "" ? { ssh-keys = var.ssh_public_key } : {}
}

# ── Private DNS (coordinator is the workers' reachable control/query endpoint) ───

resource "google_dns_managed_zone" "internal" {
  name        = "provisa-saas-internal"
  dns_name    = "provisa-saas.internal."
  description = "Private zone for intra-cluster DNS (SaaS)"
  visibility  = "private"

  private_visibility_config {
    networks {
      network_url = google_compute_network.main.id
    }
  }
}

resource "google_dns_record_set" "coordinator" {
  name         = "coordinator.provisa-saas.internal."
  managed_zone = google_dns_managed_zone.internal.name
  type         = "A"
  ttl          = 30
  rrdatas      = [google_compute_instance.coordinator.network_interface[0].network_ip]
}

# ── Coordinator — Trino coordinator + TCP listeners + stateful singletons ────────
# Runs the app tier, the query planner, and the in-process protocol listeners.
# The control-plane DB is offloaded to Cloud SQL (external_db_exports); redis/minio
# ride the coordinator at low scale (see the plan doc's service-placement table).

resource "google_compute_instance" "coordinator" {
  name         = "provisa-saas-coordinator"
  machine_type = var.coordinator_machine_type
  zone         = var.zone
  tags         = ["provisa-saas-node"]
  labels       = merge(local.all_labels, { role = "coordinator" })

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.disk_gb
      # pd-balanced: with include-coordinator=false this disk is boot/images/logs
      # only (no scan spill), so pd-ssd IOPS buys nothing here.
      type = "pd-balanced"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.nodes.id
    access_config {}
  }

  service_account {
    email  = google_service_account.provisa.email
    scopes = ["cloud-platform"]
  }

  # The front door stops/starts this instance; terraform must not "fix" the
  # resulting TERMINATED state (or flag it as drift) on the next apply.
  desired_status = null

  depends_on = [google_sql_user.provisa]

  metadata = merge(local.metadata_ssh, {
    startup-script = <<-SHELL
      ${local.base_startup}
      ${local.external_db_exports}
      /opt/Provisa.AppImage \
        --non-interactive \
        --role primary \
        --ram-gb 0
    SHELL
  })
}

# ── Worker MIG — autoscaled Trino workers (Spot, min→max) ────────────────────────
# Data plane: metered compute that scales to zero at idle. Workers are NOT in the
# LB; they reach the coordinator's query engine + control plane over the VPC.

resource "google_compute_instance_template" "worker" {
  name_prefix  = "provisa-saas-worker-"
  machine_type = var.worker_machine_type
  tags         = ["provisa-saas-node"]
  labels       = merge(local.all_labels, { role = "worker" })

  disk {
    source_image = "ubuntu-os-cloud/ubuntu-2204-lts"
    disk_size_gb = var.disk_gb
    disk_type    = "pd-ssd"
    boot         = true
    auto_delete  = true
  }

  network_interface {
    subnetwork = google_compute_subnetwork.nodes.id
    access_config {}
  }

  service_account {
    email  = google_service_account.provisa.email
    scopes = ["cloud-platform"]
  }

  # Spot for the metered scale-to-zero data plane (worker_use_spot=false only for a
  # guaranteed-warm SLA, which cannot ride a preemptible instance).
  dynamic "scheduling" {
    for_each = var.worker_use_spot ? [1] : []
    content {
      provisioning_model          = "SPOT"
      preemptible                 = true
      automatic_restart           = false
      instance_termination_action = "STOP"
    }
  }

  metadata = merge(local.metadata_ssh, {
    startup-script = <<-SHELL
      ${local.base_startup}
      ${local.external_db_exports}
      /opt/Provisa.AppImage \
        --non-interactive \
        --role secondary \
        --primary-ip coordinator.provisa-saas.internal \
        --ram-gb 0
    SHELL
  })

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [google_compute_instance.coordinator]
}

resource "google_compute_region_instance_group_manager" "workers" {
  name               = "provisa-saas-workers"
  region             = var.region
  base_instance_name = "provisa-saas-worker"

  version {
    instance_template = google_compute_instance_template.worker.id
  }

  # Autoscaler owns the running count; keep the MIG target unset so a min=0 idle
  # deployment truly scales to zero.
  dynamic "named_port" {
    for_each = local.enabled_protocols
    content {
      name = named_port.key
      port = named_port.value.port
    }
  }
}

resource "google_compute_region_autoscaler" "workers" {
  name   = "provisa-saas-workers"
  region = var.region
  target = google_compute_region_instance_group_manager.workers.id

  autoscaling_policy {
    min_replicas    = var.worker_min_nodes
    max_replicas    = var.worker_max_nodes
    cooldown_period = 60

    cpu_utilization {
      target = var.autoscale_cpu_target
    }
  }
}

# ── Front door — always-free e2-micro owning the shared IP ──────────────────────
# Replaces the passthrough NLB (which cost ~$18/mo in forwarding-rule hours and
# could never wake a stopped backend). A single-file TCP proxy splices every
# protocol port to the coordinator, starts it on a hit while it is stopped
# (HTTPS gets a "waking" page, raw TCP is held until the listener is up), and
# stops it after idle_stop_minutes of zero traffic. cloud.provisa.dev keeps
# pointing at this same static IP.

resource "google_compute_address" "shared" {
  name   = "provisa-saas-shared-ip"
  region = var.region
}

resource "google_service_account" "front_door" {
  account_id   = "provisa-saas-front-door"
  display_name = "Provisa SaaS Front Door"
}

# Bearer token for the front door's authenticated wake/verify endpoint
# (GET /status, POST /wake on front_door_status_port).
resource "random_password" "front_door_token" {
  length  = 48
  special = false
}

resource "google_compute_firewall" "front_door_status" {
  name    = "provisa-saas-allow-front-door-status"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = [tostring(var.front_door_status_port)]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["provisa-saas-node"]
}

resource "google_project_iam_custom_role" "front_door" {
  role_id     = "provisaSaasFrontDoor"
  title       = "Provisa SaaS Front Door"
  description = "Start/stop/get on compute instances for wake-on-hit and idle-stop."
  permissions = [
    "compute.instances.start",
    "compute.instances.stop",
    "compute.instances.get",
  ]
}

resource "google_project_iam_member" "front_door" {
  project = var.project
  role    = google_project_iam_custom_role.front_door.id
  member  = "serviceAccount:${google_service_account.front_door.email}"
}

locals {
  front_door_config = jsonencode({
    project      = var.project
    zone         = var.zone
    instance     = google_compute_instance.coordinator.name
    backend_host = trimsuffix(google_dns_record_set.coordinator.name, ".")
    ports = {
      for k, v in local.enabled_protocols : tostring(v.port) => {
        wake_style = k == "ui" ? "html" : (k == "api" ? "json" : "raw")
      }
    }
    idle_stop_minutes  = var.idle_stop_minutes
    boot_grace_seconds = var.front_door_boot_grace_minutes * 60
    status_port        = var.front_door_status_port
    status_token       = random_password.front_door_token.result
    tls_cert           = var.tls_cert_pem != "" ? "/etc/provisa-front-door/tls.crt" : ""
    tls_key            = var.tls_key_pem != "" ? "/etc/provisa-front-door/tls.key" : ""
  })
}

resource "google_compute_instance" "front_door" {
  name         = "provisa-saas-front-door"
  machine_type = var.front_door_machine_type
  zone         = var.zone
  tags         = ["provisa-saas-node"]
  labels       = merge(local.all_labels, { role = "front-door" })

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 30 # always-free tier ceiling (pd-standard)
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.nodes.id
    access_config {
      nat_ip = google_compute_address.shared.address
    }
  }

  service_account {
    email  = google_service_account.front_door.email
    scopes = ["cloud-platform"]
  }

  metadata = merge(local.metadata_ssh, {
    startup-script = <<-SHELL
      #!/bin/bash
      set -euo pipefail
      mkdir -p /etc/provisa-front-door /opt/provisa-front-door
      printf '%s' '${base64encode(file("${path.module}/front-door/proxy.py"))}' | base64 -d > /opt/provisa-front-door/proxy.py
      printf '%s' '${base64encode(local.front_door_config)}' | base64 -d > /etc/provisa-front-door/config.json
      %{if var.tls_cert_pem != "" && var.tls_key_pem != ""}
      printf '%s' '${base64encode(var.tls_cert_pem)}' | base64 -d > /etc/provisa-front-door/tls.crt
      printf '%s' '${base64encode(var.tls_key_pem)}' | base64 -d > /etc/provisa-front-door/tls.key
      chmod 600 /etc/provisa-front-door/tls.key
      %{endif}
      cat > /etc/systemd/system/provisa-front-door.service <<'UNIT'
      [Unit]
      Description=Provisa SaaS front-door proxy (wake-on-hit + idle-stop)
      After=network-online.target
      Wants=network-online.target
      [Service]
      ExecStart=/usr/bin/python3 /opt/provisa-front-door/proxy.py
      Restart=always
      RestartSec=3
      AmbientCapabilities=CAP_NET_BIND_SERVICE
      [Install]
      WantedBy=multi-user.target
      UNIT
      systemctl daemon-reload
      systemctl enable --now provisa-front-door
    SHELL
  })

  depends_on = [google_project_iam_member.front_door]
}
