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

resource "google_compute_firewall" "lb_health" {
  name    = "provisa-saas-allow-lb-health"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = local.protocol_ports
  }

  source_ranges = ["35.191.0.0/16", "130.211.0.0/22"]
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
      type  = "pd-ssd"
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

# ── Regional external passthrough NLB — ONE shared IP, every protocol port ────────
# Same pattern as enterprise (main.tf NLB block). Backend = the coordinator ONLY
# (raw-TCP protocol listeners live on it); workers are not LB-reachable.

resource "google_compute_address" "shared" {
  name   = "provisa-saas-shared-ip"
  region = var.region
}

resource "google_compute_region_health_check" "shared" {
  name   = "provisa-saas-shared-health"
  region = var.region

  https_health_check {
    port         = local.protocols.api.port
    request_path = local.protocols.api.path
  }

  check_interval_sec  = 30
  healthy_threshold   = 2
  unhealthy_threshold = 3
}

resource "google_compute_region_backend_service" "shared" {
  name                  = "provisa-saas-shared"
  region                = var.region
  protocol              = "TCP"
  load_balancing_scheme = "EXTERNAL"
  health_checks         = [google_compute_region_health_check.shared.id]

  backend {
    group          = google_compute_instance_group.coordinator.id
    balancing_mode = "CONNECTION"
  }
}

resource "google_compute_forwarding_rule" "shared" {
  name                  = "provisa-saas-shared"
  region                = var.region
  ip_address            = google_compute_address.shared.id
  ip_protocol           = "TCP"
  all_ports             = true
  load_balancing_scheme = "EXTERNAL"
  backend_service       = google_compute_region_backend_service.shared.id
}

# ── Unmanaged instance group — coordinator only (LB backend) ─────────────────────

resource "google_compute_instance_group" "coordinator" {
  name    = "provisa-saas-coordinator"
  zone    = var.zone
  network = google_compute_network.main.id

  instances = [google_compute_instance.coordinator.self_link]

  dynamic "named_port" {
    for_each = local.enabled_protocols
    content {
      name = named_port.key
      port = named_port.value.port
    }
  }
}
