terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

# ── Networking ─────────────────────────────────────────────────────────────────

resource "google_compute_network" "main" {
  name                    = "provisa-network"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "nodes" {
  name          = "provisa-nodes"
  region        = var.region
  network       = google_compute_network.main.id
  ip_cidr_range = var.network_cidr
}

# ── Firewall Rules ─────────────────────────────────────────────────────────────

resource "google_compute_firewall" "api" {
  name    = "provisa-allow-api"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["8000"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["provisa-node"]
}

resource "google_compute_firewall" "flight" {
  name    = "provisa-allow-flight"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["8815"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["provisa-node"]
}

resource "google_compute_firewall" "intra_cluster" {
  name    = "provisa-allow-intra-cluster"
  network = google_compute_network.main.name

  allow {
    protocol = "all"
  }

  source_tags = ["provisa-node"]
  target_tags = ["provisa-node"]
}

resource "google_compute_firewall" "ssh" {
  count   = var.ssh_public_key != "" && var.admin_cidr != "" ? 1 : 0
  name    = "provisa-allow-ssh"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = [var.admin_cidr]
  target_tags   = ["provisa-node"]
}

# ── Health check firewall (GCP LB probes) ──────────────────────────────────────

resource "google_compute_firewall" "lb_health" {
  name    = "provisa-allow-lb-health"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["8000", "8815"]
  }

  source_ranges = ["35.191.0.0/16", "130.211.0.0/22"]
  target_tags   = ["provisa-node"]
}

# ── Service Account — GCS read for AppImage download ──────────────────────────

resource "google_service_account" "provisa" {
  account_id   = "provisa-node"
  display_name = "Provisa Node"
}

resource "google_storage_bucket_iam_member" "appimage_reader" {
  bucket = var.gcs_bucket
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.provisa.email}"
}

# ── Stage release artifacts from GitHub → GCS ─────────────────────────────────
# Pulls the AppImage + core-images zip for var.provisa_version from the GitHub
# release and uploads them to the bucket the VMs read at boot. Requires `gh` and
# `gsutil` authenticated on the machine running Terraform. Disable with
# stage_from_github=false to manage the objects yourself.

resource "null_resource" "stage_artifacts" {
  count = var.stage_from_github ? 1 : 0

  triggers = {
    version = var.provisa_version
    bucket  = var.gcs_bucket
    object  = var.gcs_object
    images  = local.images_object
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      tmp="$(mktemp -d)"
      trap 'rm -rf "$tmp"' EXIT
      gh release download "${var.provisa_version}" --repo "${var.github_repo}" \
        --pattern "Provisa-${var.provisa_version}-linux-x86_64.AppImage" \
        --pattern "${local.images_zip}" -D "$tmp"
      gsutil cp "$tmp/Provisa-${var.provisa_version}-linux-x86_64.AppImage" \
        "gs://${var.gcs_bucket}/${var.gcs_object}"
      gsutil cp "$tmp/${local.images_zip}" \
        "gs://${var.gcs_bucket}/${local.images_object}"
    EOT
  }
}

# ── Locals ─────────────────────────────────────────────────────────────────────

locals {
  machine_ram = {
    "n2-standard-4"  = 16
    "n2-standard-8"  = 32
    "n2-standard-16" = 64
    "n2-standard-32" = 128
    "n2-highmem-8"   = 64
    "n2-highmem-16"  = 128
    "n2-highmem-32"  = 256
  }
  effective_ram        = var.ram_budget_gb > 0 ? var.ram_budget_gb : lookup(local.machine_ram, var.machine_type, 32)
  effective_worker_ram = var.ram_budget_gb > 0 ? var.ram_budget_gb : lookup(local.machine_ram, var.worker_machine_type, 128)

  all_labels = merge(var.labels, { project = "provisa" })

  # Core-images zip lives in the same GCS directory as the AppImage. dirname of a
  # bare object (no slash) is ".", so keep gcs_object prefixed (default: releases/).
  images_zip    = "provisa-core-images-amd64-${var.provisa_version}.zip"
  images_object = "${dirname(var.gcs_object)}/${local.images_zip}"

  base_startup = <<-SHELL
    #!/bin/bash
    set -euo pipefail
    # Ubuntu 22.04 base images ship no gcloud. Add the Cloud SDK apt repo first.
    apt-get update -qq
    apt-get install -y -qq apt-transport-https ca-certificates gnupg curl fuse unzip
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list
    apt-get update -qq
    apt-get install -y -qq google-cloud-cli
    gsutil cp gs://${var.gcs_bucket}/${var.gcs_object} /opt/Provisa.AppImage
    chmod +x /opt/Provisa.AppImage
    # Stage the amd64 core-images zip beside the AppImage. first-launch searches cwd
    # for provisa-core-images-amd64-$PROVISA_VERSION.zip and docker-loads it locally
    # (airgap path), so we cd /opt before launching below.
    gsutil cp gs://${var.gcs_bucket}/${local.images_object} /opt/${local.images_zip}
    # The metadata script runner executes as root with no HOME; first-launch.sh
    # needs it for ~/.provisa and ~/.local/bin under `set -u`.
    export HOME=/root
    export PROVISA_VERSION="${var.provisa_version}"
    # Deployment choices (parity with the desktop wizard, REQ-972..979). The Linux
    # first-launch reads these env vars in --non-interactive mode.
    export PROVISA_ENGINE="${var.federation_engine}"
    export PROVISA_ENGINE_URL="${var.engine_url}"
    export PROVISA_MATERIALIZE_URL="${var.materialize_url}"
    export PROVISA_OBS_MODE="${var.obs_mode}"
    export PROVISA_OTLP_ENDPOINT="${var.otlp_endpoint}"
    export PROVISA_INSTALL_DEMO="${var.install_demo ? "y" : "n"}"
    # Auth (REQ-972..979 parity). PROVISA_IDP drives _auto_configure_idp; the
    # server reads these at runtime, so first-launch persists them into the
    # systemd unit's EnvironmentFile.
    export PROVISA_IDP="${var.auth_provider}"
    export FIREBASE_PROJECT_ID="${var.firebase_project_id}"
    export FIREBASE_SERVICE_ACCOUNT_KEY='${var.firebase_service_account_key}'
    cd /opt
  SHELL

  metadata_ssh = var.ssh_public_key != "" ? { ssh-keys = var.ssh_public_key } : {}
}

# ── Private DNS ────────────────────────────────────────────────────────────────

resource "google_dns_managed_zone" "internal" {
  name        = "provisa-internal"
  dns_name    = "provisa.internal."
  description = "Private zone for intra-cluster DNS"
  visibility  = "private"

  private_visibility_config {
    networks {
      network_url = google_compute_network.main.id
    }
  }
}

resource "google_dns_record_set" "primary" {
  name         = "primary.provisa.internal."
  managed_zone = google_dns_managed_zone.internal.name
  type         = "A"
  ttl          = 30
  rrdatas      = [google_compute_instance.primary.network_interface[0].network_ip]
}

# ── Primary Node ───────────────────────────────────────────────────────────────

resource "google_compute_instance" "primary" {
  name         = "provisa-primary"
  machine_type = var.machine_type
  zone         = var.zone
  tags         = ["provisa-node"]
  labels       = merge(local.all_labels, { role = "primary" })

  depends_on = [null_resource.stage_artifacts]

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

  metadata = merge(local.metadata_ssh, {
    startup-script = <<-SHELL
      ${local.base_startup}
      /opt/Provisa.AppImage \
        --non-interactive \
        --role primary \
        --ram-gb ${local.effective_ram}
    SHELL
  })
}

# ── Secondary Nodes ────────────────────────────────────────────────────────────

resource "google_compute_instance" "secondary" {
  count        = max(var.node_count - 1, 0)
  name         = "provisa-secondary-${count.index + 1}"
  machine_type = var.worker_machine_type
  zone         = var.zone
  tags         = ["provisa-node"]
  labels       = merge(local.all_labels, { role = "secondary" })

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

  depends_on = [google_compute_instance.primary]

  metadata = merge(local.metadata_ssh, {
    startup-script = <<-SHELL
      ${local.base_startup}
      /opt/Provisa.AppImage \
        --non-interactive \
        --role secondary \
        --primary-ip primary.provisa.internal \
        --ram-gb ${local.effective_worker_ram}
    SHELL
  })
}

# ── Regional External Passthrough LB — HTTP API (port 8000) ───────────────────

resource "google_compute_address" "api" {
  name   = "provisa-api-ip"
  region = var.region
}

resource "google_compute_region_health_check" "api" {
  name   = "provisa-api-health"
  region = var.region

  http_health_check {
    port         = 8000
    request_path = "/health"
  }

  check_interval_sec  = 30
  healthy_threshold   = 2
  unhealthy_threshold = 3
}

resource "google_compute_region_backend_service" "api" {
  name                  = "provisa-api"
  region                = var.region
  protocol              = "TCP"
  load_balancing_scheme = "EXTERNAL"
  health_checks         = [google_compute_region_health_check.api.id]

  backend {
    group          = google_compute_instance_group.nodes.id
    balancing_mode = "CONNECTION"
  }
}

resource "google_compute_forwarding_rule" "api" {
  name                  = "provisa-api"
  region                = var.region
  ip_address            = google_compute_address.api.id
  ip_protocol           = "TCP"
  port_range            = "8000"
  load_balancing_scheme = "EXTERNAL"
  backend_service       = google_compute_region_backend_service.api.id
}

# ── Regional External Passthrough LB — Arrow Flight (port 8815) ───────────────

resource "google_compute_address" "flight" {
  name   = "provisa-flight-ip"
  region = var.region
}

resource "google_compute_region_health_check" "flight" {
  name   = "provisa-flight-health"
  region = var.region

  tcp_health_check {
    port = 8815
  }

  check_interval_sec  = 30
  healthy_threshold   = 2
  unhealthy_threshold = 3
}

resource "google_compute_region_backend_service" "flight" {
  name                  = "provisa-flight"
  region                = var.region
  protocol              = "TCP"
  load_balancing_scheme = "EXTERNAL"
  health_checks         = [google_compute_region_health_check.flight.id]

  backend {
    group          = google_compute_instance_group.nodes.id
    balancing_mode = "CONNECTION"
  }
}

resource "google_compute_forwarding_rule" "flight" {
  name                  = "provisa-flight"
  region                = var.region
  ip_address            = google_compute_address.flight.id
  ip_protocol           = "TCP"
  port_range            = "8815"
  load_balancing_scheme = "EXTERNAL"
  backend_service       = google_compute_region_backend_service.flight.id
}

# ── Instance Group (unmanaged) for LB backends ─────────────────────────────────

resource "google_compute_instance_group" "nodes" {
  name      = "provisa-nodes"
  zone      = var.zone
  network   = google_compute_network.main.id

  instances = concat(
    [google_compute_instance.primary.self_link],
    google_compute_instance.secondary[*].self_link
  )

  named_port {
    name = "api"
    port = 8000
  }

  named_port {
    name = "flight"
    port = 8815
  }
}
