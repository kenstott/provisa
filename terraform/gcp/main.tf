terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
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

  base_startup = <<-SHELL
    #!/bin/bash
    set -euo pipefail
    apt-get update -qq
    apt-get install -y -qq google-cloud-cli fuse
    gsutil cp gs://${var.gcs_bucket}/${var.gcs_object} /opt/Provisa.AppImage
    chmod +x /opt/Provisa.AppImage
  SHELL

  metadata_ssh = var.ssh_public_key != "" ? { ssh-keys = var.ssh_public_key } : {}
}

# ── Primary Node ───────────────────────────────────────────────────────────────

resource "google_compute_instance" "primary" {
  name         = "provisa-primary"
  machine_type = var.machine_type
  zone         = var.zone
  tags         = ["provisa-node"]
  labels       = merge(local.all_labels, { role = "primary" })

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
        --primary-ip ${google_compute_instance.primary.network_interface[0].network_ip} \
        --ram-gb ${local.effective_worker_ram}
    SHELL
  })
}

# ── Regional External Passthrough LB — HTTP API (port 8000) ───────────────────

resource "google_compute_address" "api" {
  name   = "provisa-api-ip"
  region = var.region
}

resource "google_compute_health_check" "api" {
  name = "provisa-api-health"

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
  health_checks         = [google_compute_health_check.api.id]

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

resource "google_compute_health_check" "flight" {
  name = "provisa-flight-health"

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
  health_checks         = [google_compute_health_check.flight.id]

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
