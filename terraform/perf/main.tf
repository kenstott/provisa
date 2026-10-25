# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

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

locals {
  machine_types = {
    server   = var.server_machine_type
    client   = var.client_machine_type
    backends = var.backends_machine_type
  }
  # Only the devices asked for: the seed job creates the backends device alone.
  devices = { for name, type in local.machine_types : name => type if contains(var.devices, name) }
  # The backends device may be provisioned apart (an on-demand seeded device beside Spot server
  # and client); unset, it is provisioned as the others are (variables.tf, backends_provisioning_model).
  provisioning_model = {
    server   = var.provisioning_model
    client   = var.provisioning_model
    backends = var.backends_provisioning_model == null ? var.provisioning_model : var.backends_provisioning_model
  }
  # A run's backends device boots from the stored seed image, its data already seeded; every other
  # device, and the seed job's backends device, from the plain image.
  boot_image = {
    server   = var.image
    client   = var.image
    backends = var.seed_mode == "from_image" ? var.backends_image : var.image
  }
}

resource "google_compute_network" "perf" {
  name                    = "provisa-perf"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "perf" {
  name          = "provisa-perf"
  region        = var.region
  network       = google_compute_network.perf.id
  ip_cidr_range = var.subnet_cidr
}

# The three devices reach each other on any port; nothing else does.
resource "google_compute_firewall" "between_devices" {
  name    = "provisa-perf-between-devices"
  network = google_compute_network.perf.name
  allow {
    protocol = "all"
  }
  source_tags = ["provisa-perf"]
  target_tags = ["provisa-perf"]
}

# The operator's SSH, from the addresses given.
resource "google_compute_firewall" "operator_ssh" {
  name    = "provisa-perf-operator-ssh"
  network = google_compute_network.perf.name
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges = var.operator_ssh_cidrs
  target_tags   = ["provisa-perf"]
}

# The devices are placed close together, so the round trip between them is short and the same
# from one run to the next.
resource "google_compute_resource_policy" "compact" {
  name   = "provisa-perf-compact"
  region = var.region
  group_placement_policy {
    collocation = "COLLOCATED"
  }
}

resource "google_compute_instance" "device" {
  for_each     = local.devices
  name         = "provisa-perf-${each.key}"
  machine_type = each.value
  zone         = var.zone
  tags         = ["provisa-perf"]
  labels = {
    provisa-perf-device = each.key
  }

  lifecycle {
    precondition {
      condition     = !(each.key == "backends" && var.seed_mode == "from_image" && var.backends_image == null)
      error_message = "seed_mode from_image needs backends_image: the seed image seed_lifecycle.sh stores."
    }
  }

  boot_disk {
    initialize_params {
      image = local.boot_image[each.key]
      size  = var.disk_gb
      type  = var.disk_type
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.perf.id
    nic_type   = "GVNIC"
    # An external address for the operator's SSH only; the firewall admits nothing else.
    access_config {}
  }

  resource_policies = [google_compute_resource_policy.compact.id]

  scheduling {
    # A compact placement policy needs instances that stop for host maintenance.
    on_host_maintenance = "TERMINATE"
    automatic_restart   = false
    provisioning_model  = local.provisioning_model[each.key]
    preemptible         = local.provisioning_model[each.key] == "SPOT"
    # A reclaimed Spot device stops (its disk and seeded data kept), as an idle one powers off.
    instance_termination_action = local.provisioning_model[each.key] == "SPOT" ? "STOP" : null
  }

  # Run at every boot: Docker, the repository at var.commit, uv sync, the idle shutdown, and on
  # the backends device the data stack, seeded (seed_mode seed) or found seeded on the image
  # (from_image) (startup.sh.tftpl).
  metadata = {
    startup-script = templatefile("${path.module}/startup.sh.tftpl", {
      device    = each.key
      commit    = var.commit
      repo_url  = var.repo_url
      repo_dir  = var.repo_dir
      seed_mode = var.seed_mode
    })
  }
}

# A run's checkpoints (three_device_run.sh copies each finished run here; bench_lifecycle.sh copies
# the bucket into the run directory before destroy). Destroy deletes it with whatever it holds.
resource "google_storage_bucket" "results" {
  count                       = var.results_bucket == null ? 0 : 1
  name                        = var.results_bucket
  location                    = upper(var.region)
  force_destroy               = true
  uniform_bucket_level_access = true
}
