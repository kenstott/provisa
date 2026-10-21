# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

# The three-device optimistic benchmark (demo/named/perf/METHODOLOGY.md): device 1 runs Provisa
# only, device 2 the load generator, device 3 the sources, the engine, the control plane, Redis
# and the telemetry pipeline.

variable "project" {
  description = "GCP project the three devices are created in."
  type        = string
}

variable "region" {
  description = "Region of var.zone."
  type        = string
}

variable "zone" {
  description = "The one zone all three devices run in."
  type        = string
}

# 16 physical cores each: a 32-vCPU type (GCP counts hardware threads).
variable "server_machine_type" {
  description = "Device 1: Provisa only."
  type        = string
  default     = "c4d-standard-32"
}

variable "client_machine_type" {
  description = "Device 2: the load generator."
  type        = string
  default     = "c4d-standard-32"
}

variable "backends_machine_type" {
  description = "Device 3: sources, engine, control plane, Redis, telemetry."
  type        = string
  default     = "c4d-standard-32"
}

variable "image" {
  description = "Boot image of every device."
  type        = string
  default     = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
}

variable "disk_gb" {
  description = "Boot disk size of every device, GB."
  type        = number
  default     = 200
}

variable "disk_type" {
  description = "Boot disk type. c4d takes Hyperdisk only."
  type        = string
  default     = "hyperdisk-balanced"
}

variable "subnet_cidr" {
  description = "The devices' private subnet."
  type        = string
  default     = "10.90.0.0/24"
}

variable "operator_ssh_cidrs" {
  description = "Addresses the operator connects over SSH from. Nothing else reaches the devices from outside."
  type        = list(string)
  validation {
    condition     = length(var.operator_ssh_cidrs) > 0
    error_message = "Name at least one address the operator connects from."
  }
}

# The maintainer's bench rule: Spot while the harness is being made to work, on-demand only for
# the final measured run, chosen explicitly.
variable "provisioning_model" {
  description = "SPOT (default) or ON_DEMAND (the final measured run)."
  type        = string
  default     = "SPOT"
  validation {
    condition     = contains(["SPOT", "ON_DEMAND"], var.provisioning_model)
    error_message = "provisioning_model is SPOT or ON_DEMAND."
  }
}

variable "commit" {
  description = "The commit under test: every device checks the repository out at it."
  type        = string
}

variable "repo_url" {
  description = "The repository the devices clone."
  type        = string
  default     = "https://github.com/kenstott/provisa.git"
}

variable "repo_dir" {
  description = "Where every device keeps the repository (PERF_REPO for three_device_run.sh)."
  type        = string
  default     = "/opt/provisa"
}

# The seed job (seed_lifecycle.sh) creates the backends device alone; a run creates all three.
variable "devices" {
  description = "The devices to create: a subset of server, client, backends."
  type        = list(string)
  default     = ["server", "client", "backends"]
  validation {
    condition     = length(var.devices) > 0 && alltrue([for d in var.devices : contains(["server", "client", "backends"], d)])
    error_message = "devices names one or more of server, client, backends."
  }
}

# seed: the seed job, which brings the data stack up and seeds it (hours at full scale).
# from_image: a run, whose backends device boots from the stored seed image and never seeds.
variable "seed_mode" {
  description = "seed (the seed job) or from_image (a run, the backends device booted from backends_image)."
  type        = string
  validation {
    condition     = contains(["seed", "from_image"], var.seed_mode)
    error_message = "seed_mode is seed or from_image."
  }
}

variable "backends_image" {
  description = "The stored seed image the backends device boots from (provisa-perf-seed-<seed hash 12>); seed_mode from_image only."
  type        = string
  default     = null
}

# Unset, the backends device is provisioned as the others are (var.provisioning_model): the
# --backends-provisioning-model option of bench_lifecycle.sh exists to keep the seeded device
# on-demand while the server and client stay on Spot, and gives this default in its contract.
variable "backends_provisioning_model" {
  description = "SPOT or ON_DEMAND for the backends device alone; unset, the same as provisioning_model."
  type        = string
  default     = null
  validation {
    condition     = var.backends_provisioning_model == null ? true : contains(["SPOT", "ON_DEMAND"], var.backends_provisioning_model)
    error_message = "backends_provisioning_model is SPOT or ON_DEMAND."
  }
}

# A run's checkpoints: each finished run's results, copied there as soon as it finishes, so a
# preempted device loses none. The seed job keeps no results and names none.
variable "results_bucket" {
  description = "Name of the bucket a run checkpoints its results to (unique per run); null for the seed job."
  type        = string
  default     = null
}
