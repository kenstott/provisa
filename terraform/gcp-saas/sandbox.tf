# The sandbox tier on a VM of its own (REQ-2006), when var.sandbox_vm: the demo sandboxes and
# the demos' stores, whose memory (measured in variables.tf, sandbox_machine_type) once ran the
# coordinator out of it. The same release and the same control-plane compose as the coordinator,
# with the "sandbox" profile alone (packaging/linux/first-launch.sh): the tier reaches the
# coordinator's control-plane database, redis and minio at its internal address, and the
# coordinator's UI server reaches the tier at this VM's.
#
# The VM stops itself once no sandbox has been used for sandbox_idle_stop_minutes
# (provisa.scheduler.jobs.stop_idle_sandbox_vm), and the UI server starts it when a request for
# the tier finds it down (provisa/sandbox_wake.py), both through the Compute API as the node
# service account, on this instance only.

locals {
  sandbox_instance_name = "provisa-saas-sandbox"
  sandbox_instance_path = "projects/${var.project}/zones/${var.zone}/instances/${local.sandbox_instance_name}"
  coordinator_ip        = google_compute_address.coordinator_internal.address

  # With the sandbox VM, the coordinator runs the control plane alone and its UI server sends
  # the tier's requests there, starting the VM when it finds it down. Without it, nothing: the
  # coordinator runs both profiles, as it always has.
  coordinator_sandbox_exports    = var.sandbox_vm ? local.coordinator_sandbox_vm_exports : ""
  coordinator_sandbox_vm_exports = <<-SHELL
    export COMPOSE_PROFILES=control
    export PROVISA_SANDBOX_API_URL="https://${var.sandbox_internal_ip}:8000"
    export PROVISA_SANDBOX_INSTANCE="${local.sandbox_instance_path}"
  SHELL

  # The demo-stores sync (scripts/demo_stores_sync.sh) and its units, installed from the repo at
  # every boot; deploy-cloud.sh refreshes them between boots.
  sandbox_demo_stores = <<-SHELL
    install -d /root/.provisa/bin
    echo '${base64encode(file("${path.module}/../../scripts/demo_stores_sync.sh"))}' | base64 -d > /root/.provisa/bin/demo_stores_sync.sh
    chmod 755 /root/.provisa/bin/demo_stores_sync.sh
    for unit in service timer path; do
      case "$unit" in
        service) b64='${base64encode(file("${path.module}/../../packaging/linux/demo-stores/provisa-demo-stores.service"))}' ;;
        timer) b64='${base64encode(file("${path.module}/../../packaging/linux/demo-stores/provisa-demo-stores.timer"))}' ;;
        path) b64='${base64encode(file("${path.module}/../../packaging/linux/demo-stores/provisa-demo-stores.path"))}' ;;
      esac
      echo "$b64" | base64 -d > /etc/systemd/system/provisa-demo-stores.$unit
    done
    cat > /root/.provisa/demo-stores.env <<'ENV'
    PROVISA_DEMO_BUNDLES=weblogs,files,sonnets,reviews
    DEMO_BUNDLES_DIR=/root/.provisa/sandbox-data/demo-bundles
    DEMO_PULL=docker exec compose-provisa-sandbox-1 python -m provisa.core.demo_bundles
    DEMO_PULL_INTO=/var/lib/provisa/demo-bundles
    DEMO_STORES_NETWORK=compose_default
    ENV
    systemctl daemon-reload
    systemctl enable provisa-demo-stores.timer provisa-demo-stores.path
  SHELL
}

resource "google_compute_address" "sandbox_internal" {
  count        = var.sandbox_vm ? 1 : 0
  name         = "provisa-saas-sandbox-internal"
  region       = var.region
  subnetwork   = google_compute_subnetwork.nodes.id
  address_type = "INTERNAL"
  address      = var.sandbox_internal_ip
}

resource "google_compute_instance" "sandbox" {
  count        = var.sandbox_vm ? 1 : 0
  name         = local.sandbox_instance_name
  machine_type = var.sandbox_machine_type
  zone         = var.zone
  # The coordinator's tag: intra_cluster admits each to the other (the database, redis and minio
  # one way, the tier the other).
  tags   = ["provisa-saas-node"]
  labels = merge(local.all_labels, { role = "sandbox" })

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.sandbox_disk_gb
      type  = "pd-standard"
    }
  }

  network_interface {
    subnetwork = google_compute_subnetwork.nodes.id
    network_ip = google_compute_address.sandbox_internal[0].address
    # Outbound only: the demo bundles and their images are pulled from the registry. Nothing
    # outside the VPC reaches it; every request comes through the coordinator's UI server.
    access_config {}
  }

  service_account {
    email  = google_service_account.provisa.email
    scopes = ["cloud-platform"]
  }

  # It stops itself and the coordinator starts it; terraform must not undo either.
  desired_status = null

  metadata = merge(local.metadata_ssh, {
    startup-script = <<-SHELL
      ${local.base_startup}
      ${local.external_db_exports}
      ${local.engine_cluster_exports}
      # The sandbox tier alone, against the coordinator's control plane.
      export COMPOSE_PROFILES=sandbox
      export CONFIG_DB_HOST="${local.coordinator_ip}"
      export PROVISA_REDIS_HOST="${local.coordinator_ip}"
      export PROVISA_MINIO_HOST="${local.coordinator_ip}"
      # Interpolated by the compose for the control profile's minio, which does not run here.
      export PROVISA_OBJECT_STORE_DIR=/var/lib/provisa-object-store
      export PROVISA_SANDBOX_INSTANCE="${local.sandbox_instance_path}"
      export PROVISA_SANDBOX_IDLE_STOP_SECONDS="${var.sandbox_idle_stop_minutes * 60}"
      ${local.sandbox_demo_stores}
      /opt/Provisa.AppImage \
        --non-interactive \
        --role control-plane \
        --ram-gb 0
    SHELL
  })
}

resource "google_project_iam_custom_role" "sandbox_power" {
  count       = var.sandbox_vm ? 1 : 0
  role_id     = "provisaSaasSandboxPower"
  title       = "Provisa SaaS sandbox VM power"
  description = "Start, stop, resume and read the sandbox VM: the coordinator wakes it, it stops itself."
  permissions = [
    "compute.instances.get",
    "compute.instances.start",
    "compute.instances.stop",
    "compute.instances.resume",
  ]
}

resource "google_compute_instance_iam_member" "sandbox_power" {
  count         = var.sandbox_vm ? 1 : 0
  project       = var.project
  zone          = var.zone
  instance_name = google_compute_instance.sandbox[0].name
  role          = google_project_iam_custom_role.sandbox_power[0].id
  member        = "serviceAccount:${google_service_account.provisa.email}"
}
