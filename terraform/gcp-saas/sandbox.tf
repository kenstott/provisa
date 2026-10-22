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
    export PROVISA_SANDBOX_REGIONS_NODES="us=https://${var.sandbox_internal_ip}:8100,warehouse=https://${var.sandbox_internal_ip}:8101"
    export PROVISA_SANDBOX_INSTANCE="${local.sandbox_instance_path}"
  SHELL

  # The demo-stores sync (scripts/demo_stores_sync.sh) and its units, installed from the repo at
  # every boot; deploy-cloud.sh refreshes them between boots.
  # The demos each sandbox tier offers, from one list (packaging/linux/demo-stores/*-bundles):
  # the stores this VM runs are both tiers' demos.
  sandbox_tier_bundles = trimspace(file("${path.module}/../../packaging/linux/demo-stores/sandbox-tier-bundles"))
  regions_tier_bundles = trimspace(file("${path.module}/../../packaging/linux/demo-stores/regions-tier-bundles"))
  # The demos the VM's sync pulls: both tiers', those with a bundle -- a demo is bundled only once
  # it is published (demo.yaml `published: true`, .github/workflows/demo-bundles.yml), so a tier
  # may name one not yet published (retail), and the sync leaves it out until it is.
  demo_stores_bundles = join(",", [
    for d in split(",", "${local.sandbox_tier_bundles},${local.regions_tier_bundles}") : d
    if can(regex("(?m)^published: true$", file("${path.module}/../../demo/named/${d}/demo.yaml")))
  ])
  # The sync's settings, rendered from the one template deploy-cloud.sh renders too, so a patch
  # rewrites the file exactly as a boot writes it.
  demo_stores_env = trimspace(templatefile(
    "${path.module}/../../packaging/linux/demo-stores/sandbox-vm.env.tpl",
    { bundles = local.demo_stores_bundles }
  ))

  sandbox_demo_stores = <<-SHELL
    install -d /root/.provisa/bin
    echo '${base64encode(file("${path.module}/../../scripts/demo_stores_sync.sh"))}' | base64 -d > /root/.provisa/bin/demo_stores_sync.sh
    chmod 755 /root/.provisa/bin/demo_stores_sync.sh
    echo '${base64encode(file("${path.module}/../../scripts/demo_stores.py"))}' | base64 -d > /root/.provisa/bin/demo_stores.py
    echo '${base64encode(file("${path.module}/../../packaging/linux/demo-stores/docker-compose.shared.yml"))}' | base64 -d > /root/.provisa/bin/demo-stores-shared.yml
    echo '${base64encode(file("${path.module}/../../packaging/linux/demo-stores/provisa-demo-stores-boot.service"))}' | base64 -d > /etc/systemd/system/provisa-demo-stores-boot.service
    for unit in service timer path; do
      case "$unit" in
        service) b64='${base64encode(file("${path.module}/../../packaging/linux/demo-stores/provisa-demo-stores.service"))}' ;;
        timer) b64='${base64encode(file("${path.module}/../../packaging/linux/demo-stores/provisa-demo-stores.timer"))}' ;;
        path) b64='${base64encode(file("${path.module}/../../packaging/linux/demo-stores/provisa-demo-stores.path"))}' ;;
      esac
      echo "$b64" | base64 -d > /etc/systemd/system/provisa-demo-stores.$unit
    done
    cat > /root/.provisa/demo-stores.env <<'ENV'
    ${local.demo_stores_env}
    ENV
    systemctl daemon-reload
    # --now: a unit enabled while the VM boots does not start until the next boot.
    # The boot unit runs at every later start, before docker; this start's markers are new.
    systemctl enable provisa-demo-stores-boot.service
    systemctl enable --now provisa-demo-stores.timer provisa-demo-stores.path
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

  # Its certificate is read from Secret Manager at boot (secrets.tf).
  depends_on = [google_secret_manager_secret_iam_member.tls_reader]

  # It stops itself and the coordinator starts it; terraform must not undo either.
  desired_status = null

  metadata = merge(local.metadata_ssh, {
    startup-script = <<-SHELL
      ${local.base_startup}
      ${local.external_db_exports}
      ${local.store_credential_exports}
      ${local.engine_cluster_exports}
      # The sandbox tier alone, against the coordinator's control plane.
      export COMPOSE_PROFILES=sandbox
      export CONFIG_DB_HOST="${local.coordinator_ip}"
      export PROVISA_REDIS_HOST="${local.coordinator_ip}"
      export PROVISA_MINIO_HOST="${local.coordinator_ip}"
      # Interpolated by the compose for the control profile's minio, which does not run here.
      export PROVISA_OBJECT_STORE_DIR=/var/lib/provisa-object-store
      # The tier's port on this VM's VPC address, where the coordinator's UI server reaches it.
      export PROVISA_SANDBOX_PUBLISH="${var.sandbox_internal_ip}:8000"
      # REQ-2068: the regions sandbox tier's, likewise.
      export PROVISA_SANDBOX_REGIONS_PUBLISH="${var.sandbox_internal_ip}:8100"
      export PROVISA_SANDBOX_REGIONS_WAREHOUSE_PUBLISH="${var.sandbox_internal_ip}:8101"
      export PROVISA_SANDBOX_INSTANCE="${local.sandbox_instance_path}"
      export PROVISA_SANDBOX_IDLE_STOP_SECONDS="${var.sandbox_idle_stop_minutes * 60}"
      ${local.sandbox_demo_stores}
      /opt/Provisa.AppImage \
        --non-interactive \
        --role control-plane \
        --ram-gb 0
      echo "$startup_sha" > "$startup_done"
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
