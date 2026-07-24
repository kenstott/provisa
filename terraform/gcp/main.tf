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

resource "google_compute_firewall" "protocols" {
  name    = "provisa-allow-protocols"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = local.protocol_ports
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
    ports    = local.protocol_ports
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
    plugins = local.plugins_object
  }

  provisioner "local-exec" {
    interpreter = ["/bin/bash", "-c"]
    command     = <<-EOT
      set -euo pipefail
      tmp="$(mktemp -d)"
      trap 'rm -rf "$tmp"' EXIT
      gh release download "${var.provisa_version}" --repo "${var.github_repo}" \
        --pattern "Provisa-${var.provisa_version}-linux-x86_64.AppImage" \
        --pattern "${local.images_zip}" \
        --pattern "${local.plugins_tarball}" -D "$tmp"
      gsutil cp "$tmp/Provisa-${var.provisa_version}-linux-x86_64.AppImage" \
        "gs://${var.gcs_bucket}/${var.gcs_object}"
      gsutil cp "$tmp/${local.images_zip}" \
        "gs://${var.gcs_bucket}/${local.images_object}"
      gsutil cp "$tmp/${local.plugins_tarball}" \
        "gs://${var.gcs_bucket}/${local.plugins_object}"
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

  # Trino custom-connector jars ride a separate release asset (the slim AppImage
  # excludes trino/plugins/ to stay under GitHub's 2 GB limit). first-launch's
  # load_trino_plugins searches cwd (/opt) for this tarball, so stage it to GCS
  # beside the AppImage — the VM has GCS read but no GitHub auth.
  plugins_tarball = "provisa-trino-plugins-${var.provisa_version}.tar.gz"
  plugins_object  = "${dirname(var.gcs_object)}/${local.plugins_tarball}"

  # ── Protocol surface ─────────────────────────────────────────────────────────
  # One row per externally-reachable protocol. `enabled` gates the whole chain:
  # firewall port, health check, backend service, forwarding rule, instance-group
  # named_port, and (for wire protocols) the container listener env var. Add a row
  # here and every layer follows. api uses an HTTPS /health probe (REQ-1227: TLS on
  # every endpoint); the rest use a TCP connect probe. `env`/`port_env` drive
  # first-launch's protocol overlay.
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
  # Ports opened to the internet / to GCP health-check probers.
  protocol_ports = [for k, v in local.enabled_protocols : tostring(v.port)]
  # Shell `export` lines fed into base_startup: the container listener env var for
  # each enabled wire protocol (first-launch persists these and its protocol
  # overlay publishes the matching container port). api/ui/flight are served
  # unconditionally by the app image, so only the opt-in protocols need a toggle.
  protocol_exports = join("\n    ", concat(
    [for k, v in local.enabled_protocols : "export ${v.env}=${v.port}" if contains(["pgwire", "bolt", "mcp", "grpc"], k)],
    var.enable_mcp ? ["export PROVISA_MCP_HOST=0.0.0.0", "export PROVISA_MCP_ROLE=${var.mcp_role}"] : []
  ))

  base_startup = <<-SHELL
    #!/bin/bash
    set -euo pipefail
    # apt's HTTP method pipelines requests by default and, when a Canonical mirror
    # (e.g. security.ubuntu.com) half-closes a socket mid-response, the method sits
    # in CLOSE-WAIT forever with no timeout — wedging `apt-get update` indefinitely.
    # Under `set -e` that never errors, so first-launch never starts. Force a hard
    # per-connection timeout, bounded retries, and disable pipelining to make the
    # fresh-boot apt phase deterministic.
    cat > /etc/apt/apt.conf.d/99provisa-resilient <<'APTCONF'
    Acquire::http::Timeout "30";
    Acquire::https::Timeout "30";
    Acquire::Retries "3";
    Acquire::http::Pipeline-Depth "0";
    APTCONF
    # Ubuntu 22.04 base images ship no gcloud. Add the Cloud SDK apt repo first.
    apt-get update -qq
    apt-get install -y -qq apt-transport-https ca-certificates gnupg curl fuse unzip
    # GCE re-runs this startup-script on every boot/reset. gpg --dearmor prompts on
    # /dev/tty before overwriting an existing keyring; with no tty the re-run dies
    # (exit 2), bricking the node on any reboot. --batch --yes forces overwrite so
    # the script is idempotent.
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --batch --yes --dearmor -o /usr/share/keyrings/cloud.google.gpg
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" > /etc/apt/sources.list.d/google-cloud-sdk.list
    apt-get update -qq
    apt-get install -y -qq google-cloud-cli
    # Rootful system Docker: on a single-tenant VM the box is the isolation
    # boundary, so the bundled rootless daemon (uidmap/iptables/non-root/lingering
    # gymnastics, and it refuses to run as root) buys nothing. first-launch attaches
    # to this socket via PROVISA_DOCKER_MODE=system.
    #
    # Use Docker's official repo, NOT Ubuntu's docker.io: the CLI (scripts/provisa
    # compose_cmd) requires Compose v2 (`docker compose`), which docker.io omits —
    # only docker-compose-plugin provides it, and the stack fails to start without it.
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --batch --yes --dearmor -o /usr/share/keyrings/docker.gpg
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" > /etc/apt/sources.list.d/docker.list
    apt-get update -qq
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin
    systemctl enable --now docker
    gsutil cp gs://${var.gcs_bucket}/${var.gcs_object} /opt/Provisa.AppImage
    chmod +x /opt/Provisa.AppImage
    # Stage the amd64 core-images zip beside the AppImage. first-launch searches cwd
    # for provisa-core-images-amd64-$PROVISA_VERSION.zip and docker-loads it locally
    # (airgap path), so we cd /opt before launching below.
    gsutil cp gs://${var.gcs_bucket}/${local.images_object} /opt/${local.images_zip}
    # Trino plugins tarball, staged beside the AppImage. first-launch's
    # load_trino_plugins finds it via cwd (we cd /opt below) and extracts it into
    # compose/trino/plugins/ so the bind-mounts resolve (else Trino crash-loops:
    # "No service providers of type io.trino.spi.Plugin").
    gsutil cp gs://${var.gcs_bucket}/${local.plugins_object} /opt/${local.plugins_tarball}
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
    # Attach to the rootful system Docker installed above, not bundled rootless.
    export PROVISA_DOCKER_MODE="system"
    # Auth (REQ-972..979 parity). PROVISA_IDP drives _auto_configure_idp; the
    # server reads these at runtime, so first-launch persists them into the
    # systemd unit's EnvironmentFile.
    export PROVISA_IDP="${var.auth_provider}"
    export FIREBASE_PROJECT_ID="${var.firebase_project_id}"
    export FIREBASE_SERVICE_ACCOUNT_KEY='${var.firebase_service_account_key}'
    # Opt-in wire-protocol listeners (pgwire/bolt/mcp/grpc). first-launch persists
    # these into the systemd EnvironmentFile and its protocol overlay publishes the
    # matching container ports; the NetLB above fronts each one.
    ${local.protocol_exports}
    # UI host-publish port. The UI container listens on 3000 (fixed in the node
    # overlay's uvicorn command); the base compose publishes $${UI_PORT}:3000 on the
    # host. Moving it to 443 lets https://cloud.provisa.dev resolve with no port
    # suffix (.dev is HSTS-preloaded, so browsers force https:443). first-launch
    # persists UI_PORT into the systemd EnvironmentFile so `provisa start`'s compose
    # interpolation picks it up.
    export UI_PORT=${local.protocols.ui.port}
    %{ if var.tls_cert_pem != "" && var.tls_key_pem != "" }
    # Operator-supplied wildcard TLS cert (REQ-1239). Written here and adopted by
    # first-launch's ensure_tls_certs via PROVISA_TLS_CERT/KEY, which fans it out to
    # every listener. base64 sidesteps PEM newline/quoting hazards in the metadata heredoc.
    mkdir -p /etc/provisa/tls
    printf '%s' '${base64encode(var.tls_cert_pem)}' | base64 -d > /etc/provisa/tls/node.crt
    printf '%s' '${base64encode(var.tls_key_pem)}' | base64 -d > /etc/provisa/tls/node.key
    chmod 600 /etc/provisa/tls/node.key
    export PROVISA_TLS_CERT=/etc/provisa/tls/node.crt
    export PROVISA_TLS_KEY=/etc/provisa/tls/node.key
    %{ endif }
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

# ── Regional External Passthrough LB — ONE shared IP, every protocol port ─────
# The subdomain-as-org model (REQ-1233/1253) requires {org}.provisa.dev to reach
# every protocol on a single A record: the org name resolves to one IP and the
# client connects the protocol's port (bolt 7687, pgwire 5439, …). A backend-
# service passthrough NLB with all_ports=true fronts every listener on one static
# IP — the destination port is preserved to the node, so a single `*.provisa.dev`
# record serves api/ui/flight/pgwire/bolt/mcp/grpc alike. One HTTPS /health probe
# on the API port (REQ-1227) gates backend liveness for the whole stack; first-
# launch brings all listeners up together, so app-level /health is the signal.
# The firewall (local.protocol_ports) still restricts which ports actually reach
# the node, so all_ports on the rule is not a widening of the attack surface.

resource "google_compute_address" "shared" {
  name   = "provisa-shared-ip"
  region = var.region
}

resource "google_compute_region_health_check" "shared" {
  name   = "provisa-shared-health"
  region = var.region

  # HTTPS health check: GCP does not validate the cert, so the node's self-signed
  # (or operator wildcard) cert is accepted.
  https_health_check {
    port         = local.protocols.api.port
    request_path = local.protocols.api.path
  }

  check_interval_sec  = 30
  healthy_threshold   = 2
  unhealthy_threshold = 3
}

resource "google_compute_region_backend_service" "shared" {
  name                  = "provisa-shared"
  region                = var.region
  protocol              = "TCP"
  load_balancing_scheme = "EXTERNAL"
  health_checks         = [google_compute_region_health_check.shared.id]

  backend {
    group          = google_compute_instance_group.nodes.id
    balancing_mode = "CONNECTION"
  }
}

resource "google_compute_forwarding_rule" "shared" {
  name                  = "provisa-shared"
  region                = var.region
  ip_address            = google_compute_address.shared.id
  ip_protocol           = "TCP"
  all_ports             = true
  load_balancing_scheme = "EXTERNAL"
  backend_service       = google_compute_region_backend_service.shared.id
}

# ── Instance Group (unmanaged) for LB backends ─────────────────────────────────

resource "google_compute_instance_group" "nodes" {
  name    = "provisa-nodes"
  zone    = var.zone
  network = google_compute_network.main.id

  instances = concat(
    [google_compute_instance.primary.self_link],
    google_compute_instance.secondary[*].self_link
  )

  dynamic "named_port" {
    for_each = local.enabled_protocols
    content {
      name = named_port.key
      port = named_port.value.port
    }
  }
}
