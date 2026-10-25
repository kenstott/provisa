terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    # gke.tf — the engine cluster only, which is declared against google-beta so that Autopilot
    # settings still landing in beta (cluster_autoscaling's auto_provisioning_defaults, REQ-1464)
    # are available on the one resource that needs them. Everything else stays on GA.
    google-beta = {
      source  = "hashicorp/google-beta"
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
    # gke.tf — the engine namespace and the RBAC that scopes the control plane to it. The
    # provisioner creates Deployments at query time but must never be able to create the Role that
    # lets it; that grant is made here, once, by the operator running terraform.
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
  zone    = var.zone
}

provider "google-beta" {
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

  # REQ-1447: the engine cluster is VPC-native, so pods and services are ALIAS ranges on this
  # same subnet rather than a route-based overlay. Flat pod addressing is what lets the
  # control-plane VM reach a shard's POD directly — the address the provisioner reads off
  # the Kubernetes API on each wake, with no cluster DNS and no load balancer in the path.
  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = var.pods_cidr
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = var.services_cidr
  }
}

# ── Firewall (reused verbatim) ──────────────────────────────────────────────────

resource "google_compute_firewall" "protocols" {
  name    = "provisa-saas-allow-protocols"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = local.protocol_ports
  }

  source_ranges = var.client_cidrs
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

# The engine reads the OTel Iceberg tables from the coordinator's MinIO, and the pods that
# do it live in the GKE cluster, not on this VM: the intra_cluster rule matches by network
# tag and the pod alias ranges carry none, so the traffic arrives untagged from pods_cidr.
resource "google_compute_firewall" "minio_from_pods" {
  name    = "provisa-saas-allow-minio-from-pods"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["9000"]
  }

  source_ranges = [var.pods_cidr]
  target_tags   = ["provisa-saas-node"]
}

# The engine pods dial the control-plane DB too (its self-catalog and the Iceberg JDBC
# metastore), at the coordinator's VPC address control-db is published on.
resource "google_compute_firewall" "control_db_from_pods" {
  name    = "provisa-saas-allow-control-db-from-pods"
  network = google_compute_network.main.name

  allow {
    protocol = "tcp"
    ports    = ["5432"]
  }

  source_ranges = [var.pods_cidr]
  target_tags   = ["provisa-saas-node"]
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

# ── Private Service Access (the VPC's peering for Google-managed services) ─────────

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

# ── Control-plane DB password ───────────────────────────────────────────────────
# The control-db service's (control_db_setup); it is never printed and lives only in the
# node's provisa.env.

resource "random_password" "db" {
  length  = 32
  special = false
}

# The control plane's Redis and MinIO, and Grafana's admin: generated for this deployment. The
# sandbox VM dials the coordinator's Redis and MinIO, so both VMs are given the same ones.
resource "random_password" "store" {
  for_each = toset(["redis", "minio", "grafana"])
  length   = 32
  special  = false
}

# ── Locals ──────────────────────────────────────────────────────────────────────

locals {
  all_labels = merge(var.labels, { project = "provisa", deployment = "saas" })

  # REQ-1330: the invite link's origin. The SaaS control plane IS `cloud.<dns_zone>` (dns.tf
  # creates that record and the org wildcard beside it), so the build knows the address without
  # being told it twice — `mail_base_url` is only for a deployment whose UI is reached at some
  # other origin. A blank on both, with no host to name, fails the plan below rather than booting
  # a node that accepts invitations it cannot send.
  mail_base_url = var.mail_base_url != "" ? var.mail_base_url : (
    var.dns_zone != "" ? "https://cloud.${var.dns_zone}" : ""
  )

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
  # the deployment env). The external-DB and engine-cluster env and the AppImage
  # launch line are appended below. One VM boots from this now that engines are pods.
  # Reproduced from the enterprise module with the SaaS delta: PROVISA_MULTITENANCY
  # is forced true (this module IS the multitenant control plane).
  base_startup = <<-SHELL
    #!/bin/bash
    set -euo pipefail
    # A boot whose startup script is the one that last ran to its end (this release, this
    # configuration) has nothing to set up: Docker's restart policy has already brought the stack
    # back. Re-running it cost every wake the apt installs, the release download, the image loads
    # and a recreate of the stack that killed requests already being served. A changed script
    # (a new release, a changed variable) runs in full, and its last line records it.
    startup_sha="$(curl -fsS -H 'Metadata-Flavor: Google' \
      http://metadata.google.internal/computeMetadata/v1/instance/attributes/startup-script \
      | sha256sum | cut -d' ' -f1)"
    startup_done=/var/lib/provisa-startup.sha256
    if [ -f "$startup_done" ] && [ "$(cat "$startup_done")" = "$startup_sha" ]; then
      echo "startup script unchanged since its last complete run: nothing to set up"
      exit 0
    fi
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
    export PROVISA_MAIL_BASE_URL="${local.mail_base_url}"
    # REQ-125: break-glass superuser. first-launch persists these into the systemd
    # EnvironmentFile and the config's auth.superuser block resolves them by ${"$"}{env:...}.
    export PROVISA_SUPERUSER_USERNAME='${var.superuser_username}'
    export PROVISA_SUPERUSER_PASSWORD='${var.superuser_password}'
    export FIREBASE_PROJECT_ID="${var.firebase_project_id}"
    export FIREBASE_SERVICE_ACCOUNT_KEY='${var.firebase_service_account_key}'
    export VITE_FIREBASE_API_KEY="${var.firebase_web_api_key}"
    export VITE_FIREBASE_AUTH_DOMAIN="${var.firebase_web_auth_domain}"
    export VITE_FIREBASE_PROJECT_ID="${var.firebase_project_id}"
    export VITE_AZURE_TENANT="${var.azure_tenant_id}"
    ${local.protocol_exports}
    export UI_PORT=${local.protocols.ui.port}
    ${local.node_tls_sync}
    export PROVISA_TLS_CERT=/etc/provisa/tls/node.crt
    export PROVISA_TLS_KEY=/etc/provisa/tls/node.key
    cd /opt
  SHELL

  # The control-plane DB: the control-db service on this node (control_db_setup). The
  # control-plane compose base interpolates PG_* / PLATFORM_DATABASE_URL /
  # TENANT_DATABASE_URL from CONFIG_DB_*, and first-launch persists these into the systemd
  # EnvironmentFile on every boot (allowlist in packaging/linux/first-launch.sh).
  # first-launch persists them into the systemd EnvironmentFile, where `provisa start` reads them.
  store_credential_exports = <<-SHELL
    export PROVISA_REDIS_PASSWORD="${random_password.store["redis"].result}"
    export PROVISA_MINIO_ROOT_PASSWORD="${random_password.store["minio"].result}"
    export PROVISA_GRAFANA_ADMIN_PASSWORD="${random_password.store["grafana"].result}"
  SHELL

  external_db_exports = <<-SHELL
    export PROVISA_EXTERNAL_CONTROL_DB=1
    export CONFIG_DB_HOST=control-db
    export CONFIG_DB_PORT=5432
    export CONFIG_DB_NAME="${local.control_db_name}"
    export CONFIG_DB_USER="${local.control_db_user}"
    export CONFIG_DB_PASSWORD='${random_password.db.result}'
  SHELL

  control_db_name = "provisa"
  control_db_user = "provisa"

  # The control-plane Postgres runs on this node, on the object-store disk (which survives an
  # instance replacement), published only on the node's VPC address, for the engine pods
  # (control_db_from_pods), which the app names to the engine. It is an
  # extension compose file, which scripts/provisa layers onto the control-plane base. A nightly
  # pg_dump goes to the backups bucket; Persistent= runs a night the node slept through at its
  # next boot. Runs after object_store_mount.
  control_db_setup = <<-SHELL
    mkdir -p /var/lib/provisa-object-store/control-db /root/.provisa/extensions/control-db
    cat > /root/.provisa/extensions/control-db/docker-compose.control-db.yml <<'YAML'
    services:
      control-db:
        image: postgres:16.14@sha256:95206741a5b214807675e14165369d05b93a9cf692223b616d07cca227e74b0b
        restart: unless-stopped
        command: ["postgres", "-c", "max_connections=200"]
        environment:
          POSTGRES_DB: "$${CONFIG_DB_NAME:?}"
          POSTGRES_USER: "$${CONFIG_DB_USER:?}"
          POSTGRES_PASSWORD: "$${CONFIG_DB_PASSWORD:?}"
        ports:
          - "${google_compute_address.coordinator_internal.address}:5432:5432"
        volumes:
          - /var/lib/provisa-object-store/control-db:/var/lib/postgresql/data
        healthcheck:
          test: ["CMD-SHELL", "pg_isready -U $${CONFIG_DB_USER} -d $${CONFIG_DB_NAME}"]
          interval: 5s
          timeout: 5s
          retries: 30
      provisa:
        environment:
          PROVISA_ENGINE_CONTROL_PLANE_HOST: "${google_compute_address.coordinator_internal.address}"
        depends_on:
          control-db:
            condition: service_healthy
      provisa-ui:
        depends_on:
          control-db:
            condition: service_healthy
      provisa-sandbox:
        depends_on:
          control-db:
            condition: service_healthy
    YAML
    cat > /usr/local/bin/provisa-control-db-backup <<'BACKUP'
    #!/bin/bash
    set -euo pipefail
    cid=$(docker ps -q --filter label=com.docker.compose.service=control-db)
    [ -n "$cid" ] || { echo "control-db is not running" >&2; exit 1; }
    docker exec "$cid" pg_dump -U ${local.control_db_user} -Fc ${local.control_db_name} \
      | gsutil -q cp - "gs://${google_storage_bucket.control_db_backups.name}/nightly/provisa-$(date -u +%Y%m%dT%H%M%SZ).dump"
    BACKUP
    chmod 755 /usr/local/bin/provisa-control-db-backup
    cat > /etc/systemd/system/provisa-control-db-backup.service <<'UNIT'
    [Unit]
    Description=pg_dump of the Provisa control-plane DB to Cloud Storage
    [Service]
    Type=oneshot
    ExecStart=/usr/local/bin/provisa-control-db-backup
    UNIT
    cat > /etc/systemd/system/provisa-control-db-backup.timer <<'UNIT'
    [Unit]
    Description=Nightly control-plane DB backup
    [Timer]
    OnCalendar=*-*-* 03:00:00 UTC
    Persistent=true
    [Install]
    WantedBy=timers.target
    UNIT
    systemctl daemon-reload
    systemctl enable --now provisa-control-db-backup.timer
  SHELL

  # Engine-cluster env (REQ-1447/1450/1451). The CLUSTER_* + IMAGE settings say this
  # process may CREATE an engine (k8s_provisioner.provisioner_settings). Nothing here
  # says where a shard is DIALED: a shard is a pod that exists only between a wake and
  # the next idle-to-zero, so its address is read from the Kubernetes API on each wake
  # rather than written once by terraform. See the dns_config note in gke.tf for why
  # there is no resolvable name to write.
  engine_cluster_exports = <<-SHELL
    export PROVISA_ENGINE_CLUSTER_PROJECT="${var.project}"
    export PROVISA_ENGINE_CLUSTER_LOCATION="${local.engine_cluster_location}"
    export PROVISA_ENGINE_CLUSTER_NAME="${local.engine_cluster_name}"
    # Which of the two topologies was built. The provisioner reads it to decide
    # whether a shard pod carries a nodeSelector and toleration, which is the only
    # thing that differs between them (REQ-1465).
    export PROVISA_ENGINE_CLUSTER_MODE="${var.engine_cluster_mode}"
    # The zone a shard pod must land in. An autopilot cluster is regional, so
    # without this a pod can come up in a zone the control-plane VM is not in and
    # every byte between them is billed as cross-zone egress (REQ-1465).
    export PROVISA_ENGINE_CLUSTER_ZONE="${var.zone}"
    export PROVISA_ENGINE_NAMESPACE="${kubernetes_namespace.engines.metadata[0].name}"
    export PROVISA_ENGINE_IMAGE="${var.engine_image}"
    # The Arrow Flight SQL proxy, which rides in the shard pod beside the coordinator
    # rather than on this VM: it holds a JDBC connection to Trino, so it must live and
    # die with the engine whose address exists only while that engine is awake (REQ-1448).
    export PROVISA_ZAYCHIK_IMAGE="${var.zaychik_image}"
    # WHICH shard the control plane's own terminal is bound to. There is no matching TRINO_HOST:
    # the provisioner brings this shard up when boot finds it at zero replicas and reads the
    # resulting pod's address from the cluster, so a written-down host could only ever disagree
    # with it (REQ-1448).
    export PROVISA_ENGINE_SHARD="shared_1"
    export PROVISA_ENGINE_PORT=8080
    # How long a shard may sit unqueried before the reaper scales it to zero. DERIVED from the front
    # door's idle window rather than set on its own, because the reaper runs inside the control
    # plane: if the front door stops this VM first, the reaper dies with a shard still up and a
    # 6 vCPU / 24 GiB pod bills with nothing left that can stop it. Half the window leaves a whole
    # check interval of margin. The app also scales its shards down on shutdown (REQ-1629), so this
    # ordering is the first line of defence and not the only one.
    export PROVISA_ENGINE_IDLE_SECONDS="${floor(var.idle_stop_minutes * 60 / 2)}"
    # The OTel Iceberg tables live in this node's MinIO, and the engine reads them itself.
    # "http://minio:9000" is a compose service name that exists only on this VM, so the shard
    # resolved nothing and every ops query failed with UnknownHostException: minio. The engine
    # gets the VPC address instead (engine_visible_s3_endpoint), and the compose file binds
    # MinIO's published port to that same address so it is reachable but not public.
    export PROVISA_MINIO_BIND_IP="${google_compute_address.coordinator_internal.address}"
    export PROVISA_ENGINE_OTEL_S3_ENDPOINT="http://${google_compute_address.coordinator_internal.address}:9000"
  SHELL

  # Mount the object-store disk before the app starts. mkfs runs only on a blank device
  # (--nodiscard is for the first format; `blkid` returning nothing is the only condition
  # under which this formats anything), so a re-boot or an instance replacement reattaches
  # the existing warehouse instead of erasing it.
  object_store_mount = <<-SHELL
    dev=/dev/disk/by-id/google-provisa-object-store
    if ! blkid "$dev" >/dev/null 2>&1; then
      mkfs.ext4 -m 0 -E lazy_itable_init=0,lazy_journal_init=0,discard "$dev"
    fi
    mkdir -p /var/lib/provisa-object-store
    grep -q "^$dev " /etc/fstab || echo "$dev /var/lib/provisa-object-store ext4 discard,defaults,nofail 0 2" >> /etc/fstab
    mountpoint -q /var/lib/provisa-object-store || mount /var/lib/provisa-object-store
    # The warehouse (MinIO) and the catalog that names its tables (the control DB) both live on
    # this disk, and Docker's restart policy brings them back on a wake that runs none of this
    # script. Docker waits for the disk: a container started before it is mounted binds the empty
    # directory beneath it -- MinIO then serves no warehouse to a catalog that still names every
    # table (ICEBERG_MISSING_METADATA), and what it writes there is hidden once the disk mounts.
    mkdir -p /etc/systemd/system/docker.service.d
    cat > /etc/systemd/system/docker.service.d/provisa-object-store.conf <<'UNIT'
    [Unit]
    RequiresMountsFor=/var/lib/provisa-object-store
    UNIT
    systemctl daemon-reload
    export PROVISA_OBJECT_STORE_DIR=/var/lib/provisa-object-store
  SHELL

  metadata_ssh = var.ssh_public_key != "" ? { ssh-keys = var.ssh_public_key } : {}
}

# There is no private zone here any more. It existed so worker VMs could find the
# coordinator by name; nothing resolves an engine by name now — a shard is dialed at
# its pod IP, read from the Kubernetes API on each wake (see gke.tf's dns_config note).

# ── Control plane — app tier, TCP listeners, stateful singletons ─────────────────
# Runs the Provisa API, the UI, and the in-process protocol listeners, and NOTHING
# of the query engine: every coordinator is a pod on the GKE cluster (REQ-1451), so
# this VM's role is `control-plane` rather than the Trino-bearing `primary`. The
# control-plane DB is the control-db service on this VM (control_db_setup); redis/minio
# ride this VM at low scale (see the plan doc's service-placement table).
#
# The resource keeps its `coordinator` name: it is the same instance, and renaming
# it would destroy and recreate the live control plane for a label.

# A reserved internal address, not the ephemeral one GCE hands out. The engine pods
# read the OTel Iceberg tables straight from this node's MinIO, so the S3 endpoint they
# are given has to be an address that outlives a stop/start of the coordinator — and it
# has to be known BEFORE the instance is created, because the instance's own startup
# script is what carries the endpoint to the app (a reference to the instance's assigned
# network_ip from inside its own metadata is a dependency cycle).
resource "google_compute_address" "coordinator_internal" {
  name         = "provisa-saas-coordinator-internal"
  region       = var.region
  subnetwork   = google_compute_subnetwork.nodes.id
  address_type = "INTERNAL"
  # Pinned to the address the coordinator already holds: reserving it converts the
  # existing ephemeral lease rather than moving the node onto a new IP, which GCE
  # cannot do while the instance runs.
  address = var.coordinator_internal_ip
}

# The object store's data, on a disk with its own lifetime. The Iceberg metastore for the
# `otel` catalog is a table in the control-plane DB, which this disk also holds. The
# warehouse it points at was once a docker named volume on the boot disk, which did not
# outlive the instance: after
# the instance was replaced, every iceberg_tables row named a metadata object that no longer
# existed and every ops query failed with ICEBERG_MISSING_METADATA. A metastore that survives
# an instance must point at a warehouse that survives one too.
resource "google_compute_disk" "coordinator_data" {
  name = "provisa-saas-coordinator-data"
  zone = var.zone
  size = var.object_store_disk_gb
  # pd-standard: telemetry parquet written and scanned in bulk, which is what a spinning-rate
  # disk is good at, and the coordinator is stopped most of the day while the disk bills
  # around the clock — capacity is the cost here, not IOPS.
  type = "pd-standard"
  # Telemetry accumulates here; losing it to a terraform-initiated instance replacement is
  # the failure this disk exists to prevent.
  lifecycle {
    prevent_destroy = true
  }
}

# The control-plane DB's nightly dumps (control_db_setup), kept 30 days. A dump outside
# nightly/ (the final Cloud SQL dump) is kept until it is removed by hand.
resource "google_storage_bucket" "control_db_backups" {
  name                        = "${var.project}-control-db-backups"
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  labels                      = local.all_labels

  lifecycle_rule {
    condition {
      age            = 30
      matches_prefix = ["nightly/"]
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket_iam_member" "control_db_backups_writer" {
  bucket = google_storage_bucket.control_db_backups.name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.provisa.email}"
}

# gsutil cp reads the destination before it writes; viewer lets it, and still deletes nothing.
resource "google_storage_bucket_iam_member" "control_db_backups_reader" {
  bucket = google_storage_bucket.control_db_backups.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.provisa.email}"
}

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
      # pd-standard: no query runs here at all, so this disk is boot/images/logs
      # only (no scan spill). It bills whether or not the instance is running, which
      # after idle-stop is most of the month; the cost is a slower cold boot.
      type = "pd-standard"
    }
  }

  attached_disk {
    source      = google_compute_disk.coordinator_data.id
    device_name = "provisa-object-store"
  }

  network_interface {
    subnetwork = google_compute_subnetwork.nodes.id
    network_ip = google_compute_address.coordinator_internal.address
    access_config {}
  }

  service_account {
    email  = google_service_account.provisa.email
    scopes = ["cloud-platform"]
  }

  # Its certificate is read from Secret Manager at boot (secrets.tf).
  depends_on = [google_secret_manager_secret_iam_member.tls_reader]

  # The front door stops/starts this instance; terraform must not "fix" the
  # resulting TERMINATED state (or flag it as drift) on the next apply.
  desired_status = null

  lifecycle {
    # REQ-1330: an invitation is undeliverable without an absolute origin to link to, and the
    # node has no way to discover one, so a build that names neither a dns_zone nor a
    # mail_base_url is refused here instead of at the first invite.
    precondition {
      condition     = local.mail_base_url != ""
      error_message = "Set dns_zone (the control plane is cloud.<dns_zone>) or mail_base_url; invite links need an absolute origin."
    }
  }

  metadata = merge(local.metadata_ssh, {
    startup-script = <<-SHELL
      ${local.base_startup}
      ${local.external_db_exports}
      ${local.store_credential_exports}
      ${local.engine_cluster_exports}
      ${local.object_store_mount}
      ${local.control_db_setup}
      ${local.coordinator_sandbox_exports}
      /opt/Provisa.AppImage \
        --non-interactive \
        --role control-plane \
        --ram-gb 0
      echo "$startup_sha" > "$startup_done"
    SHELL
  })
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

  source_ranges = var.client_cidrs
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
    project  = var.project
    zone     = var.zone
    instance = google_compute_instance.coordinator.name
    # The control plane's internal address, not a name: the private zone that used to
    # publish one existed only so worker VMs could find the coordinator, and it went
    # with them. The front door is on this same VPC, so the VM's primary internal IP
    # is what it dials.
    backend_host = google_compute_instance.coordinator.network_interface[0].network_ip
    ports = {
      for k, v in local.enabled_protocols : tostring(v.port) => {
        wake_style = k == "ui" ? "html" : (k == "api" ? "json" : "raw")
      }
    }
    idle_stop_minutes = var.idle_stop_minutes
    # Whose traffic counts as use. An unsolicited connection from the internet reaches the
    # splice like any other and reset the idle clock there, so the reaper almost never fired
    # and the coordinator billed as a 24/7 VM. Scoping activity to the same CIDRs the firewall
    # admits means a port scan can neither wake the box nor keep it up.
    activity_cidrs     = var.client_cidrs
    boot_grace_seconds = var.front_door_boot_grace_minutes * 60
    status_port        = var.front_door_status_port
    status_token       = random_password.front_door_token.result
    tls_cert           = "/etc/provisa-front-door/tls.crt"
    tls_key            = "/etc/provisa-front-door/tls.key"
  })

  # The certificate on the coordinator and the sandbox VM: where the node reads it and where
  # first-launch copies it for the containers (/app/certs), restarting those that serve it.
  node_tls_sync = templatefile("${path.module}/tls/install-sync.sh.tftpl", {
    script      = base64encode(file("${path.module}/tls/provisa-tls-sync.sh"))
    project     = var.project
    cert_secret = local.tls_cert_secret
    key_secret  = local.tls_key_secret
    targets     = "/etc/provisa/tls/node.crt:/etc/provisa/tls/node.key /root/.provisa/certs/provisa.crt:/root/.provisa/certs/provisa.key"
    restart     = "docker ps --format '{{.Names}}' | grep -E '^compose-provisa(-ui|-sandbox|-sandbox-regions)?-1$' | xargs -r docker restart"
    before      = "docker.service provisa.service"
  })
  # The front door renews it (the one VM always on).
  front_door_tls_renew = templatefile("${path.module}/tls/install-renew.sh.tftpl", {
    script       = base64encode(file("${path.module}/tls/provisa-tls-renew.sh"))
    lego_url     = "https://github.com/go-acme/lego/releases/download/v5.4.1/lego_v5.4.1_linux_amd64.tar.gz"
    lego_sha256  = "ebb33f1bead5a7c99dd46f1c5734b44cf1eab5b5c12faf397cd14d50a5916419"
    project      = var.project
    cert_secret  = local.tls_cert_secret
    key_secret   = local.tls_key_secret
    token_secret = local.tls_token_secret
    acme_email   = var.acme_email
    domain       = var.tls_domain
    renew_days   = var.tls_renew_days
  })
  # And on the front door, which serves it at the shared address.
  front_door_tls_sync = templatefile("${path.module}/tls/install-sync.sh.tftpl", {
    script      = base64encode(file("${path.module}/tls/provisa-tls-sync.sh"))
    project     = var.project
    cert_secret = local.tls_cert_secret
    key_secret  = local.tls_key_secret
    targets     = "/etc/provisa-front-door/tls.crt:/etc/provisa-front-door/tls.key"
    restart     = "systemctl restart provisa-front-door"
    before      = "provisa-front-door.service"
  })
}

resource "google_compute_instance" "front_door" {
  name         = "provisa-saas-front-door"
  machine_type = var.front_door_machine_type
  zone         = var.zone
  tags         = ["provisa-saas-node"]
  labels       = merge(local.all_labels, { role = "front-door" })

  # REQ-1779: this is the sole entry point for cloud.provisa.dev — if it is unreachable, the
  # wake mechanism that would bring the coordinator back is unreachable too. Pinned explicitly
  # (rather than left as GCE's implicit default for a non-Spot instance) so drift here is
  # visible in a plan instead of silent. Recovers the instance automatically after a host
  # crash or GCP-initiated maintenance; does not cover a wedged-but-technically-running VM —
  # that gap needs health-check-triggered auto-healing (a managed-instance-group conversion,
  # staged separately per REQ-1779's scratchpad, since it also needs a stateful external IP and
  # the Terraform provider has open issues in exactly that feature).
  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
  }

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
      printf '%s' '${base64encode(file("${path.module}/front-door/binary-streams.js"))}' | base64 -d > /opt/provisa-front-door/binary-streams.js
      printf '%s' '${base64encode(local.front_door_config)}' | base64 -d > /etc/provisa-front-door/config.json
      ${local.front_door_tls_sync}
      ${local.front_door_tls_renew}
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
      systemctl enable provisa-front-door
      # restart, not enable --now: re-running this script to ship a new proxy.py is a no-op for a
      # unit that is already active, which leaves the old code serving.
      systemctl restart provisa-front-door
    SHELL
  })

  depends_on = [
    google_project_iam_member.front_door,
    google_secret_manager_secret_iam_member.tls_reader,
    google_secret_manager_secret_iam_member.tls_renewer,
    google_secret_manager_secret_iam_member.dns_token_reader,
  ]
}
