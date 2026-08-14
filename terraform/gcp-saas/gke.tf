# Copyright (c) 2026 Kenneth Stott
#
# Engine cluster (REQ-1447, REQ-1450, REQ-1451, REQ-1464, REQ-1465).
#
# One GKE cluster runs Trino and nothing else. The control plane stays on its own
# VM with Cloud SQL — it serializes org-runtime rebuilds on an in-process lock, so
# it is deliberately not a scheduled workload (REQ-1451).
#
# TWO topologies are declared here and var.engine_cluster_mode picks one. Both are
# maintained, because which is cheaper depends on duty cycle and the duty cycle is
# not known until real customers are querying (REQ-1465):
#
#   autopilot — no node pools at all. Billing is per pod REQUEST, so a shard at
#     zero replicas bills nothing and GKE's system Deployments are Google's
#     problem. ~$0.3851/hr per running 6 vCPU / 24 GiB shard.
#   standard  — a small always-on Spot system pool (~$11/mo) carries the system
#     Deployments, and each shard owns a pool autoscaling 0..1, tainted so nothing
#     else can hold it up. ~$0.3616/hr per running shard.
#
# Crossover is ~460 shard-hours a month. Autopilot is the launch shape because with
# no customers the floor is what matters; standard wins once the shared lane runs
# more than about fifteen hours a day. This is a utilization crossover, not a
# dev/prod split.
#
# Switching REPLACES the cluster — enable_autopilot and dns_config are both
# immutable — so the switch is a scheduled maintenance window, announced by the
# control plane's maintenance banner, not a rolling change (REQ-1466).
#
# The $0.10/hr cluster fee is identical in both modes and the free tier covers
# exactly one cluster, which is why there is one.

# The GCE-only topology this replaces never touched container.googleapis.com, so a
# project that has only ever run the old stack has the API off and the cluster below
# fails with SERVICE_DISABLED. Enabled here rather than by hand so the stack applies
# from a clean project.
resource "google_project_service" "container" {
  project = var.project
  service = "container.googleapis.com"

  # Leave the API on if the stack is torn down: disabling it would take down any
  # other cluster in the project.
  disable_on_destroy = false
}

locals {
  autopilot = var.engine_cluster_mode == "autopilot"

  # Everything downstream reads the cluster through these four, so nothing else in
  # the stack has to know which of the two topologies was built.
  engine_cluster_name     = local.autopilot ? google_container_cluster.engine_autopilot[0].name : google_container_cluster.engine_standard[0].name
  engine_cluster_location = local.autopilot ? google_container_cluster.engine_autopilot[0].location : google_container_cluster.engine_standard[0].location
  engine_cluster_endpoint = local.autopilot ? google_container_cluster.engine_autopilot[0].endpoint : google_container_cluster.engine_standard[0].endpoint
  engine_cluster_ca       = local.autopilot ? google_container_cluster.engine_autopilot[0].master_auth[0].cluster_ca_certificate : google_container_cluster.engine_standard[0].master_auth[0].cluster_ca_certificate
}

# ── autopilot: the launch shape (REQ-1464) ──────────────────────────────────────

resource "google_container_cluster" "engine_autopilot" {
  count = local.autopilot ? 1 : 0

  # google-beta, for one field: dns_config.additive_vpc_scope_dns_domain below is
  # not in the GA provider's schema at v5, and it is the only DNS scope Autopilot
  # accepts.
  provider = google-beta

  name = "provisa-saas-engine"

  # REGIONAL, because the API refuses a zonal Autopilot cluster outright
  # ("Autopilot clusters must be regional clusters"). That costs nothing extra
  # here: the $0.10/hr fee is per cluster either way and there are no standing
  # per-zone nodes to replicate, which is the reason a regional STANDARD cluster
  # would have been rejected below. It does mean a pod can be placed in a zone the
  # control-plane VM is not in, so the provisioner pins shard pods to
  # PROVISA_ENGINE_CLUSTER_ZONE and cross-zone bytes stay off the bill.
  location = var.region

  depends_on = [google_project_service.container]

  # AUTOPILOT, and the reason is the zero-customer floor (REQ-1448, REQ-1464).
  #
  # The standard block below could not rest at zero without its own system pool.
  # With the default pool removed, GKE's own system workloads — kube-dns,
  # metrics-server, konnectivity-agent, event-exporter, kube-state-metrics,
  # gmp-operator — are *Deployments*, not DaemonSets, so the shard pool was the
  # only place left to schedule them. Scaling that pool to zero left them Pending
  # and the cluster autoscaler rebuilt a node within seconds (`TriggeredScaleUp
  # ... gmp-operator ... shared-1 1->2`), so the shared lane billed an
  # e2-highmem-8 around the clock whether or not anybody queried it.
  #
  # Autopilot has no node pools to hold: billing is per POD REQUEST, the system
  # workloads are Google's problem rather than ours, and a cluster with no running
  # workloads scales to zero nodes and zero cost above the cluster fee.
  enable_autopilot = true

  network    = google_compute_network.main.id
  subnetwork = google_compute_subnetwork.nodes.id

  # VPC-native: pods hold routable VPC addresses from the subnet's alias ranges,
  # so the control-plane VM reaches a coordinator Service directly and
  # PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE stays the only routing knob.
  networking_mode = "VPC_NATIVE"
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  release_channel {
    channel = var.engine_release_channel
  }

  # The control plane is a VM, not a pod, so it cannot resolve kube-dns at all —
  # svc.cluster.local means nothing outside the cluster. Cloud DNS publishes the
  # shard's Service record into this VPC under a domain unique to the cluster,
  # which is what lets PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE keep naming a
  # hostname and nothing else change (REQ-1451).
  #
  # ADDITIVE vpc scope, not VPC_SCOPE: "VPC scope is not supported on Autopilot
  # clusters; only cluster scope is supported. If you need to resolve headless
  # Service names that run in GKE Autopilot clusters, you must use additive VPC
  # scope." A shard's Service is headless by design (clusterIP: None — pod IPs are
  # VPC-native and route VPC-wide, a ClusterIP does not), so additive scope covers
  # exactly the records this dials, under the same
  # <service>.<namespace>.svc.<domain> name. Creation-time only.
  dns_config {
    cluster_dns                   = "CLOUD_DNS"
    cluster_dns_scope             = "CLUSTER_SCOPE"
    additive_vpc_scope_dns_domain = var.engine_cluster_dns_domain
  }

  # Nodes carry public addresses. Private nodes would need Cloud NAT, which bills
  # by the hour whether or not a node exists — a fixed line on the zero-customer
  # floor, which must hold at ~$19/mo (REQ-1453). Reachability is closed off at
  # the firewall instead: the node tag below is default-deny inbound, and Trino is
  # dialed only from inside the VPC. Autopilot creates the nodes, so the tag is
  # set on the auto-provisioning defaults rather than on a pool we declare.
  node_pool_auto_config {
    network_tags {
      tags = ["provisa-saas-engine"]
    }
  }

  # An engine node runs untrusted tenant SQL, so it gets telemetry write and image
  # pull and nothing else — the same separation the standard node pools have.
  cluster_autoscaling {
    auto_provisioning_defaults {
      service_account = google_service_account.engine_node.email
      oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    }
  }

  deletion_protection = false

  resource_labels = var.labels
}

# ── standard: the post-launch shape (REQ-1465) ──────────────────────────────────

resource "google_container_cluster" "engine_standard" {
  count = local.autopilot ? 0 : 1

  name = "provisa-saas-engine"

  # ZONAL. A regional control plane bills the same $0.10/hr fee the free tier
  # covers exactly once, so a second zone would put a fixed line on the floor, and
  # a regional standard cluster would also replicate every node pool per zone.
  # Engine availability is bought back by rescheduling onto a new node, not by a
  # standing hot spare (REQ-1459).
  location = var.zone

  depends_on = [google_project_service.container]

  # The default pool is removed and replaced by the two pools below: one small
  # always-on pool for GKE's system Deployments, and one autoscaling 0..1 pool per
  # shard. That split is the entire reason this topology can rest near zero.
  remove_default_node_pool = true
  initial_node_count       = 1

  network    = google_compute_network.main.id
  subnetwork = google_compute_subnetwork.nodes.id

  networking_mode = "VPC_NATIVE"
  ip_allocation_policy {
    cluster_secondary_range_name  = "pods"
    services_secondary_range_name = "services"
  }

  release_channel {
    channel = var.engine_release_channel
  }

  # VPC_SCOPE, which standard supports and autopilot does not: it publishes every
  # Service — headless or not — into this VPC under the cluster's own domain, so
  # the control-plane VM resolves the same
  # <service>.<namespace>.svc.<domain> name it resolves on autopilot.
  # Creation-time only, which is half of why switching replaces the cluster.
  dns_config {
    cluster_dns        = "CLOUD_DNS"
    cluster_dns_scope  = "VPC_SCOPE"
    cluster_dns_domain = var.engine_cluster_dns_domain
  }

  deletion_protection = false

  resource_labels = var.labels
}

# GKE's system Deployments live here and nowhere else. UNTAINTED on purpose:
# kube-dns and the rest carry no tolerations of ours, so a taint would leave them
# Pending forever and the autoscaler would fight it. Spot, because a system pod
# rescheduling on preemption costs nothing that matters, and small, because it
# carries no query work.
resource "google_container_node_pool" "system" {
  count = local.autopilot ? 0 : 1

  name     = "system"
  cluster  = google_container_cluster.engine_standard[0].id
  location = google_container_cluster.engine_standard[0].location

  node_count = 1

  node_config {
    machine_type    = var.engine_system_pool_machine_type
    spot            = true
    disk_size_gb    = 32
    disk_type       = "pd-standard"
    service_account = google_service_account.engine_node.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    tags            = ["provisa-saas-engine"]
  }
}

# One pool per shard, autoscaling from ZERO. The taint is what makes zero
# reachable: only that shard's own pod tolerates it, so nothing GKE schedules can
# hold the node up and the pool drops back to no nodes as soon as the Deployment
# goes to zero replicas. The control plane never resizes this pool — the same
# replica patch that works on autopilot drives it, through the cluster autoscaler,
# which is why the provisioner differs between the two modes by nothing but the
# pod's nodeSelector and toleration (REQ-1465).
resource "google_container_node_pool" "shard" {
  for_each = local.autopilot ? toset([]) : toset(var.shared_shards)

  name     = replace(each.value, "_", "-")
  cluster  = google_container_cluster.engine_standard[0].id
  location = google_container_cluster.engine_standard[0].location

  autoscaling {
    min_node_count = 0
    max_node_count = 1
  }

  node_config {
    machine_type    = var.shared_shard_machine_type
    disk_size_gb    = 100
    disk_type       = "pd-balanced"
    service_account = google_service_account.engine_node.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]
    tags            = ["provisa-saas-engine"]

    labels = { "provisa.dev/shard" = each.value }

    taint {
      key    = "provisa.dev/shard"
      value  = each.value
      effect = "NO_SCHEDULE"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }
}

# ── Shared shard (Starter lane, REQ-1450) ───────────────────────────────────────
# A shard is a Deployment the control plane applies
# (provisa/federation/k8s_provisioner.py). On autopilot that is the whole story: a
# node is provisioned to fit the pod's requests and removed when the Deployment
# goes to zero replicas. On standard the same replica patch drives that shard's
# 0..1 pool through the cluster autoscaler. Either way idle-to-zero is a REPLICA
# count, never a pool resize (REQ-1464, REQ-1465). Isolation is the pod's
# Guaranteed QoS — requests equal limits — plus the control plane living outside
# the cluster entirely.

# ── Node identity ───────────────────────────────────────────────────────────────
# Separate from the control-plane VM's account: an engine node runs untrusted
# tenant SQL, so it gets telemetry write and image pull, and nothing else.

resource "google_service_account" "engine_node" {
  account_id   = "provisa-saas-engine-node"
  display_name = "Provisa SaaS Engine Node"
}

resource "google_project_iam_member" "engine_node" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/monitoring.metricWriter",
    "roles/artifactregistry.reader",
  ])

  project = var.project
  role    = each.value
  member  = "serviceAccount:${google_service_account.engine_node.email}"
}

# ── Control-plane access to the cluster ─────────────────────────────────────────
# The control plane creates and scales engines in-band on the query path, so its
# VM's service account needs the cluster-read verbs directly — this is what
# replaces the mounted Docker socket the co-tenant provisioner used.

resource "google_project_iam_custom_role" "engine_operator" {
  role_id     = "provisaSaasEngineOperator"
  title       = "Provisa SaaS Engine Operator"
  description = "Read engine cluster credentials for in-band provisioning."
  # READ ONLY at the GKE control-plane API: everything the provisioner changes it
  # changes through the Kubernetes API, authorized by the namespaced RBAC below.
  # container.clusters.update was here to authorize nodePools:setSize, and Autopilot
  # has no pool to size (REQ-1464).
  permissions = [
    "container.clusters.get",
    "container.clusters.getCredentials",
  ]
}

resource "google_project_iam_member" "engine_operator" {
  project = var.project
  role    = google_project_iam_custom_role.engine_operator.id
  member  = "serviceAccount:${google_service_account.provisa.email}"
}

# Workload-level access is granted by Kubernetes RBAC, below, rather than by a
# project-wide IAM role — roles/container.developer would hand the control plane
# every namespace in the cluster, including kube-system.

# ── Engine namespace and the control plane's RBAC (REQ-1450) ────────────────────
# Applied by the operator running terraform, not by the control plane at runtime:
# the account that creates engines must not be able to widen its own grant. The
# IAM role above deliberately stops at container.clusters.getCredentials, so this
# process could not create these objects even if it tried.

data "google_client_config" "current" {}

provider "kubernetes" {
  host                   = "https://${local.engine_cluster_endpoint}"
  token                  = data.google_client_config.current.access_token
  cluster_ca_certificate = base64decode(local.engine_cluster_ca)
}

resource "kubernetes_namespace" "engines" {
  metadata {
    name   = "provisa-engines"
    labels = { "provisa.dev/managed-by" = "provisa-control-plane" }
  }
}

# GKE presents an IAM service account to Kubernetes RBAC as a User — not a
# ServiceAccount subject — but which name it presents depends on how the token was
# minted. A token from the VM's metadata server carries the account's numeric
# unique ID, and the API server reported exactly that: `User "106253682883156708989"
# cannot list resource "nodes"`. Tokens minted from the email (gcloud, terraform)
# present the email. Bind both names: either is the same identity, and binding one
# alone leaves the other unauthorized.
locals {
  control_plane_rbac_users = [
    google_service_account.provisa.email,
    google_service_account.provisa.unique_id,
  ]
}

resource "kubernetes_role" "engine_operator" {
  metadata {
    name      = "provisa-engine-operator"
    namespace = kubernetes_namespace.engines.metadata[0].name
  }

  # Exactly what k8s_provisioner.py calls: apply a ConfigMap, a Service and a
  # Deployment, read the Deployment's readyReplicas, and patch its scale to zero.
  rule {
    api_groups = [""]
    resources  = ["configmaps", "services", "pods", "secrets"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }

  rule {
    api_groups = ["apps"]
    resources  = ["deployments", "deployments/scale"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }

  # Read-only: pod logs are how a coordinator that will not start is diagnosed.
  rule {
    api_groups = [""]
    resources  = ["pods/log"]
    verbs      = ["get", "list"]
  }
}

resource "kubernetes_role_binding" "engine_operator" {
  metadata {
    name      = "provisa-engine-operator"
    namespace = kubernetes_namespace.engines.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.engine_operator.metadata[0].name
  }

  dynamic "subject" {
    for_each = local.control_plane_rbac_users
    content {
      api_group = "rbac.authorization.k8s.io"
      kind      = "User"
      name      = subject.value
    }
  }
}

# No cluster-scoped node grant. The Standard wake counted READY nodes in the
# shard's pool to decide whether a pod could be placed; on Autopilot placement is
# the scheduler's business and readiness is readyReplicas, so the namespaced Role
# above is the whole grant (REQ-1464).

resource "google_compute_firewall" "engine_internal" {
  name    = "provisa-saas-engine-internal"
  network = google_compute_network.main.name

  description = "Control plane and engine pods reach Trino; nothing from outside the VPC does."

  allow {
    protocol = "tcp"
  }

  allow {
    protocol = "udp"
  }

  allow {
    protocol = "icmp"
  }

  source_ranges = [var.network_cidr, var.pods_cidr]
  target_tags   = ["provisa-saas-engine"]
}
