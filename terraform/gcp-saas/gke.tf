# Copyright (c) 2026 Kenneth Stott
#
# Engine cluster (REQ-1447, REQ-1450, REQ-1451).
#
# One GKE cluster runs Trino and nothing else. The control plane stays on its own
# VM with Cloud SQL — it serializes org-runtime rebuilds on an in-process lock, so
# it is deliberately not a scheduled workload (REQ-1451).
#
# The cluster is ZONAL. A regional control plane bills the same $0.10/hr fee that
# the free tier covers exactly once, so a second zone would put a fixed line on a
# floor that must stay at ~$19/mo with no customers. Engine availability is bought
# back by rescheduling onto a new node, not by a standing hot spare (REQ-1459).

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

resource "google_container_cluster" "engine" {
  name     = "provisa-saas-engine"
  location = var.zone

  depends_on = [google_project_service.container]

  # The cluster is created with a default pool only so it can be destroyed
  # immediately: every node here belongs to an explicitly-managed pool whose floor
  # is zero, and a default pool would hold nodes that no shard accounts for.
  remove_default_node_pool = true
  initial_node_count       = 1

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
  # svc.cluster.local means nothing outside the cluster. Cloud DNS in VPC_SCOPE
  # publishes every Service record into this VPC under a domain unique to the
  # cluster, which is what lets PROVISA_ISOLATED_ENGINE_HOST_TEMPLATE keep naming a
  # hostname and nothing else change (REQ-1451). An internal load balancer would be
  # the alternative and would put forwarding-rule hours back on the zero-customer
  # floor (REQ-1453).
  dns_config {
    cluster_dns        = "CLOUD_DNS"
    cluster_dns_scope  = "VPC_SCOPE"
    cluster_dns_domain = var.engine_cluster_dns_domain
  }

  workload_identity_config {
    workload_pool = "${var.project}.svc.id.goog"
  }

  # Nodes carry public addresses. Private nodes would need Cloud NAT, which bills
  # by the hour whether or not a node exists — a fixed line on the zero-customer
  # floor, which must hold at ~$19/mo (REQ-1453). Reachability is closed off at
  # the firewall instead: the node tag below is default-deny inbound, and Trino is
  # dialed only from inside the VPC.
  node_config {
    tags = ["provisa-saas-engine"]
  }

  # Kubernetes' own idle cost is the control plane; the addons that bill per node
  # are left off so an empty cluster is genuinely free.
  addons_config {
    http_load_balancing {
      disabled = true
    }
  }

  deletion_protection = false

  resource_labels = var.labels
}

# ── Shared shard (Starter lane, REQ-1450) ───────────────────────────────────────
# Every Starter org queries this shard. Its floor is zero nodes: an empty pool
# costs nothing, while a node with no pods on it bills in full — which is why
# idle-to-zero scales the POOL and not the Deployment (REQ-1448).

resource "google_container_node_pool" "shared_1" {
  name     = "shared-1"
  location = var.zone
  cluster  = google_container_cluster.engine.name

  # Left unset at create so the autoscaler owns the count from the first apply
  # onward; a fixed initial_node_count would restore a node on every plan.
  initial_node_count = 0

  autoscaling {
    min_node_count = 0
    max_node_count = var.shared_shard_max_nodes
  }

  node_config {
    machine_type = var.shared_shard_machine_type
    spot         = var.shared_shard_use_spot

    service_account = google_service_account.engine_node.email
    oauth_scopes    = ["https://www.googleapis.com/auth/cloud-platform"]

    # The shard identity a Trino pod is scheduled by. `orgs.shard` names one of
    # these, and the pod's nodeSelector carries it back.
    labels = merge(var.labels, {
      "provisa.dev/lane"  = "shared"
      "provisa.dev/shard" = "shared_1"
    })

    # Nothing but the shard's own pods lands here. Without the taint, any workload
    # tolerating a general pool would land on a highmem node sized for Trino.
    taint {
      key    = "provisa.dev/shard"
      value  = "shared_1"
      effect = "NO_SCHEDULE"
    }

    tags = ["provisa-saas-engine"]

    workload_metadata_config {
      mode = "GKE_METADATA"
    }
  }

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  # A scale-in must not cut a running query. Trino drains on SIGTERM via
  # SHUTTING_DOWN; the pod's terminationGracePeriodSeconds covers the longest
  # permitted query, and this window lets the node wait for it.
  upgrade_settings {
    max_surge       = 1
    max_unavailable = 0
  }

  lifecycle {
    ignore_changes = [initial_node_count]
  }
}

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
# VM's service account needs the Kubernetes and node-pool verbs directly — this is
# what replaces the mounted Docker socket the co-tenant provisioner used.

resource "google_project_iam_custom_role" "engine_operator" {
  role_id     = "provisaSaasEngineOperator"
  title       = "Provisa SaaS Engine Operator"
  description = "Resize engine node pools and read cluster credentials for in-band provisioning."
  permissions = [
    "container.clusters.get",
    "container.clusters.getCredentials",
    # Resizing a node pool is a cluster verb, not a node-pool one: GKE exposes no
    # container.nodePools.* permission to custom roles, so setSize on a pool is
    # authorized by container.clusters.update.
    "container.clusters.update",
    "container.operations.get",
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
  host                   = "https://${google_container_cluster.engine.endpoint}"
  token                  = data.google_client_config.current.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.engine.master_auth[0].cluster_ca_certificate)
}

resource "kubernetes_namespace" "engines" {
  metadata {
    name   = "provisa-engines"
    labels = { "provisa.dev/managed-by" = "provisa-control-plane" }
  }
}

# GKE presents an IAM service account to Kubernetes RBAC as a User whose name is
# the account's email, so that — not a ServiceAccount subject — is what binds.
locals {
  control_plane_rbac_user = google_service_account.provisa.email
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

  subject {
    api_group = "rbac.authorization.k8s.io"
    kind      = "User"
    name      = local.control_plane_rbac_user
  }
}

# Nodes are cluster-scoped, and the wake path counts READY nodes to decide whether
# a pod can be placed at all — initialNodeCount only records what the pool was last
# set to. Read-only, and nodes only: this is the one thing the namespaced Role
# cannot express.
resource "kubernetes_cluster_role" "engine_nodes" {
  metadata {
    name = "provisa-engine-node-reader"
  }

  rule {
    api_groups = [""]
    resources  = ["nodes"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_cluster_role_binding" "engine_nodes" {
  metadata {
    name = "provisa-engine-node-reader"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.engine_nodes.metadata[0].name
  }

  subject {
    api_group = "rbac.authorization.k8s.io"
    kind      = "User"
    name      = local.control_plane_rbac_user
  }
}

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
