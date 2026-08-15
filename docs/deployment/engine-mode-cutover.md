# Engine cluster mode cutover (REQ-1465)

Switching `var.engine_cluster_mode` between `autopilot` and `standard` **replaces the GKE cluster**:
`enable_autopilot` and `dns_config` are creation-time settings, and Autopilot must be regional while
the Standard cluster is zonal. Every shard on the old cluster is destroyed with it. This is a
scheduled maintenance window announced through the maintenance banner (REQ-1466), not a rolling
change.

## When to switch

Autopilot bills per pod request, so an idle shard costs nothing; Standard runs ~6% cheaper per
shard-hour against a fixed ~$11/mo Spot system pool. Crossover is ~460 shard-hours a month (~15
hours a day of query time on the shared lane). Launch shape is `autopilot`. Switch to `standard`
once measured duty cycle passes the crossover, not before.

## What changes, and what does not

| | autopilot | standard |
|---|---|---|
| cluster location | `var.region` (regional) | `var.zone` (zonal) |
| node pools | none | `system` (Spot, always on, untainted) + one 0..1 pool per `var.shared_shards`, tainted `provisa.dev/shard=<shard>:NoSchedule` |
| shard pod placement | `nodeSelector: topology.kubernetes.io/zone` | that, plus `provisa.dev/shard` selector and the matching toleration |

Everything else is identical. Downstream terraform reads the cluster only through
`local.engine_cluster_{name,location,endpoint,ca}` (`terraform/gcp-saas/gke.tf:46-54`), and the
control plane changes only `PROVISA_ENGINE_CLUSTER_MODE` and `PROVISA_ENGINE_CLUSTER_LOCATION`
(`provisa/federation/k8s_provisioner.py:156` and `:180`).

`PROVISA_ENGINE_CLUSTER_ZONE` is required in **both** modes: regional Autopilot would otherwise
place a shard pod in a zone the control-plane VM is not in and bill every result set as cross-zone
egress.

## Runbook

Target instance: **cloud-dev** (`cloud.provisa.dev`, project `provisa-test-473`). Wake the
coordinator VM first — it stops when idle.

### 1. Raise the notice

```
provisa maintenance on \
  --message "Scheduled maintenance: the query engine is being rebuilt. Queries will fail until it is back." \
  --ends-at 2026-08-14T18:00:00Z
provisa maintenance status
```

`--api` / `PROVISA_API_URL` must point at the control plane and `--token` / `PROVISA_API_TOKEN` must
carry a platform_admin token; the server owns the wording and the `started_at` stamp
(`provisa/cli.py:309-378`).

### 2. Flip the mode and apply

```
cd terraform/gcp-saas
# terraform.tfvars is gitignored — edit it in place
#   engine_cluster_mode = "standard"
terraform plan   # confirm: cluster REPLACED, node pools created, VM metadata updated in place
terraform apply
```

Read the plan before approving. Expect the cluster to be destroyed and recreated, plus
`google_container_node_pool.system` and one `.shard[...]` per entry in `var.shared_shards`. The
coordinator VM is **not** replaced — only its `startup-script` metadata changes, and metadata is
updated in place.

### 3. Repoint the running control plane

Because the VM is not recreated, it never re-runs its startup script; the live node keeps the old
values until they are set by hand. On the node:

```
sudo sed -i \
  -e 's/^PROVISA_ENGINE_CLUSTER_MODE=.*/PROVISA_ENGINE_CLUSTER_MODE=standard/' \
  -e 's/^PROVISA_ENGINE_CLUSTER_LOCATION=.*/PROVISA_ENGINE_CLUSTER_LOCATION=us-central1-a/' \
  /root/.provisa/provisa.env
sudo systemctl restart provisa
```

Location is the region in `autopilot` and the zone in `standard`. A mismatched mode leaves shard
pods Pending forever (no toleration for the pool taint); a mismatched location makes every cluster
call 404.

### 4. Re-provision the shard and verify

No shard survives the replacement. The first query re-creates it — cold start is ~60-90s.

```
./scripts/deploy-cloud.sh patch
```

`preflight_engine` reprints the engine settings out of the running container (confirm the new mode),
and `verify_engine` runs a real `SELECT 1` through `/data/sql`. That query is the only end-to-end
readiness signal: `shard_status()` has no HTTP surface, and `/health` answers 200 whether or not a
shard is reachable.

### 5. Clear the notice

```
provisa maintenance off
```

## Rollback

Set `engine_cluster_mode` back, repeat steps 2-4. Rollback is another cluster replacement with the
same shard loss, so it costs a second window — decide before step 2, not during.
