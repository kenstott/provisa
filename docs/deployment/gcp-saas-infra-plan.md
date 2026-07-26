# GCP SaaS Infra Plan — Cloud SQL variant

Multi-tenant, on-demand deployment of Provisa. Distinct from the enterprise module
([terraform/gcp/](../../terraform/gcp/)), which is a fixed single-tenant N-node cluster
with postgres in the coordinator's compose stack. Pricing basis: [pricing-plan.md](../pricing/pricing-plan.md).

## Model

Four layers, each with a different scaling law and warmth:

1. **Site front-door** — always warm, no cold start. Cloud Run (`min-instances=1`)
   serves marketing/signup/login/UI/REST-API/health. HTTP only; scales up on demand,
   never to zero. This is what a prospect always hits instantly.
2. **Control plane** — durable, managed, always warm. Cloud SQL holds the multitenancy
   metadata (`PLATFORM_DATABASE_URL` / `TENANT_DATABASE_URL`); the site reads it live.
3. **Coordinator** — Trino coordinator + TCP protocol listeners + stateful singletons.
   Cold by default (scale-to-zero); a customer's first pgwire/bolt/flight connect
   cold-starts it. Always-warm only as a paid add-on (see Warmth tiers).
4. **Data plane** — Trino workers only. Autoscaled Spot MIG, min=0 → N. Metered
   compute; scales to zero at idle. Customer eats query startup on first run.

Raw-TCP protocols (pgwire 5439, bolt 7687, Flight 8815, gRPC 50051, MCP 8009) need a
persistent listener, so the coordinator is the reachable endpoint behind a regional
external passthrough NLB (all-ports, one shared IP — same pattern as enterprise
[main.tf:375-429](../../terraform/gcp/main.tf#L375-L429)). Workers are not in the LB.
The Cloud Run site front-door sits off to the side of the NLB: HTTP visitors hit Cloud
Run directly (its own managed HTTPS), TCP clients hit the NLB → coordinator.

## Warmth tiers

The warmth you keep hot maps to who needs it, so cold-start cost lands on the right party.

| Layer | Warmth | Who eats cold start | Cost basis |
|---|---|---|---|
| Site (Cloud Run min=1) | Always warm | Nobody | ~$10/mo (throttled idle) |
| Control plane (Cloud SQL f1-micro) | Always warm | Nobody | ~$9/mo |
| Trino workers (Spot MIG min=0) | Cold | Customer, first query | per-resume minimum |
| Coordinator TCP listeners | Cold by default | Customer, first connect | — |
| **Warm coordinator add-on** (e2-standard-4, on-demand) | Warm (paid) | Nobody | ~$98/mo → ~$445 @ 78% |
| **+ Warm worker add-on** (e2-standard-8, on-demand) | Warm (paid) | Nobody | ~$196/mo → ~$890 @ 78% |

Raw-TCP connections can't be woken on demand (a passthrough-TCP connect can't trigger a
wake the way an HTTP request can), so always-on TCP endpoints / ops-grade latency are
inherently the paid warm-coordinator tier. Warm add-ons use on-demand (not Spot — a
"guaranteed warm" SLA can't ride a preemptible instance) and qualify for a 1-year
committed-use discount (~37% off) since they run 24/7.

## Service placement — can every service be provisioned here?

Yes. Every service in the stack ([docker-compose.core.yml](../../docker-compose.core.yml) +
[docker-compose.app.yml](../../docker-compose.app.yml)) maps to one of the three layers,
with optional managed swaps for the stateful singletons.

| Service | Enterprise home | SaaS placement | Managed swap (optional) |
|---|---|---|---|
| postgres (control plane) | coordinator compose | **Cloud SQL** | — (this is the swap) |
| pgbouncer | coordinator compose | Cloud SQL pooling, or small pooler on coordinator | Cloud SQL built-in |
| provisa (API 8000, Flight 8815) | primary + secondaries | **coordinator** (+ workers optional) | — |
| provisa-ui (443/3000) | primary + secondaries | **coordinator** | — |
| pgwire / bolt / mcp / grpc listeners | in-process on app | **coordinator** (in-process) | — |
| trino (coordinator) | primary | **coordinator** | — |
| trino-worker | secondaries | **worker MIG** (Spot, min=0→N) | — |
| trino-exchange-init | primary | coordinator init | — |
| redis (cache) | coordinator compose | **coordinator**, or offload | Memorystore for Redis |
| minio (object store: Trino exchange, OTEL S3, Hive warehouse) | coordinator compose | **coordinator**, or offload | GCS (Trino/Hive speak S3/GCS) |
| minio-init | coordinator compose | coordinator init | — |
| zaychik | coordinator compose | **coordinator** | — |
| observability (grafana/prometheus/otel) | opt-in on node | coordinator, or `obs_mode=collector` → external OTLP | Managed Prometheus / external OTLP |

The stateful singletons (redis, minio) ride the coordinator at low scale. Offloading
them to Memorystore + GCS is a scale-up option, not a requirement — the plan provisions
everything without it.

## Terraform module — delta from enterprise

New module `terraform/gcp-saas/`. Reuses enterprise networking, firewall, health-check,
protocol-surface locals, and the NLB verbatim. Changes:

| Enterprise | SaaS |
|---|---|
| `google_compute_instance.secondary` (fixed count) | `google_compute_instance_template` + `google_compute_region_instance_group_manager` (workers) + `google_compute_region_autoscaler` |
| postgres in coordinator compose | `google_sql_database_instance` (private IP) + `google_sql_database` + `google_sql_user` |
| coordinator = `n2-standard-8` (runs DB) | coordinator = `n2-standard-4` (DB offloaded) |
| unmanaged instance group (all nodes) | unmanaged group = coordinator only (LB backend); workers in MIG, not in LB |
| `multitenancy` var (default false) | forced `true`; subdomain routing (`{org}.provisa.dev`) |

New variables: `cloudsql_tier`, `cloudsql_ha` (regional), `worker_min_nodes` (0 = pure
scale-to-zero; ≥1 = warm pool upsell), `worker_max_nodes`, `worker_use_spot`,
`autoscale_cpu_target`. Private Service Access (`google_service_networking_connection`)
is required for Cloud SQL private IP on the provisa VPC.

## Required app change

Coordinator needs an external-DB mode. Today only `ROLE=secondary` skips bundled
postgres and rewrites the DB URLs ([first-launch.sh:275](../../packaging/linux/first-launch.sh#L275),
[first-launch.sh:582-588](../../packaging/linux/first-launch.sh#L582-L588)). The change:
key the existing `skip_pattern` + URL-rewrite on an env toggle
(e.g. `PROVISA_EXTERNAL_CONTROL_DB=1` with `CONFIG_DB_HOST`/`CONFIG_DB_PASSWORD`) so a
coordinator can drop the `postgres`/`pgbouncer` services and point
`PLATFORM_DATABASE_URL`/`TENANT_DATABASE_URL` at Cloud SQL. The compose app tier already
reads `CONFIG_DB_*` ([docker-compose.app.yml:13-24](../../docker-compose.app.yml#L13-L24));
the blocker is the `depends_on: postgres` and first-launch keying the skip on `ROLE`.

## Coordinator scaling (the one vertical bottleneck)

The Trino coordinator is the single non-autoscaling layer — a query planner is a
single-writer coordination point, so it scales up, not out. But its heap is pinned at
JVM launch (`-Xmx8G`, [trino/etc/jvm.config:2](../../trino/etc/jvm.config#L2)), so **there
is no live vertical resize**: adding RAM does nothing until the JVM restarts with a higher
`-Xmx`, and a restart drops every TCP connection and in-flight query. GCE machine_type
resize needs a stop/start (same drop); GKE VPA / in-place resize can grow CPU live but not
usable heap. So the design goal is to make the coordinator *not need* to grow, and to
automate the rare resize as a graceful failover — never a live resize.

**1. Make the coordinator memory-light (highest leverage).** Flip
`node-scheduler.include-coordinator=false` in the SaaS coordinator config (enterprise ships
`=true`, [trino/etc/config.properties:2](../../trino/etc/config.properties#L2)). Today the
coordinator also runs scan tasks, which is what makes it memory-hungry; turned off, it only
plans + coordinates + streams results and memory pressure moves onto the **autoscaling Spot
workers**, which already scale automatically.

**2. Spill intermediate data off-heap (already configured).** `retry-policy=TASK` +
filesystem exchange ([config.properties:9-11](../../trino/etc/config.properties#L9-L11),
[exchange-manager.properties](../../trino/etc/exchange-manager.properties)) spools shuffle
data to minio/GCS instead of coordinator heap. Keep it; it is the spill valve that keeps a
big query from blowing the coordinator.

**3. Shard coordinators per tenant.** Multitenant routing (`{org}.provisa.dev`) lets each org
hit its own coordinator, so no single coordinator carries the whole platform — horizontal
scale *across* tenants, vertical *within* only a genuinely hot org. This is the primary
scale-out story; per-coordinator vertical resize is the exception, not the norm.

**4. Monitor + automate the rare resize as a rolling failover.** Coordinator exposes JMX
(`HeapMemoryUsage`, running/queued query counts, cluster memory-pool reserved) — scrape via
Managed Prometheus / OTLP (the obs stack already exists) and alert on heap % + queue depth.
When a tenant genuinely outgrows its coordinator, a controller drains it (stop admitting new
queries, let in-flight ones drain — FTE lets them checkpoint), brings up a larger-`-Xmx`
coordinator, and cuts the NLB/DNS over. Seconds of connect interruption, not a live resize.

## Cost points

- **Idle, zero customers:** ~$163/mo running, or **~$2/mo** absolute min (stopped
  Cloud SQL storage + stateless coordinator at MIG min=0; wake on first hit via a
  $0-idle Cloud Run signup shim). See [pricing-plan.md](../pricing/pricing-plan.md).
- **Per active cluster-hour:** ~$0.08 Spot compute + egress; priced at ~78% margin.
- **Cold start:** covered by a per-resume minimum when `worker_min_nodes=0`.

## Open items

- Confirm Trino coordinator on `n2-standard-4` is sufficient once workers are remote and
  `node-scheduler.include-coordinator=false` (coordinator does planning + exchange, not scan).
- Build the drain/rolling-upsize controller (see Coordinator scaling) — or defer until a
  tenant actually saturates a coordinator, since per-tenant sharding pushes that out.
- Cloud SQL connection pooling: decide Cloud SQL built-in vs PgBouncer sidecar under
  many-tenant connection fan-out.
- Subdomain-per-org TLS: wildcard `*.provisa.dev` cert on every listener
  ([variables.tf:100-118](../../terraform/gcp/variables.tf#L100-L118)) already covers this.
