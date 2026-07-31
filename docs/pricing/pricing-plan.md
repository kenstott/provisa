# Provisa Pricing Plan

Model source: `pricing_model.py` in the repository root. Edit the `ASSUMPTIONS`
block and re-run to reprice. All GCP costs are us-central1 list/spot, 2026-07.

## Two SKUs, two anchors

Provisa is a mix of two products with two different price anchors, so it bills on
two units:

- **Serving lane — the active-hour.** A warm, always-reachable federated endpoint:
  low-latency ops, the persistent-protocol surface, governance and auth. This is the
  Hasura workload; it bills like Hasura, on **active-hours** at **$3.25/active-hr**.
- **Analytical lane — the worker-hour.** Distributed MPP over autoscaled workers:
  large cross-source joins that spill to disk. This is the Starburst Galaxy workload;
  it bills like Galaxy, on **worker-hours** at **$2.50/worker-hr**.

A customer buys either or both. The two lanes have different cost shapes, different
competitors, and different anchors, so a single unit would misprice one of them —
active-hour framing underprices analytical compute ~8x; worker-hour framing can't
express a warm always-on endpoint. Splitting the SKU captures both markets.

| | Serving lane | Analytical lane |
| --- | --- | --- |
| Workload | warm low-latency API, persistent connections | large distributed cross-source joins |
| Anchor | Hasura v2 ($3.00/active-hr advanced source) | Starburst Galaxy Pro ($3.00/worker-hr) |
| Unit | active-hour ($3.25) | worker-hour ($2.50) |
| Cost | warm coordinator $0.15–0.42/hr | Spot worker $0.080/hr, on-demand $0.268/hr |
| Margin | ~87–97% | ~97% Spot / ~89% on-demand |
| Cold start | never (that's the product) | customer eats first-query boot |

## Serving lane — the active-hour (Hasura anchor)

The serving business is one SKU, the same one that took Hasura to ~$35M ARR almost
entirely self-serve: **billed active-hours on a warm, always-reachable endpoint.**
Any hour the endpoint sees activity bills a full hour. A production API touched even
once an hour bills ~730 hr/mo whether it served one request or a hundred thousand.

The customer is not buying a vCPU. They are buying the guarantee that their live
endpoint answers — the managed control plane, governance, auth, the federated query
surface. Compute is the meter, not the product. A warm coordinator costs us
~$0.134/hr and bills $3.25/active-hr — the software value, not a compute markup.

Revenue is welded to the customer's uptime, not their usage: their production endpoint
runs on it, so they cannot let it go cold. That is what makes the active-hour durable.

### Why Provisa's active-hour welds tighter than Hasura's

Hasura's warm surface is HTTP — request-driven, so warmth is inferred from traffic.
Provisa adds **raw-TCP protocols that hold persistent connections**: pgwire (5439),
bolt (7687), Arrow Flight (8815), gRPC (50051), MCP (8009). A DBeaver session, a Neo4j
Browser, a BI tool on pgwire keeps the socket open — the coordinator is continuously
active, and by construction continuously billing. Every persistent-protocol connection
is an always-on active-hour meter that a request-driven HTTP product structurally
cannot offer.

The two are not a fair COGS comparison, and not at parity. Hasura is GraphQL-only and
built for low-latency, small-payload ops, so its warm surface is one small instance
(~$0.005/hr). Provisa's warm hour is a distributed MPP: a **required coordinator**
(planner + persistent-protocol listeners, which can't ride a worker) always on, and —
for zero query cold-start — a **warm worker** too, plus cross-zone shuffle on
multi-worker joins. That is a structurally higher warm cost: ~$0.15/active-hr
coordinator-only, up to ~$0.42 with a warm worker (see model), against Hasura's
~$0.005. It is not inefficiency — it is the cost of serving a heavier, broader workload
class Hasura's instance cannot touch at all.

The margin still clears. At $3.25/active-hr the warm SKU runs ~95% coordinator-only and
~87% with a warm worker (~92–97% with a 1-year committed-use discount on the always-on
boxes). Lower than Hasura's ~99% on their narrow op — but that gap buys the superset.

The small active-hour premium ($3.25 vs $3.00) is a product-surface premium — broader
source class (warehouses, graph, MSSQL/Mongo) under one query, plus the raw-TCP protocol
surface — on top of a warm hour that already costs more to hold than Hasura's.

### Hasura v2 parity (the serving-lane anchor)

| | Hasura v2 Professional | Provisa (parity + small premium) |
| --- | --- | --- |
| Warm, generic Postgres source | $1.50/active-hr | — (Provisa's floor case is multi-source) |
| Warm, advanced source (Snowflake/BigQuery/Mongo/MSSQL…) | $3.00/active-hr | **$3.25/active-hr** |
| Persistent-connection protocol surface (pgwire/bolt/Flight) | none | **included in the warm hour** |
| Cross-source join scale | single serialization instance (~1 GB intermediate) | **distributed MPP workers, spills to disk, unbounded** |
| Egress (result passthrough) | $0.13/GB | **$0.13/GB** (match) |
| Free | 3 projects, 3M req/mo, 100 MB passthrough | match request + passthrough caps |
| Analytical lane | none (always-warm only) | separate worker-hour SKU (Galaxy-anchored) |

## Analytical lane — the worker-hour (Galaxy anchor)

The cross-source join that outgrows a single serialization instance is not a Hasura
workload — it is a Starburst Galaxy workload, and it is priced against Galaxy, not
against a cost-recovery floor. Galaxy is Trino-as-a-service billed on worker uptime:
6 credits/worker-hr × $0.50 = **$3.00/worker-hr** (Pro), $4.50 (Enterprise), $6.00
(Mission Critical).

Provisa's analytical lane bills the same unit — **$2.50/worker-hr**, just under Galaxy
Pro — for distributed MPP across autoscaled workers that spill to disk. Same anchor,
small discount, so it wins the analytical buyer on price while still clearing
warehouse-standard margin:

| Line | Cost | Price | Margin |
| --- | --- | --- | --- |
| Worker-hour (Spot n2-highmem-8, scale-to-zero) | $0.080/hr | **$2.50/worker-hr** | 97% |
| Worker-hour (on-demand, guaranteed-warm SLA) | $0.268/hr | $2.50/worker-hr | 89% |

This is a worker-hour, not a vCPU-hr cost-recovery meter. Pricing it as cost-plus put
it at ~$0.32/worker-hr — ~8x under the Galaxy anchor and leaving the analytical margin
on the table. A worker-hour is billed per second a worker is up on a query, scale-to-
zero between queries, so an idle analytical customer pays nothing.

Why Provisa wins this lane: Galaxy joins Trino sources; Provisa's federation reaches the
broader source class (warehouses **and** graph, MSSQL, Mongo, sheets) under one query,
with the same distributed-MPP scaling — at a lower worker-hour.

## Egress — metered passthrough, both lanes

Egress is a cost-recovery passthrough spanning both SKUs, matching Hasura's own logic —
the margin engine is the active-hour and the worker-hour, not the byte. Our only egress
cost is **result bytes leaving GCP to the consumer** (`$0.12/GB`). The source-cloud
egress a federated query triggers — AWS/Azure charging to move bytes out of the source —
is billed to the account that **owns the source**, i.e. the customer, not us; bytes
arriving at GCP are free ingress. Result sets are the small, filtered/aggregated output,
not the raw scan.

At Hasura parity `$0.13/GB` against our `$0.12` cost, egress clears +8% — no loss, but
thin. Two ways to widen it, both compatible with holding parity:

- **Bundled-egress infra.** A provider with bundled/near-zero egress (Hetzner ~20 TB/VM
  then ~$0.0013/GB, OVH unmetered) zeroes the GCP leg, taking parity from +8% to ~99%.
  This is the single highest-leverage infra change for the data plane.
- **Absorb it.** The active-hour and worker-hour margins cover the thin egress line at
  blended-account level regardless.

## Architecture split (drives the two SKUs)

- **Control plane** — durable, tiny, always-on. Multitenancy metadata
  (`superadmin_bootstrap`, `user_profiles`, `user_org_memberships`, tenant DB), auth,
  governance, connection front-door. Cloud SQL f1-micro + Cloud Run site (min=1). This is
  the fixed floor, **~$19/mo shared across all tenants** — no per-tenant fixed cost.
- **Warm coordinator** — the serving lane's active-hour engine. Trino planner + the
  persistent-protocol listeners. Warm while active, billed by the active-hour. Cost and
  revenue ride the same clock: warm ⇒ active ⇒ billed.
- **Data plane** — the analytical lane's worker-hour engine. Stateless federated query
  workers, ephemeral, scale 0→N on Spot VMs, billed by the worker-hour.

## Fixed-vs-variable invariant

**No fixed cost without a matching fixed revenue floor.** Every warmth a customer needs
is rented by a meter, so cost and revenue are welded to the same clock.

| Cost line | Shape | Matching revenue | Exposed? |
| --- | --- | --- | --- |
| Warm coordinator | fixed while on | active-hour meter (warm ⇒ active ⇒ billed) | No — welded |
| Worker (Spot, scale-to-zero) | variable | worker-hour meter | No — both variable |
| Egress (result set) | variable | $/GB meter | No — both variable |
| Shared site + control plane | **fixed, unconditional** | flat platform fee | Only line; floored below |

The sole unconditional fixed cost is the **~$19/mo shared floor** (or ~$2/mo fully
stopped pre-revenue). The minimum platform fee is set so `fee ≥ shared-floor /
paying-orgs`; with a $45 min fee and a $19 floor, org #1 covers it several times over.
The only fixed-cost-with-no-revenue window is pre-revenue — a runway line, not a
unit-economics flaw.

## Warmth tiers — the site never cold-starts; customers may

Warmth is tiered so cold-start cost lands on the party that needs it.

- **Site — always warm, free to the visitor.** Cloud Run (`min-instances=1`) serves
  marketing/signup/login/UI/REST-API/health; Cloud SQL control plane always on. Floor
  ~$19/mo, shared across all tenants. A prospect always hits an instant site, and the
  daily up-test is a free hit on the warm health endpoint.
- **Warm coordinator — the serving lane.** Persistent-protocol listeners and ops-grade
  latency need a warm coordinator; it can't be woken on demand. Sold as the active-hour.
- **Query execution — the analytical lane, cold by default.** Trino workers on a Spot
  MIG, min=0. First query wakes a worker; a per-resume minimum covers the boot, then the
  worker-hour meter runs.

## Cold start (scale-to-zero workers)

Default worker posture: `WARM_POOL_NODES = 0`. Waking a cold worker is billed a
per-resume minimum, Snowflake-style, so the boot cost lands on the customer who caused
it.

- Boot 90s → $0.002 Spot compute burned.
- Minimum billed 90s → $0.008 revenue. **Net +$0.006/resume (~75% margin).**

Tradeoff: pool-miss queries wait `BOOT_SECONDS`. Tolerable for the analytical lane,
unacceptable for interactive — interactive/ops customers buy the warm coordinator (the
serving lane) instead.

## Free bands (hard cap, under break-even)

A free tier is acquisition spend, capped at ~$3/user/mo of marginal cost. Card required
up front; at max, **throttle/suspend the data plane** — never silent billing.

| Band | Free max | Cost to us |
| --- | --- | --- |
| Compute | 200 vCPU-hr/mo (= 25 node-hr on 8-vCPU) | $2.00 |
| Egress | 8 GB/mo | $1.00 |

Egress capped tighter in dollars than compute — it is the abuse vector (cheap data pipe
/ exfil). Paid plans bake the same allowance in and meter overage above it (soft cap).
Free tier is the only hard cap.

## Platform tiers

| Tier | Fee/mo | Notes |
| --- | --- | --- |
| Free | $0 | Hard-capped free bands, card on file, always-warm site, cold query/connect |
| Starter | ~$180 | Allowance baked in, cold query/connect, metered overage |
| Team | ~$900 | Larger allowance, metered overage |
| Scale | ~$2,300 | Larger allowance, metered overage |
| Serving lane (active-hour) | $3.25/active-hr | Always-on persistent-protocol endpoints, ops latency, ~87–97% margin |
| Analytical lane (worker-hour) | $2.50/worker-hr | Distributed MPP cross-source joins, scale-to-zero, ~97% Spot / ~89% on-demand |
| + Warm worker add-on | +~$890 | Zero query cold start on the serving lane (~$196/mo cost) |

The minimum platform fee ($45/org/mo) covers the shared ~$19/mo floor at any org count
and holds ~96% margin on the fixed line. Warm compute (coordinator, and the warm-worker
add-on) uses on-demand instances — a guaranteed-warm SLA can't ride Spot — and qualifies
for a ~37% 1-year committed-use discount since it runs 24/7. The analytical lane's
worker-hour rides Spot, hence its higher margin.

## Assumptions most likely to move the answer

1. **Serving-lane attach rate.** How many endpoints stay warm and how many active hours
   each bills is the dominant serving-lane revenue variable — far more than egress.
2. **Analytical-lane worker-hours.** Galaxy-anchored worker-hours are the analytical
   revenue variable; the anchor ($2.50 vs Galaxy $3.00) sets the win-rate/margin trade.
3. `PAYING_ORGS` — drives floor-per-org and the minimum platform fee.
4. `BOOT_SECONDS` — real federated-worker + FDW warmup time. Sets whether the cold
   analytical lane is viable, and how hard the warm coordinator is to upsell.
5. `SPOT_NODE_HR` — Spot prices fluctuate and workers can be preempted. A warm pool on
   on-demand pricing takes the worker-hour margin from ~97% to ~89%.
6. **Data-plane infra.** GCP result-egress at `$0.12/GB` holds parity at only +8%;
   bundled-egress infra (Hetzner/OVH) takes it to ~99% and is the highest-leverage cost
   change.
