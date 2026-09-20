# Provisa Pricing Plan

This restates the pricing model to match what is actually implemented and live, not an
earlier design proposal. Two sources of truth, cross-checked against each other:

- **Code** — `.claude/commercial/provisa_commercial/` (the commercial plugin; proprietary,
  not part of the open-source distribution — see its own README). `entitlements.py`,
  `models.py`, `usage.py`, `trial.py`.
- **Lemon Squeezy** (the merchant of record) — store `SimpleIsHard` (store id `285474`),
  products `Provisa Compute` and `Provisa Egress`, queried live via the API on 2026-09-16.
  Every rate below is the live configured price, not a modeled/target number.

**Known drift**: the variant IDs configured in this deployment's `.env`
(`LEMONSQUEEZY_VARIANT_STARTER` etc.) 404 against the Lemon Squeezy API — they don't match
the live variant IDs the store actually has today. The rates below are still accurate (read
directly from the live variants), but the `.env` wiring that would let this deployment
actually check out against them needs to be reconciled separately from this document.

## The plan ladder

Five plans, one product family, not the "Free/Starter/Team/Scale + two lanes" shape an
earlier design pass modeled. `provisa_commercial.models.Plan`: `trial`, `starter`, `pro_s`,
`pro_m`, `pro_l`. Pro is sold as three fixed sizes — the size IS the plan, not a setting on
a single "Pro" tier, because each size is different hardware, a different price, and a
different Lemon Squeezy variant.

| Plan | Hardware | Query ceilings | Sources | Compute base fee | Included active-hrs | Overage/active-hr | Included egress | Egress overage |
|---|---|---|---|---|---|---|---|---|
| Trial | shared/pooled | 100K rows / 10GB scan / 8GB mem / 120s | 2 | — (rides Starter's LS trial) | 40 hrs or 14 days or 25GB, whichever first | — | 25 GB | — |
| Starter | shared/pooled | 1M rows / 100GB scan / 32GB mem / 300s | 10 | $25/mo | 19 hrs | $1.30/hr | 25 GB | $0.48/GB |
| Pro S | n2-highmem-4 (4 vCPU / 32 GB), dedicated | uncapped | 100 | $99/mo | 66 hrs | $1.50/hr | 50 GB | $0.48/GB |
| Pro M | n2-highmem-8 (8 vCPU / 64 GB), dedicated | uncapped | 100 | $199/mo | 72 hrs | $2.75/hr | 100 GB | $0.48/GB |
| Pro L | n2-highmem-16 (16 vCPU / 128 GB), dedicated | uncapped | 100 | $399/mo | 72 hrs | $5.50/hr | 200 GB | $0.48/GB |

Query ceilings and hardware are engineering facts (`entitlements.py`, `ProSize`); base fee,
included hours/GB, and overage rates are the live Lemon Squeezy price for each variant's
graduated tier.

**Pro is not unmetered compute.** Query *ceilings* (rows/bytes/memory/time) are uncapped on
Pro — a dedicated engine has no shared-tenant reason to throttle a single query — but active-
hour *billing* still applies to every plan, Pro included: each size's monthly base fee bundles
an included-hours allowance, then overage bills per hour beyond it, at a rate that scales with
the hardware. Uncapped query limits and metered billing are two different axes; Pro relaxes
only the first.

## Why active-hour, not vCPU-hour or per-query

The unit is occupancy (any clock hour the org submitted at least one query bills once,
regardless of query count or duration), not a per-query or per-vCPU meter — Starter's shard
hosts many orgs on the same nodes, so no per-org vCPU-hour is a real, readable number; billing
one would be inventing a figure the infrastructure cannot actually attribute
(`usage.py`). An hour containing only rejected/killed queries still bills — the shard did the
work of planning and admitting them before rejecting, so the occupancy cost was real.

The signal is a durable counter (`org_usage_hour`, upserted per `(org, hour)`), not the audit
log or the OTel trace stream — neither of those is a billing record; a billing fact needs an
idempotent row a monthly sweep can total without risk of double-counting a replay.

## The trial

14 days, capped by three independent bounds, whichever comes first (`trial.py`,
`TRIAL_DAYS`/`TRIAL_ACTIVE_HOURS`/`TRIAL_EGRESS_BYTES`): **14 calendar days**, **40 active
hours**, or **25 GB egress** (sized to match Starter's own included egress allowance, so the
trial evaluates the plan the org would actually buy). There is no separate "Trial" product in
Lemon Squeezy — a trial is a Starter subscription with Lemon Squeezy's own free-trial flag
enabled on that variant (confirmed live: the Starter variant carries `has_free_trial: true`,
`trial_interval_count: 14`, `trial_interval: day`). Lemon Squeezy can only express the
day-count bound; the active-hour and egress bounds are enforced by Provisa's own clock, which
force-converts the trial (moves `trial_ends_at` to now) when either is hit early — that's what
makes the merchant of record charge the first period on schedule rather than Provisa's code.

Entitlement is keyed on the Lemon Squeezy **subscription status**, never on a landed payment:
`on_trial`, `active`, and `cancelled` (cancel-at-period-end — the org paid for the period it's
still in) are entitled; `past_due`, `unpaid`, `expired`, and `paused` are not (`models.py`,
`ENTITLED_STATUSES`).

## Egress

One overage rate, **$0.48/GB**, identical across every plan — only the included allowance
scales with plan size (25/50/100/200 GB). Metered separately from compute because it fails
differently: a compute ceiling is enforced by Trino itself (session properties, `EXCEEDED_*`
errors); an unbounded `SELECT *` over a modest table passes every scan guard while still
shipping gigabytes, so egress is enforced on the result path
(`enforce_output_cap`/`translate_engine_error`), not the query-planning path.

## What's deliberately not billed by usage on Pro

Pro's query ceilings are uncapped by design (`entitlements.py`, REQ-1449): "the isolated lane
cannot degrade another tenant once placement is dedicated, so the size is the only limit — a
concurrency or duration ceiling on top of it would bill for hardware and then refuse to let the
org use it." The org already pays for the box; Provisa does not also meter what it does on the
box beyond the active-hour/egress lines above.

## The zero-customer cost floor

What this deployment costs with zero paying customers signed up — the number that makes the
free tier and trial genuinely low-risk to offer, verified against what's actually deployed
(`terraform/gcp-saas/`), not a modeled estimate:

| Component | Cost at zero customers | Why |
|---|---|---|
| Front-door proxy (e2-micro) | **$0** | GCP's Always Free tier covers 1 e2-micro/month in us-central1/us-west1/us-east1; this VM never stops (it's the thing that wakes everything else), so it rides the free allowance continuously. |
| Coordinator (control-plane VM) | **$0** | Idle-stopped by the front door after 20 minutes of zero traffic on every protocol port (REQ-1779; doubled from 10 min this session). Stopped = not billed for compute. |
| Engine shard (GKE, Trino/federation compute) | **$0** | Scaled to zero pods by the in-app reaper after 10 minutes of query inactivity (half the coordinator's window, so it always finishes ahead of a coordinator stop). GKE Autopilot bills per-pod resource-second; zero pods is zero compute charge. |
| GKE Autopilot cluster management fee | **$0** | $0.10/cluster-hour, but Google's Always Free tier includes $74.40/month in GKE credit — enough to cover one Autopilot or zonal-Standard cluster running continuously all month. This is the only GKE cluster in the project. |
| Cloud SQL (`db-f1-micro`, control-plane Postgres) | **~$9/mo** | The one component that's always on regardless of traffic — org/tenant registry, auth, billing state. Documented in `variables.tf`'s own comment as "the always-warm baseline." |
| Static IP (shared front-door address) | **$0** | GCP only charges for a *reserved-but-unattached* external IP; this one stays attached to the always-on front-door VM. |
| DNS (Cloudflare) | **$0** | Free-tier DNS-only records (`proxied = false`). |

**Total fixed floor: ~$9/mo**, not the ~$19/mo an earlier design pass modeled — Cloud SQL is
the only real fixed line; everything else is either genuinely free-tier or scales to zero with
no traffic. This is what makes offering a free tier and a 14-day trial cheap to run: acquiring
a customer who never converts costs Cloud SQL's per-tenant registry row, nothing else.

## Comparison to competitors' actual current pricing

Verified against each vendor's own live pricing/API as of 2026-09-16, not older or modeled
figures — two material corrections to what an earlier pass assumed:

**Hasura has moved off active-hour billing entirely for new customers.** Their current
flagship product, Hasura DDN, bills **$5–$30 per "active model"/month** (Free / Base / Advanced
tiers) — a per-table unit, not project-hours, and specifically gated on a usage threshold, per
Hasura's own pricing page: *"Active Model is any model or command in the metadata that is
accessed more than 1000 times/month."* A table queried fewer than 1000 times in the month costs
nothing; cross that line and the whole table is billed for the month, regardless of how far
over 1000 it went. This is a fundamentally different unit from what Provisa's `hasura_v2`
compatibility layer targets. The active-hour model below is **Hasura Cloud v2, legacy and
grandfathered** — still running for existing customers, not what a new Hasura signup lands on
today.

| | Hasura Cloud v2 (legacy) | Starburst Galaxy | Provisa |
|---|---|---|---|
| Unit | active-hour | worker-hour (credits) | active-hour (compute), separately metered egress |
| Base/entry rate | $1.50/active-hr (no DB connected) — confirmed live | $0.50/credit × 6 credits/worker-hr = $3.00/worker-hr (Pro) | Starter: $25/mo base (19 hrs incl.) + $1.30/hr overage |
| Higher tier | Advanced-connector surcharge exists (Snowflake/BigQuery/Mongo/MSSQL); exact current multiplier not independently confirmable via public docs — do not restate a specific number without re-verifying against a live account | Enterprise $0.75/credit ($4.50/worker-hr), Mission Critical $1.00/credit ($6.00/worker-hr) | Pro S/M/L: $99–$399/mo base + $1.50–$5.50/hr overage, scaling with dedicated hardware size |
| Egress | $0.13/GB | Not publicly itemized the same way (bundled into credit consumption) | $0.48/GB overage, 25–200 GB included by plan |
| What one price buys | GraphQL-only, request-driven warmth, single-serialization joins | Trino-as-a-service, MPP over Trino-reachable sources only | Federated MPP across a broader source class (warehouses, graph, Mongo, MSSQL) under one query, plus persistent-protocol surfaces (pgwire/Bolt/Flight/gRPC/MCP) Hasura's request-driven model can't offer |

Provisa's Starter rate ($1.30/active-hr overage) undercuts Hasura v2's base rate ($1.50/active-hr)
even before accounting for the included-hours allowance a Lemon Squeezy subscription bundles in;
Provisa's Pro tiers sit at or below Galaxy's Pro worker-hour rate while reaching a broader source
class than Galaxy's Trino-only reach. Both comparisons hold at the *entry* tier — a rigorous
apples-to-apples comparison at higher tiers would need Hasura's actual current advanced-connector
multiplier, which isn't publicly documented precisely enough to cite as fact here.

## Net-revenue projection

**Illustrative, not a forecast.** Revenue rates are the live Lemon Squeezy prices above; direct
compute costs are current GCP on-demand list prices for the actual machine types/pod sizes this
deployment runs (`ProSize` for Pro, the shared shard's Autopilot pod request for Starter);
per-org usage and the customer count per tier are assumed, not measured. The single biggest
unverified lever is the Starter/shared-lane concurrency assumption below — change it and
Starter's margin moves a lot; Pro's doesn't, because Pro's engine is never shared.

**Direct compute cost basis:**

| | Basis | $/hr |
|---|---|---|
| Starter (shared shard) | GKE Autopilot pod, 6 vCPU/24 GiB (`shared_shards`), at $0.0445/vCPU-hr + $0.0049/GiB-hr, **divided across an assumed 4 concurrently-active Starter orgs per shard-hour** | $0.385/hr shard ÷ 4 ≈ **$0.096/hr per org** |
| Pro S | n2-highmem-4 on-demand, us-central1 | **$0.26/hr** |
| Pro M | n2-highmem-8 on-demand, us-central1 | **$0.52/hr** |
| Pro L | n2-highmem-16 on-demand, us-central1 | **$1.05/hr** |

Egress costs Provisa **$0.12/GB** regardless of whether it falls inside a customer's included
allowance (that section, above) — included GB is revenue Provisa doesn't collect on cost it
still pays, not cost Provisa avoids.

**Per-org economics, assumed usage:**

| Tier | Count (assumed) | Active-hrs used/mo | Egress used/mo | Revenue/org | Direct cost/org | Margin/org | Margin % |
|---|---|---|---|---|---|---|---|
| Starter | 30 | 40 | 15 GB | $52.30 | $5.65 | $46.65 | 89% |
| Pro S | 12 | 90 | 60 GB | $139.80 | $30.60 | $109.20 | 78% |
| Pro M | 5 | 100 | 130 GB | $290.40 | $67.60 | $222.80 | 77% |
| Pro L | 3 | 110 | 250 GB | $632.00 | $145.50 | $486.50 | 77% |

**Fleet total, this mix (50 paying orgs):**

| | Monthly |
|---|---|
| Revenue | $6,594.60 |
| Direct cost (compute + egress) | $1,311.08 |
| Fixed floor (zero-customer cost, above) | $9.00 |
| **Net** | **$5,274.52** |
| **Net margin** | **80%** |

Two things this makes visible that the per-unit margin percentages alone don't: Starter's 89%
margin is the *most* sensitive number in this whole model — it's a direct function of the
concurrency assumption (4 orgs/shard-hour), which isn't measured, only asserted; a real
concurrency count from `org_usage_hour` once there's live traffic should replace it. Pro's ~77%
margin depends on no modeling assumption at all: dedicated hardware means no concurrency
number to guess at, just the org's own usage against a fixed, verifiable on-demand rate — the
only thing that would move it is which Pro sizes customers actually pick.

**Live model:** the SaaS figures above, plus on-prem/add-on SKUs and two named scenarios, are
built as an interactive workbook — `docs/pricing/provisa-revenue-model.xlsx`
(`build_revenue_model.py` regenerates it; edit the script's inputs, not the `.xlsx` directly).
Every yellow cell on its `Inputs` sheet is a knob; every other cell is a live formula. The
figures quoted below are that workbook's *current default* knob settings, not fixed numbers —
re-run the script after changing an assumption and everything downstream recomputes.

## On-prem licensing

Negotiated, not published — these are starting-assumption placeholder rates for the SKU
structure, not a price list (`Inputs` sheet, "On-prem licensing" section). One model only: a
flat per-core rate, with no hardware-class factor and no coordinator/worker distinction — a
core costs the same regardless of the chip it runs on or the role the node plays.

An earlier draft split cores into a device-class factor (Oracle's core-factor-table pattern:
0.5× commodity x86/ARM, 1.0× everything else) — dropped, because Oracle's rationale for that
split (non-x86 chips do more work per core on Oracle's own workloads) doesn't hold for Provisa:
it's software, a core is a core regardless of instruction set.

| | Basis |
|---|---|
| Rate | $3,000/core/yr |
| Memory | Bundled at 4 GiB/licensed core — informational, never billed as a separate meter |
| Delivery/support cost | 25% of license revenue (placeholder; real fully-loaded cost, not a floor) |

A representative deal (48 licensed cores, the workbook's current default): 48 × $3,000 =
**$144,000/deal license revenue**, **$108,000/deal margin** after the 25% delivery cost.

**Enforcement is contractual, not technical.** Provisa on-prem ships as a library — there is no
runtime gate that can check node count, core count, or memory against what was purchased, and no
phone-home or central key registry. The license key is scoped to a *cluster* (a group of nodes):
nodes sharing a key can sum their own reported vCPU/mem and warn once the cluster's licensed core
entitlement is exceeded, but that check only holds within one cluster's own key exchange —
nothing stops a customer copying the same key into a second, isolated cluster. The only thing
Provisa can actually withhold from an out-of-scope or unpaid deployment is service: support,
SLAs, and updates. Every license fee above is really a support/update contract with a
usage-scope clause and audit rights attached — not a technical control on total consumption.
This is the same posture Oracle and IBM's own core-factor licensing runs on in practice (audit
rights, not a runtime block), and the same free-to-run-anywhere shape as Grafana OSS or Neo4j
Desktop.

**Free tier:** orgs may run on-prem at $0/no-support if both hold: a self-attested contractual
8-core cluster cap, and under $1M in company annual revenue — license-terms limits, not
technical ones, for the same reason above.

**Support-only SKU:** for free-tier orgs who want someone to call without buying a license —
**$10,000/yr**, no license included, scoped to a 24-hour response commitment (confirm the defect
or clarify the feature, plus a resolution ETA — never a resolution-time guarantee, which the
price point can't cover). At a 40% assumed delivery cost, that nets **$6,000/yr per
subscriber**.

## Add-on SKUs

Sold on top of either SaaS or on-prem; placeholder rates, no comparable-vendor research done
on these yet (unlike the on-prem rates above, which were checked against Denodo/Starburst/
Oracle/IBM/Red Hat).

| SKU | Rate | Net/yr per attach (30% assumed delivery cost) |
|---|---|---|
| Premium connector (Splunk / SharePoint / Files) | $6,000/yr | $4,200 |
| LLM validated-reasoning add-on | $400/org/mo ($4,800/yr) | $3,360 |

The LLM add-on's 30% cost assumption is the least trustworthy number in the whole workbook —
inference cost is the real cost driver there and isn't modeled at all yet.

## Scenarios

Two named scenarios in the workbook, each with its own independent customer-count/deal-count
knobs (they don't share counts with the base `Model` sheet):

- **`Scenario - $1M Net`** — SaaS only, 473 Starter / 189 Pro S / 79 Pro M / 47 Pro L (the base
  50-org mix scaled ~15.8×) → **~$998K/yr net**, at current default assumptions.
- **`Scenario - Mixed`** — half that SaaS mix (220/90/38/22) plus 3 on-prem deals at the
  representative-deal economics above → **~$471K/yr SaaS net + $324K/yr on-prem net ≈
  $795K/yr combined**, illustrating that on-prem deals reach the same order of magnitude with
  far fewer logos than SaaS alone (3 deals vs. hundreds of orgs).

Neither is a forecast — both are starting points for a "what customer mix gets us to $X"
conversation, meant to be replaced by real attach/conversion rates once they exist.

## How this deployment reaches billing

`.claude/commercial/` is the only module that knows about any of this — `provisa.core.commerce`
is the sole seam, imported inside a `try` from core, memoized, degrading to a no-op everywhere
it's not installed. A self-hosted install with no commercial plugin present runs **unmetered,
uncapped, and unbilled** — the correct behavior for a deployment with no subscription to
enforce, not a bug. The plugin is installed only on the hosted node.

## Open questions this restatement surfaces

- **`.env` variant-ID drift** (above) — needs reconciling against the live store before the
  hosted checkout flow can be trusted to charge the right variant.
- The commercial plugin's own numbers (base fees, included hours, overage rates) live in Lemon
  Squeezy's dashboard, not in this repository, by design (`README.md`: "so the open-source
  wheel and the demo build ship neither the pricing model nor the code that charges for it").
  This document is a snapshot as of 2026-09-16 — re-pull from the API before quoting these
  numbers externally if meaningful time has passed, since nothing enforces this file staying in
  sync with a dashboard it doesn't read.
