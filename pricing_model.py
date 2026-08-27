# Copyright (c) 2026 Kenneth Stott
# Canary: 8fe4b6db-4ac1-4456-bb81-7a4f92b0d413
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa SaaS cost-to-serve + pricing, on the settled topology.

Topology priced here (REQ-1447/1448/1449/1450/1451):

  control plane   one VM + Cloud SQL, off the cluster        (REQ-1451)
  Starter         shared multi-tenant Trino, one GKE cluster (REQ-1450)
  Pro S/M/L       same manifests, single-tenant node pool     (REQ-1449)
  Enterprise      customer's own engine, metered on ITS capacity (vCPU)

Three constraints, in priority order, because they pull against each other:

  1. PRICE IS SET BY THE COMPS, not by cost-plus. Starter must look like Hasura
     Cloud v2 (per-active-hour, per-GB, no meaningful base fee); Pro must look
     like Starburst Galaxy (per-hour compute, no base fee). A cost-plus number
     that lands above either comp is not a price, it is a reason to lose.

  2. MARGINAL margin >= 75% on every customer. Marginal = revenue minus the cost
     that customer causes (engine hours, egress). This is the line that must hold
     for essentially everyone, because it is what makes each additional customer
     worth having.

  3. FULLY-LOADED margin >= 75% at a THRESHOLD org count, not at customer #1.
     Fully-loaded adds that customer's share of the fixed floor. With the floor
     divided by one customer the margin is poor by arithmetic, not by mispricing;
     it improves as the denominator grows. The model reports the crossover count
     rather than pretending it is 1.

Starter's marginal margin is additionally a function of shared-cluster DENSITY:
one org alone on a shared node pays for the whole node, four concurrent orgs pay
a quarter each. Starter is the tier where constraint 2 is also a ramp.

All costs are GCP us-central1 list as of 2026-08. Adjust ASSUMPTIONS.
"""

HOURS_MO = 730

# ------------------------- COMPETITIVE ANCHORS (hard ceilings) -------------------------
# Hasura Cloud v2 Professional: usage-priced, no base fee — $/active-hour (an hour
# in which the project served at least one request) plus $/GB data pass-through.
HASURA_ACTIVE_HR = 1.30
HASURA_EGRESS_GB = 0.13
# Starburst Galaxy Pro: 6 credits/worker-hour at $0.50/credit, no base fee. Note
# this is PER WORKER — a Galaxy cluster is a coordinator plus N workers, so the
# comparable Provisa number is the whole engine against one Galaxy worker.
GALAXY_CREDIT = 0.50
GALAXY_CREDITS_PER_WORKER_HR = 6
GALAXY_WORKER_HR = GALAXY_CREDIT * GALAXY_CREDITS_PER_WORKER_HR

# ------------------------- GCP UNIT PRICES (us-central1) -------------------------
# n2 bills per-vCPU + per-GB linearly — which is why REQ-1449 scales Pro vertically
# instead of out: the three sizes cost exactly 1x/2x/4x, no discount lost.
N2_VCPU_HR = 0.031611
N2_GB_HR = 0.004237
SPOT_DISCOUNT = 0.72
CUD_1YR_DISCOUNT = 0.37

E2_MICRO_HR = 0.0  # always-free tier, 1 per billing account (us-central1)
E2_STANDARD_4_HR = 0.134  # control-plane VM when running

GKE_CLUSTER_HR = 0.10
GKE_FREE_ZONAL_CLUSTERS = 1

CLOUDSQL_F1_MICRO_HR = 0.0105
CLOUDSQL_STORAGE_GB_MO = 0.17
CLOUDSQL_DISK_GB = 20

PD_STANDARD_GB_MO = 0.04  # a retained boot disk on a STOPPED VM still bills
CONTROL_PLANE_DISK_GB = 100
FRONT_DOOR_DISK_GB = 10

COST_EGRESS_GB = 0.12  # result bytes leaving GCP. The source-cloud leg is
# billed to whoever owns the source — the customer.
OBSERVABILITY_MO = 3.0
REGISTRY_MO = 0.5

TARGET_GROSS_MARGIN = 0.75
MARKUP = 1 / (1 - TARGET_GROSS_MARGIN)

# ------------------------- ENGINE SIZES (REQ-1449) -------------------------
SIZES = [("Pro S", 4, 32), ("Pro M", 8, 64), ("Pro L", 16, 128)]
SHARED_NODE_VCPU, SHARED_NODE_GB = 8, 64


def node_hr(vcpu, gb, spot=False, cud=False):
    base = vcpu * N2_VCPU_HR + gb * N2_GB_HR
    if spot:
        return base * (1 - SPOT_DISCOUNT)
    if cud:
        return base * (1 - CUD_1YR_DISCOUNT)
    return base


shared_node_hr = node_hr(SHARED_NODE_VCPU, SHARED_NODE_GB)

# ------------------------- 1. ZERO-CUSTOMER FLOOR -------------------------
# Held near zero by three choices, each a real constraint:
#   a. EXACTLY ONE zonal GKE cluster -> management fee is free-tier credited. The
#      second shard (shared_2) is the first $73/mo step, not the first customer.
#   b. Shared node pool min=0 with no active org (REQ-1450, as amended).
#   c. Control-plane VM idle-stops behind the front door; only its disk bills.
zero_customer = [
    (
        "GKE cluster management (1 zonal, free tier)",
        max(0, 1 - GKE_FREE_ZONAL_CLUSTERS) * GKE_CLUSTER_HR * HOURS_MO,
    ),
    ("Shared Trino node pool (min=0, no orgs)", 0.0),
    ("Isolated node pools (none provisioned)", 0.0),
    ("Front door e2-micro (always-free tier)", E2_MICRO_HR * HOURS_MO),
    ("  front door boot disk", FRONT_DOOR_DISK_GB * PD_STANDARD_GB_MO),
    ("Control-plane VM (idle-stopped)", 0.0),
    ("  control-plane boot disk (retained)", CONTROL_PLANE_DISK_GB * PD_STANDARD_GB_MO),
    ("Cloud SQL db-f1-micro (always on)", CLOUDSQL_F1_MICRO_HR * HOURS_MO),
    ("  Cloud SQL storage", CLOUDSQL_DISK_GB * CLOUDSQL_STORAGE_GB_MO),
    ("Artifact Registry", REGISTRY_MO),
    ("Logging / monitoring", OBSERVABILITY_MO),
]
ZERO_CUSTOMER_MO = sum(c for _, c in zero_customer)

# Once any customer exists the control plane is warm — nobody waits on a VM boot
# for a dashboard load. The shared node pool is NOT fixed cost: REQ-1450 scales it
# to zero when no org is active, which makes it attributable usage.
CONTROL_PLANE_WARM_MO = E2_STANDARD_4_HR * HOURS_MO

# Control-plane HA is BASELINE — it is in the shared floor, so Starter and Pro get it
# without buying it. Availability of the control plane is not a tier feature: every
# surface (UI, GraphQL, SQL, pgwire, Bolt, Flight) goes through it, so a control-plane
# outage is a total outage for every org at once, including the ones paying $25. Making
# it an upsell would mean deliberately operating a platform that is known to have a
# single point of failure for most of its customers.
# HA here means a PUBLISHED RECOVERY TIME, not zero downtime. The SLO is minutes, and
# saying so is the product: an in-flight query dies on failover and the CLIENT RETRIES,
# exactly as a client retries a database transaction that lost its connection. A customer
# whose requirement is genuinely never-down runs BYO (REQ-1412) on their own infrastructure
# and their own redundancy — Provisa does not sell that, and pretending to would be selling
# an SLO the architecture cannot keep (OSS Trino has ONE coordinator; REQ-1451 keeps ONE
# control-plane writer because OrgRegistry serialises org rebuilds on an in-process
# asyncio.Lock, and active-active needs a distributed lock — a project, not a flag).
#
# There is NO warm standby VM in the floor AT LAUNCH, and the two halves of control-plane
# HA are separated because they are not the same purchase.
# A standby VM buys AVAILABILITY: without it a zone loss costs minutes. Single-zone is the
# market default at this price point — Cloud SQL, RDS and Neon all charge extra for regional,
# and neither comp (Hasura Cloud, Starburst Galaxy) offers a $25 customer a zone guarantee;
# they publish ~99.9% and so can we. Minutes of stated recovery is inside what customers
# already tolerate, so $97.82/mo of idle e2-standard-4 is insurance bought ahead of the
# expectation it covers. It is DEFERRED, not rejected: the standby goes in when the first Pro
# or Enterprise account lands, which is both the month it pays for itself and the month a
# contract starts asking about it.
# Recovery without it is a reschedule plus a runtime rebuild — minutes, at zero idle compute —
# and it gets better on its own as the fleet grows, because MULTIPLE control planes are already
# a first-class idea (REQ-1459 gives Enterprise its own) and REQ-1451's one-writer rule is per
# ORG rather than per fleet: once a second plane exists carrying paying work, failover is
# REASSIGNMENT of the orphaned orgs to it rather than a boot.
# The REGIONAL DATABASE is kept, and it is the half that is not optional, because it buys
# against DATA LOSS rather than downtime. Losing the admin database's zone on a single f1-micro
# is a restore from backup: org config, users, compiled models and governance rules recovered
# to a point in time, with everything written since then gone. That is not an outage, it is a
# customer redoing work they already did — the one failure the market does not forgive.
# What money must ALSO buy is the DATABASE: an f1-micro's zone going down is a restore
# measured in tens of minutes, so the baseline DB moves to a regional pair. Cloud SQL HA
# cannot use db-f1-micro at all — shared-core tiers have no regional configuration — so it
# lands on the smallest dedicated-core tier, billed in both zones.
CLOUDSQL_HA_HR = 0.1400  # db-custom-1-3840, REGIONAL (both zones billed)
CP_RECOVERY_MINUTES = 5  # published control-plane RTO: reschedule + runtime rebuild
CP_HA_DEFERRED_MO = E2_STANDARD_4_HR * HOURS_MO + CONTROL_PLANE_DISK_GB * PD_STANDARD_GB_MO
ENGINE_RECOVERY_MINUTES = 5  # published engine RTO without the Pro HA add-on (cold pod)
ENGINE_HA_RECOVERY_SECONDS = 30  # with it: repoint to a running standby coordinator
CONTROL_PLANE_HA_MO = (CLOUDSQL_HA_HR - CLOUDSQL_F1_MICRO_HR) * HOURS_MO
FIXED_FLOOR_MO = ZERO_CUSTOMER_MO + CONTROL_PLANE_WARM_MO + CONTROL_PLANE_HA_MO

# ------------------------- 2. PRICE LIST (set at/under the comps) -------------------------
# Starter mirrors Hasura's SHAPE as well as its rate: active-hours, not vCPU-hours.
# An active hour is one in which the org ran at least one query — readable from the
# same Trino event listener REQ-1450 already requires, and the only per-org unit the
# shared cluster can honestly report.
STARTER = {
    "label": "Starter",
    "unit": "active-hr",
    "rate": 1.30,  # exact Hasura v2 parity
    "egress": 0.13,  # exact Hasura v2 parity
    "incl_units": 0,
    "incl_gb": 25,
    "minimum": 25.0,  # monthly minimum, CREDITED against usage
}
# Pro sits under Galaxy per engine-hour, and Galaxy's number is per WORKER.
PRO = {
    "Pro S": {
        "label": "Pro S",
        "unit": "engine-hr",
        "vcpu": 4,
        "gb": 32,
        "rate": 1.50,
        "egress": 0.13,
        "incl_units": 0,
        "incl_gb": 50,
        "minimum": 99.0,
    },
    "Pro M": {
        "label": "Pro M",
        "unit": "engine-hr",
        "vcpu": 8,
        "gb": 64,
        "rate": 2.75,
        "egress": 0.13,
        "incl_units": 0,
        "incl_gb": 100,
        "minimum": 199.0,
    },
    "Pro L": {
        "label": "Pro L",
        "unit": "engine-hr",
        "vcpu": 16,
        "gb": 128,
        "rate": 5.50,
        "egress": 0.13,
        "incl_units": 0,
        "incl_gb": 200,
        "minimum": 399.0,
    },
}
# Enterprise (BYO engine) does NOT get a flat platform fee. Removing our compute
# cost does not remove the scale of what we govern: a 500-vCPU customer runs every
# query through the same compiler, governance, catalog and event-listener path as
# an 8-vCPU one. A flat fee therefore prices the largest customer we will ever have
# the same as the smallest — the one place the whole model leaks. The meter is the
# capacity of the engine THEY operate, read from the coordinator (system.runtime.nodes),
# not self-declared. Comp is Starburst ENTERPRISE (self-managed, licensed per vCPU of
# the customer's own cluster), not Galaxy — Galaxy prices compute we would be selling.
ENTERPRISE_VCPU_MO = 75.0
BYO = {
    "label": "Enterprise",
    "unit": "vCPU-mo",
    "rate": ENTERPRISE_VCPU_MO,
    "egress": 0.13,
    "incl_units": 0,
    "incl_gb": 100,
    "minimum": 999.0,
}

# Pro's own implied capacity price, for calibration: it is what a customer pays us
# per vCPU when we also supply the compute. Enterprise must land well under it, since
# the customer supplies the machine — but not so far under that BYO becomes the
# arbitrage everyone takes.
PRO_VCPU_MO = PRO["Pro M"]["rate"] * HOURS_MO / PRO["Pro M"]["vcpu"]

# Every self-serve tier has a CEILING. Past it the tier stops being a price list and
# becomes a contract: the customer is not cut off, the meter keeps running at the
# published rate, and a negotiated arrangement replaces it. The ceiling exists because
# past these points the published price is either the wrong shape (a customer needing
# HA, multi-region or a private VPC is buying an operational commitment, not hours) or
# simply wrong (a 500-vCPU BYO customer at a self-serve rate is a discount nobody
# asked for). Hitting one is a sales trigger, never a service interruption.
CAPS = {
    "Starter": {
        "ceiling": "400 active-hr/mo, 1 shared shard, per-query caps of REQ-1044",
        "converts_to": "Pro (self-serve upgrade, no negotiation)",
    },
    "Pro": {
        "ceiling": "Pro L (16 vCPU), ONE engine, single region; the HA add-on is engine-only",
        "converts_to": (
            "negotiated: multi-engine, multi-region, private networking, "
            "an SLA tighter than the published recovery targets"
        ),
    },
    "Enterprise": {
        "ceiling": "128 vCPU of customer-operated capacity",
        "converts_to": "negotiated: capacity-band licence, SLA, support tier",
    },
}
SELF_SERVE_VCPU_CEILING = 128
ENT_EXAMPLE_VCPU = 64  # a mid-size customer-operated cluster

# Enterprise may take its own CONTROL PLANE, not just its own engine — a dedicated
# VM, a dedicated Cloud SQL, and its own GKE cluster, none of it shared with any other
# tenant. Two things make this the natural top of the ladder rather than an exotic
# option. It is the only configuration in which the customer shares NOTHING with
# another org, which is the actual ask behind most enterprise security review; and it
# lifts them clear of REQ-1451's single-replica control plane, so their org's runtime
# rebuilds no longer queue behind anyone else's. Its cost is the whole platform floor
# again, plus the GKE management fee, because the free-tier zonal cluster is already
# spent on the shared one.
DEDICATED_CP_MO = FIXED_FLOOR_MO + GKE_CLUSTER_HR * HOURS_MO
DEDICATED_CP_PRICE_MO = round(DEDICATED_CP_MO * MARKUP, -1)

# Pro HA is an ADD-ON, and it is straightforward for one specific reason: a Trino
# coordinator holds no data, and its catalogs are reissued from the org's config on
# every runtime build (isolated_provisioner.deprovision docstring). A standby is
# therefore just a second pod plus a repoint — no replication, no state to move.
# What it CANNOT be is zero-downtime. One coordinator means in-flight queries die on
# failover, because OSS Trino has no multi-coordinator mode. The add-on sells RECOVERY
# TIME (seconds instead of a cold pod start), not query survival, and it covers the
# ENGINE only: a Pro org's control plane is still the shared single-replica one
# (REQ-1451), so a control-plane outage is untouched by it. Selling it as "HA" without
# those two sentences is selling something the engine cannot do.
# The standby is pinned — a scale-to-zero standby is not a standby — so it costs a full
# second node, and is therefore billed at the SAME published engine-hour rate as the
# primary. That keeps the margin identical to a normal Pro engine and needs no separate
# rate card.
PRO_HA_MULTIPLIER = 2.0  # primary + pinned standby, both at the published rate

# Enterprise's control plane is the same active-passive design as the baseline
# (CONTROL_PLANE_HA_MO above), just dedicated to one tenant rather than shared.
DEDICATED_CP_HA_MO = DEDICATED_CP_MO + CONTROL_PLANE_HA_MO
DEDICATED_CP_HA_PRICE_MO = round(DEDICATED_CP_HA_MO * MARKUP, -1)

# Egress beyond the included allowance leaves comp parity, because parity itself
# only clears 8% on GCP egress. The allowance is sized so a normal customer never
# reaches this rate; a customer who does is genuinely expensive to serve.
EGRESS_OVERAGE = round(COST_EGRESS_GB * MARKUP, 2)

# Starter free trial — a due-diligence window, not a free tier. It is compatible
# with the every-customer margin rule precisely because it is BOUNDED on both
# axes: it expires, and it caps usage, so its worst case is a known CAC number
# rather than an open-ended subsidy. It runs on the shared lane only; a trial
# never provisions a dedicated node pool, because a dedicated pool's cost is not
# divided by anyone.
TRIAL_DAYS = 14
TRIAL_ACTIVE_HRS = 40
TRIAL_EGRESS_GB = 25
TRIAL_CARD_REQUIRED = True  # card captured at signup with a $0 authorisation.
# Conversion is Neon-shaped: the trial ends by EXPIRY or by CAP, whichever comes
# first, and at that boundary the org AUTO-CONVERTS to paid Starter rather than
# being suspended. The card is what makes that a continuation instead of a second
# signup — hitting a wall mid-evaluation is the highest-drop-off moment there is.
# The caps still exist with a card on file: they bound the trial's cost, and the
# card bounds who can repeat it. One trial per card and per org.
TRIAL_WARN_AT_CAP_FRACTION = 0.80
TRIAL_WARN_DAYS_BEFORE_EXPIRY = 3

# Shared-cluster density: how many Starter orgs are concurrently active on one
# shared node. This divides the node cost per org and is the single largest driver
# of Starter's margin.
DENSITIES = [1, 2, 4, 8]


def starter_cost_hr(density):
    return shared_node_hr / density


def bill(plan, units, gb):
    # The minimum floors the COMPUTE line only. Egress overage is additive on top of it,
    # never absorbed by it: the allowance is already the concession, and letting a
    # below-minimum month swallow the overage hands out the one line that has a real
    # per-GB cost behind it (COST_EGRESS_GB) for nothing.
    over_g = max(0, gb - plan["incl_gb"])
    compute = max(plan["minimum"], units * plan["rate"])
    return compute + over_g * EGRESS_OVERAGE


def marginal_cost(unit_cost_hr, units, gb):
    return units * unit_cost_hr + gb * COST_EGRESS_GB


def rule(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


rule("1. ZERO-CUSTOMER OPERATING COST  (nobody signed up)")
for label, cost in zero_customer:
    print(f"  {label:52} {cost:8.2f}")
print(f"  {'-' * 52} {'-' * 8}")
print(f"  {'TOTAL':52} {ZERO_CUSTOMER_MO:8.2f} /mo   ({ZERO_CUSTOMER_MO / 30:.2f}/day)")
print()
print(
    f"  live fixed floor (control plane warm, >=1 customer): "
    f"${FIXED_FLOOR_MO:.2f}/mo  (${FIXED_FLOOR_MO / 30:.2f}/day)"
)
print(
    f"  each ADDITIONAL shared shard (shared_2, ...): "
    f"+${GKE_CLUSTER_HR * HOURS_MO:.0f}/mo, no free tier"
)

rule("2. PRICE VS COMPETITIVE ANCHOR")
print(
    f"  Hasura Cloud v2 Professional : ${HASURA_ACTIVE_HR:.2f}/active-hr + "
    f"${HASURA_EGRESS_GB:.2f}/GB, no base fee"
)
print(
    f"  Starburst Galaxy Pro         : {GALAXY_CREDITS_PER_WORKER_HR} credits x "
    f"${GALAXY_CREDIT:.2f} = ${GALAXY_WORKER_HR:.2f}/worker-hr, no base fee"
)
print()
print(
    f"  {'tier':9} {'unit':>11} {'rate':>7} {'anchor':>8} {'vs anchor':>10} "
    f"{'min/mo':>8} {'incl GB':>8}"
)
rows = [STARTER] + [PRO[k] for k in ("Pro S", "Pro M", "Pro L")] + [BYO]
for p in rows:
    if p["label"] == "Starter":
        anchor, delta = HASURA_ACTIVE_HR, p["rate"] / HASURA_ACTIVE_HR - 1
    elif p["label"] == "Enterprise":
        anchor, delta = None, None
    else:
        # Galaxy bills per WORKER; its worker is ~8 vCPU. Compare like for like by
        # scaling the anchor to the size's vCPU, or Pro L looks expensive when it is
        # simply two workers' worth of engine.
        anchor = GALAXY_WORKER_HR * p["vcpu"] / SHARED_NODE_VCPU
        delta = p["rate"] / anchor - 1
    a = f"{anchor:.2f}" if anchor else "—"
    d = f"{delta:+.0%}" if delta is not None else "—"
    r = f"{p['rate']:.2f}" if p["unit"] != "none" else "—"
    print(
        f"  {p['label']:9} {p['unit']:>11} {r:>7} {a:>8} {d:>10} "
        f"{p['minimum']:8.0f} {p['incl_gb']:8d}"
    )
print(
    f"  egress: ${STARTER['egress']:.2f}/GB at Hasura parity inside the allowance, "
    f"${EGRESS_OVERAGE:.2f}/GB beyond it"
)
print("  Galaxy's rate is PER WORKER; Provisa's is the whole engine, so Pro M at")
print(
    f"  ${PRO['Pro M']['rate']:.2f} undercuts a 1-worker Galaxy cluster and is a fraction of a real one."
)

rule("3. ENGINE COST TO SERVE (REQ-1449)")
print(f"  {'size':10} {'vCPU':>5} {'GB':>5} {'$/hr':>8} {'$/mo 24x7':>11} {'$/mo CUD':>10}")
for label, vcpu, gb in SIZES:
    hr = node_hr(vcpu, gb)
    print(
        f"  {label:10} {vcpu:5d} {gb:5d} {hr:8.4f} {hr * HOURS_MO:11.0f} "
        f"{node_hr(vcpu, gb, cud=True) * HOURS_MO:10.0f}"
    )
print(
    f"  {'shared':10} {SHARED_NODE_VCPU:5d} {SHARED_NODE_GB:5d} {shared_node_hr:8.4f}"
    f" {shared_node_hr * HOURS_MO:11.0f} {node_hr(SHARED_NODE_VCPU, SHARED_NODE_GB, cud=True) * HOURS_MO:10.0f}"
)

rule("4. MARGINAL MARGIN — must hold >= 75% for essentially every customer")
print("  Pro (dedicated node pool, cost is the org's own engine hours):")
print(f"    {'tier':8} {'hrs':>5} {'GB':>5} {'bill':>9} {'cost':>8} {'gross':>9} {'margin':>7}")
pro_fail = []
for key in ("Pro S", "Pro M", "Pro L"):
    p = PRO[key]
    c_hr = node_hr(p["vcpu"], p["gb"])
    for units, gb in [(40, 20), (160, p["incl_gb"]), (400, 150), (730, 400)]:
        b = bill(p, units, gb)
        c = marginal_cost(c_hr, units, gb)
        m = (b - c) / b
        flag = "" if m >= TARGET_GROSS_MARGIN else "  <-- BELOW"
        if m < TARGET_GROSS_MARGIN:
            pro_fail.append((key, units, gb, m))
        print(f"    {key:8} {units:5d} {gb:5d} {b:9.2f} {c:8.2f} {b - c:9.2f} {m:7.0%}{flag}")
print()
print("  Starter (shared node, cost divided by how many orgs are concurrently active):")
print(
    f"    {'density':>7} {'$/hr cost':>10} {'160hr bill':>11} {'cost':>8} "
    f"{'gross':>9} {'margin':>7}"
)
for d in DENSITIES:
    c_hr = starter_cost_hr(d)
    units, gb = 160, STARTER["incl_gb"]
    b = bill(STARTER, units, gb)
    c = marginal_cost(c_hr, units, gb)
    m = (b - c) / b
    flag = "" if m >= TARGET_GROSS_MARGIN else "  <-- BELOW"
    print(f"    {d:7d} {c_hr:10.4f} {b:11.2f} {c:8.2f} {b - c:9.2f} {m:7.0%}{flag}")
print("  Density is the Starter lever: the first Starter alone on a node is thin,")
print("  and every additional concurrent org improves it without changing the price.")

rule("5. FULLY-LOADED MARGIN RAMP — floor divided across N paying orgs")
print(f"  fixed floor ${FIXED_FLOOR_MO:.2f}/mo spread over N orgs, at typical usage")
print()
scenarios = [
    ("Starter @ 160 active-hr, density=N", None),
    ("Pro S @ 160 engine-hr", "Pro S"),
    ("Pro M @ 160 engine-hr", "Pro M"),
    ("Enterprise @ 64 vCPU", "BYO"),
]
print(f"  {'N orgs':>7} " + " ".join(f"{s[0].split(' @')[0].split(',')[0]:>12}" for s in scenarios))
crossover = {}
for n in (1, 2, 3, 5, 8, 10, 15, 25, 50):
    share = FIXED_FLOOR_MO / n
    cells = []
    for name, key in scenarios:
        if key is None:
            c_hr = starter_cost_hr(min(n, 8))  # density tracks org count, capped
            b = bill(STARTER, 160, STARTER["incl_gb"])
            c = marginal_cost(c_hr, 160, STARTER["incl_gb"]) + share
        elif key == "BYO":
            b = bill(BYO, ENT_EXAMPLE_VCPU, BYO["incl_gb"])
            # We supply no compute here; the marginal cost is egress plus the floor share.
            c = marginal_cost(0.0, 0, BYO["incl_gb"]) + share
        else:
            p = PRO[key]
            b = bill(p, 160, p["incl_gb"])
            c = marginal_cost(node_hr(p["vcpu"], p["gb"]), 160, p["incl_gb"]) + share
        m = (b - c) / b
        cells.append(f"{m:11.0%} ")
        if m >= TARGET_GROSS_MARGIN and name not in crossover:
            crossover[name] = n
    print(f"  {n:7d} " + " ".join(cells))
print()
for name, _ in scenarios:
    n = crossover.get(name)
    print(
        f"  {name:38} crosses {TARGET_GROSS_MARGIN:.0%} fully-loaded at "
        f"{'N = ' + str(n) if n else 'not on this grid'}"
    )

rule("6. THE THRESHOLD, STATED PLAINLY")
share1 = FIXED_FLOOR_MO
b = bill(STARTER, 160, STARTER["incl_gb"])
c1 = marginal_cost(starter_cost_hr(1), 160, STARTER["incl_gb"]) + share1
print(
    f"  Customer #1 (a lone Starter) is fully-loaded {(b - c1) / b:.0%} margin on a ${b:.0f} bill —"
)
print(f"  it is carrying the entire ${FIXED_FLOOR_MO:.0f}/mo floor by itself. That is arithmetic,")
print("  not mispricing: the price already clears 75% marginally at any real density.")
print()
print("  What the ramp actually needs is not a higher price, it is a denominator.")
print(
    f"  The floor is small enough (${FIXED_FLOOR_MO:.0f}/mo) that a single Pro M or Enterprise org"
)


def _cx(label):
    return crossover.get(next(n for n, _ in scenarios if n.startswith(label)))


print(
    f"  covers it outright. Starter crosses at {_cx('Starter')} orgs, "
    f"Pro M at {_cx('Pro M')}, Pro S at {_cx('Pro S')} —"
)
print("  the mix matters more than the count: one Enterprise account clears the")
print(f"  whole floor on its own, at customer #{_cx('Enterprise')}.")
print()
print(
    f"  Do NOT pin the shared node pool above zero. Pinned, it adds "
    f"${shared_node_hr * HOURS_MO:.0f}/mo of"
)
print("  fixed cost with no customer to attribute it to, which pushes the crossover")
print(
    f"  from a ${FIXED_FLOOR_MO:.0f} floor to a ${FIXED_FLOOR_MO + shared_node_hr * HOURS_MO:.0f} one — "
    f"{(FIXED_FLOOR_MO + shared_node_hr * HOURS_MO) / FIXED_FLOOR_MO:.1f}x the denominator"
)
print("  needed at every tier. This is why REQ-1450 was amended.")

rule("7. STARTER FREE TRIAL — bounded due-diligence window, not a free tier")
print(
    f"  {TRIAL_DAYS} days, capped at {TRIAL_ACTIVE_HRS} active-hr and "
    f"{TRIAL_EGRESS_GB} GB, shared lane only."
)
print(f"  card at signup: {'required ($0 auth)' if TRIAL_CARD_REQUIRED else 'not required'}")
print("  ends on: expiry OR cap, whichever first -> AUTO-CONVERTS to paid Starter")
print(
    f"  warnings: at {TRIAL_WARN_AT_CAP_FRACTION:.0%} of cap, and "
    f"{TRIAL_WARN_DAYS_BEFORE_EXPIRY} days before expiry"
)
print(f"  first paid month lands at the ${STARTER['minimum']:.0f} minimum if usage stays low")
print()
print(f"  {'density':>7} {'worst-case cost':>16} {'  = CAC per trial'}")
for d in DENSITIES:
    c = TRIAL_ACTIVE_HRS * starter_cost_hr(d) + TRIAL_EGRESS_GB * COST_EGRESS_GB
    print(f"  {d:7d} {c:16.2f}")
worst = TRIAL_ACTIVE_HRS * starter_cost_hr(1) + TRIAL_EGRESS_GB * COST_EGRESS_GB
print()
print(f"  Worst case is ${worst:.2f} per trial — a trialist alone on a shared node")
print(
    f"  burning every capped hour. At $500/mo of trial budget that is {int(500 / worst)} concurrent"
)
print("  trials; at realistic density it is several times more.")
print()
b160 = bill(STARTER, 160, STARTER["incl_gb"])
print(f"  Payback: a converted Starter at ${b160:.0f}/mo repays the worst-case trial in")
print(
    f"  {worst / (b160 - marginal_cost(starter_cost_hr(4), 160, STARTER['incl_gb'])) * 30:.1f} days of gross profit at density 4."
)
print()
print("  The caps are the design, not the duration. An expiring window with no usage")
print("  cap still lets one trialist run a shared node flat out for two weeks, which")
print("  is the same unbounded subsidy a free tier would be — just shorter.")
print()
print("  Billing-side consequence: this is a Lemon Squeezy subscription created WITH a")
print("  trial period at signup, not a checkout deferred until the trial ends. The")
print("  subscription exists from day one, the card is on file from day one, and the")
print("  conversion is LS moving it out of trial — no second checkout to drop off at.")
print("  It also means the subscription_created webhook fires before any revenue, so")
print("  entitlement must key off subscription STATUS, not off a payment having landed.")

rule("8. WHERE THE COMPS CONSTRAIN THE MARGIN RULE")
print(
    f"  Egress at Hasura parity (${HASURA_EGRESS_GB:.2f}/GB) clears only "
    f"{(HASURA_EGRESS_GB - COST_EGRESS_GB) / HASURA_EGRESS_GB:.0%} on GCP egress."
)
print(
    f"  It is priced at parity INSIDE the included allowance and at "
    f"${EGRESS_OVERAGE:.2f}/GB beyond it,"
)
print("  so the headline matches the comp and only genuinely egress-heavy customers")
print("  pay the real rate. Getting parity to clear 75% on the headline means leaving")
print("  GCP egress rates, not discounting.")
print()
print(f"  Starter's ${STARTER['minimum']:.0f}/mo minimum is what stops a signed-up-but-idle org")
print("  from consuming a floor share for free. Both comps use $0 base fees; a")
print("  credited minimum keeps the usage-priced shape while refusing that hole.")
print()
if pro_fail:
    print(f"  Pro marginal-margin failures: {pro_fail}")
else:
    print("  Pro clears 75% marginally at every usage point tested, at every size.")

rule("9. ENTERPRISE — capacity meter, not a flat fee")
print(f"  Enterprise bills ${ENTERPRISE_VCPU_MO:.0f} per vCPU-month of the capacity the CUSTOMER")
print("  operates, read from their coordinator (system.runtime.nodes), with a")
print(f"  ${BYO['minimum']:.0f}/mo minimum. A flat platform fee was the model's one real leak:")
print("  it priced a 500-vCPU bank the same as an 8-vCPU team, while both run every")
print("  query through the same compiler, governance and catalog path.")
print()
print(f"  Pro's implied capacity price (we supply the compute) : ${PRO_VCPU_MO:,.0f}/vCPU-mo")
print(
    f"  Enterprise capacity price (customer supplies compute): ${ENTERPRISE_VCPU_MO:,.0f}/vCPU-mo"
)
print(
    f"  ratio: {ENTERPRISE_VCPU_MO / PRO_VCPU_MO:.0%} of Pro — the discount IS the compute they bring,"
)
print("  and it is deliberately not so deep that BYO becomes the arbitrage everyone takes.")
print()
print(f"  {'customer vCPU':>14} {'$/mo':>10} {'$/yr':>12}  {'self-serve?':>12}")
for v in (8, 16, 32, 64, 128, 256, 512):
    m = max(BYO["minimum"], v * ENTERPRISE_VCPU_MO)
    ok = "yes" if v <= SELF_SERVE_VCPU_CEILING else "NEGOTIATED"
    print(f"  {v:14d} {m:10,.0f} {m * 12:12,.0f}  {ok:>12}")
print()
print("  Dedicated control plane (own VM, own Cloud SQL, own GKE cluster):")
print(
    f"    cost ${DEDICATED_CP_MO:,.2f}/mo -> ${DEDICATED_CP_PRICE_MO:,.0f}/mo at the {MARKUP:.0f}x markup,"
)
print("    ADDED to the capacity licence. It is the only configuration where the")
print("    customer shares nothing with another org, and it lifts them clear of the")
print("    REQ-1451 single-replica control plane, so their rebuilds queue behind nobody.")
print(
    f"    Its own floor is a full platform floor plus ${GKE_CLUSTER_HR * HOURS_MO:.0f}/mo of GKE management,"
)
print("    because the free-tier zonal cluster is already spent on the shared one.")
print()
print("  Dedicated control plane WITH the regional database (isolation, not a")
print("  different availability class):")
print(
    f"    cost ${DEDICATED_CP_HA_MO:,.2f}/mo -> ${DEDICATED_CP_HA_PRICE_MO:,.0f}/mo at the {MARKUP:.0f}x markup"
)
print("    What Enterprise buys here is ISOLATION. Availability is already baseline,")
print("    and the recovery target is the same one everybody else gets.")

rule("9b. WHAT 'HA' MEANS HERE — a published recovery time, not zero downtime")
print("  The SLO is stated in MINUTES and sold as minutes. Every tier gets it; none of")
print("  it is an upsell, because a control-plane outage takes out every org at once.")
print()
print(f"  {'surface':<34} {'recovery target':>18}  who")
print(
    f"  {'control plane (reschedule + rebuild)':<38} {f'~{CP_RECOVERY_MINUTES} min':>14}  every tier, baseline"
)
print(f"  {'admin DB (regional Cloud SQL)':<34} {'~1-2 min':>18}  every tier, baseline")
print(
    f"  {'engine, no add-on (cold pod)':<34} {f'~{ENGINE_RECOVERY_MINUTES} min':>18}  Starter + Pro"
)
print(
    f"  {'engine, Pro HA add-on (repoint)':<34} {f'~{ENGINE_HA_RECOVERY_SECONDS}s':>18}  Pro only"
)
print()
print(
    f"  A WARM STANDBY control plane (${CP_HA_DEFERRED_MO:,.0f}/mo) is DEFERRED, not refused. It would take"
)
print(
    f"  the control-plane target from ~{CP_RECOVERY_MINUTES} min to ~1 min, which is a zone-outage"
)
print("  guarantee neither comp offers a $25 customer and the market does not yet expect.")
print("  It goes in when the first Pro or Enterprise account lands — the month it pays for")
print("  itself and the month a contract starts asking. The REGIONAL DATABASE is not")
print("  deferred: it buys against DATA LOSS, not downtime. A zone loss on a single")
print("  f1-micro is a restore from backup — org config, users, compiled models and")
print("  governance rules rolled back to a point in time — which is a customer redoing")
print("  work they already did. Downtime is forgiven; that is not.")
print()
print("  Three things follow, and all three go in the literature rather than the")
print("  footnotes, because an SLO nobody stated is an SLO the customer invents:")
print("   1. A FAILOVER KILLS IN-FLIGHT QUERIES. HA does not mean a query cannot burp.")
print("      The contract is the same one a database gives a client whose transaction")
print("      lost its connection: the query fails cleanly and THE CLIENT RETRIES. Every")
print("      surface we ship (GraphQL, SQL, pgwire, Bolt, Flight) is retry-safe on a")
print("      read; committing to more would mean claiming an OSS Trino coordinator can")
print("      hand off a running query, and it cannot.")
print("   2. NEVER-DOWN IS BYO. A customer whose actual requirement is continuous")
print("      availability runs Enterprise/BYO on their own engine, their own redundancy,")
print("      their own operators. That is a real answer to a real requirement — and it")
print("      is a better one than selling them a managed SLO we would have to break.")
print("   3. Starter gets NO engine HA, deliberately. A shared shard is one coordinator;")
print("      if it restarts, every Starter org on it takes a cold start. That is the")
print("      right trade at a $25 minimum — the tier is SMBs and tinkerers, and a pinned")
print("      standby shard would cost more than the orgs sitting on it pay.")
print()
print("  PRO HA is an ADD-ON that buys ONE thing: recovery time on the ENGINE, seconds")
print("  instead of a cold pod start. It is simple to build — a coordinator holds no data")
print("  and reissues its catalogs from the org's config on every runtime build, so a")
print("  standby is a second pod plus a repoint, no replication, no state to move.")
print(
    f"    price: {PRO_HA_MULTIPLIER:.0f}x the size's engine-hour rate (primary + PINNED standby),"
)
print("           because a scale-to-zero standby is not a standby. Same rate, same margin.")
for label, vcpu, gb in SIZES:
    p_ = PRO[label]
    print(
        f"      {label:6} ${p_['rate']:.2f}/hr -> ${p_['rate'] * PRO_HA_MULTIPLIER:.2f}/hr HA "
        f"(${p_['rate'] * PRO_HA_MULTIPLIER * HOURS_MO:,.0f}/mo at 24x7)"
    )
print("    It does NOT shorten the control-plane target and does NOT stop a query from")
print("    dying on failover. Marketing it as zero downtime is the one claim to refuse.")
print()
print("  FAILURE DOMAIN — what the standby does NOT cover:")
print("  Everything above is ZONE-level. The warm standby plane, the regional Cloud SQL")
print("  pair and the shared shard all live in ONE REGION, so a region-wide outage takes")
print("  the control plane and every engine down together and no standby inside it helps.")
print("  That is a deliberate scope, and the number to publish is a separate one: regional")
print("  recovery is a RESTORE somewhere else, measured in HOURS, from cross-region backups.")
print("  Buying past it means a second region — a standby plane at full price again, a")
print("  cross-region Cloud SQL replica, and continuous replication egress — which is a")
print("  NEGOTIATED Enterprise item (REQ-1458), not a line on the shared floor. A customer")
print("  who cannot accept hours of regional recovery is, again, a BYO customer: their")
print("  engine, their regions, their redundancy.")

rule("10. CAPS — where the price list stops and a contract starts")
print("  Every tier has a ceiling. Past it the customer is NOT cut off: the meter keeps")
print("  running at the published rate and a negotiated arrangement replaces it. A cap")
print("  is a sales trigger, never a service interruption — cutting off the largest")
print("  customer at the moment they commit is the one outcome worth engineering against.")
print()
for tier, c in CAPS.items():
    print(f"  {tier}")
    print(f"    ceiling : {c['ceiling']}")
    print(f"    becomes : {c['converts_to']}")
print()
print("  Starter's ceiling converts SELF-SERVE (to Pro) rather than to a negotiation,")
print("  because a growing Starter org is the funnel working, not an exception to it.")
print("  Pro's and Enterprise's ceilings convert to a contract, because past them the")
print("  customer is buying an operational commitment — HA, regions, private networking,")
print("  an SLA — which is not a thing an hourly rate can express.")

# ------------------------- 11. FULL WARM STANDBY OF THE SHARED LANE -------------------------
# The question this answers: what if the whole shared lane — control plane AND engine —
# must have a warm twin in a SECOND REGION, so a regional outage is a repoint rather than
# a restore. Priced as an OPTION, deliberately not folded into the floor, because the
# numbers below are what decide whether it can be.
#
# GCP charges the same n2/e2/Cloud SQL list rates in the major US regions, so a second
# region costs what the first one does — the expense is duplication, not geography. The
# standby control-plane VM is billed IN FULL here: the floor carries no standby at all
# (section 9b deferred it), so there is nothing to relocate and region B's plane is net
# new spend rather than a move.
CROSS_REGION_EGRESS_GB = 0.02  # same-continent inter-region
DR_WAL_GB_MO = 50  # admin-DB change volume shipped to the replica
CLOUDSQL_REPLICA_HR = CLOUDSQL_HA_HR / 2  # cross-region replica: one zone, not a pair

DR_WARM_CP = [
    ("Standby control-plane VM in region B", CP_HA_DEFERRED_MO),  # net new: no standby in the floor
    ("Cross-region Cloud SQL replica", CLOUDSQL_REPLICA_HR * HOURS_MO),
    ("  its storage", CLOUDSQL_DISK_GB * CLOUDSQL_STORAGE_GB_MO),
    ("  replication egress", DR_WAL_GB_MO * CROSS_REGION_EGRESS_GB),
    ("Second GKE cluster (free zonal tier already spent)", GKE_CLUSTER_HR * HOURS_MO),
]
DR_WARM_CP_MO = sum(c for _, c in DR_WARM_CP)
DR_WARM_ENGINE_MO = shared_node_hr * HOURS_MO  # a PINNED shard node; min=0 is not warm
DR_FULL_MO = DR_WARM_CP_MO + DR_WARM_ENGINE_MO

rule("11. FULL WARM STANDBY OF THE SHARED LANE (second region)")
for label, cost in DR_WARM_CP:
    print(f"  {label:<52} {cost:8,.2f}")
print(f"  {'-' * 52} {'-' * 8}")
print(f"  {'warm control plane + replicated DB in region B':<52} {DR_WARM_CP_MO:8,.2f} /mo")
print(f"  {'PINNED warm shared shard node in region B':<52} {DR_WARM_ENGINE_MO:8,.2f} /mo")
print(f"  {'FULL warm standby of the lane':<52} {DR_FULL_MO:8,.2f} /mo")
print()
print(f"  live floor today                  ${FIXED_FLOOR_MO:8,.2f}/mo")
print(
    f"  + warm CP/DB only (engine cold)    ${FIXED_FLOOR_MO + DR_WARM_CP_MO:8,.2f}/mo   "
    f"({(FIXED_FLOOR_MO + DR_WARM_CP_MO) / FIXED_FLOOR_MO:.1f}x)"
)
print(
    f"  + FULL warm lane                   ${FIXED_FLOOR_MO + DR_FULL_MO:8,.2f}/mo   "
    f"({(FIXED_FLOOR_MO + DR_FULL_MO) / FIXED_FLOOR_MO:.1f}x)"
)
print()
print("  The split is the whole answer: the CONTROL PLANE half is cheap and the ENGINE")
print(
    f"  half is not. Warming the plane and its database costs ${DR_WARM_CP_MO:,.0f}/mo — a standby VM"
)
print(
    f"  (${CP_HA_DEFERRED_MO:,.0f}, net new since the floor carries none), a cross-region replica and a"
)
print(f"  second cluster fee. Warming the SHARD costs ${DR_WARM_ENGINE_MO:,.0f}/mo,")
print(
    f"  {DR_WARM_ENGINE_MO / DR_WARM_CP_MO:.1f}x more, because a node pool at min=0 is not a standby — and pinning one"
)
print("  in region B is exactly the fixed cost REQ-1450 was amended to refuse in region A.")
print()
print("  What each buys, stated as the recovery target it changes:")
print("    region outage today                    hours (restore from backup)")
print(
    f"    with warm CP/DB, cold engine           ~{ENGINE_RECOVERY_MINUTES + CP_RECOVERY_MINUTES} min "
    f"(plane repoints, shard scales 0->1)"
)
print(f"    with the FULL warm lane                ~{CP_RECOVERY_MINUTES} min (repoint only)")
print()
print("  So the recommendation is the ASYMMETRIC standby: warm plane, warm replicated")
print("  database, second cluster present, node pool at min=0. It turns a multi-hour")
print(
    f"  restore into a ~{ENGINE_RECOVERY_MINUTES + CP_RECOVERY_MINUTES}-minute recovery for {DR_WARM_CP_MO / DR_FULL_MO:.0%} of the cost, and the"
)
print(
    f"  {ENGINE_RECOVERY_MINUTES} extra minutes it concedes are a cold shard start — which is exactly what"
)
print("  Starter already accepts every day under REQ-1450's scale-to-zero.")
print()
n_needed_cp = (DR_WARM_CP_MO * MARKUP) / (STARTER["minimum"])
n_needed_full = (DR_FULL_MO * MARKUP) / (STARTER["minimum"])
print("  Who pays for it, if it is not sold separately:")
print(
    f"    warm CP/DB, recovered at the {MARKUP:.0f}x markup, needs {n_needed_cp:,.0f} Starter minimums"
)
print(f"    the FULL warm lane needs {n_needed_full:,.0f} Starter minimums")
print("  That is the argument for pricing regional standby as a NEGOTIATED Enterprise")
print("  item rather than a floor line: the shared lane cannot absorb the engine half,")
print("  and the customers who need it are not the ones on the $25 minimum.")
