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
  BYO             customer's own engine, no compute meter

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

E2_MICRO_HR = 0.0             # always-free tier, 1 per billing account (us-central1)
E2_STANDARD_4_HR = 0.134      # control-plane VM when running

GKE_CLUSTER_HR = 0.10
GKE_FREE_ZONAL_CLUSTERS = 1

CLOUDSQL_F1_MICRO_HR = 0.0105
CLOUDSQL_STORAGE_GB_MO = 0.17
CLOUDSQL_DISK_GB = 20

PD_STANDARD_GB_MO = 0.04      # a retained boot disk on a STOPPED VM still bills
CONTROL_PLANE_DISK_GB = 100
FRONT_DOOR_DISK_GB = 10

COST_EGRESS_GB = 0.12         # result bytes leaving GCP. The source-cloud leg is
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
    ("GKE cluster management (1 zonal, free tier)",
     max(0, 1 - GKE_FREE_ZONAL_CLUSTERS) * GKE_CLUSTER_HR * HOURS_MO),
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
FIXED_FLOOR_MO = ZERO_CUSTOMER_MO + CONTROL_PLANE_WARM_MO

# ------------------------- 2. PRICE LIST (set at/under the comps) -------------------------
# Starter mirrors Hasura's SHAPE as well as its rate: active-hours, not vCPU-hours.
# An active hour is one in which the org ran at least one query — readable from the
# same Trino event listener REQ-1450 already requires, and the only per-org unit the
# shared cluster can honestly report.
STARTER = {
    "label": "Starter", "unit": "active-hr",
    "rate": 1.30,            # exact Hasura v2 parity
    "egress": 0.13,          # exact Hasura v2 parity
    "incl_units": 0, "incl_gb": 25,
    "minimum": 25.0,         # monthly minimum, CREDITED against usage
}
# Pro sits under Galaxy per engine-hour, and Galaxy's number is per WORKER.
PRO = {
    "Pro S": {"label": "Pro S", "unit": "engine-hr", "vcpu": 4, "gb": 32,
              "rate": 1.50, "egress": 0.13, "incl_units": 0, "incl_gb": 50,
              "minimum": 99.0},
    "Pro M": {"label": "Pro M", "unit": "engine-hr", "vcpu": 8, "gb": 64,
              "rate": 2.75, "egress": 0.13, "incl_units": 0, "incl_gb": 100,
              "minimum": 199.0},
    "Pro L": {"label": "Pro L", "unit": "engine-hr", "vcpu": 16, "gb": 128,
              "rate": 5.50, "egress": 0.13, "incl_units": 0, "incl_gb": 200,
              "minimum": 399.0},
}
BYO = {"label": "BYO", "unit": "none", "rate": 0.0, "egress": 0.13,
       "incl_units": 0, "incl_gb": 100, "minimum": 299.0}

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
TRIAL_CARD_REQUIRED = False   # no card: maximises top-of-funnel, and the caps are
                              # what bound the exposure instead of the card.

# Shared-cluster density: how many Starter orgs are concurrently active on one
# shared node. This divides the node cost per org and is the single largest driver
# of Starter's margin.
DENSITIES = [1, 2, 4, 8]


def starter_cost_hr(density):
    return shared_node_hr / density


def bill(plan, units, gb):
    over_g = max(0, gb - plan["incl_gb"])
    usage = units * plan["rate"] + over_g * EGRESS_OVERAGE
    return max(plan["minimum"], usage)


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
print(f"  {'TOTAL':52} {ZERO_CUSTOMER_MO:8.2f} /mo   "
      f"({ZERO_CUSTOMER_MO / 30:.2f}/day)")
print()
print(f"  live fixed floor (control plane warm, >=1 customer): "
      f"${FIXED_FLOOR_MO:.2f}/mo  (${FIXED_FLOOR_MO / 30:.2f}/day)")
print(f"  each ADDITIONAL shared shard (shared_2, ...): "
      f"+${GKE_CLUSTER_HR * HOURS_MO:.0f}/mo, no free tier")

rule("2. PRICE VS COMPETITIVE ANCHOR")
print(f"  Hasura Cloud v2 Professional : ${HASURA_ACTIVE_HR:.2f}/active-hr + "
      f"${HASURA_EGRESS_GB:.2f}/GB, no base fee")
print(f"  Starburst Galaxy Pro         : {GALAXY_CREDITS_PER_WORKER_HR} credits x "
      f"${GALAXY_CREDIT:.2f} = ${GALAXY_WORKER_HR:.2f}/worker-hr, no base fee")
print()
print(f"  {'tier':9} {'unit':>11} {'rate':>7} {'anchor':>8} {'vs anchor':>10} "
      f"{'min/mo':>8} {'incl GB':>8}")
rows = [STARTER] + [PRO[k] for k in ("Pro S", "Pro M", "Pro L")] + [BYO]
for p in rows:
    if p["label"] == "Starter":
        anchor, delta = HASURA_ACTIVE_HR, p["rate"] / HASURA_ACTIVE_HR - 1
    elif p["label"] == "BYO":
        anchor, delta = None, None
    else:
        anchor, delta = GALAXY_WORKER_HR, p["rate"] / GALAXY_WORKER_HR - 1
    a = f"{anchor:.2f}" if anchor else "—"
    d = f"{delta:+.0%}" if delta is not None else "—"
    r = f"{p['rate']:.2f}" if p["unit"] != "none" else "—"
    print(f"  {p['label']:9} {p['unit']:>11} {r:>7} {a:>8} {d:>10} "
          f"{p['minimum']:8.0f} {p['incl_gb']:8d}")
print(f"  egress: ${STARTER['egress']:.2f}/GB at Hasura parity inside the allowance, "
      f"${EGRESS_OVERAGE:.2f}/GB beyond it")
print("  Galaxy's rate is PER WORKER; Provisa's is the whole engine, so Pro M at")
print(f"  ${PRO['Pro M']['rate']:.2f} undercuts a 1-worker Galaxy cluster and is a fraction of a real one.")

rule("3. ENGINE COST TO SERVE (REQ-1449)")
print(f"  {'size':10} {'vCPU':>5} {'GB':>5} {'$/hr':>8} {'$/mo 24x7':>11} {'$/mo CUD':>10}")
for label, vcpu, gb in SIZES:
    hr = node_hr(vcpu, gb)
    print(f"  {label:10} {vcpu:5d} {gb:5d} {hr:8.4f} {hr * HOURS_MO:11.0f} "
          f"{node_hr(vcpu, gb, cud=True) * HOURS_MO:10.0f}")
print(f"  {'shared':10} {SHARED_NODE_VCPU:5d} {SHARED_NODE_GB:5d} {shared_node_hr:8.4f}"
      f" {shared_node_hr * HOURS_MO:11.0f} {node_hr(SHARED_NODE_VCPU, SHARED_NODE_GB, cud=True) * HOURS_MO:10.0f}")

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
        print(f"    {key:8} {units:5d} {gb:5d} {b:9.2f} {c:8.2f} {b - c:9.2f} "
              f"{m:7.0%}{flag}")
print()
print("  Starter (shared node, cost divided by how many orgs are concurrently active):")
print(f"    {'density':>7} {'$/hr cost':>10} {'160hr bill':>11} {'cost':>8} "
      f"{'gross':>9} {'margin':>7}")
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
    ("BYO", "BYO"),
]
print(f"  {'N orgs':>7} " + " ".join(f"{s[0].split(' @')[0].split(',')[0]:>12}" for s in scenarios))
crossover = {}
for n in (1, 2, 3, 5, 8, 10, 15, 25, 50):
    share = FIXED_FLOOR_MO / n
    cells = []
    for name, key in scenarios:
        if key is None:
            c_hr = starter_cost_hr(min(n, 8))     # density tracks org count, capped
            b = bill(STARTER, 160, STARTER["incl_gb"])
            c = marginal_cost(c_hr, 160, STARTER["incl_gb"]) + share
        elif key == "BYO":
            b = bill(BYO, 0, BYO["incl_gb"])
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
    print(f"  {name:38} crosses {TARGET_GROSS_MARGIN:.0%} fully-loaded at "
          f"{'N = ' + str(n) if n else 'not on this grid'}")

rule("6. THE THRESHOLD, STATED PLAINLY")
share1 = FIXED_FLOOR_MO
b = bill(STARTER, 160, STARTER["incl_gb"])
c1 = marginal_cost(starter_cost_hr(1), 160, STARTER["incl_gb"]) + share1
print(f"  Customer #1 (a lone Starter) is fully-loaded {(b - c1) / b:.0%} margin on a "
      f"${b:.0f} bill —")
print(f"  it is carrying the entire ${FIXED_FLOOR_MO:.0f}/mo floor by itself. That is arithmetic,")
print("  not mispricing: the price already clears 75% marginally at any real density.")
print()
print("  What the ramp actually needs is not a higher price, it is a denominator.")
print(f"  The floor is small enough (${FIXED_FLOOR_MO:.0f}/mo) that a single Pro M or BYO org")
print("  covers it outright, so the crossover arrives at a customer count in single")
print("  digits rather than at scale.")
print()
print(f"  Do NOT pin the shared node pool above zero. Pinned, it adds "
      f"${shared_node_hr * HOURS_MO:.0f}/mo of")
print("  fixed cost with no customer to attribute it to, which pushes the crossover")
print(f"  from single digits to roughly {int((FIXED_FLOOR_MO + shared_node_hr * HOURS_MO) / (FIXED_FLOOR_MO / max(1, crossover.get('Pro M @ 160 engine-hr', 1)))) or 1}x further out. This is why REQ-1450 was amended.")

rule("7. WHERE THE COMPS CONSTRAIN THE MARGIN RULE")
print(f"  Egress at Hasura parity (${HASURA_EGRESS_GB:.2f}/GB) clears only "
      f"{(HASURA_EGRESS_GB - COST_EGRESS_GB) / HASURA_EGRESS_GB:.0%} on GCP egress.")
print(f"  It is priced at parity INSIDE the included allowance and at "
      f"${EGRESS_OVERAGE:.2f}/GB beyond it,")
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
