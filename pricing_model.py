# Copyright (c) 2026 Kenneth Stott
# Canary: 8fe4b6db-4ac1-4456-bb81-7a4f92b0d413
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Provisa cost-to-serve + pricing model.

Goal: back out free-band thresholds (egress GB, vCPU-hrs) that sit safely
under break-even, and size platform fee + overage rates for a target margin.

All costs are GCP us-central1 list/spot as of 2026-07. Adjust ASSUMPTIONS.
"""

# ------------------------- ASSUMPTIONS (edit these) -------------------------
# Marginal (per-unit) cash costs to serve — what you owe GCP.
SPOT_NODE_HR = 0.080          # e2-standard-8 (8 vCPU) on Spot, $/hr
ONDEMAND_NODE_HR = 0.268      # e2-standard-8 on-demand, $/hr (warm pool)
VCPU_PER_NODE = 8
COST_VCPU_HR = SPOT_NODE_HR / VCPU_PER_NODE   # $/vCPU-hr  -> 0.010

# Warm pool: min=N on-demand workers held hot for interactive latency.
# Its cost is FIXED (always-on), amortized per paying org like the floor.
# Set to 0 to rely purely on scale-to-zero + resume-minimum billing.
WARM_POOL_NODES = 0
WARM_POOL_MO = WARM_POOL_NODES * ONDEMAND_NODE_HR * 730   # $/mo always-on

# ── MPP ongoing warm cost (the structural delta vs Hasura's single instance) ──
# Trino is multi-instance MPP: a warm coordinator (planner + persistent-protocol
# listeners; node-scheduler.include-coordinator=false so it can't execute) is a
# REQUIRED always-on box, and interactive zero-cold-start needs a warm worker too.
# Hasura's warm surface is one instance (~$0.005/hr). So Provisa's warm-hour COGS
# is structurally higher, not at parity — the active-hour margin below reflects it.
COORD_HR = 0.134              # e2-standard-4 coordinator on-demand, $/hr
CUD_DISCOUNT = 0.37           # 1-yr committed-use discount on always-on warm instances
SHUFFLE_GB_PER_ACTIVE_HR = 2  # cross-zone intra-cluster shuffle on multi-worker joins
COST_XZONE_GB = 0.01          # GCP cross-zone egress, $/GB (same-zone is free)
ACTIVE_HOUR_PRICE = 3.25      # Hasura parity + small premium, $/active-hr

# ── Two anchors: Provisa is a mix of Hasura (warm low-latency API, active-hour)
# and Starburst Galaxy (analytical MPP, worker-hour). Galaxy is Trino-as-a-service
# — the true comp for the analytical lane. It bills 6 credits/worker-hr at
# $0.50/credit (Pro) = $3.00/worker-hr, autoscaled, regardless of underlying Spot
# (~$0.08/worker-hr). That is the software-rent price the analytical lane should
# anchor to — NOT cost/(1-margin) over Spot, which underprices it ~9x.
GALAXY_CREDIT = 0.50          # Pro tier, $/credit (AWS us-east); Ent $0.75, MC $1.00
GALAXY_CREDITS_PER_WORKER_HR = 6
GALAXY_WORKER_HR = GALAXY_CREDIT * GALAXY_CREDITS_PER_WORKER_HR   # $3.00/worker-hr Pro
WORKER_HR_PRICE = 2.50        # Provisa analytical lane, $/worker-hr (just under Galaxy Pro)

# Egress. Our ONLY egress cost is result bytes leaving GCP to the consumer.
# The source-cloud (AWS/Azure) egress a query triggers is billed to the account
# that OWNS the source — the customer — not us; bytes arriving at GCP are free
# ingress. So no source leg on our books. Result sets are the small, filtered/
# aggregated output, not the raw scan (which never leaves the source's cloud on
# our bill).
COST_EGRESS_GB = 0.12         # GCP internet egress on result bytes, $/GB

# Fixed floor — shared, unconditional. Cloud Run site (min=1) + Cloud SQL f1-micro.
# The ONLY fixed cost with no matching usage meter, so it must stay small and be
# covered by the flat-fee minimum (fee >= floor / paying_orgs). Everything warm beyond
# the site is rented by the active-hour (warm add-on), so cost<->revenue are welded.
CONTROL_PLANE_MO = 19.0       # Cloud Run min=1 (~$10) + Cloud SQL f1-micro (~$9)
FLOOR_TOTAL_MO = CONTROL_PLANE_MO + WARM_POOL_MO   # control plane + hot worker(s)
PAYING_ORGS = 10              # conservative early denominator
FLOOR_PER_ORG_MO = FLOOR_TOTAL_MO / PAYING_ORGS

# Cold start: waking a scaled-to-zero worker burns boot time you can't query on.
# Bill a per-resume minimum (Snowflake charges 60s) to cover it.
BOOT_SECONDS = 90            # time to provision+warm a fresh worker
MIN_BILLED_SECONDS = 90      # minimum cluster-seconds billed per resume
RESUMES_PER_MO = {           # cold wakes/mo by profile (pool misses)
    "Light / interactive": 40,
    "Team / analytics":    120,
    "Heavy / batch":       30,   # long jobs -> fewer, longer sessions
}

# Business targets.
TARGET_GROSS_MARGIN = 0.75    # 75% gross margin on metered usage (SaaS norm)
FREE_TIER_MAX_MARGINAL_SPEND = 3.00  # $/free-user/mo you'll tolerate as CAC

# Overage price = cost / (1 - margin).
def price(cost):
    return cost / (1 - TARGET_GROSS_MARGIN)

PRICE_VCPU_HR = price(COST_VCPU_HR)
PRICE_EGRESS_GB = price(COST_EGRESS_GB)

# ------------------------- EGRESS RECONCILIATION -------------------------
# Egress is priced to Hasura parity ($0.13/GB). Our only cost is the GCP result-
# egress leg ($0.12), so parity clears +8% — no loss, but below the 75% target.
# A bundled-egress provider (Hetzner/OVH) zeroes that leg, taking parity to ~99%.
HASURA_EGRESS = 0.13
egress_scenarios = [
    ("GCP result egress (current)", 0.12),
    ("Bundled-egress infra (Hetzner/OVH)", 0.0013),
]

# Cold-start economics, per resume.
resume_cost = (BOOT_SECONDS / 3600) * SPOT_NODE_HR                 # boot compute burned
resume_billed_vcpu_hr = (MIN_BILLED_SECONDS / 3600) * VCPU_PER_NODE
resume_revenue = resume_billed_vcpu_hr * PRICE_VCPU_HR            # minimum billed to customer

# ------------------------- WORKLOAD PROFILES -------------------------
# (name, vCPU-hrs/mo, egress GB/mo)
profiles = [
    ("Light / interactive",  20,   5),
    ("Team / analytics",    200,  60),
    ("Heavy / batch",      2000, 800),
]

def marginal_cost(vcpu_hr, egress_gb):
    return vcpu_hr * COST_VCPU_HR + egress_gb * COST_EGRESS_GB

def revenue(vcpu_hr, egress_gb):
    return vcpu_hr * PRICE_VCPU_HR + egress_gb * PRICE_EGRESS_GB

print("=" * 68)
print("UNIT ECONOMICS")
print("=" * 68)
print(f"  cost  compute : ${COST_VCPU_HR:.4f}/vCPU-hr   "
      f"(node ${SPOT_NODE_HR:.3f}/hr / {VCPU_PER_NODE} vCPU, Spot burst)")
print(f"  cost  egress  : ${COST_EGRESS_GB:.3f}/GB   "
      f"(GCP result-egress only; source-cloud leg is the customer's bill)")
print(f"  warm pool     : ${WARM_POOL_MO:.0f}/mo  "
      f"({WARM_POOL_NODES}x on-demand ${ONDEMAND_NODE_HR:.3f}/hr, always-on)")
print(f"  price compute : ${PRICE_VCPU_HR:.4f}/vCPU-hr   "
      f"(cost / (1-{TARGET_GROSS_MARGIN:.0%}))")
print(f"  price egress  : ${PRICE_EGRESS_GB:.4f}/GB")
print(f"  fixed floor   : ${FLOOR_TOTAL_MO:.0f}/mo total, "
      f"${FLOOR_PER_ORG_MO:.0f}/org @ {PAYING_ORGS} orgs")

print()
print("=" * 68)
print(f"EGRESS @ HASURA PARITY (${HASURA_EGRESS:.2f}/GB) — margin by infra/source")
print("=" * 68)
max_cost_at_target = HASURA_EGRESS * (1 - TARGET_GROSS_MARGIN)
print(f"  to clear {TARGET_GROSS_MARGIN:.0%} at ${HASURA_EGRESS:.2f}/GB, egress cost must be "
      f"<= ${max_cost_at_target:.4f}/GB")
print(f"  {'scenario':38} {'cost':>8} {'margin@$0.13':>12} {'75% price':>10}")
for label, cost in egress_scenarios:
    m = (HASURA_EGRESS - cost) / HASURA_EGRESS
    p = cost / (1 - TARGET_GROSS_MARGIN)
    flag = "OK" if cost <= max_cost_at_target else "MISS"
    print(f"  {label:38} {cost:8.4f} {m:11.0%} {flag:>4} {p:9.3f}")

print()
print("=" * 68)
print(f"ACTIVE-HOUR MARGIN  (warm SKU @ ${ACTIVE_HOUR_PRICE:.2f}/active-hr)")
print("=" * 68)
print("  Not Hasura-parity COGS: MPP keeps a coordinator warm (+ a worker for zero")
print("  cold-start) vs Hasura's one small GQL-only instance (~$0.005/hr). Higher")
print("  warm cost buys a heavier/broader workload class; margin still clears.")
shuffle_hr = SHUFFLE_GB_PER_ACTIVE_HR * COST_XZONE_GB
active_scenarios = [
    ("coordinator only (listeners warm)",      COORD_HR),
    ("coordinator only, 1yr CUD",              COORD_HR * (1 - CUD_DISCOUNT)),
    ("coordinator + warm worker (0 cold-start)", COORD_HR + ONDEMAND_NODE_HR),
    ("coordinator + warm worker, 1yr CUD",     (COORD_HR + ONDEMAND_NODE_HR) * (1 - CUD_DISCOUNT)),
]
print(f"  {'scenario':40} {'cost/hr':>8} {'margin':>7}")
for label, warm in active_scenarios:
    c = warm + shuffle_hr
    m = (ACTIVE_HOUR_PRICE - c) / ACTIVE_HOUR_PRICE
    print(f"  {label:40} {c:8.3f} {m:7.0%}")
print(f"  (+ ${shuffle_hr:.3f}/hr cross-zone shuffle on multi-worker joins; "
      f"same-zone placement = free)")

print()
print("=" * 68)
print(f"ANALYTICAL LANE — GALAXY ANCHOR (worker-hour, MPP compute uptime)")
print("=" * 68)
print(f"  Starburst Galaxy (Trino SaaS): {GALAXY_CREDITS_PER_WORKER_HR} credits/worker-hr "
      f"x ${GALAXY_CREDIT:.2f} = ${GALAXY_WORKER_HR:.2f}/worker-hr (Pro)")
for tier, cr in [("Pro", 0.50), ("Enterprise", 0.75), ("Mission Critical", 1.00)]:
    print(f"    {tier:16} ${cr*GALAXY_CREDITS_PER_WORKER_HR:.2f}/worker-hr")
print(f"  Provisa analytical lane : ${WORKER_HR_PRICE:.2f}/worker-hr (just under Galaxy Pro)")
for label, cost_hr in [("Spot e2-standard-8", SPOT_NODE_HR), ("on-demand e2-standard-8", ONDEMAND_NODE_HR)]:
    m = (WORKER_HR_PRICE - cost_hr) / WORKER_HR_PRICE
    print(f"    on {label:24} cost ${cost_hr:.3f}/hr -> margin {m:.0%}")
print(f"  vs current cost+margin price: ${PRICE_VCPU_HR*VCPU_PER_NODE:.2f}/worker-hr "
      f"({PRICE_VCPU_HR*VCPU_PER_NODE/WORKER_HR_PRICE-1:+.0%} vs Galaxy anchor) "
      f"-> underpriced ~{WORKER_HR_PRICE/(PRICE_VCPU_HR*VCPU_PER_NODE):.0f}x")

print()
print("=" * 68)
print("COLD START  (per resume, scale-to-zero)")
print("=" * 68)
print(f"  boot time      : {BOOT_SECONDS}s   min billed: {MIN_BILLED_SECONDS}s "
      f"({MIN_BILLED_SECONDS}s x {VCPU_PER_NODE} vCPU)")
print(f"  cost / resume  : ${resume_cost:.4f}  (boot compute burned)")
print(f"  revenue/resume : ${resume_revenue:.4f}  (minimum billed)")
print(f"  net / resume   : ${resume_revenue - resume_cost:+.4f}  "
      f"-> minimum {'COVERS' if resume_revenue >= resume_cost else 'MISSES'} boot cost")

print()
print("=" * 68)
print("PROFILE P&L  (metered usage + resumes, before platform fee)")
print("=" * 68)
print(f"{'profile':22} {'resumes':>7} {'cost':>8} {'revenue':>9} {'gross$':>8} {'margin':>7}")
for name, cpu, eg in profiles:
    n = RESUMES_PER_MO[name]
    c = marginal_cost(cpu, eg) + n * resume_cost
    r = revenue(cpu, eg) + n * resume_revenue
    gm = (r - c) / r
    print(f"{name:22} {n:7d} {c:8.2f} {r:9.2f} {r-c:8.2f} {gm:7.0%}")

# ------------------------- FREE-BAND THRESHOLDS -------------------------
# Free band must keep a free user's marginal cost <= FREE_TIER_MAX_MARGINAL_SPEND.
# Split the CAC budget across compute vs egress. Egress is the abuse vector,
# so cap it tighter: give egress 1/3 of the budget, compute 2/3.
budget = FREE_TIER_MAX_MARGINAL_SPEND
egress_budget = budget / 3
compute_budget = budget - egress_budget

free_egress_gb = egress_budget / COST_EGRESS_GB
free_vcpu_hr   = compute_budget / COST_VCPU_HR
free_node_hr   = free_vcpu_hr / VCPU_PER_NODE

print()
print("=" * 68)
print(f"FREE-BAND THRESHOLDS  (hard cap, <= ${budget:.2f}/user/mo marginal)")
print("=" * 68)
print(f"  compute free band : {free_vcpu_hr:6.0f} vCPU-hr/mo "
      f"(= {free_node_hr:.0f} node-hr on 8-vCPU)  costs ${compute_budget:.2f}")
print(f"  egress  free band : {free_egress_gb:6.0f} GB/mo"
      f"                       costs ${egress_budget:.2f}")
print("  -> at max: THROTTLE/SUSPEND data plane; card required to convert to overage")

# ------------------------- PLATFORM FEE FLOOR CHECK -------------------------
# Platform fee must cover fixed floor per org + desired base margin.
platform_fee = FLOOR_PER_ORG_MO / (1 - TARGET_GROSS_MARGIN)
print()
print("=" * 68)
print("PLATFORM FEE (covers fixed floor at target margin)")
print("=" * 68)
print(f"  min platform fee : ${platform_fee:.0f}/org/mo "
      f"(floor ${FLOOR_PER_ORG_MO:.0f} / (1-{TARGET_GROSS_MARGIN:.0%}))")
print(f"  suggested tiers  : Starter ${platform_fee*4:.0f}  |  "
      f"Team ${platform_fee*20:.0f}  |  Scale ${platform_fee*50:.0f}")
print("  (bake the free-band allowance into each paid tier; meter overage above)")
