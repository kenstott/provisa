#!/usr/bin/env python3
# Copyright (c) 2026 Kenneth Stott
# Canary: 53b1e264-bb56-4266-a8d3-bc97308d4616
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Builds docs/pricing/provisa-revenue-model.xlsx — a live-formula net-revenue
projection with adjustable inputs. Re-run after editing this script; do not
hand-edit the generated .xlsx (edit the knobs inside Excel/Sheets instead —
only formulas on the Model sheet reference the Inputs sheet)."""

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.worksheet.worksheet import Worksheet

wb = Workbook()

INPUT_FILL = PatternFill("solid", fgColor="FFF2CC")  # pale yellow — the knobs
HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
HEADER_FONT = Font(color="FFFFFF", bold=True)
BOLD = Font(bold=True)
TITLE_FONT = Font(bold=True, size=14)
NOTE_FONT = Font(italic=True, size=9, color="666666")
THIN = Side(style="thin", color="CCCCCC")
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
CUR = '"$"#,##0.00'
CUR0 = '"$"#,##0'
PCT = "0.0%"


def header_row(ws: Worksheet, row: int, values: list[str], start_col: int = 1) -> None:
    for i, v in enumerate(values):
        c = ws.cell(row=row, column=start_col + i, value=v)
        c.fill = HEADER_FILL
        c.font = HEADER_FONT
        c.border = BORDER
        c.alignment = Alignment(horizontal="center", wrap_text=True)


def input_cell(ws: Worksheet, row: int, col: int, value, fmt: str | None = None):
    c = ws.cell(row=row, column=col, value=value)
    c.fill = INPUT_FILL
    c.border = BORDER
    if fmt:
        c.number_format = fmt
    return c


def formula_cell(ws: Worksheet, row: int, col: int, formula: str, fmt: str | None = None):
    c = ws.cell(row=row, column=col, value=formula)
    c.border = BORDER
    if fmt:
        c.number_format = fmt
    return c


def label(ws: Worksheet, row: int, col: int, text: str, bold: bool = False):
    c = ws.cell(row=row, column=col, value=text)
    if bold:
        c.font = BOLD
    return c


# ── Inputs sheet ──────────────────────────────────────────────────────────
ws_in = wb.active
assert ws_in is not None
ws_in.title = "Inputs"
ws_in.sheet_view.showGridLines = False
ws_in.column_dimensions["A"].width = 34
for col in "BCDEF":
    ws_in.column_dimensions[col].width = 16

ws_in["A1"] = "Provisa Net-Revenue Model — Inputs (yellow = adjustable knob)"
ws_in["A1"].font = TITLE_FONT
ws_in["A2"] = (
    "Rates are live Lemon Squeezy / GCP prices as of 2026-09-16 "
    "(see docs/pricing/pricing-plan.md). Usage and customer counts are illustrative assumptions."
)
ws_in["A2"].font = NOTE_FONT
ws_in.merge_cells("A2:F2")

tiers = ["Starter", "Pro S", "Pro M", "Pro L"]

# Rate card
r = 4
label(ws_in, r, 1, "Rate card", bold=True)
r += 1
header_row(
    ws_in,
    r,
    [
        "Tier",
        "Base fee/mo",
        "Included active-hrs",
        "Overage $/hr",
        "Included GB",
        "Egress overage $/GB",
    ],
)
rate_card = {
    "Starter": (25, 19, 1.30, 25, 0.48),
    "Pro S": (99, 66, 1.50, 50, 0.48),
    "Pro M": (199, 72, 2.75, 100, 0.48),
    "Pro L": (399, 72, 5.50, 200, 0.48),
}
rate_row: dict[str, int] = {}
for i, t in enumerate(tiers):
    rr = r + 1 + i
    rate_row[t] = rr
    label(ws_in, rr, 1, t)
    base, incl_hrs, over_rate, incl_gb, egress_rate = rate_card[t]
    input_cell(ws_in, rr, 2, base, CUR0)
    input_cell(ws_in, rr, 3, incl_hrs, "0")
    input_cell(ws_in, rr, 4, over_rate, CUR)
    input_cell(ws_in, rr, 5, incl_gb, "0")
    input_cell(ws_in, rr, 6, egress_rate, CUR)

# Usage & customer-count assumptions
r = 11
label(ws_in, r, 1, "Usage & customer mix", bold=True)
r += 1
header_row(ws_in, r, ["Tier", "Customers (count)", "Active-hrs used/mo", "Egress used/mo (GB)"])
usage = {
    "Starter": (30, 40, 15),
    "Pro S": (12, 90, 60),
    "Pro M": (5, 100, 130),
    "Pro L": (3, 110, 250),
}
usage_row: dict[str, int] = {}
for i, t in enumerate(tiers):
    rr = r + 1 + i
    usage_row[t] = rr
    label(ws_in, rr, 1, t)
    count, hrs, gb = usage[t]
    input_cell(ws_in, rr, 2, count, "0")
    input_cell(ws_in, rr, 3, hrs, "0")
    input_cell(ws_in, rr, 4, gb, "0")

# Compute cost basis
r = 18
label(ws_in, r, 1, "Compute cost basis", bold=True)
r += 1
label(ws_in, r, 1, "GKE Autopilot: $/vCPU-hr")
input_cell(ws_in, r, 2, 0.0445, CUR)
AP_VCPU = r
r += 1
label(ws_in, r, 1, "GKE Autopilot: $/GiB-hr")
input_cell(ws_in, r, 2, 0.0049, CUR)
AP_GIB = r
r += 1
label(ws_in, r, 1, "Shared shard pod: vCPU")
input_cell(ws_in, r, 2, 6, "0")
SHARD_VCPU = r
r += 1
label(ws_in, r, 1, "Shared shard pod: GiB")
input_cell(ws_in, r, 2, 24, "0")
SHARD_GIB = r
r += 1
label(ws_in, r, 1, "Assumed concurrent Starter orgs per shard-hour")
input_cell(ws_in, r, 2, 4, "0")
ws_in.cell(row=r, column=3, value="< biggest unverified lever in the model").font = NOTE_FONT
CONCURRENCY = r
r += 1
label(ws_in, r, 1, "Pro S compute: n2-highmem-4 $/hr (on-demand)")
input_cell(ws_in, r, 2, 0.26, CUR)
PS_RATE = r
r += 1
label(ws_in, r, 1, "Pro M compute: n2-highmem-8 $/hr (on-demand)")
input_cell(ws_in, r, 2, 0.52, CUR)
PM_RATE = r
r += 1
label(ws_in, r, 1, "Pro L compute: n2-highmem-16 $/hr (on-demand)")
input_cell(ws_in, r, 2, 1.05, CUR)
PL_RATE = r
r += 1
label(ws_in, r, 1, "Provisa's own egress cost $/GB (GCP->internet)")
input_cell(ws_in, r, 2, 0.12, CUR)
EGRESS_COST = r
r += 1
label(ws_in, r, 1, "Fixed floor $/mo (Cloud SQL db-f1-micro etc.)")
input_cell(ws_in, r, 2, 9, CUR0)
FIXED = r

r += 2
label(ws_in, r, 1, "Derived: shared shard $/hr", bold=True)
formula_cell(ws_in, r, 2, f"=B{SHARD_VCPU}*B{AP_VCPU}+B{SHARD_GIB}*B{AP_GIB}", CUR)
SHARD_HR_ROW = r
r += 1
label(ws_in, r, 1, "Derived: shared compute $/hr per Starter org", bold=True)
formula_cell(ws_in, r, 2, f"=B{SHARD_HR_ROW}/B{CONCURRENCY}", CUR)
STARTER_COMPUTE_ROW = r

compute_rate_cell = {
    "Starter": f"Inputs!$B${STARTER_COMPUTE_ROW}",
    "Pro S": f"Inputs!$B${PS_RATE}",
    "Pro M": f"Inputs!$B${PM_RATE}",
    "Pro L": f"Inputs!$B${PL_RATE}",
}

# On-prem licensing (negotiated — these rates are an unverified starting assumption, not a
# published price list). ONE model: a flat per-core rate. No hardware-class factor (dropped —
# Oracle's core-factor precedent exists because non-x86 chips do more work per core on Oracle's
# own workloads; that rationale doesn't hold for Provisa, it's software, a core is a core) and no
# coordinator/worker distinction — a core is a core regardless of the role the node plays.
#
# License key covers a CLUSTER (a group of nodes), not a single node: nodes that join with the
# same key can report vCPU/mem to each other, and the software can sum that total and warn (or
# eventually refuse to add more nodes) once it exceeds the cluster's licensed core entitlement.
# That check only holds within one cluster's own key exchange. Provisa on-prem is a LIBRARY — no
# phone-home, no central registry of issued keys — so nothing stops a customer copying the same
# key into a second, entirely separate cluster. Real within a cluster, honor-system across
# clusters. Every "license fee" below is really a support/update/SLA contract with a usage-scope
# clause and audit rights — not a technical control on total consumption.
r += 2
label(
    ws_in, r, 1, "On-prem licensing — flat per-core model (cluster-scoped key; see note)", bold=True
)
ws_in.cell(
    row=r,
    column=3,
    value="< cluster-key sum-check holds only within one cluster's key exchange; cannot stop a copied key powering a separate cluster",
).font = NOTE_FONT
r += 1
label(ws_in, r, 1, "Free-tier scope: cluster-core cap (self-attested, unenforced)")
input_cell(ws_in, r, 2, 8, "0")
ws_in.cell(
    row=r,
    column=3,
    value="< no runtime gate exists (library); this is a license-terms limit only — the lever is no support/SLA/updates, not a block",
).font = NOTE_FONT
FREE_TIER_THRESHOLD = r
r += 1
label(ws_in, r, 1, "Free-tier scope: max company annual revenue (self-attested, unenforced)")
input_cell(ws_in, r, 2, 1000000, CUR0)
ws_in.cell(
    row=r,
    column=3,
    value="< both this and the core cap must hold — self-attested at signup, unenforced, audit-rights clause backs it",
).font = NOTE_FONT
FREE_TIER_REVENUE_CAP = r
r += 1
label(ws_in, r, 1, "Free-tier on-prem installs (informational only, $0 rev)")
input_cell(ws_in, r, 2, 0, "0")
FREE_TIER_COUNT = r
r += 1
label(ws_in, r, 1, "Support-only SKU: $/yr (24hr: defect confirm/feature clarify + resolution ETA)")
input_cell(ws_in, r, 2, 10000, CUR0)
SUPPORT_SKU_RATE = r
r += 1
label(ws_in, r, 1, "Support-only SKU: delivery cost %")
input_cell(ws_in, r, 2, 0.40, PCT)
ws_in.cell(
    row=r, column=3, value="< placeholder; labor-heavy relative to the $10K ticket"
).font = NOTE_FONT
SUPPORT_SKU_COST_PCT = r
r += 1
label(ws_in, r, 1, "Support-only SKU: subscribers (count)")
input_cell(ws_in, r, 2, 0, "0")
SUPPORT_SKU_COUNT = r
r += 1
label(ws_in, r, 1, "Rate $/core/yr")
input_cell(ws_in, r, 2, 3000, CUR0)
CORE_RATE = r
r += 1
label(ws_in, r, 1, "GiB mem bundled per licensed core (informational, not billed)")
input_cell(ws_in, r, 2, 4, "0")
GIB_PER_CORE = r
r += 1
label(ws_in, r, 1, "Delivery/support cost, % of license revenue")
input_cell(ws_in, r, 2, 0.25, PCT)
ws_in.cell(
    row=r,
    column=3,
    value="< placeholder; raised from 20% per market-comp review (real fully-loaded cost, not just a floor)",
).font = NOTE_FONT
SUPPORT_PCT = r
r += 1
label(ws_in, r, 1, "Licensed cores (this deal, cluster total)")
input_cell(ws_in, r, 2, 48, "0")
DEAL_CORES = r
r += 1
label(ws_in, r, 1, "On-prem deals (count)")
input_cell(ws_in, r, 2, 0, "0")
DEAL_COUNT = r

# Add-on SKUs (premium connectors, LLM reasoning) — orthogonal to SaaS tier / on-prem model, sold
# on top of either. Rates are placeholders (no comparable-vendor research done on these yet).
r += 2
label(ws_in, r, 1, "Add-on SKUs (placeholder rates)", bold=True)
r += 1
label(ws_in, r, 1, "Premium connector (Splunk/SharePoint/Files): $/yr")
input_cell(ws_in, r, 2, 6000, CUR0)
CONNECTOR_RATE = r
r += 1
label(ws_in, r, 1, "Premium connector: attach count (connector-orgs, not orgs)")
input_cell(ws_in, r, 2, 0, "0")
CONNECTOR_COUNT = r
r += 1
label(ws_in, r, 1, "LLM validated-reasoning add-on: $/org/mo")
input_cell(ws_in, r, 2, 400, CUR0)
LLM_ADDON_RATE = r
r += 1
label(ws_in, r, 1, "LLM validated-reasoning: attach count (orgs)")
input_cell(ws_in, r, 2, 0, "0")
LLM_ADDON_COUNT = r
r += 1
label(ws_in, r, 1, "Add-on delivery cost, % of add-on revenue")
input_cell(ws_in, r, 2, 0.30, PCT)
ws_in.cell(
    row=r, column=3, value="< placeholder; LLM inference cost dominates here, unmodeled"
).font = NOTE_FONT
ADDON_COST_PCT = r


# ── Model / scenario sheets ──────────────────────────────────────────────
def build_scenario_sheet(
    sheet_name: str, title: str, note: str, fixed_counts: dict[str, int] | None
):
    """fixed_counts=None: customer counts come from Inputs!B (the base scenario).
    fixed_counts={tier: n}: counts are this sheet's own yellow knobs (a named scenario),
    everything else (rates, per-org usage, compute cost) still pulled from Inputs."""
    ws = wb.create_sheet(sheet_name)
    ws.sheet_view.showGridLines = False
    ws.column_dimensions["A"].width = 12
    for col in "BCDEFGHI":
        ws.column_dimensions[col].width = 15

    ws["A1"] = title
    ws["A1"].font = TITLE_FONT
    ws["A2"] = note
    ws["A2"].font = NOTE_FONT
    ws.merge_cells("A2:I2")

    r = 4
    header_row(
        ws,
        r,
        [
            "Tier",
            "Customers",
            "Overage hrs/org",
            "Overage GB/org",
            "Revenue/org",
            "Cost/org",
            "Margin/org",
            "Fleet revenue",
            "Fleet cost",
        ],
    )
    for i, t in enumerate(tiers):
        rr = r + 1 + i
        rc = rate_row[t]
        uc = usage_row[t]
        label(ws, rr, 1, t)
        if fixed_counts is None:
            formula_cell(ws, rr, 2, f"=Inputs!B{uc}", "0")
        else:
            input_cell(ws, rr, 2, fixed_counts[t], "0")
        formula_cell(ws, rr, 3, f"=MAX(0,Inputs!C{uc}-Inputs!C{rc})", "0.0")
        formula_cell(ws, rr, 4, f"=MAX(0,Inputs!D{uc}-Inputs!E{rc})", "0.0")
        formula_cell(ws, rr, 5, f"=Inputs!B{rc}+C{rr}*Inputs!D{rc}+D{rr}*Inputs!F{rc}", CUR)
        formula_cell(
            ws,
            rr,
            6,
            f"=Inputs!C{uc}*{compute_rate_cell[t]}+Inputs!D{uc}*Inputs!$B${EGRESS_COST}",
            CUR,
        )
        formula_cell(ws, rr, 7, f"=E{rr}-F{rr}", CUR)
        formula_cell(ws, rr, 8, f"=B{rr}*E{rr}", CUR)
        formula_cell(ws, rr, 9, f"=B{rr}*F{rr}", CUR)

    total_row = r + 1 + len(tiers)
    label(ws, total_row, 1, "Fleet total", bold=True)
    formula_cell(ws, total_row, 2, f"=SUM(B{r + 1}:B{r + len(tiers)})", "0")
    formula_cell(ws, total_row, 8, f"=SUM(H{r + 1}:H{r + len(tiers)})", CUR)
    formula_cell(ws, total_row, 9, f"=SUM(I{r + 1}:I{r + len(tiers)})", CUR)
    for col in (2, 8, 9):
        ws.cell(row=total_row, column=col).font = BOLD

    r2 = total_row + 2
    label(ws, r2, 1, "Summary", bold=True)
    r2 += 1
    label(ws, r2, 1, "Total revenue")
    formula_cell(ws, r2, 2, f"=H{total_row}", CUR)
    rev_row = r2
    r2 += 1
    label(ws, r2, 1, "Total direct cost")
    formula_cell(ws, r2, 2, f"=I{total_row}", CUR)
    cost_row = r2
    r2 += 1
    label(ws, r2, 1, "Fixed floor")
    formula_cell(ws, r2, 2, f"=Inputs!B{FIXED}", CUR)
    floor_row = r2
    r2 += 1
    label(ws, r2, 1, "Net (monthly)", bold=True)
    formula_cell(ws, r2, 2, f"=B{rev_row}-B{cost_row}-B{floor_row}", CUR)
    ws.cell(row=r2, column=2).font = BOLD
    net_row = r2
    r2 += 1
    label(ws, r2, 1, "Net (annualized)", bold=True)
    formula_cell(ws, r2, 2, f"=B{net_row}*12", CUR)
    ws.cell(row=r2, column=2).font = BOLD
    r2 += 1
    label(ws, r2, 1, "Net margin %", bold=True)
    formula_cell(ws, r2, 2, f"=B{net_row}/B{rev_row}", PCT)
    ws.cell(row=r2, column=2).font = BOLD
    return {"sheet": ws, "annual_net_row": net_row + 1, "next_row": r2 + 2}


build_scenario_sheet(
    "Model",
    "Provisa Net-Revenue Projection — Model",
    "Customer counts come from Inputs!B. Change a yellow cell on Inputs to see outcomes recompute here.",
    fixed_counts=None,
)

scenario_1m = build_scenario_sheet(
    "Scenario - $1M Net",
    "Scenario: customer mix to clear $1M/yr net profit",
    "Same rate card / per-org usage / compute cost as Inputs, but its own customer-count knobs (yellow, "
    "this sheet only) — the base mix scaled ~15.8x. Edit the yellow counts to explore other paths to $1M.",
    fixed_counts={"Starter": 473, "Pro S": 189, "Pro M": 79, "Pro L": 47},
)
SCENARIO_1M_ANNUAL_NET_ROW = scenario_1m["annual_net_row"]

# ── On-prem licensing sheet ──────────────────────────────────────────────
ws_op = wb.create_sheet("On-Prem")
ws_op.sheet_view.showGridLines = False
ws_op.column_dimensions["A"].width = 26
for col in "BCDE":
    ws_op.column_dimensions[col].width = 18

ws_op["A1"] = "On-Prem Licensing — Revenue Model"
ws_op["A1"].font = TITLE_FONT
ws_op["A2"] = (
    "Single per-core model, no coordinator/worker or hardware-class distinction — a "
    "core is a core. Cluster-scoped license key (see Inputs note) — real within a "
    "cluster's key exchange, honor-system across separate clusters."
)
ws_op["A2"].font = NOTE_FONT
ws_op.merge_cells("A2:E2")

r = 4
label(ws_op, r, 1, "License revenue per deal", bold=True)
r += 1
label(ws_op, r, 1, "Bundled GiB mem (informational, not billed)")
formula_cell(ws_op, r, 2, f"=Inputs!B{DEAL_CORES}*Inputs!B{GIB_PER_CORE}", "0")
r += 1
label(ws_op, r, 1, "Total license revenue/deal", bold=True)
formula_cell(ws_op, r, 2, f"=Inputs!B{DEAL_CORES}*Inputs!B{CORE_RATE}", CUR)
PER_DEAL_REV_ROW = r
r += 1
label(ws_op, r, 1, "Delivery/support cost/deal")
formula_cell(ws_op, r, 2, f"=B{PER_DEAL_REV_ROW}*Inputs!B{SUPPORT_PCT}", CUR)
PER_DEAL_COST_ROW = r
r += 1
label(ws_op, r, 1, "Margin/deal", bold=True)
formula_cell(ws_op, r, 2, f"=B{PER_DEAL_REV_ROW}-B{PER_DEAL_COST_ROW}", CUR)
ws_op.cell(row=r, column=2).font = BOLD
PER_DEAL_MARGIN_ROW = r

r += 2
label(ws_op, r, 1, "Deal count (from Inputs)")
formula_cell(ws_op, r, 2, f"=Inputs!B{DEAL_COUNT}", "0")
DEAL_COUNT_ROW = r
r += 1
label(ws_op, r, 1, "Total revenue/yr", bold=True)
formula_cell(ws_op, r, 2, f"=B{DEAL_COUNT_ROW}*B{PER_DEAL_REV_ROW}", CUR)
OP_REV_ROW = r
r += 1
label(ws_op, r, 1, "Total cost/yr", bold=True)
formula_cell(ws_op, r, 2, f"=B{DEAL_COUNT_ROW}*B{PER_DEAL_COST_ROW}", CUR)
OP_COST_ROW = r
r += 1
label(ws_op, r, 1, "Net on-prem/yr", bold=True)
formula_cell(ws_op, r, 2, f"=B{OP_REV_ROW}-B{OP_COST_ROW}", CUR)
ws_op.cell(row=r, column=2).font = BOLD
OP_NET_ROW = r

r += 2
label(ws_op, r, 1, "Support-only SKU (free-tier upsell)", bold=True)
r += 1
label(ws_op, r, 1, "Subscribers (from Inputs)")
formula_cell(ws_op, r, 2, f"=Inputs!B{SUPPORT_SKU_COUNT}", "0")
SUPPORT_SKU_SUBS_ROW = r
r += 1
label(ws_op, r, 1, "Revenue/yr")
formula_cell(ws_op, r, 2, f"=B{SUPPORT_SKU_SUBS_ROW}*Inputs!B{SUPPORT_SKU_RATE}", CUR)
SUPPORT_SKU_REV_ROW = r
r += 1
label(ws_op, r, 1, "Cost/yr")
formula_cell(ws_op, r, 2, f"=B{SUPPORT_SKU_REV_ROW}*Inputs!B{SUPPORT_SKU_COST_PCT}", CUR)
SUPPORT_SKU_COST_ROW = r
r += 1
label(ws_op, r, 1, "Net/yr", bold=True)
formula_cell(ws_op, r, 2, f"=B{SUPPORT_SKU_REV_ROW}-B{SUPPORT_SKU_COST_ROW}", CUR)
ws_op.cell(row=r, column=2).font = BOLD
SUPPORT_SKU_NET_ROW = r

# ── Add-on SKUs sheet ─────────────────────────────────────────────────────
ws_add = wb.create_sheet("Add-Ons")
ws_add.sheet_view.showGridLines = False
ws_add.column_dimensions["A"].width = 34
ws_add.column_dimensions["B"].width = 18

ws_add["A1"] = "Add-On SKUs — Premium Connectors + LLM Reasoning"
ws_add["A1"].font = TITLE_FONT
ws_add["A2"] = (
    "Sold on top of either SaaS or on-prem. Rates are placeholders — no comparable-vendor research done on these yet."
)
ws_add["A2"].font = NOTE_FONT
ws_add.merge_cells("A2:B2")

r = 4
label(ws_add, r, 1, "Premium connectors: revenue/yr")
formula_cell(ws_add, r, 2, f"=Inputs!B{CONNECTOR_COUNT}*Inputs!B{CONNECTOR_RATE}", CUR)
CONNECTOR_REV_ROW = r
r += 1
label(ws_add, r, 1, "LLM validated-reasoning: revenue/yr")
formula_cell(ws_add, r, 2, f"=Inputs!B{LLM_ADDON_COUNT}*Inputs!B{LLM_ADDON_RATE}*12", CUR)
LLM_ADDON_REV_ROW = r
r += 1
label(ws_add, r, 1, "Total add-on revenue/yr", bold=True)
formula_cell(ws_add, r, 2, f"=B{CONNECTOR_REV_ROW}+B{LLM_ADDON_REV_ROW}", CUR)
ws_add.cell(row=r, column=2).font = BOLD
ADDON_TOTAL_REV_ROW = r
r += 1
label(ws_add, r, 1, "Delivery cost/yr")
formula_cell(ws_add, r, 2, f"=B{ADDON_TOTAL_REV_ROW}*Inputs!B{ADDON_COST_PCT}", CUR)
ADDON_COST_ROW = r
r += 1
label(ws_add, r, 1, "Net add-on/yr", bold=True)
formula_cell(ws_add, r, 2, f"=B{ADDON_TOTAL_REV_ROW}-B{ADDON_COST_ROW}", CUR)
ws_add.cell(row=r, column=2).font = BOLD
ADDON_NET_ROW = r

# ── Scenario: mixed SaaS + on-prem deal mix ──────────────────────────────
mixed = build_scenario_sheet(
    "Scenario - Mixed",
    "Scenario: mixed SaaS + on-prem path to ~$1M net",
    "SaaS counts below are this sheet's own knobs (half the pure-SaaS $1M mix). On-prem deals further "
    "down are also this sheet's own knobs, priced from the same single per-core rate on Inputs.",
    fixed_counts={"Starter": 220, "Pro S": 90, "Pro M": 38, "Pro L": 22},
)
ws_mx = mixed["sheet"]
r = mixed["next_row"]

label(ws_mx, r, 1, "On-prem (per-core)", bold=True)
r += 1
label(ws_mx, r, 1, "Deals (this scenario)")
input_cell(ws_mx, r, 2, 3, "0")
MIXED_OP_DEALS_ROW = r
r += 1
label(ws_mx, r, 1, "Revenue/deal")
formula_cell(ws_mx, r, 2, f"=Inputs!B{DEAL_CORES}*Inputs!B{CORE_RATE}", CUR)
MIXED_OP_REV_PER_DEAL_ROW = r
r += 1
label(ws_mx, r, 1, "Margin/deal (after support %)")
formula_cell(ws_mx, r, 2, f"=B{MIXED_OP_REV_PER_DEAL_ROW}*(1-Inputs!B{SUPPORT_PCT})", CUR)
MIXED_OP_MARGIN_PER_DEAL_ROW = r
r += 1
label(ws_mx, r, 1, "On-prem: net/yr", bold=True)
formula_cell(ws_mx, r, 2, f"=B{MIXED_OP_DEALS_ROW}*B{MIXED_OP_MARGIN_PER_DEAL_ROW}", CUR)
ws_mx.cell(row=r, column=2).font = BOLD
MIXED_OP_NET_ROW = r

r += 2
label(ws_mx, r, 1, "Combined summary", bold=True)
r += 1
label(ws_mx, r, 1, "SaaS net/yr")
formula_cell(ws_mx, r, 2, f"=B{mixed['annual_net_row']}", CUR)
MIXED_SAAS_NET_ROW = r
r += 1
label(ws_mx, r, 1, "On-prem net/yr")
formula_cell(ws_mx, r, 2, f"=B{MIXED_OP_NET_ROW}", CUR)
r += 1
label(ws_mx, r, 1, "Combined net/yr", bold=True)
formula_cell(ws_mx, r, 2, f"=B{MIXED_SAAS_NET_ROW}+B{MIXED_OP_NET_ROW}", CUR)
ws_mx.cell(row=r, column=2).font = BOLD

# ── Combined sheet: SaaS ($1M scenario) + On-prem ────────────────────────
ws_c = wb.create_sheet("Combined")
ws_c.sheet_view.showGridLines = False
ws_c.column_dimensions["A"].width = 30
ws_c.column_dimensions["B"].width = 18

ws_c["A1"] = "Combined Net — SaaS ($1M scenario sheet) + On-Prem"
ws_c["A1"].font = TITLE_FONT
ws_c["A2"] = (
    "Turn on-prem deal counts up on Inputs to see how licensing reduces the SaaS org count needed."
)
ws_c["A2"].font = NOTE_FONT
ws_c.merge_cells("A2:B2")

r = 4
label(ws_c, r, 1, "SaaS net/yr (Scenario - $1M Net sheet)")
formula_cell(ws_c, r, 2, f"='Scenario - $1M Net'!B{SCENARIO_1M_ANNUAL_NET_ROW}", CUR)
SAAS_NET_ROW = r
r += 1
label(ws_c, r, 1, "On-prem net/yr")
formula_cell(ws_c, r, 2, f"=On-Prem!B{OP_NET_ROW}", CUR)
COMBINED_OP_ROW = r
r += 1
label(ws_c, r, 1, "Support-only SKU net/yr (free-tier upsell)")
formula_cell(ws_c, r, 2, f"=On-Prem!B{SUPPORT_SKU_NET_ROW}", CUR)
COMBINED_SUPPORT_SKU_ROW = r
r += 1
label(ws_c, r, 1, "Add-on SKUs net/yr (connectors + LLM reasoning)")
formula_cell(ws_c, r, 2, f"='Add-Ons'!B{ADDON_NET_ROW}", CUR)
COMBINED_ADDON_ROW = r
r += 1
label(ws_c, r, 1, "Combined net/yr", bold=True)
formula_cell(
    ws_c,
    r,
    2,
    f"=B{SAAS_NET_ROW}+B{COMBINED_OP_ROW}+B{COMBINED_SUPPORT_SKU_ROW}+B{COMBINED_ADDON_ROW}",
    CUR,
)
ws_c.cell(row=r, column=2).font = BOLD

wb.save("docs/pricing/provisa-revenue-model.xlsx")
print("wrote docs/pricing/provisa-revenue-model.xlsx")
