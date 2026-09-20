# Provisa vs. the Market

*Sales & marketing reference — pricing current as of September 2026. Verify against each
vendor's live pricing page before quoting a prospect; public rates change without notice.*

## The one-liner

Provisa undercuts or matches every direct competitor's entry rate, while reaching a broader set
of source types — warehouses, graph databases, MongoDB, MSSQL, file sources, and more — under
one federated query, through persistent protocol surfaces (SQL, GraphQL, Cypher, Bolt, gRPC,
MCP) that request-driven competitors can't offer.

## Cloud / managed pricing

| | **Provisa** | **Hasura Cloud v2** | **Hasura DDN** | **Starburst Galaxy** |
|---|---|---|---|---|
| Billing unit | Active-hour (compute) + metered egress | Active-hour | Per "active model" (table), usage-gated | Worker-hour, billed in credits |
| Entry rate | Starter: $25/mo + **$1.30/hr** overage | **$1.50/hr** (no DB connected) | $5–$30/active model/mo (Free/Base/Advanced) | $0.50/credit × 6 credits/hr = **$3.00/worker-hr** (Pro) |
| Higher tier | Pro S/M/L: $99–$399/mo + $1.50–$5.50/hr, scales with dedicated hardware | Advanced-connector surcharge on top (exact current multiplier not public) | Base/Advanced tiers scale per-model rate | Enterprise $4.50/worker-hr, Mission Critical $6.00/worker-hr |
| Egress | $0.48/GB, 25–200 GB included by plan | $0.13/GB | Not itemized the same way | Bundled into credit consumption |
| What one price buys | Federated MPP across warehouses, graph, Mongo, MSSQL, files — one query, persistent protocols (SQL/GraphQL/Cypher/Bolt/gRPC/MCP) | GraphQL-only, request-driven, single-serialization joins | Same GraphQL-only reach as v2, priced per table instead of per hour | Trino-as-a-service, MPP over Trino-reachable sources only |

**Key talking point:** Provisa's Starter overage rate ($1.30/hr) undercuts Hasura v2's base rate
($1.50/hr) before even counting Provisa's included-hours allowance. Provisa's Pro tiers sit at
or below Galaxy's Pro rate while reaching sources Galaxy's Trino-only model can't.

**On Hasura DDN:** Hasura's current flagship product no longer bills by the hour at all — it
bills **$5–$30 per "active model" per month**, where an active model is *"any model or command
in the metadata that is accessed more than 1000 times/month"* (Hasura's own wording). Query a
table fewer than 1,000 times and it's free; cross that line and the whole table bills for the
month. This is a genuinely different pricing unit, not a rate to put side-by-side with an
hourly number — lead with the structural difference (usage-gated per-table vs. Provisa's
predictable hourly occupancy) rather than trying to force a single "cheaper/more expensive"
comparison.

## On-premises licensing

| | **Provisa** | **Denodo** | **Starburst Enterprise** |
|---|---|---|---|
| Model | Flat per-core rate | Per-CPU subscription | Quote-only, per-node/core |
| Published rate | **$3,000/core/yr** | Not public; UK G-Cloud rate card shows £124,222 (~$155K) for a 4-CPU subscription (~$39K/CPU/yr) | Not public |
| Coordinator/worker split | None — a core is a core | N/A (per-CPU) | Typically differentiated by role |
| Typical entry deal | ~$144,000/yr (48-core deployment) | Six-figure and up | Six-figure and up |
| Free tier | Yes — under $1M company revenue and ≤8 cores | No | No |

**Key talking point:** Provisa's on-prem rate is transparent and public — no "contact sales for
a quote" friction. At a comparable footprint, Provisa's per-core rate runs well under Denodo's
implied per-CPU rate, with no separate coordinator/worker pricing to negotiate.

## Quick-reference cheat sheet

- **Cheaper entry, cloud:** Provisa beats Hasura v2 on rate; matches or beats Galaxy on rate,
  wins decisively on source breadth against both.
- **Simpler than DDN:** Provisa's active-hour billing is predictable up front; DDN's per-model,
  usage-gated billing is hard for a prospect to estimate before they've run real traffic.
- **Cheaper and simpler on-prem:** Provisa publishes its rate; Denodo and Starburst Enterprise
  both require a sales conversation to get a number.
- **Broader reach, every comparison:** Provisa is the only one of the four that reaches
  warehouses, graph, MongoDB, and MSSQL under a single federated query with persistent
  protocol surfaces — not just GraphQL (Hasura) or Trino-reachable sources (Starburst).

## Sourcing

Hasura v2/DDN rates: Hasura's public pricing page, confirmed live. Starburst Galaxy rates:
Starburst's public pricing page. Denodo rate: UK Government G-Cloud 14 published rate card (the
only public Denodo number found; direct-quote pricing may differ). Starburst Enterprise: no
public rate exists; six-figure-and-up is the consistent characterization across buyer reviews
and analyst commentary, not a quoted price. Re-verify all of the above before using in a
customer-facing quote or contract.
