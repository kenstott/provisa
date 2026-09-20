# Provisa Pricing

Provisa is a federated query platform — one connection reaches your warehouses, databases,
files, and SaaS sources through SQL, GraphQL, Cypher, and more. Pricing is simple: pay for the
compute time you actually use, plus egress if you move a lot of data out.

## Cloud plans

| Plan | Best for | Monthly base | Included compute | Overage rate | Included egress |
|---|---|---|---|---|---|
| **Trial** | Evaluating Provisa | Free | 40 hours or 14 days, whichever comes first | — | 25 GB |
| **Starter** | Small teams, shared infrastructure | $25/mo | 19 hours | $1.30/hour | 25 GB |
| **Pro S** | Dedicated performance, small workloads | $99/mo | 66 hours | $1.50/hour | 50 GB |
| **Pro M** | Dedicated performance, growing workloads | $199/mo | 72 hours | $2.75/hour | 100 GB |
| **Pro L** | Dedicated performance, heavy workloads | $399/mo | 72 hours | $5.50/hour | 200 GB |

**How billing works:** an hour bills when your team runs at least one query in it — not per
query, not per row scanned. Pro plans run on dedicated hardware with no row, scan-size, or
query-duration limits; Starter runs on shared infrastructure sized for smaller workloads.

**Egress overage:** $0.48/GB beyond your plan's included allowance, on every plan.

**14-day free trial** on every new account — no credit card charged until the trial ends or its
limits are reached, whichever comes first.

## On-premises licensing

For customers who need Provisa inside their own network. One simple structure: a flat
per-core license fee.

| | |
|---|---|
| **Rate** | $3,000 per core, per year |
| **Memory** | Included with your cores — no separate charge |

*Example: a 48-core deployment runs $144,000/year.*

No distinction between coordinator and worker nodes, and no hardware-class pricing — license by
total core count across your deployment, however you split it.

**Free for small companies.** If your company has less than $1M in annual revenue and your
deployment is small (up to 8 cores), Provisa on-prem is free — self-supported.

**Support-only plan.** Already running Provisa on-prem and just want a lifeline? **$10,000/year**
gets you a named point of contact: a 24-hour response committing to confirm any issue, clarify
any question, and give you a resolution timeline.

## Add-ons

Available on any Cloud or on-premises plan.

| Add-on | Price |
|---|---|
| **Premium connectors** (Splunk, SharePoint, file sources) | $6,000/connector/year |
| **LLM-powered validated reasoning** | $400/org/month |

## Questions?

Reach out and we'll help you find the right plan for your workload.
