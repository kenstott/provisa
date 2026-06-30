# Provisa Go-To-Market Strategy

## Product Philosophy

Provisa follows the Hasura V2 PLG model: **connect a source, register tables, suggest relationships, query with GraphQL** — zero required governance config. Domains, security policies, and steward workflows are progressive disclosures, not prerequisites. Governance is the upgrade path, not the entry tax.

## SKUs

| SKU | Delivery | Price | Target |
|---|---|---|---|
| **Community Edition (CE)** | Self-hosted | Free | Developers, small teams |
| **SaaS** (in development) | Managed cloud | Subscription | SMB |
| **Enterprise** | Self-hosted or private cloud | Contract | Regulated enterprise |
| **Vertical Packages** | Config artifacts (any tier) | Bundled or add-on | Domain-specific buyers |

The SaaS SKU is not yet generally available. It requires the productized multi-tenant platform (REQ-073), which is partial today: tenant context (JWT `tenant_id` claim), Postgres RLS on meta tables, tenant-prefixed cache isolation, and Stripe billing/org provisioning are in place; per-org schema isolation (REQ-695/702) and at-rest per-tenant config encryption (REQ-684/685/694) are not. Until the platform is complete, SaaS customers are served manually (see GTM Motion 2).

### CE vs Enterprise gate

CE is full-featured. Governance ships in the open-source core: RLS enforcement, column masking, the append-only query audit log, ABAC hooks, and SSO (Firebase, Keycloak OIDC) are all CE features, not paywalled. Enterprise adds only: SLA guarantees, dedicated support, advanced audit logging, and compliance reporting. No bait-and-switch — CE stays genuinely capable. The GraphQL/Hasura community has a sharp nose for hobbled community editions; trust is the asset.

BSL 1.1 license: source-available, converts to Apache 2 after time window, prevents commercial cloud hosting without a deal. Explicitly communicated in docs and marketing.

## GTM Motions

### 1. PLG — Community (developer / small team)

**Hook:** Disaffected Hasura V2 community. Hasura DDN (V3) is a forced rewrite with a new mental model and cloud-first pricing. V2 CE is abandoned. Teams are actively evaluating alternatives now.

**Message:** *"Everything you loved about Hasura V2. Connect your database, track your tables, get GraphQL. Plus governance when your team needs it."*

**Tactics:**
- Hasura V2 metadata migration tool as the top-of-funnel hook — zero-risk trial for teams already burned by migration work
- SEO: "Hasura V2 alternative", "Hasura DDN migration", "self-hosted GraphQL" — low competition, high intent
- Show up in Hasura GitHub Discussions, /r/graphql, Hacker News — genuine participation, not ads
- Honest comparison page: V2 vs DDN vs Provisa. The community respects directness and shares it
- Broader GraphQL community angle: "GraphQL over anything" — 30+ sources including Kafka, Iceberg, things Hasura never touched

**Conversion moment:** Team grows → needs governance, SSO, or managed hosting → upgrade to SaaS or Enterprise.

### 2. SaaS — SMB

**Hook:** Teams that want Provisa without the ops burden. Managed hosting, no Trino/Redis/Postgres to run themselves.

**Gate:** Requires productized multi-tenant SaaS platform (REQ-073). Until then, manual "we'll run it for you" approach for early SMB customers.

### 3. Direct + SI — Regulated Enterprise

**Buyers:** CDOs, Data Platform teams, compliance/IT in healthcare, financial services, government.

**Hook:** Governance, RLS, audit trail, HIPAA/FedRAMP, air-gap deployment, vendor SLA.

**SI channel:** Regulated verticals rarely buy without a systems integrator they already trust. SI is the actual seller. Requirements: certification path, reference implementations, deal registration. SI builds a repeatable Hasura migration practice on top of Provisa.

**Sales cycle:** 6–12 months. Need 2–3 reference enterprise logos before this motion scales.

**Accelerant:** Community adoption in the wild helps enterprise procurement — "our team already knows it."

## Vertical Strategy (Land and Expand)

Vertical packages are pre-built configuration artifacts — source configs, registered tables, relationships, sample queries, dashboards — installable in under an hour. Low engineering cost (mostly config, not code). Ship on any tier; function as both a free CE demo and a paid enterprise accelerant.

**Wedge motion:** Deploy vertical package → team proves value fast → expand horizontally to internal sources → org-wide adoption.

### Priority verticals

| Vertical | Pre-built content | Compliance hook | Graph angle |
|---|---|---|---|
| **Cybersecurity** | CVE/NVD feeds, MITRE ATT&CK, CISA KEV, asset inventory | SOC 2, FedRAMP | CVE → CWE → affected product → asset traversal |
| **Financial services** | SEC/EDGAR filings, market data, regulatory feeds | SOC 2, PCI-DSS | Entity → filing → risk traversal |
| **Healthcare** | Claims, formulary, provider directories | HIPAA | Provider → patient → claims traversal |
| **Government** | Open data feeds, geospatial | FedRAMP | Opens federal SI channel |

**Cybersecurity is the strongest wedge:** data sources are public and well-known, security teams think in graph relationships, Cypher traversal is a natural fit, FedRAMP overlap opens the federal SI channel.

## Summary

The GTM stack (community → SaaS → enterprise + SI) is the standard playbook for successful dev-tools companies (Hashicorp, Hasura, Grafana, dbt). Differentiation comes from execution: how clean the Hasura V2 migration story is, and how strong the governance features are for regulated verticals. The vertical packages reduce time-to-value for enterprise and give the SI channel a concrete, repeatable practice to sell.
