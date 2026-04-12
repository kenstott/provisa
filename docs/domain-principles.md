# Domain Model Principles

---

## 1. Governance

### Core Principles

1. **Every resource must be owned by a domain.** Tables, views, and relationships are all domain assets. There are no ungoverned floating resources. The domain is the unit of accountability.
2. **Every domain must have a steward.** A domain may exist in a pending state until a steward is assigned, but it cannot serve governed data without one.
3. **The admin owns sources.** Sources are infrastructure, not domain resources. The admin registers and manages connections to external data systems.
4. **Stewards can claim tables for a domain.** Claiming is exclusive — a table belongs to exactly one domain. This is the governed act that bridges infrastructure and the semantic layer.
5. **Stewards can create derivatives from domain assets.** Views express business logic — joins, aggregations, derived metrics — over assets the steward owns.
6. **Anyone can request access to a domain resource.** Access is granted at the resource level, not the query level. If you have access to a resource, you can query it. Governance is enforced at execution time through the pipeline.

### Resources: Tables and Views as Peers

The distinction between a table and a view is origin only — a table is claimed from a source, a view is defined by a steward. Once either exists as a domain asset, the governance model treats them identically:

- Both are first-class domain assets visible in the catalog
- Both can be the target of a relationship
- Both can be granted under Principle 6
- Both are subject to the same governance pipeline

A steward can claim tables privately and expose only curated views as public-facing data products.

### View Composition

A view may reference:
- Claimed tables within the same domain
- Approved foreign domain assets (treated as peers once granted)
- One other view within the same domain, where the variation is purposeful: field restriction, aggregation, or enrichment via an additional join

Composition depth is not technically enforced — steward judgment during HITL review is the quality control mechanism.

Every view carries a declared business purpose, stated at creation time:
- Part of the governed artifact — stewards approve knowing what the view is for
- Referenced by access requests under Principle 6 so the steward can assess fit
- Travels from view creation through the full governance workflow

### Relationships

A relationship is a catalog fact — a declared semantic traversal path between two domain assets, independent of any view or query.

**Ownership and approval:**
- The source domain proposes — they hold the foreign key and have the business motivation
- The target domain steward approves — confirming semantic validity and accepting the source as a known consumer
- Both must approve before the relationship becomes a permanent catalog entry
- Approving a relationship builds the target steward's dependency graph, enabling proactive schema evolution notifications

Relationships are created by demand, not speculatively. The first team with the business need does the work; subsequent teams inherit the infrastructure.

### Field Access Grants

A field access grant is a domain-to-domain permission — Domain A may use specific fields from Domain B in its views.

**Grant lifecycle:**
- Prompted by view creation when foreign fields are identified as needed
- Approved once by the target domain steward
- Belongs to the requesting domain, not to the view that prompted it
- Any subsequent view in the requesting domain may use the granted fields without further cross-domain involvement
- Additional ungranated fields require a new request

**Post-use notification:** When a view is created using granted fields, the source steward is notified — not asked to approve. The notification includes the view name, declared business purpose, specific fields used, and which steward approved it. This gives the source steward:
- **Visibility** — awareness of how their data is being used
- **Oversight** — grounds to raise a concern if usage looks inappropriate
- **Recourse** — ability to revoke the grant, invalidating dependent views

The tradeoff: the source domain approves field access without knowing every future use. Per-view approval is correct in theory and unworkable in practice.

### Cross-Domain View Creation Workflow

Two distinct requests, in order.

**Request 1 — Relationship creation** (more consequential — structural and permanent):
- Raised to both stewards simultaneously
- Source steward: is this a legitimate semantic declaration for your domain?
- Target steward: is this join semantically valid? Do you accept this domain as a consumer?
- Both must approve; relationship becomes a permanent catalog entry

**Request 2 — Field access grant:**
- Analyst builds the view in GraphQL, traversing the approved relationship and selecting fields
- Field selection surfaces which foreign fields are needed; grant request is raised to target steward
- Grant is approved once and belongs to the requesting domain
- All subsequent views using those fields are governed by the requesting domain's steward alone

### HITL as the Primary Control

Technical rules handle what is objective — field provenance tracking, domain boundary enforcement, compiler validation. Contextual judgment stays with the steward. Constraints such as view composition depth, per-query purpose requirements, and relationship approval decisions are HITL concerns, not compiler-enforced rules.

**Source domain neutrality:** The source domain steward approves the relationship once and the field grant once. After that, downstream domains operate within those granted boundaries:
- **High consideration** at the boundary-crossing decision
- **Lightweight awareness** thereafter via notifications and query history

---

## 2. Discoverability

### Discovery Tiers

Discovery is structured across five tiers of increasing governance. Each tier is a prerequisite for the next.

| Tier | Description | Governance state |
|---|---|---|
| 1 — Registered source schema | Every table, column, and type from a registered source. Admin-level visibility. | None — raw inventory |
| 2 — Unclaimed tables | Tables introspected from registered sources with no domain owner. Visible to stewards with source access. | Available but ungoverned |
| 3 — Domain assets | Claimed tables and steward-defined views. Fully governed, owned, catalog-visible. GraphQL operates here. | Fully governed |
| 4 — Relationships | Approved traversal paths between Tier 3 assets. Prerequisite for cross-domain view creation. | Approved by both stewards |
| 5 — Field grants | Domain-to-domain field access permissions. The most specific and deliberate governed access. | Approved by source steward |

An unclaimed table is a gap signal — if needed data exists only at Tier 2, a steward must claim it before governance can proceed. Absence of any candidate across all tiers requires admin escalation.

### FK Constraints

FK constraints are a source-level construct — they cannot span data sources. Cross-source join paths are derived entirely from approved catalog relationships (Tier 4), which are stronger, having been validated by both stewards.

Within a source:
- FK constraints are surfaced automatically as candidate relationships on source registration
- They represent explicit modeling intent — unenforced in most analytical SQL systems but purposefully declared
- Steward validation is still required before a candidate becomes an approved relationship

### Relationship Confidence Hierarchy

| Evidence | Confidence |
|---|---|
| Approved catalog relationship — cross-source, validated by both stewards | Highest |
| Intra-source FK constraint — explicit modeling intent, unenforced but purposeful | High |
| Intra-source semantic inference — column name/type similarity within a consistent schema | Medium |
| Cross-source semantic inference — naming conventions diverge across systems; high false positive risk | Low |

Suggestions corroborated by multiple evidence types accumulate confidence.

### Data Probing and Correlation

For semantically inferred candidates, data probing provides a validation step:
- **Value overlap** — proportion of source column values that appear in the target column
- **Cardinality** — whether distribution matches the expected relationship type
- **Null rate** — proportion of source column that is null, indicating optionality

High correlation elevates confidence; low correlation suppresses or demotes the candidate. Probing is corroborating evidence, not proof — integer ranges can overlap coincidentally and partial referential integrity is common in analytical systems. Significant room for error remains. Steward semantic judgment is the only reliable final check.

### LLM-Assisted Discovery

The LLM operates across all five tiers simultaneously, suggesting relationships, candidate claims, and traversal paths ranked by confidence.

**What the LLM surfaces:**
- Candidate relationships ranked by confidence
- Unclaimed tables that may satisfy a data need, with a prompt to initiate claiming
- Absence of any candidate — signal to escalate to admin

**View design from business description:**

The analyst provides a natural language description and optional constraints. The LLM produces a suggested view structure.

*Input:*
- Business description: entities, metrics, relationships, intent
- Optional constraints: filters, time windows, aggregations, excluded fields, sensitivity restrictions

*Example:*
> "Daily trade volumes by counterparty for the last 30 days, active counterparties only, showing counterparty legal name and credit rating. No PII."

*LLM process:*
1. Parse — identify entities, metrics, dimensions, filters, exclusions
2. Search — all catalog tiers for matching assets
3. Suggest — domain assets, relationships, fields, aggregation structure
4. Score — confidence per component based on tier evidence
5. Prerequisites — ordered list of claims, relationships, and field grants required
6. Gaps — entities or fields with no candidate in any tier, flagged for admin escalation

*Output:*
- Draft GraphQL query for analyst review and refinement
- Per-component confidence scores
- Ordered prerequisite list
- Gap list

The business description becomes the view's declared business purpose once the view is formally created.

**SQL-first view creation:**

An alternative path for analysts who already have working SQL:
1. Submit SQL view definition
2. System validates against registered source schemas
3. SQL AST is parsed — each join condition becomes a candidate relationship
4. Missing relationships and field grants are surfaced as prerequisite requests
5. Once prerequisites are met, the SQL is promoted to a governed domain asset

The LLM infers a suggested business purpose and view name from the SQL, and validates that the logic matches the stated intent.

---

## 3. Usage

### Query Audit Trail

Every query that touches a domain asset is logged:
- Identity and timestamp
- Fields accessed and query volume
- Access grant under which it was authorised
- Per-query purpose statement (where required)

The steward's query history report is an aggregated view over this log, filterable by asset, role, and time window. The catalog is a live governance instrument — stewards maintain awareness of how their assets are used as it happens, not after the fact.

**Two visibility mechanisms:**
- **Push** — post-use notifications for structural acts (a new view was created using your fields)
- **Pull** — query history for runtime usage patterns

### Per-Query Business Purpose

Certain identities, roles, or domains can be configured to require a purpose statement on every query execution. Applicable to:
- High-privilege roles
- External contractors
- Audited identities
- Domains containing regulated or sensitive data

The purpose is expressed as a value within the query itself — no separate API call or protocol change:
- **GraphQL**: `@purpose(reason: "regulatory reporting Q1 2026")`
- **SQL**: `/* @provisa:purpose="regulatory reporting Q1 2026" */`
- **Cypher**: `// @provisa:purpose="regulatory reporting Q1 2026"`

**Rules:**
- The compiler extracts the purpose before execution and logs it against the query
- If required but absent, the query is rejected
- Purpose requirements apply only to domains whose assets are **directly referenced** in the query — indirect domain involvement through relationship paths is not traced; doing so is unbounded and unenforceable
- Per-query purpose statements serve as an anomaly detection signal — divergence between stated purpose and fields accessed is flagged for steward review
- Whether to require per-query purposes, and for which roles, is a steward decision (HITL)
