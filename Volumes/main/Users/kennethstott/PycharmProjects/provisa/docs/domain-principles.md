# Domain Model Principles

---

## 1. Governance

### Core Principles

1. **Every resource must be owned by a domain.** Tables, views, and relationships are all domain assets. (REQ-367, REQ-433) There are no ungoverned floating resources. The domain is the unit of accountability.
2. **Every domain must have a steward.** A domain may exist in a pending state until a steward is assigned, but it cannot serve governed data without one. (REQ-609)
3. **The admin owns sources.** Sources are infrastructure, not domain resources. (REQ-012) The admin registers and manages connections to external data systems.
4. **Stewards can claim tables for a domain.** Claiming is exclusive — a table belongs to exactly one domain. (REQ-433) This is the governed act that bridges infrastructure and the semantic layer.
5. **Stewards can create intradomain views from domain assets.** Views express business logic — joins, aggregations, derived metrics — over assets the steward owns within the same domain. (REQ-367, REQ-366) Views create new semantic meaning and require steward approval.
6. **Analysts can create cross-domain queries from approved relationships.** Queries are interdomain views expressed in any supported query language. (REQ-001) They do not create new semantics — they traverse approved relationship paths. No additional approval is required: governance is handled upstream at the Relationship and column visibility layers. The catalog is the enforcement mechanism: the compiler rejects traversals not in the approved relationship catalog. (REQ-011, REQ-443)
7. **Anyone can request access to a domain resource.** Access is granted at the resource level, not the query level. (REQ-003) If you have access to a resource, you can query it. Governance is enforced at execution time through the pipeline. (REQ-002)

### Resources: Tables and Views as Peers

The distinction between a table and a view is origin only — a table is claimed from a source, a view is defined by a steward. Once either exists as a domain asset, the governance model treats them identically: (REQ-367)

- Both are first-class domain assets visible in the catalog
- Both can be the target of a relationship
- Both can be granted under Principle 6
- Both are subject to the same governance pipeline (REQ-134)

A steward can claim tables privately and expose only curated views as public-facing data products.

### View Composition

Views are strictly intradomain. (REQ-367) A view may reference:
- Claimed tables within the same domain
- One other view within the same domain, where the variation is purposeful: field restriction, aggregation, or enrichment via an additional join

Composition depth is not technically enforced — steward judgment during HITL review is the quality control mechanism.

### Queries

A Query traverses approved relationship paths over domain assets. Unlike Views, Queries do not create new semantic meaning — they traverse the approved structure of the model. Queries may be expressed in any supported query language (SQL, GraphQL, Cypher). (REQ-001, REQ-345)

**Structural enforcement:** The relationship catalog is the enforcement mechanism. The compiler validates every traversal against approved catalog entries and rejects queries that reference unapproved paths. (REQ-011, REQ-443) Governance is structural, not a runtime check. (REQ-002)

**No approval required:** Governance happens upstream — at the Relationship and column visibility layers. If a user has access to the columns and the traversal path is approved, the Query is valid usage. No additional gate. (REQ-003)

**Distinction from Views:**
- Views: intradomain, introduce new semantic meaning, steward-curated (REQ-367)
- Queries: traverse approved relationships, no new semantics, no approval gate (REQ-001)

**Domain expression by query language:**

Each supported language surfaces the domain as a structural namespace native to that language: (REQ-154, REQ-351)

| Language | Domain expression | Example |
|---|---|---|
| GraphQL | Type and field name prefix | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Schema name | `SELECT * FROM sales.orders` |
| Cypher | Additional node label (domain only required when type name is ambiguous) | `MATCH (o:Sales:Order)` |

The compiler resolves domain membership from these structural positions — no annotation or hint is required.

### Relationships

A relationship is an approved traversal path between two assets. Domain boundaries are irrelevant to what a relationship is — they only determine who approves it.

**Approval:**
- Approval is required from every distinct steward who owns an asset involved in the relationship (REQ-366)
- If one steward owns both assets, one approval is required. If two stewards are involved, two approvals are required (REQ-366)
- There is no intradomain/cross-domain classification — ownership determines the approval burden naturally
- Relationships are versioned and flagged for re-review on schema changes affecting join fields (REQ-020)

Relationships are created by demand, not speculatively. The first team with the business need does the work; subsequent teams inherit the infrastructure.

**Optimization consequence:** A relationship declaration is not only a governance artifact — it is also a structural description of a join shape. The two tables, two columns, and join type that define a relationship are exactly what the query optimizer needs to pre-materialize that join. Cross-source relationships automatically generate pre-materialized join tables; same-source relationships can opt in via `materialize: true`. (REQ-158, REQ-159) Stewards who think through and approve valid relationships get query acceleration as a direct byproduct — governance work and optimization work are the same act.

### Field Access Grants

A field access grant is a domain-to-domain permission — Domain A may use specific fields from Domain B in its views. (REQ-440, REQ-436)

**Grant lifecycle:**
- Prompted by view creation when foreign fields are identified as needed
- Approved once by the target domain steward (REQ-366)
- Belongs to the requesting domain, not to the view that prompted it (REQ-610)
- Any subsequent view in the requesting domain may use the granted fields without further cross-domain involvement
- Additional ungranated fields require a new request

The tradeoff: the source domain approves field access without knowing every future use. Per-view approval is correct in theory and unworkable in practice.

### Query Creation Workflow

Three stages, in order.

**Stage 1 — Shaping (SQL discovery, from the Relationships page):**
- Analyst opens the Shaping tool from the Relationships page to explore potential join paths in raw SQL (REQ-419)
- SQL is run against accessible data, subject to existing RLS and column masking (REQ-040)
- JOINs in the SQL are parsed and surfaced as candidate Relationship proposals (REQ-419)
- Machine-suggested candidates (FK inference, semantic inference) are shown alongside the analyst's SQL exploration in the same view (REQ-018, REQ-413)
- Analyst selects candidates to promote to a formal Relationship request (REQ-428)

**Stage 2 — Relationship approval** (consequential — structural and permanent):
- Raised to every distinct steward who owns an asset involved in the relationship (REQ-366)
- Is this a legitimate traversal path? Is the join semantically valid?
- All implicated stewards must approve; relationship becomes a permanent catalog entry (REQ-020)

**Stage 3 — Query creation:**
- Analyst builds the Query in any supported language (SQL, GraphQL, Cypher), traversing approved relationship paths (REQ-001, REQ-345)
- Only approved catalog relationships are traversable — the compiler enforces this structurally (REQ-011, REQ-443)
- No approval required — column visibility and relationship approval are the only gates (REQ-003)

### HITL as the Primary Control

Technical rules handle what is objective — field provenance tracking, domain boundary enforcement, compiler validation. (REQ-262, REQ-443) Contextual judgment stays with the steward. Constraints such as view composition depth, per-query purpose requirements, and relationship approval decisions are HITL concerns, not compiler-enforced rules.

**Source domain neutrality:** The source domain steward approves the relationship once and the field grant once. After that, downstream domains operate within those granted boundaries:
- **High consideration** at the boundary-crossing decision
- **Lightweight awareness** thereafter via notifications and query history

---

## 2. Discoverability

### Discovery Tiers

Discovery is structured across five tiers of increasing governance. Each tier is a prerequisite for the next. (REQ-611)

| Tier | Description | Governance state |
|---|---|---|
| 1 — Registered source schema | Every table, column, and type from a registered source. Admin-level visibility. | None — raw inventory |
| 2 — Unclaimed tables | Tables introspected from registered sources with no domain owner. Visible to stewards with source access. | Available but ungoverned |
| 3 — Domain assets | Claimed tables and steward-defined views. Fully governed, owned, catalog-visible. | Fully governed |
| 4 — Relationships | Approved traversal paths between Tier 3 assets. Prerequisite for cross-domain view creation. | Approved by both stewards |
| 5 — Field grants | Domain-to-domain field access permissions. The most specific and deliberate governed access. | Approved by source steward |

An unclaimed table is a gap signal — if needed data exists only at Tier 2, a steward must claim it before governance can proceed. (REQ-014) Absence of any candidate across all tiers requires admin escalation.

### FK Constraints

FK constraints are a source-level construct — they cannot span data sources. Cross-source join paths are derived entirely from approved catalog relationships (Tier 4), which are stronger, having been validated by both stewards. (REQ-019)

Within a source:
- FK constraints are surfaced automatically as candidate relationships on source registration (REQ-018, REQ-413)
- They represent explicit modeling intent — unenforced in most analytical SQL systems but purposefully declared
- Steward validation is still required before a candidate becomes an approved relationship (REQ-366)

### Relationship Confidence Hierarchy

| Evidence | Confidence |
|---|---|
| Approved catalog relationship — cross-source, validated by both stewards | Highest |
| Intra-source FK constraint — explicit modeling intent, unenforced but purposeful | High |
| Intra-source semantic inference — column name/type similarity within a consistent schema | Medium |
| Cross-source semantic inference — naming conventions diverge across systems; high false positive risk | Low |

Suggestions corroborated by multiple evidence types accumulate confidence. (REQ-612)

### LLM-Assisted Discovery

The LLM operates across all five tiers simultaneously, suggesting relationships, candidate claims, and traversal paths ranked by confidence. (REQ-167, REQ-612)

**What the LLM surfaces:**
- Candidate relationships ranked by confidence (REQ-167)
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
- Draft query for analyst review and refinement
- Per-component confidence scores
- Ordered prerequisite list
- Gap list

**SQL-first relationship discovery (Modeling tool):**

Accessed as a modal from the Relationships page. (REQ-419) The intent is to build the semantic model — identifying structural join paths before formalising them as governed relationships.

1. Analyst writes freeform SQL against accessible tables (RLS and masking still applied) (REQ-040)
2. SQL AST is parsed — each JOIN condition becomes a candidate Relationship proposal (REQ-419)
3. Candidate list is shown alongside machine-suggested candidates (FK inference, semantic inference) for unified review (REQ-018, REQ-413)
4. Analyst promotes selected candidates to formal Relationship requests (REQ-428)
5. Approved Relationships are added to the catalog and become traversable in Queries (REQ-020)

The Modeling tool may show all registered tables for structural exploration, even where the analyst cannot see the underlying data — steward approval governs actual data access, not schema visibility. (REQ-039)

---

## 3. Usage

### Query Audit Trail

Every query that touches a domain asset is logged: (REQ-613)
- Identity and timestamp
- Tables accessed and role

The audit log is append-only — records cannot be deleted or updated. (REQ-613)

**Two visibility mechanisms:**
- **Push** — post-use notifications for structural acts (a new view was created using your fields)
- **Pull** — query history for runtime usage patterns
