# Domain Model Principles

---

## 1. Governance

### Core Principles

1. **Every resource must be owned by a domain.** Tables, views, and relationships are all domain assets. There are no ungoverned floating resources. The domain is the unit of accountability.
2. **Every domain must have a steward.** A domain may exist in a pending state until a steward is assigned, but it cannot serve governed data without one.
3. **The admin owns sources.** Sources are infrastructure, not domain resources. The admin registers and manages connections to external data systems.
4. **Stewards can claim tables for a domain.** Claiming is exclusive — a table belongs to exactly one domain. This is the governed act that bridges infrastructure and the semantic layer.
5. **Stewards can create intradomain views from domain assets.** Views express business logic — joins, aggregations, derived metrics — over assets the steward owns within the same domain. Views create new semantic meaning and require steward approval.
6. **Analysts can create cross-domain queries from approved relationships.** Queries are interdomain views expressed in any supported query language. They do not create new semantics — they traverse approved relationship paths. No additional approval is required: governance is handled upstream at the Relationship and column visibility layers. The catalog is the enforcement mechanism: the compiler rejects traversals not in the approved relationship catalog.
7. **Anyone can request access to a domain resource.** Access is granted at the resource level, not the query level. If you have access to a resource, you can query it. Governance is enforced at execution time through the pipeline.

### Resources: Tables and Views as Peers

The distinction between a table and a view is origin only — a table is claimed from a source, a view is defined by a steward. Once either exists as a domain asset, the governance model treats them identically:

- Both are first-class domain assets visible in the catalog
- Both can be the target of a relationship
- Both can be granted under Principle 6
- Both are subject to the same governance pipeline

A steward can claim tables privately and expose only curated views as public-facing data products.

### View Composition

A view always belongs to a single domain — there is only one view type, always intradomain. A view exists for one of two purposes:

- **Cross-domain import** — the source is outside the domain. Cross-domain data may only enter a domain via a view, which acts as a read-only adapter naming the external data as a domain business concept.
- **Local derivation** — the source is same-domain. The view derives new or calculated data from existing domain assets. New or derived data may only exist as a view.

A view may reference:

- Claimed tables within the same domain
- Fields imported from another domain under a field access grant
- One other view within the same domain, where the variation is purposeful: field restriction, aggregation, or enrichment via an additional join

Composition depth is not technically enforced — steward judgment during HITL review is the quality control mechanism.

Every view carries a declared business purpose, stated at creation time:

- Part of the governed artifact — stewards approve knowing what the view is for
- Referenced by access requests under Principle 7 so the steward can assess fit
- Travels from view creation through the full governance workflow

### Queries

A Query traverses approved relationship paths over domain assets. Unlike Views, Queries do not create new semantic meaning — they traverse the approved structure of the model. Queries may be expressed in any supported query language (SQL, GraphQL, Cypher).

**Structural enforcement:** The relationship catalog is the enforcement mechanism. The compiler validates every traversal against approved catalog entries and rejects queries that reference unapproved paths. Governance is structural, not a runtime check.

**No approval required:** Governance happens upstream — at the Relationship and column visibility layers. If a user has access to the columns and the traversal path is approved, the Query is valid usage. No additional gate.

**Distinction from Views:**

- Views: intradomain, introduce new semantic meaning, steward-curated
- Queries: traverse approved relationships, no new semantics, no approval gate

**Domain expression by query language:**

Each supported language surfaces the domain as a structural namespace native to that language:

| Language | Domain expression | Example |
| --- | --- | --- |
| GraphQL | Type and field name prefix | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Schema name | `SELECT * FROM sales.orders` |
| Cypher | Additional node label (domain only required when type name is ambiguous) | `MATCH (o:Sales:Order)` |

The compiler resolves domain membership from these structural positions — no annotation or hint is required.

### Relationships

A relationship is an approved traversal path between two assets. Domain boundaries are irrelevant to what a relationship is — they only determine who approves it.

**Approval:**

- Approval is required from every distinct steward who owns an asset involved in the relationship
- If one steward owns both assets, one approval is required. If two stewards are involved, two approvals are required
- There is no intradomain/cross-domain classification — ownership determines the approval burden naturally
- Approving a relationship builds each steward's dependency graph, enabling proactive schema evolution notifications

Relationships are created by demand, not speculatively. The first team with the business need does the work; subsequent teams inherit the infrastructure.

**Optimization consequence:** A relationship declaration is not only a governance artifact — it is also a structural description of a join shape. The two tables, two columns, and join type that define a relationship are exactly what the query optimizer needs to pre-materialize that join. Cross-source relationships automatically generate pre-materialized join tables; same-source relationships can opt in via `materialize: true`. Stewards who think through and approve valid relationships get query acceleration as a direct byproduct — governance work and optimization work are the same act.

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

### Query Creation Workflow

Three stages, in order.

**Stage 1 — Shaping (SQL discovery, from the Relationships page):**

- Analyst opens the Shaping tool from the Relationships page to explore potential join paths in raw SQL
- SQL is run against accessible data, subject to existing RLS and column masking
- JOINs in the SQL are parsed and surfaced as candidate Relationship proposals
- Machine-suggested candidates (FK inference, semantic inference) are shown alongside the analyst's SQL exploration in the same view
- Analyst selects candidates to promote to a formal Relationship request

**Stage 2 — Relationship approval** (consequential — structural and permanent):

- Raised to every distinct steward who owns an asset involved in the relationship
- Is this a legitimate traversal path? Is the join semantically valid?
- All implicated stewards must approve; relationship becomes a permanent catalog entry

**Stage 3 — Query creation:**

- Analyst builds the Query in any supported language (SQL, GraphQL, Cypher), traversing approved relationship paths
- Only approved catalog relationships are traversable — the compiler enforces this structurally
- No approval required — column visibility and relationship approval are the only gates

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
| --- | --- | --- |
| 1 — Registered source schema | Every table, column, and type from a registered source. Admin-level visibility. | None — raw inventory |
| 2 — Unclaimed tables | Tables introspected from registered sources with no domain owner. Visible to stewards with source access. | Available but ungoverned |
| 3 — Domain assets | Claimed tables and steward-defined views. Fully governed, owned, catalog-visible. | Fully governed |
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
| --- | --- |
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

High correlation raises confidence; low correlation suppresses or demotes the candidate. Probing is corroborating evidence, not proof — integer ranges can overlap coincidentally and partial referential integrity is common in analytical systems. Significant room for error remains. Steward semantic judgment is the only reliable final check.

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

- Draft query for analyst review and refinement
- Per-component confidence scores
- Ordered prerequisite list
- Gap list

The business description becomes the view's declared business purpose once the view is formally created.

**SQL-first relationship discovery (Modeling tool):**

Accessed as a modal from the Relationships page. The intent is to build the semantic model — identifying structural join paths before formalising them as governed relationships.

1. Analyst writes freeform SQL against accessible tables (RLS and masking still applied)
2. SQL AST is parsed — each JOIN condition becomes a candidate Relationship proposal
3. Candidate list is shown alongside machine-suggested candidates (FK inference, semantic inference) for unified review
4. Analyst promotes selected candidates to formal Relationship requests
5. Approved Relationships are added to the catalog and become traversable in Queries

The Modeling tool may show all registered tables for structural exploration, even where the analyst cannot see the underlying data — steward approval governs actual data access, not schema visibility.

---

## 3. Usage

### Query Audit Trail

Every query that touches a domain asset is recorded in an append-only `query_audit_log`. Each entry captures:

- `tenant_id`, `user_id`, `role_id` — the identity context
- A SHA-256 hash of the query — the verbatim query text is never stored
- `table_ids` — the domain assets the query touched
- `source`, `status_code`, `duration_ms`
- `logged_at` — the timestamp

The log is append-only (DELETE and UPDATE blocked at the database level) and indexed by `(tenant_id, logged_at)` and `(user_id, logged_at)`.

The steward's query history report is an aggregated view over this log, filterable by asset, role, and time window. The catalog is a live governance instrument — stewards maintain awareness of how their assets are used as it happens, not after the fact.

**Two visibility mechanisms:**

- **Push** — post-use notifications for structural acts (a new view was created using your fields)
- **Pull** — query history for runtime usage patterns

---

## 4. Data Products (REQ-1634)

A data product is a named, owned bundle of tables published together for consumption. It is the unit the catalog exposes to consumers — not individual tables, but a curated surface a domain explicitly declares ready. Fields follow the ODPS (Open Data Product Standard) vocabulary where Provisa already has the source of truth. [tool-verified: `provisa/core/models.py:318-342`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json`]

### Domain ownership rule

Every data product is owned by exactly one domain (`domain_id` is a required field). A table may join a data product only when both share the same `domain_id`. The UI scopes the table picker to the product's domain; the backend rejects a `product_id` assignment whose domain does not match the product's at save time. [tool-verified: `provisa/core/models.py:320`, `docs/arch/requirements.yaml:54573-54574`]

A product that needs data from another domain must bring that data in as a domain view first, then include the view as a member.

### Output ports

The tables and commands assigned to a data product are its **output ports** — the queryable surface consumers see. Assigning a table sets `Table.product_id`; clearing it removes the membership. A table belongs to at most one product. Commands in the same domain may also be assigned as members. [tool-verified: `provisa/core/models.py:941`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json:tablesLabel,commandsLabel`]

### Detail panel sections

Opening a data product in the admin UI shows these panels:

| Panel | What it shows |
| --- | --- |
| Output Ports | Member tables and their columns; member commands; sample queries (GraphQL, SQL, Cypher, gRPC, JSON:API, REST) |
| Related Terms | Glossary terms linked to the product's member tables |
| Related Tables | Tables reachable from member tables through approved relationships but not yet part of the product |
| Relationships | Approved relationships between this product's member tables |
| Lineage | Column lineage graph showing member tables as the published endpoint plus every upstream table. Requires the `view_governance` capability |
| Input Ports | One-hop inputs → transform → outputs derived from lineage. Requires `view_governance` |
| Data Quality | Checker tables whose contracts scan this product's output ports; one row per check per run. Includes a rules modal and PII tag display |

[tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:detail`]

### Metadata export

Only tables assigned to a product publish to external catalogs by default. `build_snapshot` applies a `data_products_only` filter: unassigned tables are withheld, along with their relationship edges, lineage edges, and governance tags. Sources and domains always publish regardless. [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

A product with no exported members does not publish — an empty listing would claim a product exists with nothing behind it. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

Each product publishes as a first-class listing in every connected catalog:

| Catalog | Published as |
| --- | --- |
| Snowflake Horizon | SHARE + organization listing; `publish=false` keeps it DRAFT, `publish=true` takes it live |
| BigQuery Analytics Hub | Analytics Hub listing |
| OpenMetadata | DataProduct entity |
| DataHub | URN-addressed listing |
| Apache Atlas | `provisa_data_product` asset |
| Atlan | DataProduct asset |
| OpenLineage | Members mapped per `DataProductAsset` |

[tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:389-418`, `provisa/api/metadata_export/bigquery_dataplex.py:112-136`, `provisa/api/metadata_export/openmetadata.py:326-331`, `provisa/api/metadata_export/openlineage.py:348`]

### Fields

| Field | Required | Notes |
| --- | --- | --- |
| `id` | Yes | Machine-readable stable identifier, e.g. `customer_360` |
| `domain_id` | Yes | Owning domain; membership rule enforced against this |
| `name` | Yes | Display name |
| `owner_role` | No | Role accountable for this product; distinct from the domain steward |
| `team_role` | No | Role whose holders form the day-to-day working team; resolves to individuals |
| `purpose` | No | What this product publishes and why |
| `limitations` | No | Known constraints, caveats, or exclusions |
| `usage` | No | How to consume this product |
| `version` | No | e.g. `1.2.0` |
| `status` | No | e.g. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | No | Service-level commitments; prose only — a product spans multiple member tables and a structured SLA cannot unambiguously name which member it describes |
| `support` | No | Free-text support guidance |
| `support_contact` | No | Email or URL; required by Snowflake Horizon Catalog organization listing manifests (REQ-1635) |
| `publish` | No | `true` to publish Horizon Catalog listings immediately; new listings default to DRAFT (REQ-1635) |
| `custom_properties` | No | Arbitrary key-value metadata not covered by the standard fields |

[tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/types.py:104-118,538-551`]
