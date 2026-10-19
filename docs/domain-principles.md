# Domain Model Principles

---

## 1. Governance

### Core Principles

1. **Every resource must be owned by a domain.** Tables, views, and relationships are all domain assets. There are no ungoverned floating resources. The domain is the unit of accountability.
2. **Every domain must have a steward.** A domain may exist in a pending state until a steward is assigned, but it cannot serve governed data without one.
3. **The admin owns sources.** Sources are infrastructure, not domain resources. The admin registers and manages connections to external data systems.
4. **Stewards can claim tables for a domain.** A claim is exclusive: a table belongs to exactly one domain. Claiming is the governed act that connects infrastructure to the model.
5. **Stewards can create intradomain views from domain assets.** Views express business logic, such as joins, aggregations and derived metrics, over assets the steward owns in the same domain. A view creates new semantic meaning and needs steward approval.
6. **Analysts can create cross-domain queries from approved relationships.** Queries are interdomain views written in any supported query language. They create no new semantics; they traverse approved relationship paths. No further approval is needed, because governance happens earlier, at the Relationship and column visibility layers. The catalog does the enforcing: the compiler refuses any traversal that isn't in the approved relationship catalog.
7. **Anyone can request access to a domain resource.** Access is granted per resource, never per query: if you have access to a resource, you can query it. Governance is enforced at execution time through the pipeline.

### Resources: Tables and Views as Peers

A table and a view differ only in where they come from: a table is claimed from a source, and a view is defined by a steward. Once either is a domain asset, the governance model treats them the same:

- Both are domain assets in their own right, visible in the catalog
- Both can be the target of a relationship
- Both can be granted under Principle 6
- Both are subject to the same governance pipeline

A steward can claim tables privately and expose only curated views as public-facing data products.

### View Composition

A view always belongs to a single domain. There's only one kind of view, and it's always intradomain. A view exists for one of two purposes:

- **Cross-domain import** — the source is outside the domain. Cross-domain data may only enter a domain via a view, which acts as a read-only adapter naming the external data as a domain business concept.
- **Local derivation** — the source is same-domain. The view derives new or calculated data from existing domain assets. New or derived data may only exist as a view.

A view may reference:

- Claimed tables within the same domain
- Fields imported from another domain under a field access grant
- One other view within the same domain, where the variation is purposeful: field restriction, aggregation, or enrichment via an additional join

Nothing technical limits how deeply views compose. The steward's judgment during HITL review is the quality control.

Every view carries a declared business purpose, stated at creation time:

- Part of the governed artifact, so stewards approve knowing what the view is for
- Referenced by access requests under Principle 7 so the steward can assess fit
- Travels from view creation through the full governance workflow

### Queries

A Query traverses approved relationship paths over domain assets. Unlike a View, it creates no new semantic meaning; it follows the model's approved structure. A Query can be written in any supported query language (SQL, GraphQL, Cypher).

**Structural enforcement:** The relationship catalog does the enforcing. The compiler checks every traversal against the approved catalog entries and refuses a query that names an unapproved path. Governance is built into the structure, so there's no separate check at run time.

**No approval required:** Governance happens earlier, at the Relationship and column visibility layers. If a user can see the columns and the traversal path is approved, the Query is valid usage, with no further gate.

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

The compiler works out domain membership from these structural positions, so no annotation or hint is needed.

### Relationships

A relationship is an approved traversal path between two assets. Domain boundaries don't change what a relationship is; they only decide who approves it.

**Approval:**

- A relationship request is decided by the domains it touches: the domain of its source table and the domain of its target table
- An approval counts only from a user whose right to create relationships reaches one of those domains; a rejection may come from any such user
- The request is carried out once two different users, neither of them the requester, have approved and every domain involved has an approver: a relationship across two domains needs a yes from each side, and a relationship inside one domain needs two approvers of that domain
- Approving a relationship builds each steward's dependency graph, so stewards hear about schema changes before they land

Relationships are created when someone needs them, never speculatively. The first team with the business need does the work, and later teams inherit it.

**Optimization consequence:** A relationship declaration is a governance artifact and also a structural description of a join shape. The two tables, two columns and join type that define a relationship are exactly what the query optimizer needs to pre-materialize that join. Cross-source relationships get pre-materialized join tables automatically, and same-source relationships can opt in with `materialize: true`. Stewards who think through and approve valid relationships get faster queries as a direct result: the governance work and the optimization work are the same act.

### Field Access Grants

A field access grant is a permission from one domain to another: Domain A may use specific fields from Domain B in its views.

**Grant lifecycle:**

- Prompted by view creation when foreign fields are identified as needed
- Approved once by the target domain steward
- Belongs to the requesting domain, not to the view that prompted it
- Any subsequent view in the requesting domain may use the granted fields without further cross-domain involvement
- Additional ungranted fields require a new request

**Post-use notification:** When a view is created with granted fields, the source steward is told about it, without being asked to approve. The notification gives the view name, its declared business purpose, the fields used and the steward who approved it. That gives the source steward:

- **Visibility** — awareness of how their data is being used
- **Oversight** — grounds to raise a concern if usage looks inappropriate
- **Recourse** — ability to revoke the grant, invalidating dependent views

The tradeoff: the source domain approves field access without knowing every future use. Per-view approval is correct in theory and unworkable in practice.

### Query Creation Workflow

Three stages, in order.

**Stage 1 — Shaping (SQL discovery, from the Relationships page):**

- Analyst opens the Shaping tool from the Relationships page to try out join paths in raw SQL
- SQL is run against accessible data, subject to existing RLS and column masking
- JOINs in the SQL are parsed and surfaced as candidate Relationship proposals
- Machine-suggested candidates (FK inference, semantic inference) are shown alongside the analyst's SQL in the same view
- Analyst selects candidates to promote to a formal Relationship request

**Stage 2 — Relationship approval** (consequential — structural and permanent):

- Decided by the domains the relationship touches, as described under [Relationships](#relationships)
- Is this a legitimate traversal path? Is the join semantically valid?
- Two different users, neither of them the requester, must approve, and every domain involved must have an approver; the relationship then becomes a permanent catalog entry

**Stage 3 — Query creation:**

- Analyst builds the Query in any supported language (SQL, GraphQL, Cypher), traversing approved relationship paths
- Only approved catalog relationships are traversable, and the compiler enforces this structurally
- No approval required: column visibility and relationship approval are the only gates

### HITL as the Primary Control

Technical rules handle what is objective: field provenance tracking, domain boundary enforcement and compiler validation. Judgment that depends on context stays with the steward. Limits such as view composition depth, a stated purpose for each query and relationship approval decisions are HITL concerns, which the compiler doesn't enforce.

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

An unclaimed table points to a gap. If the data you need exists only at Tier 2, a steward must claim it before governance can go further. When no tier has any candidate, escalate to an admin.

### FK Constraints

FK constraints belong to a single source, so they can't span sources. Cross-source join paths come entirely from approved catalog relationships (Tier 4), which are stronger evidence because both stewards have validated them.

Within a source:

- FK constraints are surfaced automatically as candidate relationships on source registration
- They represent explicit modeling intent: unenforced in most analytical SQL systems, but declared on purpose
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

High correlation raises confidence, and low correlation suppresses or demotes the candidate. Probing corroborates; it doesn't prove. Integer ranges can overlap by coincidence, and partial referential integrity is common in analytical systems, so there's plenty of room for error. The steward's semantic judgment is the only reliable final check.

### LLM-Assisted Discovery

The LLM operates across all five tiers simultaneously, suggesting relationships, candidate claims, and traversal paths ranked by confidence.

**What the LLM surfaces:**

- Candidate relationships ranked by confidence
- Unclaimed tables that may satisfy a data need, with a prompt to initiate claiming
- No candidate at all is the signal to escalate to an admin

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

It opens as a modal from the Relationships page. Its purpose is building the semantic model: finding structural join paths before they are made formal as governed relationships.

1. Analyst writes freeform SQL against accessible tables (RLS and masking still applied)
2. The SQL AST is parsed, and each JOIN condition becomes a candidate Relationship proposal
3. Candidate list is shown alongside machine-suggested candidates (FK inference, semantic inference) for unified review
4. Analyst promotes selected candidates to formal Relationship requests
5. Approved Relationships are added to the catalog and become traversable in Queries

While you work out the structure, the Modeling tool may show every registered table, including ones whose data the analyst can't see. Schema visibility is a separate matter; steward approval is what governs access to the data itself.

---

## 3. Usage

### Query Audit Trail

Every query that touches a domain asset is recorded in an append-only `query_audit_log`. Each entry captures:

- `tenant_id`, `user_id`, `role_id` — the identity context
- A SHA-256 hash of the query; the verbatim query text is never stored
- `table_ids` — the domain assets the query touched
- `source`, `status_code`, `duration_ms`
- `logged_at` — the timestamp

The log is append-only (DELETE and UPDATE blocked at the database level) and indexed by `(tenant_id, logged_at)` and `(user_id, logged_at)`.

The steward's query history report is an aggregated view over this log, which you can filter by asset, role and time window. That makes the catalog a live governance instrument: stewards see how their assets are used as it happens, and don't find out after the fact.

**Two visibility mechanisms:**

- **Push** — post-use notifications for structural acts (a new view was created using your fields)
- **Pull** — query history for runtime usage patterns
