# Provisa Release Plan

> Generated from `docs/arch/release-plan.json` by `scripts/gen_roadmap.py`. Do not hand-edit.

One release every 2 days, from v0.1.0-alpha.513 to v0.1.0-alpha.524. Dates are publish dates; planned releases are not committed.

| Release | Date | Status | Requirements | Product rules |
|---|---|---|---|---|
| 513 | 2026-11-27 | converging | 18 | shipped |
| 514 | 2026-11-29 | on main (cut from main) | 12 | shipped |
| 515 | 2026-12-01 | planned | 18 | 30 |
| 516 | 2026-12-03 | planned | 14 | 28 |
| 517 | 2026-12-05 | planned | 17 | 30 |
| 518 | 2026-12-07 | planned | 11 | 24 |
| 519 | 2026-12-09 | planned | 9 | 30 |
| 520 | 2026-12-11 | planned | 17 | 30 |
| 521 | 2026-12-13 | planned | 11 | 30 |
| 522 | 2026-12-15 | planned | 13 | 28 |
| 523 | 2026-12-17 | planned | 8 | 30 |
| 524 | 2026-12-19 | planned | 21 | 26 |

## v0.1.0-alpha.513: 2026-11-27 (converging)

### Querying & protocols

- **REQ-2204** BI-tool catalog query replay [SHOULD] [complete] — DBeaver, Metabase, Grafana and psql browse Provisa schemas the same way they browse PostgreSQL, tested against captured real tool traffic.
- **REQ-2176** Ephemeral session objects in every language [SHOULD] [complete] — Users create temporary nodes, rows and graphs in a session without writing to sources, for what-if analysis.

### Sources

- **REQ-1986** Parameterized tables [MUST] [complete] — API and native-query tables take typed parameters, so a caller can read an endpoint like /orders/{id} as a table without custom code.
- **REQ-2112** VS Code extension for code sources [SHOULD] [complete] — Developers see which functions Provisa will serve, with inferred columns, and test edits live from their editor before registering anything.
- **REQ-2270** Spark reads governed Provisa tables [MUST] [complete] — Data engineers read a governed table as a Spark DataFrame, with row rules and masks applied before data leaves Provisa.
- **REQ-2138** OWL reasoning on SPARQL sources [SHOULD] [complete] — Inferred class memberships from an ontology become queryable, so rule-derived categories need no hand-maintained tables.

### Governance & security

- **REQ-1058** Anonymous access to published data [SHOULD] [complete] — An org can publish data to the open web from its own subdomain with no login, served only through a read-only public role.
- **REQ-2059** Per-reader masks [SHOULD] [complete] — One column can show cleartext to some roles and different masks to others, in a visible precedence order an auditor can check.
- **REQ-2145** PII tag by meaning, including future columns [MUST] [complete] — Tagging a business term as PII covers every column with that meaning, including columns added later, so new data is not left unprotected.
- **REQ-2219** Per-person entitlements on coarse roles (delivered rules; the rest is REQ-2348) [SHOULD] [complete] — The identity provider grants a person specific tables and columns as attributes, so roles stay few instead of one role per person.

### Identity & organizations

- **REQ-1246** Firebase sign-in creates the user profile [MUST] [complete] — Users who sign in with Google or email links get an account on first login without a separate password to manage.

### Modeling & semantic layer

- **REQ-194** One naming authority for client-facing names [MUST] [complete] — Every client sees the same governed names on SQL, GraphQL and Cypher, and physical column names never leak to users.

### UI & modeling

- **REQ-2259** Context navigation group [MUST] [complete] — Glossary, ontology, metrics, tags, classification and documents sit under one menu, so stewards find the context layer in one place.

### Install & ops

- **REQ-2269** One-script Linux host install with Trino [SHOULD] [complete] — An operator installs Provisa with a Trino engine on an existing Linux VM or server with one command, without Kubernetes.

### Engines & performance

- **REQ-2003** Freshness rules for per-statement tables [MUST] [complete] — Native-query and parameterized tables have predictable caching, so users know when they see live data and when a kept copy.

### Environments & change control

- **REQ-1919** The model store (delivered rules; the rest is REQ-2308) [MUST] [complete] — Sources, tables, roles, rules and products are held in one versioned store per org, the base for environments, sync and git projection.

### Catalog & data products

- **REQ-2122** Catalogs admin panel (publish and read) (delivered rules; the rest is REQ-2331) [SHOULD] [complete] — Each catalog connection is set up once and used both to publish and to read metadata.

### Modeling & naming

- **REQ-194** Names across the whole data estate [MUST] [complete] — Every admitted object, export and grounded column term gets a checked, clash-free name, and only the worker that changed the model rewrites the names, so names stay consistent as the model grows.

## v0.1.0-alpha.514: 2026-11-29 (on main (cut from main))

> Provisa's GitHub organization becomes simpleishard-llc with this release: the repository, release assets and container images (ghcr.io/simpleishard-llc) move there (maintainer 2026-11-27).

### Querying & protocols

- **REQ-2194** Rust pgwire row encoder [SHOULD] [complete] — SQL clients over the PostgreSQL protocol get large results many times faster (measured 14x text, 40x binary) with every PostgreSQL type supported.
- **REQ-1961** Exact numeric precision and scale [MUST] [complete] — Money and decimal columns keep their exact digits on every engine and protocol, so financial figures never round differently by client.

### UI & modeling

- **REQ-2262** Relate tables right after registering them [MUST] [complete] — Source setup ends with proposed relationships for the new tables, so a first query can join them minutes after connecting.
- **REQ-2239** One 2D graph component [MUST] [complete] — The ERD, lane diagram and ontology graph look and behave alike, so users read every diagram the same way.

### AI assistance

- **REQ-2115** Polly operates the UI visibly [SHOULD] [complete] — The assistant fills forms and presses buttons while the user watches, so new users learn the product by seeing each step done.

### Install & ops

- **REQ-1212** Docker image builds its own UI [MUST] [complete] — Every published image carries the UI built from the same commit, so the console always matches the server.

### Sources

- **REQ-1874** Wrapper-catalog services as brands (delivered rules; the rest is REQ-2306) [MAY] [complete] — Stripe, OpenMetadata and DataHub connect as brands over the OpenAPI source, with no API spec to find.
- **REQ-2225** Spark connector in Sources (delivered rules; the rest is REQ-2350) [SHOULD] [complete] — Teams connect a Spark cluster as a source from the UI with the cluster as structured fields, without writing glue code.
- **REQ-2267** Discovered code-source tables and live API sources (delivered rules; the rest is REQ-2360) [SHOULD] [complete] — A code source can generate its own tables and relationships at discovery, and OpenAPI and GraphQL sources run as built-in code sources.

### Catalog & data products

- **REQ-2180** Data product access requests (delivered rules; the rest is REQ-2340) [MUST] [complete] — A user who can see a product but not read it asks its owner in one step, and the grant is recorded instead of handled by email.

### Governance & security

- **REQ-2278** Column rules by tag or classification [MUST] [complete] — One rule hides or masks every column carrying a tag such as PII, wherever it appears, instead of maintaining rules column by column.
- **REQ-2279** Template fakes for JSON columns [SHOULD] [complete] — Masked JSON columns return realistic fake objects with the right shape, so test and demo environments work without real personal data.

## v0.1.0-alpha.515: 2026-12-01 (planned)

### Identity & organizations

- **REQ-1022** Email approvers on new requests [MUST] [in-progress] — Approvers get an email with approve and deny links when a creation request is raised, so requests do not sit unseen.
- **REQ-2290** Generated break-glass password with forced rotation [MUST] [in-progress] — The emergency admin account cannot be used from a leaked config file or a reused password.
- **REQ-2291** Last-platform-admin guard and recovery [MUST] [in-progress] — An installation can never lock itself out of platform administration.
- **REQ-2293** Audit entry for org deletion [MUST] [in-progress] — Auditors can see who deleted an org and when, after its data is gone.

### Install & ops

- **REQ-1459** High availability as published recovery time [MUST] [in-progress] — Every tier gets a stated recovery time in minutes, so buyers can plan around it without paying for an HA upgrade.
- **REQ-2288** Every protocol on secondary nodes [MUST] [in-progress] — Clients on pgwire, Bolt, gRPC or MCP are load-balanced across every node of a cluster.
- **REQ-2289** cert-manager certificates in Helm [MUST] [in-progress] — Kubernetes operators get certificates issued and renewed automatically instead of managing TLS Secrets by hand.

### Engines & performance

- **REQ-2284** Tier-keyed Trino resource groups [MUST] [in-progress] — Higher tiers get more concurrency and memory on shared Trino, so paying tenants are not throttled like free ones.
- **REQ-2300** Schema-only boot replicas on every engine [MUST] [in-progress] — Restarts are fast and never re-copy source data.
- **REQ-2307** Edit conflicts and one reconcile path [MUST] [in-progress] — Two admins can no longer silently overwrite each other's model edits.

### Catalog & data products

- **REQ-2294** URI reserved words and rename aliases [MUST] [in-progress] — Catalogs that stored an object's old address still find it after a rename.
- **REQ-2296** OneLake catalog target; user descriptions kept [MUST] [in-progress] — Microsoft Fabric shops publish to OneLake, and no catalog path overwrites descriptions people wrote.

### Environments & change control

- **REQ-2298** Bind environment sources from the CLI [MUST] [in-progress] — Environment setup is scriptable in CI without the UI.
- **REQ-2299** Present-and-inert environment state [MUST] [in-progress] — Admins see at a glance which environments still need data bindings.
- **REQ-2308** Model store owns every write [MUST] [in-progress] — Every model change is versioned and synced the same way, whatever was edited.

### Governance & security

- **REQ-2302** Complete typed engine authorization [MUST] [in-progress] — No engine statement runs without a verified authorization, closing the last ungoverned paths.
- **REQ-2319** Public metrics [MUST] [in-progress] — Company-wide metrics are defined once and visible to every domain.

### Modeling & naming

- **REQ-2361** One naming authority end to end [MUST] [in-progress] — Names suggested before saving and names shown after saving come from one place, so a table or relationship is called the same thing in the UI, the API and every query language.

## v0.1.0-alpha.516: 2026-12-03 (planned)

### Querying & protocols

- **REQ-2320** Portable JSON operations [MUST] [in-progress] — JSON queries give the same answer on every engine, and type drift is caught instead of returning wrong data.
- **REQ-2337** Total landing of subtypes [MUST] [in-progress] — inet, money and range columns are always readable on every engine.
- **REQ-2352** Polyfill provenance and hint rows [MUST] [in-progress] — Buyers see where each PostgreSQL equivalence was verified and which source-specific hints apply.
- **REQ-1987** Shared named types on every surface [SHOULD] [in-progress] — Nested objects and enums appear as named, reused types in GraphQL and gRPC, so generated client code stays small and stable.

### Governance & security

- **REQ-2323** Generated files always in the org folder [MUST] [in-progress] — One org's generated code and data never land where another org's process can read them.
- **REQ-2342** Metric ownership by domain [MUST] [in-progress] — Each metric has an accountable owner, and only they or an org admin change it.

### Catalog & data products

- **REQ-2330** Alation export target [MUST] [in-progress] — Alation customers get Provisa's model in their catalog like the other supported catalogs.
- **REQ-2336** Contract type check and alias names [MUST] [in-progress] — Consumers are warned when a source breaks a data contract, and never see physical column names.
- **REQ-2340** Role and column requests; suggestion outcomes [MUST] [in-progress] — Buyers ask for the role and columns they need in one form and see what was decided.

### Demos & website

- **REQ-2286** Contact-sales form [MUST] [in-progress] — Enterprise prospects reach sales from the site without needing a mail client.

### Engines & performance

- **REQ-1042** Separate interactive and batch workloads [SHOULD] [in-progress] — Materialized-view refreshes cannot slow interactive queries.
- **REQ-2283** Opt out of view answers per request [SHOULD] [in-progress] — An analyst who needs the live base-table answer bypasses pre-aggregated views for one query without changing the model.

### Install & ops

- **REQ-2285** Single-node Trino and queue-aware autoscaling [SHOULD] [in-progress] — A small Helm install runs Trino in one pod, and larger ones add workers when queries queue, not only when CPU is busy.

### Identity & organizations

- **REQ-2292** Mandatory config download on org deletion [SHOULD] [in-progress] — No org is deleted without a restorable copy of its configuration.

## v0.1.0-alpha.517: 2026-12-05 (planned)

### Install & ops

- **REQ-2295** Ops trend, usage and freshness reports [SHOULD] [in-progress] — Operators see trends, resource use and data freshness in built-in reports instead of ad-hoc log queries.
- **REQ-2314** Prometheus metrics and cluster health [SHOULD] [in-progress] — Operators monitor Provisa in their existing Prometheus and OpenTelemetry stack.
- **REQ-2315** Helm values schema [SHOULD] [in-progress] — A wrong Helm value fails at install with a clear message instead of a broken deployment.
- **REQ-2316** WSL2 network bindings [SHOULD] [in-progress] — Windows users on WSL2 reach the UI from their Windows browser without manual port setup.

### Modeling & semantic layer

- **REQ-2297** column_tags relation [SHOULD] [in-progress] — Analysts join tag assignments with data in SQL and see only what their role may see.

### Catalog & data products

- **REQ-2301** Domain in Horizon listings [SHOULD] [in-progress] — Snowflake consumers see which business domain owns a listed product.

### Sources

- **REQ-2303** Crawl-scope fields in the source form [SHOULD] [in-progress] — Admins limit what an HTTP crawl follows from the UI instead of editing config.
- **REQ-2309** Wikidata endpoints, Atlassian brands, Wikidata grounding [SHOULD] [in-progress] — Atlassian Cloud or Data Center and custom Wikidata endpoints connect by brand, and terms can point to shared Wikidata meanings.
- **REQ-2311** Scalar OpenAPI and gRPC operations as functions [SHOULD] [in-progress] — Single-value API calls are used in queries as functions, the same as GraphQL scalars.
- **REQ-2313** Google Sheets OAuth, per-sheet tables, refresh [SHOULD] [in-progress] — Each tab of a spreadsheet becomes its own table under the user's own Google sign-in.
- **REQ-2317** Protobuf topics and schema evolution [SHOULD] [in-progress] — Protobuf Kafka topics are queryable, and schema changes are caught before they break readers.
- **REQ-2318** Exclusions in the source form [SHOULD] [in-progress] — Admins hide unwanted API endpoints from the UI without editing config.

### Engines & performance

- **REQ-2305** Cached rewrite on Flight SQL, Cypher and Bolt [SHOULD] [in-progress] — Repeated queries over Flight SQL, Cypher and Bolt return faster.

### Querying & protocols

- **REQ-2312** One Cypher and GraphQL catalog [SHOULD] [in-progress] — Graph tools see the same labels whether they connect over Bolt or HTTP.

### UI & modeling

- **REQ-2321** Confirm every delete [SHOULD] [in-progress] — Users cannot remove saved settings with one accidental click.

### Governance & security

- **REQ-2322** Deprecate metrics, roles and row rules [SHOULD] [in-progress] — Teams retire any model object with a warning period, not only tables and columns.

### AI assistance

- **REQ-2324** Sandbox and daily assistant budgets [SHOULD] [in-progress] — Trial assistant use has a hard, visible spending cap.

## v0.1.0-alpha.518: 2026-12-07 (planned)

### Sources

- **REQ-2327** Jira Data Center [SHOULD] [in-progress] — Self-hosted Jira shops query issues the same way Cloud users do.
- **REQ-2328** Registered remote hosts for code sources [SHOULD] [in-progress] — Code sources run on the customer's own machines, close to their data.
- **REQ-2335** Custom classification kinds [SHOULD] [in-progress] — Organizations detect their own identifiers, such as employee ids, the way built-in kinds are detected.
- **REQ-2343** Teradata, Db2, Vertica, Netezza, Informix, Sybase, Spanner sources [SHOULD] [in-progress] — Enterprises with legacy warehouses query them directly beside modern sources.

### Catalog & data products

- **REQ-2329** Catalog approvers and Alation data products [SHOULD] [in-progress] — Catalog users request access from the right approvers, and Alation shops see products too.
- **REQ-2331** Catalog pick list, refresh and lock [SHOULD] [in-progress] — Stewards reuse descriptions already written in their catalog and keep one source of truth.

### UI & modeling

- **REQ-2332** Ontology graph export; focus carried to queries [SHOULD] [in-progress] — Teams take the governed ontology into other RDF tools and keep their focus when moving to a query.
- **REQ-2338** One-request ontology graph [SHOULD] [in-progress] — Large ontology graphs open in one request.

### Modeling & semantic layer

- **REQ-2334** owl:sameAs and Purview term roles [SHOULD] [in-progress] — Ontology counts are per real-world entity, and Purview shows each term's role.
- **REQ-2341** Workbook row citations; uncited passages [SHOULD] [in-progress] — Citations to spreadsheets stay correct as the sheet changes.

### AI assistance

- **REQ-2339** Found terms to relationships [SHOULD] [in-progress] — Terms found in a document turn into model relationships in one step.

## v0.1.0-alpha.519: 2026-12-09 (planned)

### Governance & security

- **REQ-2344** Policy tags with gates and why-not answers [SHOULD] [in-progress] — Every refusal explains which policy caused it, and policy tags enforce exports and promotion without custom code.
- **REQ-2348** Entitlements tab, why-not trace, preview as [SHOULD] [in-progress] — Admins see and test exactly what a person can reach without signing in as them.

### Sources

- **REQ-2349** Code connector catalog, setup fields and Flight [SHOULD] [in-progress] — Organizations build full connectors in code, with typed setup forms and fast Arrow reads.
- **REQ-2350** Spark named demo [SHOULD] [in-progress] — Prospects see Spark data governed and joined with other sources end to end.
- **REQ-2360** One serving path for OpenAPI and GraphQL [SHOULD] [in-progress] — API sources behave identically however they were added, with one code path to fix and secure.

### Modeling & semantic layer

- **REQ-2351** dbt tests as data quality [SHOULD] [in-progress] — dbt test results show up as Provisa data quality, with no second test framework.
- **REQ-2353** Ontology releases with reviewed differences [SHOULD] [in-progress] — Teams adopt new versions of standard ontologies like FIBO deliberately, seeing what changed.
- **REQ-2355** Sync schedules, webhooks and full take-from-catalog [SHOULD] [in-progress] — Catalog and Provisa stay in step on a schedule, with differences decided by people or the customer's own workflow.
- **REQ-2356** Restore on re-register; retired-link findings [SHOULD] [in-progress] — Undoing a removal keeps history and links, and broken references are found, not discovered by users.

## v0.1.0-alpha.520: 2026-12-11 (planned)

### Sources

- **REQ-454** Identical remote-schema handling for all adapters [SHOULD] [in-progress] — GraphQL, gRPC and OpenAPI sources classify and register operations the same way, so behavior is predictable across source types.
- **REQ-2186** HTML lists read as tables [SHOULD] [in-progress] — Bulleted and numbered lists on web pages become tables, widening what a file source can read.
- **REQ-2304** Visible discovery failures [MAY] [in-progress] — Admins see why no tables were discovered instead of an empty list.
- **REQ-2254** Shared pgwire-calcite runtime [MUST] [accepted] — The Calcite connector runtime loads once and is shared by every pgwire server, cutting memory and start time per source.
- **REQ-2255** pgwire-calcite protocol parity [MUST] [accepted] — Sources reached through Calcite report column sizes and types like Provisa's own server, so SQL tools display them correctly.
- **REQ-2256** Calcite dialect fixes at the source [MUST] [accepted] — SQL sent to JDBC sources is fixed in the Calcite dialect, so every source on that dialect benefits.
- **REQ-2257** Production Db2, Trino, Vertica and Firebolt dialects [MUST] [accepted] — These four databases get correct pushed-down SQL, so queries against them return right answers.

### UI & modeling

- **REQ-1062** Org branding on portal surfaces [SHOULD] [in-progress] — An org publishing a data portal shows its own brand on the glossary and Explore pages it exposes.

### AI assistance

- **REQ-1955** One endpoint for valid join paths [SHOULD] [in-progress] — Agents and the join guard get the same list of valid paths between two tables, so AI-generated joins follow approved relationships.
- **REQ-2357** Domain-scoped generated descriptions [MUST] [accepted] — AI-written descriptions never read or change tables outside the domains a steward is working in.
- **REQ-2177** Citations agents can hand over intact [MUST] [accepted] — Agent answers carry a citation of the governed path and its trust, so a person can verify where an answer came from.

### Demos & website

- **REQ-2325** Customer 360 demo: match quality and a second question [SHOULD] [in-progress] — Prospects see entity-resolution quality measured, not asserted.
- **REQ-2326** OSV data in the vulnerabilities demo [SHOULD] [in-progress] — The demo answers exposure questions with real public vulnerability data.

### Governance & security

- **REQ-1060** Rate and cost caps on public endpoints [MUST] [accepted] — Published anonymous endpoints have the tightest limits, so a public data portal cannot run up engine costs or be abused.

### Environments & change control

- **REQ-1969** One consequences gate on every model save [MUST] [accepted] — Every create, update or delete from UI, API, config load or agent is checked the same way before it reaches the model.

### Modeling & semantic layer

- **REQ-1329** Base and derived relations without a sentinel source [MUST] [accepted] — Views and metrics show their real lineage instead of a placeholder source, so provenance reads correctly.

### Catalog & data products

- **REQ-2181** Buyer view of a data product [MUST] [accepted] — A user who cannot yet read a product sees what it offers and how to request it, which turns the catalog into a self-serve storefront.

## v0.1.0-alpha.521: 2026-12-13 (planned)

### Sources

- **REQ-2258** JDBC packs for hard-to-reach sources [MUST] [accepted] — SQL Server, Oracle, ClickHouse, Redshift, Exasol and others are reached through a dedicated JDBC pack where the engines' own connectors fall short.
- **REQ-2272** Spark Connect catalog as a source [MUST] [accepted] — Provisa reads a Spark cluster's databases and tables through Spark Connect, so lakehouse tables join with other sources in one query.

### Identity & organizations

- **REQ-1251** Approve or deny org join requests [SHOULD] [accepted] — Org admins approve people who ask to join, so teams onboard colleagues without a support ticket.
- **REQ-2346** Role members from SCIM, LDAP, Graph and Google [SHOULD] [in-progress] — Reviews and reports see who actually holds a role, whatever system manages membership.

### Environments & change control

- **REQ-1507** Environments gated by entitlement [SHOULD] [accepted] — Environments are enabled by license feature rather than product name, so edition upgrades turn them on without a reinstall.

### Install & ops

- **REQ-2164** Redacted log export for support [SHOULD] [accepted] — Operators send support a log file with secrets and personal data removed.

### Engines & performance

- **REQ-2183** Edge index via relationship materialize [SHOULD] [accepted] — Graph traversals over a materialized relationship read a prebuilt edge index, speeding large-graph queries.
- **REQ-2206** Volatile tables never served from copies [SHOULD] [accepted] — Tables marked volatile always read live, so time-critical data is never stale.

### Governance & security

- **REQ-2209** Periodic access reviews with attestation [SHOULD] [accepted] — Roles, rules and masks carry review schedules with recorded sign-off, which auditors ask for.
- **REQ-2210** Access requests from the catalog [SHOULD] [accepted] — Users ask for a table or column they found, and the request becomes a reviewed grant.
- **REQ-2211** Effective-access explainer [SHOULD] [accepted] — Admins see exactly what a set of roles and session values can read and why, answering access questions in seconds.

## v0.1.0-alpha.522: 2026-12-15 (planned)

### Querying & protocols

- **REQ-2212** SQL dialect per connection [SHOULD] [accepted] — A client written for SQL Server or another database connects without rewriting its SQL.
- **REQ-2213** Text search as an API query pattern [SHOULD] [accepted] — Tables offer stemmed text search from their API tab with no search engine to set up.
- **REQ-2268** Tags in the Cypher graph [SHOULD] [accepted] — Tags appear as nodes linked to the tables and columns they mark, so graph queries can traverse governance.
- **REQ-2310** Bind parameters in command calls [SHOULD] [accepted] — Commands are called from SQL and Cypher with parameters and column values, so queries stay parameterised.
- **REQ-2345** Case-insensitive SQL names [SHOULD] [accepted] — SQL users and BI tools find tables regardless of letter case.

### Engines & performance

- **REQ-2271** Engine impersonation (opt-in) [SHOULD] [accepted] — Engines can run queries as the requesting user, so engine-side audit logs name the real person.

### Governance & security

- **REQ-2287** Published data over pgwire and Bolt [SHOULD] [accepted] — Published open data is readable from SQL clients and graph tools without a login, not only over HTTP.

### Environments & change control

- **REQ-2347** Production access requests built on branches [SHOULD] [accepted] — Production users get access through a reviewed, time-limited request instead of new roles.

### AI assistance

- **REQ-2354** Assistant tests columns against policy sections [SHOULD] [accepted] — Stewards get a fast, governed first check of policy compliance without the assistant making legal claims.

### Modeling & semantic layer

- **REQ-2358** OpenLineage import of sources and tables [SHOULD] [accepted] — Teams that already emit OpenLineage bootstrap their Provisa model from it instead of registering tables by hand.

### Catalog & data products

- **REQ-2359** Collibra, Alation, Atlan, OneLake and Dataplex as sources [SHOULD] [accepted] — Governance teams query catalog metadata beside live data to find where catalog and enforcement disagree.

### Sources

- **REQ-1872** Engine discover() introspection fix [SHOULD] [accepted] — Source introspection through each engine returns complete table lists.
- **REQ-1941** Copy or move a table to another source [SHOULD] [accepted] — A table is copied under governance to another source, for migrations or offloading.

## v0.1.0-alpha.523: 2026-12-17 (planned)

### Catalog & data products

- **REQ-1966** dbt and Cube model export [SHOULD] [accepted] — The model exports to dbt semantic and Cube formats, so existing BI layers reuse it.

### Modeling & semantic layer

- **REQ-1995** Refer creation requests to another domain [SHOULD] [accepted] — Stewards hand a request to the right domain with a note instead of rejecting it.
- **REQ-2178** Terms linked to commands [SHOULD] [accepted] — Process terms such as "close of day" point to the commands that carry them out.

### Sources

- **REQ-2018** Prometheus tables from PromQL [SHOULD] [accepted] — Metrics from a PromQL query join business data in SQL, so operations and business numbers sit in one report.
- **REQ-2028** gRPC tables from request templates [SHOULD] [accepted] — A unary gRPC call becomes a parameterized table.
- **REQ-2029** Full Qdrant Query API tables [SHOULD] [accepted] — Hybrid dense and sparse vector search with fusion is available as a table.
- **REQ-2192** Generic OData source [SHOULD] [accepted] — SAP and Microsoft OData services connect from a service URL with no handwritten spec.

### Platform principles

- **REQ-2169** Drivers for sources, engines and secrets [SHOULD] [accepted] — Every built-in implementation is a driver, so partners extend Provisa the same way.

## v0.1.0-alpha.524: 2026-12-19 (planned)

### UI & modeling

- **REQ-2207** My Reports (saved queries) [SHOULD] [accepted] — Users save a named query and return to it, without creating a shared view.

### Sources

- **REQ-2251** One descriptor per source type [SHOULD] [accepted] — Each source type's facts live in one descriptor, so adding a source type touches one place.
- **REQ-2273** Spark time travel [SHOULD] [accepted] — Delta, Iceberg and Hudi tables on Spark answer as-of queries, so audits and restatements read past versions directly.
- **REQ-2274** Spark change data [SHOULD] [accepted] — Inserts, updates and deletes on Delta and Iceberg tables are served as change data for downstream sync.
- **REQ-2275** Spark procedures as commands [SHOULD] [accepted] — Maintenance such as compaction and snapshot expiry runs as governed commands from Provisa.
- **REQ-2280** OneDrive and Google Drive file crawling [SHOULD] [accepted] — Hosted and server installs read files straight from OneDrive and Google Drive, with no desktop sync client.
- **REQ-2306** Twelve SaaS brands over OpenAPI [MAY] [accepted] — Teams connect common SaaS tools by picking a brand and signing in, with no API spec to find.
- **REQ-1664** Custom Great Expectations checks as commands [MAY] [accepted] — Teams extend quality checks with their own Python functions through the same governed command surface.
- **REQ-2264** Marquez source [MAY] [accepted] — Lineage held in Marquez is queryable through its public API.
- **REQ-2276** Databricks catalog functions and models [MAY] [accepted] — Unity Catalog functions and MLflow models are offered as governed functions.

### Modeling & semantic layer

- **REQ-2281** Entity resolution from tags [SHOULD] [accepted] — Records that refer to the same customer or product across sources are linked in one shared map, so cross-source questions return one answer per entity.
- **REQ-2282** Entities found in text [SHOULD] [accepted] — Notes and documents link to the products and tables they mention, so unstructured text joins structured data.

### Demos & website

- **REQ-2139** Beneficial ownership demo [SHOULD] [accepted] — Shows KYC ownership chains with OWL transitive control reasoning.
- **REQ-2182** Internal data marketplace demo [SHOULD] [accepted] — Shows products offered to an organisation's own people with existing primitives.
- **REQ-2220** Masked-column request demo [SHOULD] [accepted] — Shows an analyst seeing why a column is masked, requesting it, and getting a governed grant.
- **REQ-2232** dbt project demo [SHOULD] [accepted] — Shows a dbt project governed by role with lineage, unchanged.
- **REQ-2250** Wildfire exposure demo [SHOULD] [accepted] — Shows insured value inside this week's wildfire perimeter and which policyholders to call first.

### Environments & change control

- **REQ-1499** Replicate a whole environment [MAY] [accepted] — An org copies an environment with bindings, secrets and users when it wants the control plane as it stands.
- **REQ-1500** Export an environment to desktop [MAY] [accepted] — A developer works against a non-prod environment's model on their own machine.

### Querying & protocols

- **REQ-2333** SPARQL Update to curation and commands [MAY] [accepted] — RDF tools curate the ontology and run commands under the same rights as the UI.

### Catalog & data products

- **REQ-1646** Horizon _provisa exposure views [MAY] [accepted] — Snowflake users see governed views organized by domain in a clean database.

## Decisions applied

- **REQ-1367, REQ-1991, REQ-1756, REQ-2235, REQ-2277, REQ-2167** Standing principles or processes, off the release train (amended; REQ-2277 accepted).
- **REQ-1027, REQ-1240, REQ-1563, REQ-1564, REQ-1779** Cloud-dev operations track, off the release train (amended).
- **REQ-818** Superseded by REQ-2176 and REQ-1925 (superseded_by REQ-2176); REQ-346 repointed to REQ-2176 and REQ-798 to REQ-1925, since a superseded requirement cannot be a successor.
- **REQ-1536** Superseded by REQ-2014.
- **REQ-1871, REQ-1366** Spikes closed complete with findings recorded in the description.
- **REQ-1859, REQ-1860** Results internal: REQ-1860 complete with its public format withdrawn; REQ-1859 stays open as internal work, off the train.
- **REQ-1964** Accepted and handed to the govdata session, off the train.
- **REQ-1793** Kept; behaviour present since 2026-09, completes once tested (no product work planned).
- **all proposed** Accepted (maintainer accept-all); every one is planned below or completed.
- **REQ-1742** Complete in 513, but it is end-to-end test coverage, not a product feature, so it is left out of the release notes.
- **REQ-2166** Accepted; no product work remains (only tests), so not planned as a feature.
- **REQ-2242** Accepted and parked until sqlglot allows interpreted subclasses of its compiled classes; not planned.

## Off the release train

REQ-818, REQ-1027, REQ-1240, REQ-1366, REQ-1367, REQ-1536, REQ-1563, REQ-1564, REQ-1756, REQ-1779, REQ-1859, REQ-1860, REQ-1871, REQ-1964, REQ-1991, REQ-2167, REQ-2235, REQ-2277.

## Appendix: already shipped

- **REQ-116** Kafka Schema Registry integration (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2317)
- **REQ-259** Apollo Federation v2 subgraph as a declared role (tests present at v0.1.0-alpha.512; now in-progress)
- **REQ-450** One-to-many nested fields carry target's root args (tests present at v0.1.0-alpha.512; now complete)
- **REQ-451** Object relationships expose no collection args (behaviour present, no test dates it; now in-progress)
- **REQ-453** Scalar remote operations become tracked functions (behaviour present, no test dates it; now in-progress; remainder REQ-2311)
- **REQ-469** One catalog module per query language (behaviour present, no test dates it; now in-progress; remainder REQ-2312)
- **REQ-477** use_domains=false forces domain_prefix off (tests present at v0.1.0-alpha.512; now complete)
- **REQ-491** Google Sheets source (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2313)
- **REQ-506** SLA monitoring with stored breaches (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2314)
- **REQ-507** Helm chart (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2315)
- **REQ-509** WSL2 install handling (behaviour present, no test dates it; now in-progress; remainder REQ-2316)
- **REQ-882** Aggregate queries answered from pre-aggregating views (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2283)
- **REQ-1040** Per-org Trino resource groups by tier (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2284)
- **REQ-1041** Trino worker scale-out path and autoscaling (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2285)
- **REQ-1051** Hosted docs on Cloudflare Pages (behaviour present, no test dates it; now in-progress)
- **REQ-1052** Marketing site calls to action (tests present at v0.1.0-alpha.512; now in-progress; remainder REQ-2286)
- **REQ-1059** Publishing via a built-in public role (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2287)
- **REQ-1211** AppImage stages compose files to a writable home (behaviour present, no test dates it; now in-progress)
- **REQ-1230** Secondary cluster nodes run the app tier only (behaviour present, no test dates it; now in-progress; remainder REQ-2288)
- **REQ-1256** Helm chart serves the UI (behaviour present, no test dates it; now in-progress)
- **REQ-1257** Helm chart TLS on every endpoint (behaviour present, no test dates it; now in-progress)
- **REQ-1258** Helm Service exposes every protocol port (behaviour present, no test dates it; now in-progress)
- **REQ-1259** Helm certificate source: Secret or cert-manager (behaviour present, no test dates it; now in-progress; remainder REQ-2289)
- **REQ-1261** Self-service multi-tenant onboarding (tests present at v0.1.0-alpha.512; now complete)
- **REQ-1264** Seeded break-glass platform admin account (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2290)
- **REQ-1296** Claimed platform admin lands in a populated root org (tests present at v0.1.0-alpha.512; now in-progress)
- **REQ-1299** Never zero platform admins (tests present at v0.1.0-alpha.512; now in-progress; remainder REQ-2291)
- **REQ-1300** Deleting an organization (tests present at v0.1.0-alpha.512; now in-progress)
- **REQ-1303** Audited recovery of an org's administration (tests present at v0.1.0-alpha.512; now in-progress)
- **REQ-1304** Org config downloaded before deletion (tests present at v0.1.0-alpha.512; now in-progress; remainder REQ-2292)
- **REQ-1305** Offboarding a member from both planes (tests present at v0.1.0-alpha.512; now complete)
- **REQ-1312** Deletion scrubs personal data, keeps the record (tests present at v0.1.0-alpha.512; now in-progress; remainder REQ-2293)
- **REQ-1327** Platform admin is control-plane only; administration layers (tests present at v0.1.0-alpha.512; now complete)
- **REQ-1364** Config export JSON Pointer refs and resourcePath (tests present at v0.1.0-alpha.512; now complete)
- **REQ-1382** IdP vs local user attributes precedence (tests present at v0.1.0-alpha.512; now complete)
- **REQ-1383** Attributes in user.* namespace with provenance display (tests present at v0.1.0-alpha.512; now complete)
- **REQ-1385** provisa:// semantic URIs on exported assets (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2294)
- **REQ-1386** Ops-domain management reports (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2295)
- **REQ-1389** Catalog upsert preserves human enrichment (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2296)
- **REQ-1414** Tag assignments as a queryable SQL relation (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2297)
- **REQ-1498** Environment operations on the CLI (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2298)
- **REQ-1501** Environments: Hasura structure, LaunchDarkly state (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2299)
- **REQ-1508** Environment vs lane vocabulary (behaviour present, no test dates it; now in-progress)
- **REQ-1632** Boot-time replicas are schema-only and idempotent (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2300)
- **REQ-1639** Data product Related Tables section (tests present at v0.1.0-alpha.512; now complete)
- **REQ-1645** Horizon listing description from product page (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2301)
- **REQ-1760** Typed authorization on every engine execution (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2302)
- **REQ-1783** Kaggle two-step source form (tests present at v0.1.0-alpha.512; now complete)
- **REQ-1785** HTTP crawl scope settings (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2303)
- **REQ-1786** Table discovery on every source creation (behaviour present, no test dates it; now in-progress; remainder REQ-2304)
- **REQ-1787** Skip Kaggle refresh when unchanged (behaviour present, no test dates it; now in-progress)
- **REQ-1788** Files source Calcite refreshInterval passthrough (behaviour present, no test dates it; now in-progress)
- **REQ-1793** Double opt-in web license registration (behaviour present, no test dates it; now in-progress)
- **REQ-1866** Cache semantic rewrite on raw-SQL surfaces (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2305)
- **REQ-1914** Model/governance changes reach every worker (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2307)
- **REQ-1923** Branded sources over generic API sources (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2309)
- **REQ-1928** Command calls take bind params and column refs (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2310)
- **REQ-1932** Platform state store (tests present at v0.1.0-alpha.512; now complete)
- **REQ-1953** Data product shows its tables' profiles (behaviour present, no test dates it; now in-progress)
- **REQ-1957** Exclusions on spec-built sources (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2318)
- **REQ-1959** Public items served to all domains (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2319)
- **REQ-1965** Nested columns as JSON; typed arrays in the IR (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2320)
- **REQ-1975** Non-modal working surfaces and delete confirmation (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2321)
- **REQ-1979** Deprecated tag on every model object (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2322)
- **REQ-1992** Per-org file folder with vault-keyed encryption (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2323)
- **REQ-2005** Customer sizing guide and sizing tool docs (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2006** Live demos page and no-signup demo sandboxes (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2012** Polly spend metering and sandbox caps (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2324)
- **REQ-2016** Sonnets named demo and demo setup.yaml/extras (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2017** Elasticsearch native search tables (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2030** Reviews named demo (MongoDB, Elasticsearch, Neo4j) (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2032** Files named demo (instant data lake) (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2034** Customer 360 demo with hub and entity resolution (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2325)
- **REQ-2036** Vulnerabilities named demo (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2326)
- **REQ-2038** Pharma vertical demo (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2040** Audit firm vertical demo (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2050** timestamptz instant type end to end (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2051** Data quality medallion demo (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2056** Insider review demo at an investment bank (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2058** Composite (all-of) grant entries (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2069** Jira Cloud source through a Calcite pgwire adapter (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2327)
- **REQ-2071** Explore Python and Notebook tabs on self-hosted Pyodide (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2092** Benchmark governance knobs and local pass (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2098** Remote hosts for Node/Python code sources (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2328)
- **REQ-2107** Banking end-of-day reporting demo (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2118** Data product listings in internal catalogs (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2329)
- **REQ-2119** Real-service e2e test for every metadata export target (tests present at v0.1.0-alpha.512; now in-progress; remainder REQ-2330)
- **REQ-2136** Ontology graph: Open as query and graph export (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2332)
- **REQ-2137** SPARQL Update lowered to governed writes (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2333)
- **REQ-2141** Term roles: Class, Property, Concept; table grounding (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2334)
- **REQ-2143** Ontology View reads its graph on the server; budget, search syntax (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2147** Profiler classification kinds (SSN, IBAN, cities...) (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2335)
- **REQ-2150** Term read view neighbourhood graph (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2158** Data contracts per output port (ODPS/ODCS) (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2336)
- **REQ-2168** APOC calls and command bindings (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2171** IR subtypes (inet, money, ranges...) and per-dialect function rules (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2337)
- **REQ-2172** Demo: Give an agent your data (agentdata) (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2174** 3D/2D graph stage: best view, layouts, lanes, one ontology graph (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2338)
- **REQ-2179** Find terms in a document (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2339)
- **REQ-2184** Deploy a single-source view to its source (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2185** Reference library with graph retrieval (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2341)
- **REQ-2188** Row rules with R/U/D rights; product ownership via rules (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2342)
- **REQ-2190** pgwire catalog answered from the meta domain (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2196** Teradata, Db2, Vertica, Netezza, Informix, Sybase, Spanner source types (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2343)
- **REQ-2203** PostgreSQL drivers' own test suites against pgwire (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2205** Dev container and three-step Codespace demo (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2208** Policy citations and the tag builder (tags explain why) (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2344)
- **REQ-2214** SQL names are case-insensitive (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2345)
- **REQ-2215** Resolve a role to its members across membership sources (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2346)
- **REQ-2217** Access built in branches; production access requests (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2347)
- **REQ-2221** Organization code connectors built from git (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2349)
- **REQ-2222** Config-driven CLI: validate, diff, apply, export, query, explain (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2228** dbt source type (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2351)
- **REQ-2231** provisa.dev home page structure (behaviour present, no test dates it; now complete)
- **REQ-2233** Works-with pages on the site (behaviour present, no test dates it; now complete)
- **REQ-2236** Polyfill grid (PostgreSQL behaviour on other engines) (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2352)
- **REQ-2237** Import external ontologies (OWL/RDF, Ossie) (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2353)
- **REQ-2240** Select and bulk delete in every list (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2244** UI wording pairs (Show/Hide, Expand/Collapse, Enlarge, Stop, Derived) (tests present at v0.1.0-alpha.512; now complete)
- **REQ-2245** Assistant tests a column against a cited section (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2354)
- **REQ-2247** Syncing page: catalog differences with Accept and standing choices (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2355)
- **REQ-2248** Retire, never hard-delete, model objects (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2356)
- **REQ-2253** AI features work only within selected domains (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2357)
- **REQ-2263** OpenLineage as a way to add sources and tables (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2358)
- **REQ-2265** Governance catalogs as live branded sources (tests present at v0.1.0-alpha.512; now complete; remainder REQ-2359)

## Planning basis

Capacity: about 30 missing product rules per release.

Capacity is planned in missing product rules, not requirements: about 30 per 2-day release. Basis: 53 requirements reached complete in the 14 days to 2026-11-27, at about 3.9 delivered rules each (the verification's mean), so about 15 rules a day. It is recalibrated after each release from the product rules actually delivered. Rules are counted from the 2026-11-27 rule-by-rule verification (req-verify-1127 batches 1-7) against origin/main 9a5d7e81ed; REQ-2196 counts 7 (seven source types) and REQ-2018 counts 10 (ten native-query sources). Coverage-only gaps are not features and are not planned. 513 and 514 list what their code ships: a requirement belongs to the first release whose branch (tag v0.1.0-alpha.512, origin/release/v0.1.0-alpha.513, origin/main for 514) holds every test named for its delivered rules, plus the requirements v1 recorded as completed in that window; requirements whose delivered tests were already at 512 are in the appendix. From 515: the 78 split requirements (REQ-2283..REQ-2360) and the not-started requirements, ordered in-progress first, then MUST, SHOULD, MAY, then remaining work of shipped features and correctness before new surface (demos last within a priority), dependencies first. 138 requirements, 286 product rules.
