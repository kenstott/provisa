# Roadmap

This roadmap shows where Provisa is headed, one release at a time; a release ships every two days. Groupings and timing reflect current plans and may shift as priorities develop. Items listed here are things a user will see—internal engineering work is not shown.

## v0.1.0-alpha.513 (2026-11-27)

### Querying & protocols

- **BI-tool catalog query replay.** DBeaver, Metabase, Grafana and psql browse Provisa schemas the same way they browse PostgreSQL, tested against captured real tool traffic.
- **Ephemeral session objects in every language.** Users create temporary nodes, rows and graphs in a session without writing to sources, for what-if analysis.

### Sources

- **Parameterized tables.** API and native-query tables take typed parameters, so a caller can read an endpoint like /orders/{id} as a table without custom code.
- **VS Code extension for code sources.** Developers see which functions Provisa will serve, with inferred columns, and test edits live from their editor before registering anything.
- **Spark reads governed Provisa tables.** Data engineers read a governed table as a Spark DataFrame, with row rules and masks applied before data leaves Provisa.
- **OWL reasoning on SPARQL sources.** Inferred class memberships from an ontology become queryable, so rule-derived categories need no hand-maintained tables.

### Governance & security

- **Anonymous access to published data.** An org can publish data to the open web from its own subdomain with no login, served only through a read-only public role.
- **Per-reader masks.** One column can show cleartext to some roles and different masks to others, in a visible precedence order an auditor can check.
- **PII tag by meaning, including future columns.** Tagging a business term as PII covers every column with that meaning, including columns added later, so new data is not left unprotected.
- **Per-person entitlements on coarse roles.** The identity provider grants a person specific tables and columns as attributes, so roles stay few instead of one role per person.

### Identity & organizations

- **Firebase sign-in creates the user profile.** Users who sign in with Google or email links get an account on first login without a separate password to manage.

### Modeling & semantic layer

- **One naming authority for client-facing names.** Every client sees the same governed names on SQL, GraphQL and Cypher, and physical column names never leak to users.

### UI & modeling

- **Context navigation group.** Glossary, ontology, metrics, tags, classification and documents sit under one menu, so stewards find the context layer in one place.

### Install & ops

- **One-script Linux host install with Trino.** An operator installs Provisa with a Trino engine on an existing Linux VM or server with one command, without Kubernetes.

### Engines & performance

- **Freshness rules for per-statement tables.** Native-query and parameterized tables have predictable caching, so users know when they see live data and when a kept copy.

### Environments & change control

- **The model store.** Sources, tables, roles, rules and products are held in one versioned store per org, the base for environments, sync and git projection.

### Catalog & data products

- **Catalogs admin panel (publish and read).** Each catalog connection is set up once and used both to publish and to read metadata.

### Modeling & naming

- **Names across the whole data estate.** Every admitted object, export and grounded column term gets a checked, clash-free name, and only the worker that changed the model rewrites the names, so names stay consistent as the model grows.

## v0.1.0-alpha.514 (2026-11-29)

### Querying & protocols

- **Rust pgwire row encoder.** SQL clients over the PostgreSQL protocol get large results many times faster (measured 14x text, 40x binary) with every PostgreSQL type supported.
- **Exact numeric precision and scale.** Money and decimal columns keep their exact digits on every engine and protocol, so financial figures never round differently by client.

### UI & modeling

- **Relate tables right after registering them.** Source setup ends with proposed relationships for the new tables, so a first query can join them minutes after connecting.
- **One 2D graph component.** The ERD, lane diagram and ontology graph look and behave alike, so users read every diagram the same way.

### AI assistance

- **Polly operates the UI visibly.** The assistant fills forms and presses buttons while the user watches, so new users learn the product by seeing each step done.

### Install & ops

- **Docker image builds its own UI.** Every published image carries the UI built from the same commit, so the console always matches the server.

### Sources

- **Wrapper-catalog services as brands.** Stripe, OpenMetadata and DataHub connect as brands over the OpenAPI source, with no API spec to find.
- **Spark connector in Sources.** Teams connect a Spark cluster as a source from the UI with the cluster as structured fields, without writing glue code.
- **Discovered code-source tables and live API sources.** A code source can generate its own tables and relationships at discovery, and OpenAPI and GraphQL sources run as built-in code sources.

### Catalog & data products

- **Data product access requests.** A user who can see a product but not read it asks its owner in one step, and the grant is recorded instead of handled by email.

### Governance & security

- **Column rules by tag or classification.** One rule hides or masks every column carrying a tag such as PII, wherever it appears, instead of maintaining rules column by column.
- **Template fakes for JSON columns.** Masked JSON columns return realistic fake objects with the right shape, so test and demo environments work without real personal data.

## v0.1.0-alpha.515 (2026-12-01)

### Identity & organizations

- **Email approvers on new requests.** Approvers get an email with approve and deny links when a creation request is raised, so requests do not sit unseen.
- **Generated break-glass password with forced rotation.** The emergency admin account cannot be used from a leaked config file or a reused password.
- **Last-platform-admin guard and recovery.** An installation can never lock itself out of platform administration.
- **Audit entry for org deletion.** Auditors can see who deleted an org and when, after its data is gone.

### Install & ops

- **High availability as published recovery time.** Every tier gets a stated recovery time in minutes, so buyers can plan around it without paying for an HA upgrade.
- **Every protocol on secondary nodes.** Clients on pgwire, Bolt, gRPC or MCP are load-balanced across every node of a cluster.
- **cert-manager certificates in Helm.** Kubernetes operators get certificates issued and renewed automatically instead of managing TLS Secrets by hand.

### Engines & performance

- **Tier-keyed Trino resource groups.** Higher tiers get more concurrency and memory on shared Trino, so paying tenants are not throttled like free ones.
- **Schema-only boot replicas on every engine.** Restarts are fast and never re-copy source data.
- **Edit conflicts and one reconcile path.** Two admins can no longer silently overwrite each other's model edits.

### Catalog & data products

- **URI reserved words and rename aliases.** Catalogs that stored an object's old address still find it after a rename.
- **OneLake catalog target; user descriptions kept.** Microsoft Fabric shops publish to OneLake, and no catalog path overwrites descriptions people wrote.

### Environments & change control

- **Bind environment sources from the CLI.** Environment setup is scriptable in CI without the UI.
- **Present-and-inert environment state.** Admins see at a glance which environments still need data bindings.
- **Model store owns every write.** Every model change is versioned and synced the same way, whatever was edited.

### Governance & security

- **Complete typed engine authorization.** No engine statement runs without a verified authorization, closing the last ungoverned paths.
- **Public metrics.** Company-wide metrics are defined once and visible to every domain.

### Modeling & naming

- **One naming authority end to end.** Names suggested before saving and names shown after saving come from one place, so a table or relationship is called the same thing in the UI, the API and every query language.

## v0.1.0-alpha.516 (2026-12-03)

### Querying & protocols

- **Portable JSON operations.** JSON queries give the same answer on every engine, and type drift is caught instead of returning wrong data.
- **Total landing of subtypes.** inet, money and range columns are always readable on every engine.
- **Polyfill provenance and hint rows.** Buyers see where each PostgreSQL equivalence was verified and which source-specific hints apply.
- **Shared named types on every surface.** Nested objects and enums appear as named, reused types in GraphQL and gRPC, so generated client code stays small and stable.

### Governance & security

- **Generated files always in the org folder.** One org's generated code and data never land where another org's process can read them.
- **Metric ownership by domain.** Each metric has an accountable owner, and only they or an org admin change it.

### Catalog & data products

- **Alation export target.** Alation customers get Provisa's model in their catalog like the other supported catalogs.
- **Contract type check and alias names.** Consumers are warned when a source breaks a data contract, and never see physical column names.
- **Role and column requests; suggestion outcomes.** Buyers ask for the role and columns they need in one form and see what was decided.

### Demos & website

- **Contact-sales form.** Enterprise prospects reach sales from the site without needing a mail client.

### Engines & performance

- **Separate interactive and batch workloads.** Materialized-view refreshes cannot slow interactive queries.
- **Opt out of view answers per request.** An analyst who needs the live base-table answer bypasses pre-aggregated views for one query without changing the model.

### Install & ops

- **Single-node Trino and queue-aware autoscaling.** A small Helm install runs Trino in one pod, and larger ones add workers when queries queue, not only when CPU is busy.

### Identity & organizations

- **Mandatory config download on org deletion.** No org is deleted without a restorable copy of its configuration.

## v0.1.0-alpha.517 (2026-12-05)

### Install & ops

- **Ops trend, usage and freshness reports.** Operators see trends, resource use and data freshness in built-in reports instead of ad-hoc log queries.
- **Prometheus metrics and cluster health.** Operators monitor Provisa in their existing Prometheus and OpenTelemetry stack.
- **Helm values schema.** A wrong Helm value fails at install with a clear message instead of a broken deployment.
- **WSL2 network bindings.** Windows users on WSL2 reach the UI from their Windows browser without manual port setup.

### Modeling & semantic layer

- **column_tags relation.** Analysts join tag assignments with data in SQL and see only what their role may see.

### Catalog & data products

- **Domain in Horizon listings.** Snowflake consumers see which business domain owns a listed product.

### Sources

- **Crawl-scope fields in the source form.** Admins limit what an HTTP crawl follows from the UI instead of editing config.
- **Wikidata endpoints, Atlassian brands, Wikidata grounding.** Atlassian Cloud or Data Center and custom Wikidata endpoints connect by brand, and terms can point to shared Wikidata meanings.
- **Scalar OpenAPI and gRPC operations as functions.** Single-value API calls are used in queries as functions, the same as GraphQL scalars.
- **Google Sheets OAuth, per-sheet tables, refresh.** Each tab of a spreadsheet becomes its own table under the user's own Google sign-in.
- **Protobuf topics and schema evolution.** Protobuf Kafka topics are queryable, and schema changes are caught before they break readers.
- **Exclusions in the source form.** Admins hide unwanted API endpoints from the UI without editing config.

### Engines & performance

- **Cached rewrite on Flight SQL, Cypher and Bolt.** Repeated queries over Flight SQL, Cypher and Bolt return faster.

### Querying & protocols

- **One Cypher and GraphQL catalog.** Graph tools see the same labels whether they connect over Bolt or HTTP.

### UI & modeling

- **Confirm every delete.** Users cannot remove saved settings with one accidental click.

### Governance & security

- **Deprecate metrics, roles and row rules.** Teams retire any model object with a warning period, not only tables and columns.

### AI assistance

- **Sandbox and daily assistant budgets.** Trial assistant use has a hard, visible spending cap.

## v0.1.0-alpha.518 (2026-12-07)

### Sources

- **Jira Data Center.** Self-hosted Jira shops query issues the same way Cloud users do.
- **Registered remote hosts for code sources.** Code sources run on the customer's own machines, close to their data.
- **Custom classification kinds.** Organizations detect their own identifiers, such as employee ids, the way built-in kinds are detected.
- **Teradata, Db2, Vertica, Netezza, Informix, Sybase, Spanner sources.** Enterprises with legacy warehouses query them directly beside modern sources.

### Catalog & data products

- **Catalog approvers and Alation data products.** Catalog users request access from the right approvers, and Alation shops see products too.
- **Catalog pick list, refresh and lock.** Stewards reuse descriptions already written in their catalog and keep one source of truth.

### UI & modeling

- **Ontology graph export; focus carried to queries.** Teams take the governed ontology into other RDF tools and keep their focus when moving to a query.
- **One-request ontology graph.** Large ontology graphs open in one request.

### Modeling & semantic layer

- **owl:sameAs and Purview term roles.** Ontology counts are per real-world entity, and Purview shows each term's role.
- **Workbook row citations; uncited passages.** Citations to spreadsheets stay correct as the sheet changes.

### AI assistance

- **Found terms to relationships.** Terms found in a document turn into model relationships in one step.

## v0.1.0-alpha.519 (2026-12-09)

### Governance & security

- **Policy tags with gates and why-not answers.** Every refusal explains which policy caused it, and policy tags enforce exports and promotion without custom code.
- **Entitlements tab, why-not trace, preview as.** Admins see and test exactly what a person can reach without signing in as them.

### Sources

- **Code connector catalog, setup fields and Flight.** Organizations build full connectors in code, with typed setup forms and fast Arrow reads.
- **Spark named demo.** Prospects see Spark data governed and joined with other sources end to end.
- **One serving path for OpenAPI and GraphQL.** API sources behave identically however they were added, with one code path to fix and secure.

### Modeling & semantic layer

- **dbt tests as data quality.** dbt test results show up as Provisa data quality, with no second test framework.
- **Ontology releases with reviewed differences.** Teams adopt new versions of standard ontologies like FIBO deliberately, seeing what changed.
- **Sync schedules, webhooks and full take-from-catalog.** Catalog and Provisa stay in step on a schedule, with differences decided by people or the customer's own workflow.
- **Restore on re-register; retired-link findings.** Undoing a removal keeps history and links, and broken references are found, not discovered by users.

## v0.1.0-alpha.520 (2026-12-11)

### Sources

- **Identical remote-schema handling for all adapters.** GraphQL, gRPC and OpenAPI sources classify and register operations the same way, so behavior is predictable across source types.
- **HTML lists read as tables.** Bulleted and numbered lists on web pages become tables, widening what a file source can read.
- **Visible discovery failures.** Admins see why no tables were discovered instead of an empty list.
- **Shared pgwire-calcite runtime.** The Calcite connector runtime loads once and is shared by every pgwire server, cutting memory and start time per source.
- **pgwire-calcite protocol parity.** Sources reached through Calcite report column sizes and types like Provisa's own server, so SQL tools display them correctly.
- **Calcite dialect fixes at the source.** SQL sent to JDBC sources is fixed in the Calcite dialect, so every source on that dialect benefits.
- **Production Db2, Trino, Vertica and Firebolt dialects.** These four databases get correct pushed-down SQL, so queries against them return right answers.

### UI & modeling

- **Org branding on portal surfaces.** An org publishing a data portal shows its own brand on the glossary and Explore pages it exposes.

### AI assistance

- **One endpoint for valid join paths.** Agents and the join guard get the same list of valid paths between two tables, so AI-generated joins follow approved relationships.
- **Domain-scoped generated descriptions.** AI-written descriptions never read or change tables outside the domains a steward is working in.
- **Citations agents can hand over intact.** Agent answers carry a citation of the governed path and its trust, so a person can verify where an answer came from.

### Demos & website

- **Customer 360 demo: match quality and a second question.** Prospects see entity-resolution quality measured, not asserted.
- **OSV data in the vulnerabilities demo.** The demo answers exposure questions with real public vulnerability data.

### Governance & security

- **Rate and cost caps on public endpoints.** Published anonymous endpoints have the tightest limits, so a public data portal cannot run up engine costs or be abused.

### Environments & change control

- **One consequences gate on every model save.** Every create, update or delete from UI, API, config load or agent is checked the same way before it reaches the model.

### Modeling & semantic layer

- **Base and derived relations without a sentinel source.** Views and metrics show their real lineage instead of a placeholder source, so provenance reads correctly.

### Catalog & data products

- **Buyer view of a data product.** A user who cannot yet read a product sees what it offers and how to request it, which turns the catalog into a self-serve storefront.

## v0.1.0-alpha.521 (2026-12-13)

### Sources

- **JDBC packs for hard-to-reach sources.** SQL Server, Oracle, ClickHouse, Redshift, Exasol and others are reached through a dedicated JDBC pack where the engines' own connectors fall short.
- **Spark Connect catalog as a source.** Provisa reads a Spark cluster's databases and tables through Spark Connect, so lakehouse tables join with other sources in one query.

### Identity & organizations

- **Approve or deny org join requests.** Org admins approve people who ask to join, so teams onboard colleagues without a support ticket.
- **Role members from SCIM, LDAP, Graph and Google.** Reviews and reports see who actually holds a role, whatever system manages membership.

### Environments & change control

- **Environments gated by entitlement.** Environments are enabled by license feature rather than product name, so edition upgrades turn them on without a reinstall.

### Install & ops

- **Redacted log export for support.** Operators send support a log file with secrets and personal data removed.

### Engines & performance

- **Edge index via relationship materialize.** Graph traversals over a materialized relationship read a prebuilt edge index, speeding large-graph queries.
- **Volatile tables never served from copies.** Tables marked volatile always read live, so time-critical data is never stale.

### Governance & security

- **Periodic access reviews with attestation.** Roles, rules and masks carry review schedules with recorded sign-off, which auditors ask for.
- **Access requests from the catalog.** Users ask for a table or column they found, and the request becomes a reviewed grant.
- **Effective-access explainer.** Admins see exactly what a set of roles and session values can read and why, answering access questions in seconds.

## v0.1.0-alpha.522 (2026-12-15)

### Querying & protocols

- **SQL dialect per connection.** A client written for SQL Server or another database connects without rewriting its SQL.
- **Text search as an API query pattern.** Tables offer stemmed text search from their API tab with no search engine to set up.
- **Tags in the Cypher graph.** Tags appear as nodes linked to the tables and columns they mark, so graph queries can traverse governance.
- **Bind parameters in command calls.** Commands are called from SQL and Cypher with parameters and column values, so queries stay parameterised.
- **Case-insensitive SQL names.** SQL users and BI tools find tables regardless of letter case.

### Engines & performance

- **Engine impersonation (opt-in).** Engines can run queries as the requesting user, so engine-side audit logs name the real person.

### Governance & security

- **Published data over pgwire and Bolt.** Published open data is readable from SQL clients and graph tools without a login, not only over HTTP.

### Environments & change control

- **Production access requests built on branches.** Production users get access through a reviewed, time-limited request instead of new roles.

### AI assistance

- **Assistant tests columns against policy sections.** Stewards get a fast, governed first check of policy compliance without the assistant making legal claims.

### Modeling & semantic layer

- **OpenLineage import of sources and tables.** Teams that already emit OpenLineage bootstrap their Provisa model from it instead of registering tables by hand.

### Catalog & data products

- **Collibra, Alation, Atlan, OneLake and Dataplex as sources.** Governance teams query catalog metadata beside live data to find where catalog and enforcement disagree.

### Sources

- **Engine discover() introspection fix.** Source introspection through each engine returns complete table lists.
- **Copy or move a table to another source.** A table is copied under governance to another source, for migrations or offloading.

## v0.1.0-alpha.523 (2026-12-17)

### Catalog & data products

- **dbt and Cube model export.** The model exports to dbt semantic and Cube formats, so existing BI layers reuse it.

### Modeling & semantic layer

- **Refer creation requests to another domain.** Stewards hand a request to the right domain with a note instead of rejecting it.
- **Terms linked to commands.** Process terms such as "close of day" point to the commands that carry them out.

### Sources

- **Prometheus tables from PromQL.** Metrics from a PromQL query join business data in SQL, so operations and business numbers sit in one report.
- **gRPC tables from request templates.** A unary gRPC call becomes a parameterized table.
- **Full Qdrant Query API tables.** Hybrid dense and sparse vector search with fusion is available as a table.
- **Generic OData source.** SAP and Microsoft OData services connect from a service URL with no handwritten spec.

### Platform principles

- **Drivers for sources, engines and secrets.** Every built-in implementation is a driver, so partners extend Provisa the same way.

## v0.1.0-alpha.524 (2026-12-19)

### UI & modeling

- **My Reports (saved queries).** Users save a named query and return to it, without creating a shared view.

### Sources

- **One descriptor per source type.** Each source type's facts live in one descriptor, so adding a source type touches one place.
- **Spark time travel.** Delta, Iceberg and Hudi tables on Spark answer as-of queries, so audits and restatements read past versions directly.
- **Spark change data.** Inserts, updates and deletes on Delta and Iceberg tables are served as change data for downstream sync.
- **Spark procedures as commands.** Maintenance such as compaction and snapshot expiry runs as governed commands from Provisa.
- **OneDrive and Google Drive file crawling.** Hosted and server installs read files straight from OneDrive and Google Drive, with no desktop sync client.
- **Twelve SaaS brands over OpenAPI.** Teams connect common SaaS tools by picking a brand and signing in, with no API spec to find.
- **Custom Great Expectations checks as commands.** Teams extend quality checks with their own Python functions through the same governed command surface.
- **Marquez source.** Lineage held in Marquez is queryable through its public API.
- **Databricks catalog functions and models.** Unity Catalog functions and MLflow models are offered as governed functions.

### Modeling & semantic layer

- **Entity resolution from tags.** Records that refer to the same customer or product across sources are linked in one shared map, so cross-source questions return one answer per entity.
- **Entities found in text.** Notes and documents link to the products and tables they mention, so unstructured text joins structured data.

### Demos & website

- **Beneficial ownership demo.** Shows KYC ownership chains with OWL transitive control reasoning.
- **Internal data marketplace demo.** Shows products offered to an organisation's own people with existing primitives.
- **Masked-column request demo.** Shows an analyst seeing why a column is masked, requesting it, and getting a governed grant.
- **dbt project demo.** Shows a dbt project governed by role with lineage, unchanged.
- **Wildfire exposure demo.** Shows insured value inside this week's wildfire perimeter and which policyholders to call first.

### Environments & change control

- **Replicate a whole environment.** An org copies an environment with bindings, secrets and users when it wants the control plane as it stands.
- **Export an environment to desktop.** A developer works against a non-prod environment's model on their own machine.

### Querying & protocols

- **SPARQL Update to curation and commands.** RDF tools curate the ontology and run commands under the same rights as the UI.

### Catalog & data products

- **Horizon _provisa exposure views.** Snowflake users see governed views organized by domain in a clean database.
