// Copyright (c) 2026 Kenneth Stott
// Canary: d4048df5-f4dd-41f9-9f43-7993cd6a493e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * Declarative feature-tour definition. Consumed by {@link useTour}, which
 * drives react-router navigation and driver.js highlighting from this list.
 *
 * A step is pure data — no DOM access here. The runner interprets the optional
 * `clickBefore` / `clickAfterNext` selectors to open and then cancel forms so
 * the demo walks the *real* UI (add a source, then back out) without mutating
 * anything.
 *
 * Order is a narrative: a SPINE (register a source → expose tables → query it
 * eight ways) that IS the five-minute core, a "that's the core" divider where a
 * user can bail, then the GROW chapters (graph, governance, pipeline, operate)
 * as optional depth, and a closing call to action.
 */
export interface TourStep {
  /** Route to navigate to before the step is shown. Omit to stay put. */
  route?: string;
  /**
   * Named side-effect run *before* navigating — resolved by the runner's prep
   * registry. Used to seed demo state (e.g. a canned NL result) so the tour can
   * show a feature that would otherwise need external credentials.
   */
  prep?: string;
  /** CSS selector of the element to highlight. */
  element: string;
  /**
   * When the highlighted element is a `<select>`, expand it into an inline list
   * box (via the `size` attribute) so its options and `<optgroup>` headers are
   * visible — a native dropdown can't be opened programmatically. The form is
   * torn down on leaving the step, so no restore is needed.
   */
  expandSelect?: boolean;
  /**
   * Carry the NL-generated query for this branch into the explorer on navigate
   * and auto-run it — mirrors the NL page's "Open in X" buttons. The runner maps
   * the branch to the explorer's state key. Only meaningful with a `route`.
   */
  openBranch?: "sql" | "graphql" | "cypher" | "grpc" | "jsonapi" | "openapi";
  title: string;
  description: string;
  /**
   * Selector clicked (and awaited) *before* highlighting — used to reveal the
   * target, e.g. opening an add-form or the ERD modal.
   */
  clickBefore?: string;
  /**
   * Selector clicked when the user advances *past* this step via Next — used to
   * undo `clickBefore`, e.g. cancelling the form or closing a modal.
   */
  clickAfterNext?: string;
}

const SOURCES_ADD = '[data-tour="sources-add"]';
const TABLES_ADD = '[data-tour="tables-add"]';
const RELS_ADD = '[data-tour="rels-add"]';

// A complex-enough query over the same proven demo tables the SQL surface runs (`default.users`
// JOIN `default.inquiries`), plus an UPPER() transform so the DAG shows a real source → transform →
// result trace. LineagePage auto-builds the statement graph from `?sql=` on mount (it only analyzes,
// never runs), so deep-linking it avoids any click-timing race.
const LINEAGE_DEMO_SQL =
  'SELECT users.name, UPPER(users.name) AS name_upper, COUNT(inquiries.id) AS inquiry_count ' +
  'FROM "default"."users" JOIN "default"."inquiries" ON users.id = inquiries.user_id ' +
  "GROUP BY users.name";

export const TOUR_STEPS: TourStep[] = [
  // ─── SPINE: the five-minute core (register a source → expose tables → query it eight ways) ───
  {
    route: "/sources",
    element: ".navbar-tour-btn",
    title: "Welcome, let's take the tour",
    description:
      "🧭 This compass reopens the tour anytime. Leave whenever — click the app or press <kbd>Esc</kbd>." +
      "<p><strong>Provisa</strong> is a governed query compiler for federated data that stays out of the data path.</p>" +
      "<p>🎯 <strong>The whole core is one short exercise:</strong> connect some databases, then query them <strong>seven ways</strong> — or skip the syntax and just ask in plain English. About five minutes, <strong>right from your desktop</strong>: no servers, no infrastructure to stand up (that comes later, when you're ready).</p>" +
      "<div class='tour-tags'>" +
      "<span>Natural Language</span><span>SQL</span><span>GraphQL</span><span>Cypher</span><span>gRPC</span><span>JSON:API</span><span>REST</span><span>MCP</span>" +
      "<span>Postgres · pgwire</span><span>Neo4j · Bolt</span>" +
      "<span>JDBC · Java/JVM</span><span>Arrow Flight SQL · ADBC</span><span>DuckDB · airport</span>" +
      "</div>" +
      "<p>Everything <em>after</em> that — more sources, federation, files, relationships, views, scheduled pipelines — is how you <strong>grow</strong> with the platform, not what you need on day one. We'll flag where the core ends.</p>" +
      "<p>Ready? Let's connect a source. →</p>",
  },
  {
    route: "/sources",
    element: '[data-tour="nav-sources"]',
    title: "Start with Sources",
    description:
      "Everything you want to federate is registered here:" +
      "<div class='tour-tags'>" +
      "<span>🗄️ Databases</span><span>🏢 Warehouses</span><span>🔌 APIs</span><span>📄 Files</span><span>☁️ SaaS</span>" +
      "</div>" +
      "<p>Provisa unifies them all into one queryable <strong>graph</strong>.</p>",
  },
  {
    element: SOURCES_ADD,
    title: "Register a source",
    description:
      "➕ New connections start with this button — PostgreSQL, Snowflake, MongoDB, a REST API, a CSV, and 30+ more.",
  },
  {
    element: '[data-tour="sources-type"]',
    expandSelect: true,
    title: "35+ source types",
    description:
      "One connector list spans every category:" +
      "<ul>" +
      "<li><strong>Databases</strong> — PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, Greenplum</li>" +
      "<li><strong>Cloud warehouses</strong> — Snowflake, BigQuery, Databricks, Redshift, Microsoft Fabric, Azure Synapse</li>" +
      "<li><strong>Analytics / OLAP</strong> — ClickHouse, Druid, Pinot</li>" +
      "<li><strong>Lakes &amp; files</strong> — Iceberg, Delta Lake, Hive, Parquet, CSV, JSON</li>" +
      "<li><strong>NoSQL &amp; graph</strong> — MongoDB, Cassandra, Redis, Neo4j</li>" +
      "<li><strong>APIs</strong> — REST, GraphQL, gRPC</li>" +
      "<li><strong>Streaming</strong> — Kafka</li>" +
      "<li><strong>Enterprise SaaS</strong> — SharePoint, Splunk · plus government data subscriptions</li>" +
      "</ul>" +
      "Pick one, fill the connection, Save.",
    clickBefore: SOURCES_ADD,
    clickAfterNext: SOURCES_ADD,
  },
  {
    route: "/tables",
    element: '[data-tour="nav-tables"]',
    title: "Everything becomes a table",
    description:
      "Whatever the source — a Mongo collection, a Kafka topic, a REST endpoint, a graph — Provisa decomposes it into a 2D <strong>table</strong>: one uniform shape to query across every source." +
      "<p>📁 Group tables into <strong>domains</strong>, each owned by a data steward who guards the model's quality as it evolves.</p>",
  },
  {
    // Own the route so Back from the first surface (/sql) returns here instead of hanging on
    // clickBefore (the tables-add button only exists on /tables).
    route: "/tables",
    element: '[data-tour="tables-form"]',
    title: "Pick columns & policy",
    description:
      "Choose a source, schema, and table, then pick columns — each with its own policy:" +
      "<ul>" +
      "<li>🎭 <strong>Masking</strong> — redact values per role</li>" +
      "<li>👁️ <strong>Visibility</strong> — show or hide the column</li>" +
      "<li>🏷️ <strong>Aliases</strong> — rename for the model</li>" +
      "</ul>" +
      "<p>That's the whole setup — a source and a few tables. 🚀 Now let's query it. →</p>",
    clickBefore: TABLES_ADD,
    clickAfterNext: TABLES_ADD,
  },
  {
    route: "/sql",
    openBranch: "sql",
    element: '.subnav a[href="/sql"]',
    title: "SQL (1 of 7 surfaces)",
    description:
      "SQL lives in the SQL explorer. Standard SQL federated across every source — join Postgres to Mongo to a CSV in one statement. And the model's own metadata and every activity trace are themselves queryable tables here — join your audit log or lineage straight to live data." +
      "<p>🔌 Not just this explorer — the same governed SQL runs through multiple wire protocols; your tool picks the one it already speaks:</p>" +
      "<ul>" +
      "<li><strong>pgwire</strong> — every Postgres-compatible client — psql, DBeaver, Tableau, Power BI — connects out of the box.</li>" +
      "<li><strong>JDBC</strong> — drops into any Java/JVM app, ETL pipeline, or JDBC-capable BI tool.</li>" +
      "<li><strong>Arrow Flight SQL</strong> — columnar streaming for ADBC clients — no row re-encoding, built for large result sets.</li>" +
      "<li><strong>DuckDB · airport</strong> — ATTACH Provisa from DuckDB and query the governed federation in place.</li>" +
      "</ul>",
  },
  {
    route: "/query",
    openBranch: "graphql",
    element: '.subnav a[href="/query"]',
    title: "GraphQL (2 of 7 surfaces)",
    description:
      "◈ The same query, typed as GraphQL:" +
      "<p><code>{ inquiries { groupBy { user }, count } }</code></p>" +
      "<p>Grouping and aggregates over your relationships — 🎯 one endpoint, 📖 self-documenting schema.</p>" +
      "<p>🔌 Hit it from <strong>any GraphQL client</strong> — Apollo, Relay, urql — and, with federation enabled, drop Provisa into an <strong>Apollo supergraph as a subgraph</strong>, so your federated model composes alongside your existing graphs.</p>",
  },
  {
    route: "/graph",
    openBranch: "cypher",
    element: '.subnav a[href="/graph"]',
    title: "Cypher (3 of 7 surfaces)",
    description:
      "🕸️ Traverse the federated model as a graph with Cypher:" +
      "<p><code>MATCH (u:Users)-[:SUBMITTED]->(i:Inquiries)</code></p>" +
      "<p>↔️ That traversal runs <strong>across sources</strong> — no single graph database required.</p>" +
      "<p>🔌 And any <strong>Bolt</strong> client — Neo4j Browser, Bloom — runs that same traversal over the wire, no code change.</p>",
  },
  {
    route: "/grpc",
    openBranch: "grpc",
    element: '.subnav a[href="/grpc"]',
    title: "gRPC (4 of 7 surfaces)",
    description:
      "⚙️ Every registered entity is also a gRPC service:" +
      "<p><code>rpc QueryInquiries(...) returns (...)</code></p>" +
      "<p>Call it from any gRPC client — 📐 strongly typed, 🚀 high-throughput.</p>",
  },
  {
    route: "/jsonapi",
    openBranch: "jsonapi",
    element: '.subnav a[href="/jsonapi"]',
    title: "JSON:API (5 of 7 surfaces)",
    description:
      "🧩 A spec-compliant JSON:API surface:" +
      "<p><code>GET /data/jsonapi/pet-store/inquiries?page[size]=20</code></p>" +
      "<p>📄 Paging and 🔍 filtering out of the box.</p>",
  },
  {
    route: "/openapi",
    openBranch: "openapi",
    element: '.subnav a[href="/openapi"]',
    title: "OpenAPI / REST (6 of 7 surfaces)",
    description:
      "🌐 And a plain REST endpoint, described by OpenAPI:" +
      "<p><code>GET /data/rest/pet-store/inquiries</code></p>" +
      "<p>📄 Fully described, so any REST client or code generator can consume it.</p>",
  },
  {
    route: "/explore",
    prep: "seedMcp",
    element: '.subnav a[href="/explore"]',
    title: "MCP (7 of 7 surfaces)",
    description:
      "🤖 The seventh surface lets an agent <strong>reason with your data</strong>. It drives the very same governed tools — search the catalog, describe tables, run SQL, follow relationships — and <em>chains</em> them to answer questions no single query could, every call bound by <strong>your</strong> role's governance." +
      "<p>🔌 The same <strong>MCP server</strong> plugs into Claude Desktop, Claude Code, or any MCP client, so agents explore and reason over your model the governed way.</p>" +
      "<p>🔑 Uses your own LLM key; this exchange is a canned example.</p>" +
      "<p>One surface left — the one that writes all seven for you. →</p>",
  },
  {
    route: "/nl",
    prep: "seedNl",
    element: '[data-testid="nl-question-input"]',
    title: "Just ask — the surface that writes all seven",
    description:
      "💬 <em>\"show inquiry count by user\"</em>" +
      "<p>🎁 Here's the capstone: <strong>you don't have to learn any of those seven.</strong> Ask in plain English and Provisa compiles your question into every query language at once — each panel below is that one sentence as SQL, GraphQL, Cypher, gRPC, JSON:API, and REST.</p>" +
      "<p>Use the surface your tools already speak — or skip them all and just ask.</p>" +
      "<p>🔑 Live natural-language queries use your own LLM key; this one's a canned example.</p>" +
      "<p>✅ One model, <strong>eight surfaces</strong> — seven to query, and the one that writes them.</p>" +
      "<div class='tour-tags'>" +
      "<span>Natural Language</span><span>SQL</span><span>GraphQL</span><span>Cypher</span><span>gRPC</span><span>JSON:API</span><span>REST</span><span>MCP</span>" +
      "</div>",
  },
  // ─── DIVIDER: the core is done; everything past here is optional depth. A natural bail point. ───
  {
    element: ".navbar-tour-btn",
    title: "That's the five-minute core",
    description:
      "✅ You just saw it end to end: <strong>one source, queried seven ways — or just asked in plain English.</strong> That's the whole promise — and you could stop right here." +
      "<p>Press <kbd>Esc</kbd> anytime; 🧭 the compass up top replays this tour whenever you want.</p>" +
      "<p>👉 Everything ahead is <strong>how Provisa grows with you</strong> — optional depth, not required steps:</p>" +
      "<ul>" +
      "<li>🔗 Link tables across sources into one <strong>graph</strong></li>" +
      "<li>🔐 <strong>Govern</strong> it — roles, row-level security, column masking</li>" +
      "<li>🏗️ Build a <strong>delivery pipeline</strong> — views, a materialized warehouse, lineage</li>" +
      "<li>⚙️ <strong>Operate</strong> it — pick an engine, ship to production</li>" +
      "</ul>" +
      "<p>Keep going? →</p>",
  },
  // ─── GROW: optional depth — link the graph, govern it, build the pipeline, operate it. ───
  {
    route: "/relationships",
    element: '[data-tour="rels-add"]',
    title: "Connect the graph",
    description:
      "<svg class='tour-graph' viewBox='0 0 260 96' role='img' aria-label='Tables in two different sources linked into one graph'>" +
      "<line x1='42' y1='30' x2='118' y2='24' /><line x1='42' y1='30' x2='58' y2='72' />" +
      "<line x1='58' y1='72' x2='142' y2='74' />" +
      "<line class='xsrc' x1='118' y1='24' x2='210' y2='30' />" +
      "<line class='xsrc' x1='142' y1='74' x2='210' y2='30' /><line x1='210' y1='30' x2='222' y2='70' />" +
      "<circle cx='42' cy='30' r='9' /><circle cx='58' cy='72' r='9' /><circle cx='118' cy='24' r='9' />" +
      "<circle class='n2' cx='142' cy='74' r='9' /><circle class='n2' cx='210' cy='30' r='9' /><circle class='n2' cx='222' cy='70' r='9' />" +
      "</svg>" +
      "<p>Relationships link tables across sources — even across different databases — turning flat tables into a traversable <strong>graph</strong>.</p>" +
      "<p>🛡️ They can also be <strong>enforced</strong>: hand people freedom to explore, with sensible guardrails.</p>",
  },
  {
    element: '[data-tour="rels-form"]',
    title: "Define a relationship",
    description:
      "🔗 Map a source column to a target column and set <strong>cardinality</strong>:" +
      "<div class='tour-tags'>" +
      "<span>1 → 1</span><span>1 → ∞</span><span>∞ → ∞</span>" +
      "</div>" +
      "<p>✨ Or let Provisa <strong>infer</strong> relationships for you with AI.</p>",
    clickBefore: RELS_ADD,
    clickAfterNext: RELS_ADD,
  },
  {
    // Own the route so Back from RBAC (/security/roles) returns here instead of hanging on
    // clickBefore (the ERD trigger only exists on /relationships).
    route: "/relationships",
    element: '[data-tour="rels-erd-modal"]',
    title: "See the ERD",
    description:
      "The entity-relationship diagram renders your whole federated model — every registered table and the relationships between them.",
    clickBefore: '[data-tour="rels-erd"]',
    clickAfterNext: '[data-testid="erd-close"]',
  },
  {
    route: "/security/roles",
    element: '.subnav a[href="/security/roles"]',
    title: "Access control (RBAC)",
    description:
      "🔐 Registering a table sets per-column access, wired straight into roles:" +
      "<div class='tour-tags'>" +
      "<span>👁️ Read</span><span>✏️ Write</span>" +
      "</div>" +
      "<p>That's <strong>role-based access control</strong> — and the roles themselves are defined here on the Security page.</p>",
  },
  {
    route: "/security/rls",
    element: '.subnav a[href="/security/rls"]',
    title: "Row-level security",
    description:
      "🔎 Once roles exist, go finer — restrict which <strong>rows</strong> each role sees with attribute-based predicates:" +
      "<p><code>region = user.region AND status != 'archived'</code></p>" +
      "<p>📐 Data-driven and enforced per row — across every source and protocol.</p>",
  },
  {
    route: "/views",
    element: '.subnav a[href="/views"]',
    title: "Build your delivery pipeline",
    description:
      "🔧 Compose your whole delivery pipeline right here — no separate ETL tool:" +
      "<ul>" +
      "<li>👁️ <strong>Views</strong> — over any table, ephemeral or materialized, and views over views.</li>" +
      "<li>⚡ <strong>Commands</strong> — governed, typed operations (an HTTP service, a Python model, a stored procedure) that do double duty: a step inside your pipeline, <em>and</em> a first-class API that downstream apps and agents call to act on your data — invokable from every surface, fully governed and traced.</li>" +
      "<li>⭐ <strong>Entities &amp; facts</strong> — declare a dimension or fact once and Provisa generates the warehouse, methodology-neutral: 3NF (Inmon), star schema (Kimball), or Data Vault.</li>" +
      "</ul>",
  },
  {
    route: "/views",
    element: '.subnav a[href="/views"]',
    title: "Materialize into a live warehouse",
    description:
      "🏗️ Materialize a view and you get a full ETL engine, declared not scripted:" +
      "<ul>" +
      "<li>📅 <strong>Scheduled snapshots</strong> on real <strong>business calendars</strong> — recurrence rules, holidays, business-day gating, and a close grace period, so it fires on <em>your</em> reporting calendar, not just a clock.</li>" +
      "<li>🗄️ <strong>Store As…</strong> — <strong>replace</strong>, <strong>upsert</strong>, or keep history with <strong>snapshot</strong> / <strong>delta</strong>, plus incremental compute.</li>" +
      "<li>🕰️ <strong>Time travel</strong> — bitemporal history; query the model as of any point in time.</li>" +
      "<li>⚡ <strong>Near-real-time</strong> refresh with debounce, or freshness-driven republish as inputs change.</li>" +
      "</ul>",
  },
  {
    route: `/lineage?sql=${encodeURIComponent(LINEAGE_DEMO_SQL)}`,
    element: '[data-testid="lineage-dag"]',
    title: "Lineage, end to end",
    description:
      "🧬 <strong>Lineage isn't opaque.</strong> This is a real query, analyzed. Each result column is traced back through its transforms — follow <code>name_upper</code> — and joins to the exact <strong>source columns</strong> that feed it." +
      "<p>Commands are declaratively defined, so every step is captured; materialized views retain full lineage by definition. Nothing runs — the query is only analyzed.</p>" +
      "<p>→ Last stop: operating it in production.</p>",
  },
  {
    route: "/admin/overview",
    element: '[data-tour="nav-admin"]',
    title: "Operate it",
    description:
      "The Admin pages run the platform:" +
      "<ul>" +
      "<li>⚙️ Choose and configure your <strong>federation engine</strong> — pluggable: DuckDB (embedded, zero-config), Trino, PostgreSQL, ClickHouse, or a cloud warehouse as a first-class engine (Snowflake, BigQuery, Databricks, Microsoft Fabric, Azure Synapse). Swap it anytime — the model, the surfaces, and the governance above it never change.</li>" +
      "<li>🔗 Whichever engine you pick brings its own <strong>live external data</strong> reach — Parquet, Iceberg, Delta read in place, zero-copy, credentials auto-provisioned. Anything it can't link live lands as a governed replica.</li>" +
      "<li>🔑 Manage <strong>encryption keys</strong> and wire up <strong>auth providers</strong></li>" +
      "<li>📊 Full <strong>observability</strong> — redirect to any OpenTelemetry collector for enterprise-class trace management</li>" +
      "</ul>" +
      "<p>The same model travels from local dev to production, validated at every step — no rewrite:</p>" +
      "<ul>" +
      "<li><strong>Develop</strong> — embedded DuckDB, no Docker. Build &amp; validate locally.</li>" +
      "<li><strong>Validate at scale</strong> — Docker, spin up an engine + observability. Prove behavior against production engines.</li>" +
      "<li><strong>Promote</strong> — point at your own infrastructure, build through CI/CD, ship to production.</li>" +
      "</ul>",
  },
  // ─── CLOSE ───
  {
    route: "/sources",
    element: '[data-tour="sources-add"]',
    title: "That's the tour — now make it yours",
    description:
      "🎉 You're back where you started. Now make it yours:" +
      "<ul>" +
      "<li>1️⃣ Register one of your real <strong>sources</strong></li>" +
      "<li>2️⃣ Expose a few <strong>tables</strong>, draw a <strong>relationship</strong> or two</li>" +
      "<li>3️⃣ Query it from <strong>Explore</strong> in any language:</li>" +
      "</ul>" +
      "<div class='tour-tags'>" +
      "<span>SQL</span><span>GraphQL</span><span>Cypher</span><span>gRPC</span><span>JSON:API</span><span>REST</span>" +
      "</div>" +
      "<p>Point <kbd>pgwire</kbd> or <kbd>Bolt</kbd> tools at it — everything just works.</p>" +
      "<p>📚 The <strong>Docs</strong> tab has the full guides. 🧭 The compass up top replays this tour anytime. Enjoy Provisa.</p>",
  },
];
