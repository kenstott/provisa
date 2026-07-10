// Copyright (c) 2026 Kenneth Stott
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

export const TOUR_STEPS: TourStep[] = [
  {
    route: "/sources",
    element: ".navbar-tour-btn",
    title: "Welcome — let's take the tour",
    description:
      "This compass button starts the tour, and it's how you come back to it. Leave anytime — just click the app or press Esc — and pick up right here whenever you like. Quick preview of where we're headed: Provisa is a query-language compiler — one federated model served as GraphQL, SQL, Cypher, gRPC, JSON:API and REST, and spoken over wire protocols like Postgres (pgwire) and Neo4j (Bolt) so existing tools connect unchanged. It runs here on an embedded DuckDB; swap in Trino or ClickHouse and the same model scales to the largest enterprises. Ready? Let's go.",
  },
  {
    route: "/sources",
    element: '[data-tour="nav-sources"]',
    title: "Start with Sources",
    description:
      "Every database, warehouse, API, file, or SaaS you want to federate is registered here. Provisa unifies them into one queryable graph.",
  },
  {
    element: SOURCES_ADD,
    title: "Register a source",
    description:
      "New connections start with this button — PostgreSQL, Snowflake, MongoDB, a REST API, a CSV, and 30+ more.",
  },
  {
    element: '[data-tour="sources-type"]',
    expandSelect: true,
    title: "30+ source types",
    description:
      "One connector list spans every category: Subscriptions (gov data), RDBMS, Cloud DWs (Snowflake, BigQuery), Analytics/OLAP, Data Lakes, NoSQL, Graph, Files, REST/GraphQL/gRPC APIs, Streaming (Kafka), and enterprise SaaS like SharePoint & Splunk. Pick one, fill the connection, Save.",
    clickBefore: SOURCES_ADD,
    clickAfterNext: SOURCES_ADD,
  },
  {
    route: "/tables",
    element: '[data-tour="nav-tables"]',
    title: "Everything becomes a table",
    description:
      "Whatever the source — a Mongo collection, a Kafka topic, a REST endpoint, a graph — Provisa decomposes it into a 2D dataset it calls a table. One uniform shape to query across every source. Tables can be grouped into domains, each owned by a data steward who oversees the model's quality as it evolves.",
  },
  {
    element: '[data-tour="tables-form"]',
    title: "Pick columns & policy",
    description:
      "Choose a source, schema, and table, then select columns — with per-column role-based masking, visibility, and aliases.",
    clickBefore: TABLES_ADD,
    clickAfterNext: TABLES_ADD,
  },
  {
    route: "/relationships",
    element: '[data-tour="rels-add"]',
    title: "Connect the graph",
    description:
      "Relationships link tables across sources — even across different databases — turning flat tables into a traversable graph. They can also be enforced, so you can hand people the freedom to explore with sensible guardrails.",
  },
  {
    element: '[data-tour="rels-form"]',
    title: "Define a relationship",
    description:
      "Map a source column to a target column and set cardinality. Provisa can also infer these for you with AI.",
    clickBefore: RELS_ADD,
    clickAfterNext: RELS_ADD,
  },
  {
    element: ".modal--erd",
    title: "See the ERD",
    description:
      "The entity-relationship diagram renders your whole federated model — every registered table and the relationships between them.",
    clickBefore: '[data-tour="rels-erd"]',
    clickAfterNext: ".modal--erd .modal-close",
  },
  {
    route: "/security/roles",
    element: '.subnav a[href="/security/roles"]',
    title: "Access control (RBAC)",
    description:
      "Registering a table sets per-column read and write access — that's role-based access control, wired straight into roles. The roles themselves are defined here on the Security page.",
  },
  {
    route: "/security/rls",
    element: '.subnav a[href="/security/rls"]',
    title: "Row-level security",
    description:
      "Once roles exist, go finer: restrict which rows each role can see with attribute-based predicates — data-driven, enforced per row across every source and protocol.",
  },
  {
    route: "/views",
    element: '[data-tour="nav-model"]',
    title: "Build your delivery pipeline",
    description:
      "Define views over any of these tables — ephemeral or materialized — and views over views. What makes this a real pipeline: liveness guarantees live on the tables and views themselves, so every view republishes as its inputs' freshness changes. Compose your whole delivery pipeline from views and commands — no separate ETL tool. Now let's query it.",
  },
  {
    route: "/nl",
    prep: "seedNl",
    element: ".nl-panels",
    title: "Ask in plain English",
    description:
      "Natural-language queries need your own LLM key, so here's a canned example — \"show inquiry count by user.\" Provisa compiled that one question into all six ways to query the graph at once. Every panel below is the same request.",
  },
  {
    route: "/sql",
    openBranch: "sql",
    element: '.subnav a[href="/sql"]',
    title: "1 · SQL",
    description:
      "The NL SQL branch, live in the SQL explorer. Standard SQL federated across every source — join Postgres to Mongo to a CSV in one statement. And the model's own metadata and every activity trace are themselves queryable tables here — join your audit log or lineage straight to live data.",
  },
  {
    route: "/query",
    openBranch: "graphql",
    element: '.subnav a[href="/query"]',
    title: "2 · GraphQL",
    description:
      "The same question as a typed GraphQL query, with grouping and aggregates over your relationships — one endpoint, self-documenting schema.",
  },
  {
    route: "/graph",
    openBranch: "cypher",
    element: '.subnav a[href="/graph"]',
    title: "3 · Cypher",
    description:
      "Traverse the federated model as a graph with Cypher — `MATCH (u:Users)-[:SUBMITTED]->(i:Inquiries)` runs across sources.",
  },
  {
    route: "/grpc",
    openBranch: "grpc",
    element: '.subnav a[href="/grpc"]',
    title: "4 · gRPC",
    description:
      "Every registered entity is also a gRPC service. Call `QueryInquiries` from any gRPC client — strongly typed, high-throughput.",
  },
  {
    route: "/jsonapi",
    openBranch: "jsonapi",
    element: '.subnav a[href="/jsonapi"]',
    title: "5 · JSON:API",
    description:
      "A spec-compliant JSON:API surface — `/data/jsonapi/pet-store/inquiries?page[size]=20` — with paging and filtering out of the box.",
  },
  {
    route: "/openapi",
    openBranch: "openapi",
    element: '.subnav a[href="/openapi"]',
    title: "6 · OpenAPI / REST",
    description:
      "And a plain REST endpoint — `GET /data/rest/pet-store/inquiries` — described by OpenAPI. One model, six protocols.",
  },
  {
    route: "/admin/overview",
    element: '[data-tour="nav-admin"]',
    title: "Operate it",
    description:
      "The Admin pages run the platform: choose and configure your federation engine, manage encryption keys, and wire up auth providers. Full observability lives here for the platform owner too — and it can be redirected to any OpenTelemetry collector for enterprise-class trace management.",
  },
  {
    route: "/sources",
    element: '[data-tour="sources-add"]',
    title: "That's the tour — now make it yours",
    description:
      "You're back where you started. From here, try it on your own: register one of your real sources, expose a few tables, draw a relationship or two, then query the model from the Explore tab in whichever language you prefer — SQL, GraphQL, Cypher, gRPC, JSON:API or REST. Point pgwire or Bolt tools at it and everything just works. Want to go deeper? The Docs tab has the full guides — Getting Started, Sources, Configuration and more. And the compass button up top replays this tour anytime. Enjoy exploring Provisa.",
  },
];
