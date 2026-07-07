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
    element: '[data-tour="sources-form"]',
    title: "Connection details",
    description:
      "Pick a connector, enter host / credentials, and Save. We'll cancel here — nothing is created during the tour.",
    clickBefore: SOURCES_ADD,
    clickAfterNext: SOURCES_ADD,
  },
  {
    route: "/tables",
    element: '[data-tour="nav-tables"]',
    title: "Register tables",
    description:
      "Once a source is connected, you expose the tables (or collections, indices, endpoints) you want in the federated schema.",
  },
  {
    element: '[data-tour="tables-form"]',
    title: "Pick columns & policy",
    description:
      "Choose a source, schema, and table, then select columns — with per-column masking, visibility, and aliases. Cancelling for the demo.",
    clickBefore: TABLES_ADD,
    clickAfterNext: TABLES_ADD,
  },
  {
    route: "/relationships",
    element: '[data-tour="rels-add"]',
    title: "Connect the graph",
    description:
      "Relationships link tables across sources — even across different databases — turning flat tables into a traversable graph.",
  },
  {
    element: '[data-tour="rels-form"]',
    title: "Define a relationship",
    description:
      "Map a source column to a target column and set cardinality. Provisa can also infer these for you with AI. Cancelling for the demo.",
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
    route: "/nl",
    prep: "seedNl",
    element: ".nl-panels",
    title: "Ask in plain English",
    description:
      "Natural-language queries need your own LLM key, so here's a canned example — \"show inquiry count by user.\" Provisa compiled that one question into all six ways to query the graph at once. Every panel below is the same request.",
  },
  {
    route: "/sql",
    element: '.subnav a[href="/sql"]',
    title: "1 · SQL",
    description:
      "The NL SQL branch, live in the SQL explorer. Standard SQL federated across every source — join Postgres to Mongo to a CSV in one statement.",
  },
  {
    route: "/query",
    element: '.subnav a[href="/query"]',
    title: "2 · GraphQL",
    description:
      "The same question as a typed GraphQL query, with grouping and aggregates over your relationships — one endpoint, self-documenting schema.",
  },
  {
    route: "/graph",
    element: '.subnav a[href="/graph"]',
    title: "3 · Cypher",
    description:
      "Traverse the federated model as a graph with Cypher — `MATCH (u:Users)-[:SUBMITTED]->(i:Inquiries)` runs across sources.",
  },
  {
    route: "/grpc",
    element: '.subnav a[href="/grpc"]',
    title: "4 · gRPC",
    description:
      "Every registered entity is also a gRPC service. Call `QueryInquiries` from any gRPC client — strongly typed, high-throughput.",
  },
  {
    route: "/jsonapi",
    element: '.subnav a[href="/jsonapi"]',
    title: "5 · JSON:API",
    description:
      "A spec-compliant JSON:API surface — `/data/jsonapi/pet-store/inquiries?page[size]=20` — with paging and filtering out of the box.",
  },
  {
    route: "/openapi",
    element: '.subnav a[href="/openapi"]',
    title: "6 · OpenAPI / REST",
    description:
      "And a plain REST endpoint — `GET /data/rest/pet-store/inquiries` — described by OpenAPI. One model, six protocols. That's Provisa — enjoy!",
  },
];
