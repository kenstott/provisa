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
 *
 * `title`/`description` are not stored here — they live in the `tour.steps.<key>`
 * i18n namespace (src/i18n/locales/<lng>/tour.json), keyed by `key`, so the
 * narration is translatable. This file only carries the layout/navigation data.
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
  /** Key into the `tour.steps` i18n namespace for this step's title/description. */
  key: string;
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
  /**
   * Selector awaited *before* highlighting `element`, without being the highlight
   * target itself — used when `element` lives in a persistent shell (e.g. the
   * navbar) and would otherwise resolve instantly on route change, showing the
   * popover over a destination page that's still loading its own data.
   */
  readySelector?: string;
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
  'FROM "pet_store"."inquiries" JOIN "pet_store"."users" ON inquiries.user_id = users.id ' +
  "GROUP BY users.name";

export const TOUR_STEPS: TourStep[] = [
  // ─── SPINE: the five-minute core (register a source → expose tables → query it eight ways) ───
  {
    route: "/sources",
    element: ".navbar-tour-btn",
    key: "step0",
  },
  {
    route: "/sources",
    element: '[data-tour="nav-sources"]',
    key: "step1",
  },
  {
    element: SOURCES_ADD,
    key: "step2",
  },
  {
    element: '[data-tour="sources-type"]',
    expandSelect: true,
    key: "step3",
    clickBefore: SOURCES_ADD,
    clickAfterNext: SOURCES_ADD,
  },
  {
    route: "/tables",
    element: '[data-tour="nav-tables"]',
    // nav-tables lives in the always-mounted NavBar, so it resolves the instant navigation
    // happens — before TablesPage has finished loading. Gate on tables-add (page content,
    // rendered only past TablesPage's loading state) so the popover doesn't appear over a
    // page still stuck on "Loading tables…".
    readySelector: '[data-tour="tables-add"]',
    key: "step4",
  },
  {
    // Own the route so Back from the first surface (/sql) returns here instead of hanging on
    // clickBefore (the tables-add button only exists on /tables).
    route: "/tables",
    element: '[data-tour="tables-form"]',
    key: "step5",
    clickBefore: TABLES_ADD,
    clickAfterNext: TABLES_ADD,
  },
  {
    route: "/sql",
    openBranch: "sql",
    element: '.subnav a[href="/sql"]',
    key: "step6",
  },
  {
    route: "/query",
    openBranch: "graphql",
    element: '.subnav a[href="/query"]',
    key: "step7",
  },
  {
    route: "/graph",
    openBranch: "cypher",
    element: '.subnav a[href="/graph"]',
    key: "step8",
  },
  {
    route: "/grpc",
    openBranch: "grpc",
    element: '.subnav a[href="/grpc"]',
    key: "step9",
  },
  {
    route: "/jsonapi",
    openBranch: "jsonapi",
    element: '.subnav a[href="/jsonapi"]',
    key: "step10",
  },
  {
    route: "/openapi",
    openBranch: "openapi",
    element: '.subnav a[href="/openapi"]',
    key: "step11",
  },
  {
    route: "/explore",
    prep: "seedMcp",
    element: '.subnav a[href="/explore"]',
    key: "step12",
  },
  {
    route: "/nl",
    prep: "seedNl",
    element: '[data-testid="nl-question-input"]',
    key: "step13",
  },
  // ─── DIVIDER: the core is done; everything past here is optional depth. A natural bail point. ───
  {
    element: ".navbar-tour-btn",
    key: "step14",
  },
  // ─── GROW: optional depth — link the graph, govern it, build the pipeline, operate it. ───
  {
    route: "/relationships",
    element: '[data-tour="rels-add"]',
    key: "step15",
  },
  {
    element: '[data-tour="rels-form"]',
    key: "step16",
    clickBefore: RELS_ADD,
    clickAfterNext: RELS_ADD,
  },
  {
    // Own the route so Back from RBAC (/security/roles) returns here instead of hanging on
    // clickBefore (the ERD trigger only exists on /relationships).
    route: "/relationships",
    element: '[data-tour="rels-erd-modal"]',
    key: "step17",
    clickBefore: '[data-tour="rels-erd"]',
    clickAfterNext: '[data-testid="erd-close"]',
  },
  {
    route: "/security/roles",
    element: '.subnav a[href="/security/roles"]',
    key: "step18",
  },
  {
    route: "/security/rls",
    element: '.subnav a[href="/security/rls"]',
    key: "step19",
  },
  {
    route: "/views",
    element: '.subnav a[href="/views"]',
    key: "step20",
  },
  {
    route: "/views",
    element: '.subnav a[href="/views"]',
    key: "step21",
  },
  {
    route: `/lineage?sql=${encodeURIComponent(LINEAGE_DEMO_SQL)}`,
    element: '[data-testid="lineage-dag"]',
    key: "step22",
  },
  {
    route: "/admin/overview",
    element: '[data-tour="nav-admin"]',
    key: "step23",
  },
  // ─── CLOSE ───
  {
    route: "/sources",
    element: '[data-tour="sources-add"]',
    key: "step24",
  },
];
