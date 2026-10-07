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
 * Order is a narrative: a SPINE (register a source → expose tables → query it) that IS the five-minute core, a "that's the core" divider where a
 * user can bail, then the GROW chapters (graph, governance, pipeline, operate)
 * as optional depth, and a closing call to action.
 *
 * `title`/`description` are not stored here — they live in the `tour.steps.<key>`
 * i18n namespace (src/i18n/locales/<lng>/tour.json), keyed by `key`, so the
 * narration is translatable. This file only carries the layout/navigation data.
 */
import type { Capability } from "../types/auth";

export interface TourStep {
  /** Route to navigate to before the step is shown. Omit to stay put. */
  route?: string;
  /**
   * The capability `route`'s gate in App.tsx requires. Declared beside the route it belongs to, and
   * asserted equal to that gate by tourRouteCapabilities.test.ts — the two must not drift.
   *
   * The tour is taken by whoever is signed in, and not every viewer holds every right. A step whose
   * page the viewer cannot open renders "You do not have permission to view this page", so its
   * anchor never mounts and the runner waits out the full anchor window before offering its stuck
   * panel: a tour that appears to hang. Denial is knowable up front, so it is answered up front —
   * {@link tourItinerary} drops the step before the run starts, and the visitor sees a shorter tour
   * of the product they actually have rather than a broken one. Only steps that OWN a route carry
   * this; a routeless step inherits its owner's fate (see `tourItinerary`).
   */
  capability?: Capability;
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
  /**
   * Named warm-up for *this* step's backend work, resolved by the runner's prefetch registry and
   * fired one step early (while the previous popover is on screen). The start-up prefetch only
   * covers page chunks and the shared GraphQL queries; a step whose anchor waits on its own REST
   * round-trip — the lineage analysis, the platform settings that gate TablesPage's loading state —
   * otherwise pays that latency at the moment Next is clicked, which is what makes an advance feel
   * dead on a loaded machine.
   */
  prefetch?: string;
  /**
   * REQ-1945: the step refers to Polly, so the runner opens the Polly panel (through the launcher's own
   * handler) before anchoring, and the tour closes it on moving to a step without this flag.
   */
  pollyOpen?: boolean;
  /** Key into the `tour.steps` i18n namespace for this step's title/description. */
  key: string;
  /**
   * Clicks that open what `clickBefore` needs, run in order and only when it is not already there:
   * each is clicked unless its `unlessPresent` selector already matches. A step that can be entered
   * cold (Back from a later page, or a resume) names how to reach its own starting state, so it never
   * depends on a predecessor having left the page open.
   */
  ensureOpen?: { click: string; unlessPresent: string }[];
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
  /**
   * Overrides the runner's default anchor wait for steps whose target only mounts after a backend
   * round-trip the tour cannot prefetch. Not a leniency knob: an anchor that never appears still
   * aborts the tour, this only sets how long "never" takes to establish.
   */
  waitMs?: number;
}

const SOURCES_ADD = '[data-tour="sources-add"]';
const TABLES_ADD = '[data-tour="tables-add"]';
const RELS_ADD = '[data-tour="rels-add"]';
// The Views page is TablesPage in viewsOnly mode, which omits tables-add — its header is the one
// marker both modes paint past the loading state.
const VIEWS_READY = '[data-tour="tables-header"]';

// A complex-enough query over the same proven demo tables the SQL surface runs. The schema is the
// logical domain (`domain_to_sql_name("pet-store")` → `pet_store`), never the source's physical
// schema. Plus an UPPER() transform so the DAG shows a real source → transform →
// result trace. LineagePage auto-builds the statement graph from `?sql=` on mount (it only analyzes,
// never runs), so deep-linking it avoids any click-timing race.
// Exported so the runner's `lineageDemo` prefetch warms the exact statement this step deep-links.
export const LINEAGE_DEMO_SQL =
  "SELECT users.name, UPPER(users.name) AS name_upper, COUNT(inquiries.id) AS inquiry_count " +
  'FROM "pet_store"."inquiries" JOIN "pet_store"."users" ON inquiries.user_id = users.id ' +
  "GROUP BY users.name";

// The Data Products list marks each row with aria-expanded; the tour clicks the first collapsed
// row to open it and the (single) expanded row to close it again.
const DATA_PRODUCTS_COLLAPSED_ROW =
  '[data-testid="data-products-table"] tbody tr[aria-expanded="false"]';
const DATA_PRODUCTS_EXPANDED_ROW =
  '[data-testid="data-products-table"] tbody tr[aria-expanded="true"]';

// Rows of the Tables page by their `data-table-row` anchor (source.table). A step that opens one
// particular table names its row: the generic first row is whatever the filter put first, and while
// a route is still changing it can be a row of the page being left.
const QUALITY_ROW = '[data-table-row="dq-checker.pets_scan"]';
const PROFILER_ROW = '[data-table-row="pet-store-sqlite.pets"]';

export const TOUR_STEPS: TourStep[] = [
  // ─── SPINE: the five-minute core (register a source → expose tables → query it) ───
  {
    route: "/sources",
    capability: "source_registration",
    prefetch: "settings",
    element: ".navbar-tour-btn",
    readySelector: SOURCES_ADD,
    key: "step0",
  },
  {
    // REQ-1945: the step shows Polly open, never the closed launcher: the runner opens the panel through
    // the launcher's own handler and the tour closes it again on moving to a step that does not
    // reference Polly. Routeless: stays on step0's page.
    element: '[data-testid="chat-panel"]',
    pollyOpen: true,
    key: "stepPolly",
  },
  {
    route: "/sources",
    capability: "source_registration",
    prefetch: "settings",
    element: '[data-tour="nav-sources"]',
    readySelector: SOURCES_ADD,
    key: "step1",
  },
  {
    element: SOURCES_ADD,
    key: "step2",
  },
  {
    // REQ-1938: clicking the Type field opens the picker dialog; the search box is the anchor. Next
    // closes the dialog. The form stays open until the tour leaves /sources.
    element: '[data-testid="source-type-picker-search"]',
    key: "step3",
    // Entered cold (Back from /tables, or a resume) the form is closed: open it, then the picker.
    ensureOpen: [{ click: SOURCES_ADD, unlessPresent: '[data-tour="sources-type"]' }],
    clickBefore: '[data-tour="sources-type"]',
    clickAfterNext: '[data-testid="source-type-picker-close"]',
  },
  {
    route: "/tables",
    capability: "table_registration",
    prefetch: "settings",
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
    capability: "table_registration",
    prefetch: "settings",
    element: '[data-tour="tables-form"]',
    key: "step5",
    clickBefore: TABLES_ADD,
    clickAfterNext: TABLES_ADD,
  },
  {
    // REQ-1392: expand the first table row so the Preview button exists, then
    // collapse it again on Next. Rows render past the loading state, so the
    // clickBefore await also gates on page readiness.
    route: "/tables",
    capability: "table_registration",
    prefetch: "settings",
    element: '[data-testid="table-read-view-preview"]',
    key: "stepPreview",
    clickBefore: ".data-table tbody tr.clickable",
    clickAfterNext: ".data-table tbody tr.clickable",
  },
  {
    // REQ-1945: the core's one query card, general; every surface's detail is the Query it everywhere topic.
    route: "/sql",
    capability: "query_development",
    openBranch: "sql",
    element: '.subnav a[href="/sql"]',
    pollyOpen: true,
    key: "stepQuery",
  },
  {
    route: "/sql",
    capability: "query_development",
    openBranch: "sql",
    element: '.subnav a[href="/sql"]',
    key: "step6",
  },
  {
    route: "/query",
    capability: "query_development",
    openBranch: "graphql",
    element: '.subnav a[href="/query"]',
    key: "step7",
  },
  {
    route: "/graph",
    capability: "query_development",
    openBranch: "cypher",
    element: '.subnav a[href="/graph"]',
    key: "step8",
  },
  {
    route: "/grpc",
    capability: "query_development",
    openBranch: "grpc",
    element: '.subnav a[href="/grpc"]',
    key: "step9",
  },
  {
    route: "/jsonapi",
    capability: "query_development",
    openBranch: "jsonapi",
    element: '.subnav a[href="/jsonapi"]',
    key: "step10",
  },
  {
    route: "/openapi",
    capability: "query_development",
    openBranch: "openapi",
    element: '.subnav a[href="/openapi"]',
    key: "step11",
  },
  {
    route: "/explore",
    capability: "query_development",
    prep: "seedMcp",
    element: '.subnav a[href="/explore"]',
    key: "step12",
  },
  {
    route: "/nl",
    capability: "query_development",
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
    capability: "create_relationship",
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
    capability: "create_relationship",
    element: '[data-tour="rels-erd-modal"]',
    key: "step17",
    clickBefore: '[data-tour="rels-erd"]',
    clickAfterNext: '[data-testid="erd-close"]',
  },
  {
    route: "/security/roles",
    capability: "access_config",
    element: '.subnav a[href="/security/roles"]',
    readySelector: '[data-testid="toggle-role-form"]',
    key: "step18",
  },
  {
    route: "/security/rls",
    capability: "access_config",
    element: '.subnav a[href="/security/rls"]',
    readySelector: '[data-testid="toggle-rule-form"]',
    key: "step19",
  },
  {
    // REQ-1443: the demo's checker source lands its scans as an ordinary table, so the quality
    // contract is edited where every other table setting is. `?source=` seeds the filter box, and
    // the checker's results table is opened by its row anchor — expand it and its edit control
    // appears.
    route: "/tables?source=dq-checker",
    capability: "table_registration",
    prefetch: "settings",
    element: '[data-testid="table-read-view-edit"]',
    key: "stepQualityTable",
    clickBefore: QUALITY_ROW,
  },
  {
    // The edit form is already open from the previous step's highlight target; opening it is this
    // step's clickBefore. Next collapses the row, which also cancels the edit.
    element: '[data-tour="dq-panel"]',
    key: "stepQualityPanel",
    clickBefore: '[data-testid="table-read-view-edit"]',
    clickAfterNext: QUALITY_ROW,
  },
  {
    // REQ-1934: a table joins a Data Profiler from its editor. The profiler panel is not shown on a
    // checker's results table, so these steps open the demo's plain pet-store `pets` table.
    route: "/tables?source=pet-store-sqlite",
    capability: "table_registration",
    prefetch: "settings",
    element: '[data-testid="table-read-view-edit"]',
    key: "stepProfilerTable",
    clickBefore: PROFILER_ROW,
  },
  {
    // The edit form opens as this step's clickBefore; the fill-from-profile step's Next collapses
    // the row, which also cancels the edit.
    element: '[data-tour="profiler-panel"]',
    key: "stepProfilerPanel",
    clickBefore: '[data-testid="table-read-view-edit"]',
  },
  {
    // REQ-1934: drift, checker exceptions, constraints and external expectations are surfaced from
    // the profiler's runs; the panel is the nearest anchor that exists on every build.
    element: '[data-tour="profiler-panel"]',
    key: "stepProfilerChecks",
  },
  {
    // REQ-1494: a column's kind of fake is declared in the column list's Test data mode, so the
    // step switches the column list to that mode and points at the test-data columns.
    ensureOpen: [
      {
        click: '[data-tour="table-columns-mode"] input[value="testdata"]',
        unlessPresent: '[data-testid="testdata-columns"]',
      },
    ],
    element: '[data-testid="testdata-columns"]',
    key: "stepFakes",
  },
  {
    // REQ-1494: the fill-from-profile action sits with the column list's test-data mode.
    element: '[data-tour="table-columns-mode"]',
    key: "stepFakesFill",
    clickAfterNext: PROFILER_ROW,
  },
  {
    // REQ-1493: an environment is read or read-write, and may be marked test data. Both are set on
    // the Environments page; its root is the anchor.
    route: "/admin/environments",
    capability: "environment_management",
    element: '[data-testid="environments-tab"]',
    key: "stepEnvKinds",
  },
  {
    // REQ-1939: synthetic datasets live on the Environments page, in a tab of their own. The tab
    // itself is the anchor: the panel's body depends on a non-production environment existing.
    element: '[data-tour="synthetic-tab"]',
    key: "stepSynthetic",
  },
  {
    // REQ-1939: differential privacy is a setting of the dataset; the tab is the anchor.
    element: '[data-tour="synthetic-tab"]',
    key: "stepSyntheticPrivacy",
  },
  {
    // REQ-1387: the business glossary — curation plus AI-assisted definitions/relationships.
    route: "/admin/glossary",
    // REQ-1590: stricter than the route's own gate. /admin/glossary opens to `glossary_read`, but
    // what this step points at is the AI-generation buttons, which only a curator is shown — so a
    // read-only viewer skips the step rather than waiting out an anchor that never mounts.
    capability: "glossary_rw",
    element: '[data-testid="glossary-bulk-definitions-btn"]',
    key: "stepGlossary",
  },
  {
    // REQ-1660: the ODPS-aligned data product catalog, and its always-reconciled auto-publish
    // to warehouse-native product surfaces (Snowflake Horizon, BigQuery Dataplex).
    route: "/data-products",
    capability: "data_product_read",
    // Expand the first product so every panel the description names -- ODPS fields, member tables,
    // lineage, related terms -- is on screen, then collapse it on Next. `prep` clears the page's
    // persisted expansion first, so the click lands on a collapsed row and always expands.
    prep: "collapseDataProducts",
    clickBefore: DATA_PRODUCTS_COLLAPSED_ROW,
    clickAfterNext: DATA_PRODUCTS_EXPANDED_ROW,
    element: '[data-tour="data-products-content"]',
    readySelector: '[data-testid="data-product-detail"]',
    key: "stepDataProducts",
  },
  {
    // REQ-1945: publishing the model, its data products and lineage out to a catalog. The anchor is
    // the Metadata Export page root, which paints in both the entitled and not-entitled states.
    route: "/admin/metadata-export",
    capability: "org_settings",
    element: '[data-tour="metadata-export"]',
    key: "stepPublish",
  },
  {
    route: "/views",
    capability: "table_registration",
    prefetch: "settings",
    element: '.subnav a[href="/views"]',
    readySelector: VIEWS_READY,
    key: "step20",
  },
  {
    route: "/views",
    capability: "table_registration",
    prefetch: "settings",
    element: '.subnav a[href="/views"]',
    readySelector: VIEWS_READY,
    key: "step21",
  },
  {
    route: `/lineage?sql=${encodeURIComponent(LINEAGE_DEMO_SQL)}`,
    capability: "table_registration",
    prefetch: "lineageDemo",
    element: '[data-testid="lineage-dag"]',
    // The DAG mounts only once LineagePage's deep-link effect has finished analyzing the statement
    // server-side; nothing paints it before that, so this step's anchor is bounded by the backend,
    // not by rendering. A cold analysis outruns the 15s default and the tour ends while the page is
    // still working — exactly the "tour stalled on the lineage page" symptom.
    waitMs: 60000,
    key: "step22",
  },
  {
    // REQ-1390: the seeded ops-domain management reports and the add-your-own flow.
    route: "/admin/reports",
    capability: "observability",
    element: '[data-testid="reports-list"]',
    key: "stepReports",
  },
  {
    route: "/admin/overview",
    capability: "observability",
    element: '[data-tour="nav-admin"]',
    readySelector: '[data-tour="admin-content"]',
    key: "step23",
  },
];

/**
 * REQ-1945: the tour is a short CORE tour plus a menu of Deep Dives, each its own tour. Every step
 * belongs to exactly one scope (asserted by tourScopes.test.ts), named by its `key`. Order inside a
 * scope is TOUR_STEPS order, so the narrative of the flat list is kept and a routeless step still
 * inherits the nearest preceding routed step, wherever that step's own scope lies.
 */
export const TOPIC_IDS = [
  "connect",
  "relationships",
  "model",
  "govern",
  "query",
  "testdata",
  "publish",
  "operate",
] as const;
export type TopicId = (typeof TOPIC_IDS)[number];
export type TourScope = "core" | TopicId;

export const TOUR_SCOPES: Record<TourScope, readonly string[]> = {
  core: ["step0", "stepPolly", "step1", "step2", "stepQuery", "step14"],
  connect: ["step3", "step4", "step5", "stepPreview"],
  relationships: ["step15", "step16", "step17"],
  model: ["step20", "step21", "step22"],
  govern: ["step18", "step19"],
  query: ["step6", "step7", "step8", "step9", "step10", "step11", "step12", "step13"],
  testdata: [
    "stepQualityTable",
    "stepQualityPanel",
    "stepProfilerTable",
    "stepProfilerPanel",
    "stepProfilerChecks",
    "stepFakes",
    "stepFakesFill",
    "stepEnvKinds",
    "stepSynthetic",
    "stepSyntheticPrivacy",
  ],
  // The glossary step stays here until its topic is decided (REQ-1945 open question).
  publish: ["stepGlossary", "stepDataProducts", "stepPublish"],
  operate: ["stepReports", "step23"],
};

/**
 * The route step `index` must be on, walking back to the nearest preceding step that declares one.
 *
 * A step omits `route` when it continues on the page its predecessor navigated to (e.g. the
 * sources add-form steps after the /sources step). Stepping forward that works, but resuming
 * straight into such a step lands on whatever page the visitor was already on, its anchor never
 * appears, and the runner's wait times out — the tour button looks dead. Resolving the inherited
 * route makes any index directly enterable.
 */
export function stepRoute(index: number): string | undefined {
  for (let i = index; i >= 0; i--) {
    const route = TOUR_STEPS[i].route;
    if (route) return route;
  }
  return undefined;
}

/**
 * The steps this viewer can actually be shown, as indices into {@link TOUR_STEPS} in order.
 *
 * The tour narrates surfaces, and a surface the viewer's rights do not open cannot be narrated: the
 * route paints NotAuthorized, the anchor never mounts, and the runner sits on its waiting overlay
 * for the whole anchor window before offering Retry / Skip / Exit. That is a permission answer
 * delivered as a hang, and it is avoidable — the rights are in hand before the tour starts, so the
 * step is dropped up front and the popover's "n of N" counts only what the visitor will see.
 *
 * A routeless step continues on its owner's page (see {@link stepRoute}) and, in several cases,
 * depends on that step's `clickBefore` having run — so it goes wherever its owner goes. Dropping the
 * owner alone would leave a step navigating nowhere and highlighting an anchor that was never
 * opened, which is the same hang by a different path.
 *
 * `meets` is passed in rather than imported so this stays pure data logic; the runner supplies
 * `meetsRequirement` bound to the signed-in capabilities.
 */
export function tourItinerary(
  meets: (capability: Capability) => boolean,
  scope?: TourScope,
): number[] {
  const out: number[] = [];
  // Whether the route currently in force is one this viewer may open. Steps before the first
  // routed step (there are none today) would inherit `true` — no gate to fail.
  let ownerAllowed = true;
  TOUR_STEPS.forEach((step, i) => {
    if (step.route) {
      if (step.capability === undefined) {
        throw new Error(`Tour: step ${i} declares a route with no capability`);
      }
      ownerAllowed = meets(step.capability);
    }
    // The owner walk covers every step, so a scoped run whose first step is routeless still
    // resolves its owner's rights; the scope only filters what is kept.
    if (ownerAllowed && (scope === undefined || TOUR_SCOPES[scope].includes(step.key))) out.push(i);
  });
  return out;
}
