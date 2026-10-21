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
import type { OrgHas, TourPrerequisites } from "./tourPrerequisites";
import type { UiAction } from "../mcp/uiActions";
import PREREQUISITES from "./tourPrerequisites.json" with { type: "json" };

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
   * REQ-2010: a statement of a named demo's own tour, opened on the step's explorer and run — the
   * explorers' navigation hand-off, under the state key the explorer reads (`sql` for the SQL
   * explorer, `query` for GraphiQL and the Graph page, `openApiUrl`, `jsonapiUrl`, `grpcMethod`).
   * Handed over on every entry, even when the explorer is already the page, since each such step
   * opens its own statement.
   */
  openQuery?: {
    // "role": a set of roles the SQL explorer runs as (REQ-2057). "asOf"/"asRecorded": the SQL
    // explorer's time-travel pickers (REQ-1163).
    stateKey:
      | "sql"
      | "query"
      | "openApiUrl"
      | "jsonapiUrl"
      | "grpcMethod"
      | "role"
      | "asOf"
      | "asRecorded";
    text: string;
    /** REQ-2083: the text is a value of an earlier tour statement's answer, read when the step
     *  runs: `sql` run again as it ran (role, time travel), row `row`'s `column`. */
    from?: {
      sql: string;
      role?: string;
      asOf?: string;
      asRecorded?: string;
      row: number;
      column: string;
    };
  };
  /**
   * The state handed to `route`'s page once per visit to the step, with no statement: the Graph
   * page's `view` (REQ-2010: `groupBy`, its newest frame grouped by that property, its query kept)
   * or the Notebook tab's `cells` (REQ-2084: added to the open notebook, and run with `autoRun`), or
   * the Python tab's `code` (REQ-2071: into its editor, and run with `autoRun`).
   */
  navState?:
    | { view: { groupBy: string } }
    | { cells: string[]; autoRun: boolean }
    | { code: string; autoRun: boolean };
  /**
   * REQ-2010: on Next only, open the picker `open` and choose its option `option`, then advance —
   * a choice the step shows (the picker is highlighted) and makes when the visitor moves on. Back
   * and close leave the picker as it was.
   */
  chooseOnNext?: { open: string; option: string };
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
   * REQ-1945: the step shows Polly's panel, so the runner opens it (through the launcher's own
   * handler) before anchoring, and the tour closes it on moving to a step without this flag. Set on
   * the steps that show the panel, not on every step whose words name Polly.
   */
  pollyOpen?: boolean;
  /**
   * REQ-2012: a question and the answer stored for it, shown in the open Polly panel as an
   * exchange; nothing is sent.
   */
  pollyExchange?: { question: string; answer: string };
  /** A picture shown in the step's popover, below its words (a named demo's `image` step): a URL
   *  the browser loads as it is (the server sends a demo's as a data URL). */
  image?: string;
  /** Another named demo, by name (a named demo's `demo` field): the popover offers to open it, a
   *  button to `/demos/<name>` titled with that demo's `tour.demos.<name>.title`. */
  demo?: string;
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
   * REQ-2115: what the tour does on the step's screen before explaining it (act, then explain):
   * fill, choose, press and show, run by Polly's own UI actions (mcp/uiActions), outlined as each
   * is taken, under the step's `tour.steps.<key>.acting` caption and with no popover; then the
   * popover anchors on `element`, the result. A press that changes something is the visitor's
   * own tour doing it and asks nothing more. Not run when `element` is on screen already (the
   * step entered again by Back or a resume finds what it built). Only what is very unlikely to
   * fail live is done so: a static known address, or what Provisa handles itself.
   */
  actions?: UiAction[] | ((ctx: TourContext) => UiAction[]);
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
// REQ-2115: the Parquet file Provisa publishes (REQ-2113), the core tour's live-built source.
export const SUPPLIES_PARQUET_URL = "https://provisa.dev/demo-data/supplies.parquet";
/** REQ-2101 (amended): the copy a demo ships into its org's own folder (starthere's org_files/). */
export const SUPPLIES_ORG_FILE = "supplies.parquet";

/** What a step's actions may read about where the tour runs. */
export interface TourContext {
  /** The org files the demo ships, each in the org's folder as {files}/<path>. */
  orgFiles: string[];
}

/** Where the live-built source reads supplies.parquet: the org's own copy where its demo ships
 * one, read in place with no network (a sandbox may reach no address its demo does not declare);
 * else the copy Provisa publishes. */
export function suppliesAddress(orgFiles: string[]): string {
  return orgFiles.includes(SUPPLIES_ORG_FILE)
    ? `{files}/${SUPPLIES_ORG_FILE}`
    : SUPPLIES_PARQUET_URL;
}
const SUPPLIES_SOURCE_ID = "supplies";
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
// The canned pet-store statement the explorers' cards run (the NL demo's SQL branch, useTour), read
// from Python: as a script on the Python tab, as cells on the Notebook tab. The last expression is
// the DataFrame each tab shows as a table.
export const PET_STORE_DEMO_SQL =
  'SELECT species, COUNT(*) AS pet_count FROM "pet_store"."pets" GROUP BY species LIMIT 100';
const PYTHON_DEMO_QUERY = `provisa_client.query(\n    '${PET_STORE_DEMO_SQL}'\n)`;
const PYTHON_DEMO_CODE = `import provisa_client\nprovisa_client.sign_in()\n\n${PYTHON_DEMO_QUERY}\n`;
// One cell after the notebook's own starter cell (liteBridge NOTEBOOK_STARTER), whole in itself: a
// kernel the visitor restarted has the client again before the statement runs.
const NOTEBOOK_DEMO_CELLS = [
  `%pip install provisa-client\nimport provisa_client\n\n${PYTHON_DEMO_QUERY}`,
];

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
const QUALITY_ROW = '[data-table-row="dq-checker.pets_quality_dq_checker_dq"]';
const PROFILER_ROW = '[data-table-row="pet-store-sqlite.pets"]';
// Opens the pets table's edit form from cold on its Quality section, where the profiler is: open
// the row unless it (or the form) is already open, click Edit unless the form is already there,
// then the Quality tab unless the profiler is already shown. Every card of the profiler/fakes run
// names it so each can be entered directly (Back, resume) and never depends on its predecessor.
const TABLE_EDITING = '[data-testid="table-edit-cancel"]';
const PETS_EDIT_OPEN = [
  {
    click: PROFILER_ROW,
    unlessPresent: `[data-testid="table-read-view-edit"], ${TABLE_EDITING}`,
  },
  { click: '[data-testid="table-read-view-edit"]', unlessPresent: TABLE_EDITING },
  { click: '[data-testid="table-section-quality"]', unlessPresent: '[data-tour="profiler-panel"]' },
];
// The column list's Testing tab, on the table's Columns section.
const PETS_TESTING_OPEN = [
  ...PETS_EDIT_OPEN,
  {
    click: '[data-testid="table-section-columns"]',
    unlessPresent: '[data-testid="column-tab-testing"]',
  },
  {
    click: '[data-testid="column-tab-testing"]',
    unlessPresent: '[data-testid="testdata-columns"]',
  },
];

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
    // REQ-1938: "+ Source" opens the type picker dialog; its search box is the anchor. Next
    // closes the dialog without a choice, so no form opens.
    element: '[data-testid="source-type-picker-search"]',
    key: "step3",
    clickBefore: SOURCES_ADD,
    clickAfterNext: '[data-testid="source-type-picker-close"]',
  },
  {
    // REQ-2115: a source built live, as the visitor would build it: the picker opened and its
    // whole list shown, Parquet File chosen, the file given as its address, and saved. A static
    // known address, so very unlikely to fail; the popover then explains the row.
    actions: ({ orgFiles }) => [
      { do: "press", target: { testId: "sources-add-toggle" } },
      { do: "show", target: { testId: "source-type-picker-body" } },
      { do: "press", target: { testId: "source-type-option-parquet" } },
      { do: "fill", target: { testId: "sources-id-input" }, value: SUPPLIES_SOURCE_ID },
      { do: "fill", target: { testId: "source-file-location" }, value: suppliesAddress(orgFiles) },
      { do: "press", target: { role: "button", name: "Save" } },
    ],
    // The new row's first cell, its ID: the row runs on under the source's detail, which Save
    // opens beside the list, and a spotlight on the whole row lit that panel too.
    element: `[data-testid="sources-row-${SUPPLIES_SOURCE_ID}"] td:first-child`,
    key: "stepBuildSource",
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
    // The Python tab: the canned pet-store statement read as a DataFrame and run, in the browser.
    route: "/python",
    capability: "query_development",
    navState: { code: PYTHON_DEMO_CODE, autoRun: true },
    element: '.subnav a[href="/python"]',
    key: "stepPython",
  },
  {
    // The Notebook tab: the same statement as a cell added after the notebook's starter cell.
    route: "/notebook",
    capability: "query_development",
    navState: { cells: NOTEBOOK_DEMO_CELLS, autoRun: true },
    element: '.subnav a[href="/notebook"]',
    key: "stepNotebook",
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
    // REQ-1945: the agents point shows Polly, so the runner opens the panel (pollyOpen); the list always renders.
    route: "/relationships",
    capability: "create_relationship",
    element: '[data-testid="relationships-list"]',
    pollyOpen: true,
    key: "stepRelsAgents",
  },
  {
    element: '[data-tour="rels-form"]',
    key: "step16",
    clickBefore: RELS_ADD,
    clickAfterNext: RELS_ADD,
  },
  {
    route: "/relationships",
    capability: "create_relationship",
    element: '[data-tour="rels-suggest"]',
    key: "stepRelsSuggest",
  },
  {
    route: "/relationships",
    capability: "create_relationship",
    element: '[data-testid="relationships-list"]',
    key: "stepRelsList",
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
    route: "/security/roles",
    capability: "access_config",
    // "+ Role" opens a menu (a granted role, or one derived from an attribute): its granted-role
    // item opens the plain role form, whose parent select this step shows. No readySelector: the
    // runner checks it after clickBefore, and the open form hides "+ Role"; ensureOpen's wait for
    // "+ Role" is the page-ready gate.
    ensureOpen: [
      {
        click: '[data-testid="toggle-role-form"]',
        unlessPresent: '[data-testid="add-granted-role"]',
      },
    ],
    clickBefore: '[data-testid="add-granted-role"]',
    // The opener is hidden while its form is open: the form's own Cancel closes it.
    clickAfterNext: '[data-testid="cancel-role"]',
    element: '[data-testid="role-parent-select"]',
    key: "stepRoleInherit",
  },
  {
    route: "/security/rls",
    capability: "access_config",
    element: '.subnav a[href="/security/rls"]',
    readySelector: '[data-testid="toggle-rule-form"]',
    key: "step19",
  },
  {
    route: "/admin/domains",
    capability: "org_settings",
    element: '[data-testid="domain-settings-panel"]',
    key: "stepDomains",
  },
  {
    route: "/admin/tags",
    capability: "org_settings",
    element: '[data-testid="tags-scope-bar"]',
    key: "stepTags",
  },
  {
    route: "/admin/requests",
    capability: "org_settings",
    element: '[data-testid="requests-tab-pending"]',
    key: "stepRequests",
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
    // The previous step highlights Edit; this one opens the form on its Quality section, where
    // the contract is. Next collapses the row, which also cancels the edit.
    ensureOpen: [
      { click: '[data-testid="table-read-view-edit"]', unlessPresent: TABLE_EDITING },
      { click: '[data-testid="table-section-quality"]', unlessPresent: '[data-tour="dq-panel"]' },
    ],
    element: '[data-tour="dq-panel"]',
    key: "stepQualityPanel",
    clickAfterNext: QUALITY_ROW,
  },
  {
    // REQ-1934: a table joins a Data Profiler on its editor's Quality tab. The profiler panel is not shown on a
    // checker's results table, so the profiler cards open the demo's plain pet-store `pets` table.
    // The quality topic's reading of the panel: what a run measures and where results land.
    route: "/tables?source=pet-store-sqlite",
    capability: "table_registration",
    prefetch: "settings",
    ensureOpen: PETS_EDIT_OPEN,
    element: '[data-tour="profiler-panel"]',
    key: "stepQualityProfile",
  },
  {
    // REQ-1934: drift, checker exceptions, constraints and external expectations are surfaced from
    // the profiler's runs; the panel is the nearest anchor that exists on every build. Next collapses
    // the row, which also cancels the edit.
    ensureOpen: PETS_EDIT_OPEN,
    element: '[data-tour="profiler-panel"]',
    key: "stepProfilerChecks",
    clickAfterNext: PROFILER_ROW,
  },
  {
    // REQ-1443 clause 10: a checker table whose contract scans a product's output ports is listed in
    // the product's Data Quality panel. The panel mounts for every expanded product (an empty state
    // when no checker scans it), so it is a stable anchor. Expand the first product, collapse it on
    // Next; `prep` clears the page's persisted expansion so the click always expands.
    route: "/data-products",
    capability: "data_product_read",
    prep: "collapseDataProducts",
    clickBefore: DATA_PRODUCTS_COLLAPSED_ROW,
    clickAfterNext: DATA_PRODUCTS_EXPANDED_ROW,
    element: '[data-tour="data-product-dq"]',
    readySelector: '[data-testid="data-product-detail"]',
    key: "stepQualityProduct",
  },
  {
    // REQ-1945: the test-data topic's opening map. Anchored on the pets table's edit control: the
    // Tables surface needs only `table_registration` (or a hiding right), a right far more roles
    // hold than the environments page's `environment_management`, so the map survives for most
    // viewers. The row is expanded (not edited) so the control exists.
    route: "/tables?source=pet-store-sqlite",
    capability: "table_registration",
    prefetch: "settings",
    ensureOpen: [
      // Entered from the card after it (Back) the edit form is open, and while a table is edited
      // the panel's head holds Save and Cancel in place of the table's actions: leave the form
      // first, so the control this card points at is there.
      {
        click: '[data-testid="table-edit-cancel"]',
        unlessPresent: 'body:not(:has([data-testid="table-edit-cancel"]))',
      },
      {
        click: PROFILER_ROW,
        unlessPresent: '[data-testid="table-read-view-edit"]',
      },
    ],
    element: '[data-testid="table-read-view-edit"]',
    key: "stepTestDataMap",
  },
  {
    // REQ-1934: the profiler, as what fakes and synthetic data are built from. The same panel as the
    // quality topic's profiler card, with its own text.
    ensureOpen: PETS_EDIT_OPEN,
    element: '[data-tour="profiler-panel"]',
    key: "stepTestDataProfiler",
  },
  {
    // REQ-1494: a column's kind of fake is declared on the column list's Testing tab, so the
    // step opens that tab and points at the test-data columns.
    ensureOpen: PETS_TESTING_OPEN,
    element: '[data-testid="testdata-columns"]',
    key: "stepFakes",
  },
  {
    // REQ-1494: the fill-from-profile action sits on the column list's Testing tab.
    ensureOpen: PETS_TESTING_OPEN,
    element: '[data-tour="fill-from-profile"]',
    key: "stepFakesFill",
    clickAfterNext: PROFILER_ROW,
  },
  {
    // REQ-1493, REQ-1942: an environment is a branch of the model; at creation it takes one of the
    // data modes. Both are set on the Environments page; its root is the anchor.
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
    route: "/data-products",
    capability: "data_product_rw",
    clickBefore: '[data-testid="data-products-new-button"]',
    clickAfterNext: '[data-testid="data-product-cancel-button"]',
    element: '[data-testid="data-product-form"]',
    key: "stepProductForm",
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
    route: "/admin/glossary",
    capability: "glossary_rw",
    element: '[data-testid="glossary-new-btn"]',
    key: "stepGlossaryTerm",
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
    route: "/metrics",
    capability: "table_registration",
    element: '[data-testid="metrics-new-button"]',
    key: "stepMetrics",
  },
  {
    route: "/commands",
    capability: "table_registration",
    element: '[data-testid="commands-toggle-form"]',
    key: "stepCommands",
  },
  {
    route: `/lineage?sql=${encodeURIComponent(LINEAGE_DEMO_SQL)}`,
    capability: "view_governance",
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
    route: "/admin/observability",
    capability: "observability",
    element: '[data-testid="observability-debug-trace-tab"]',
    key: "stepObservability",
  },
  {
    route: "/admin/cache",
    capability: "org_settings",
    element: '[data-testid="cache-tab-response"]',
    key: "stepCache",
  },
  {
    route: "/admin/scheduled-tasks",
    capability: "org_settings",
    element: '[data-testid="scheduled-tasks-toggle-form"]',
    key: "stepScheduled",
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
  "relationships",
  "model",
  "govern",
  "quality",
  "query",
  "testdata",
  "publish",
  "operate",
] as const;
export type TopicId = (typeof TOPIC_IDS)[number];
export type TourScope = "core" | TopicId;
/**
 * REQ-2013: what each tour's steps need of the org, beyond rights. The core tour and these deep
 * dives are told with the standard demo's objects (its pet store, quality checker and data
 * products); a scope not listed here needs nothing but its pages. A tour whose prerequisites the
 * org lacks is not offered (tourPrerequisites.ts). They are a JSON file so the Start here demo's
 * test (tests/integration/test_starthere_demo.py) checks the same list against a running server:
 * - core: stepQuery hands the SQL explorer the canned pet-store statement.
 * - query: the explorers' cards hand over the canned pet-store queries.
 * - quality: the checker's results table, the pets table's profiler, and a product carrying its
 *   quality.
 * - model: the lineage card analyses LINEAGE_DEMO_SQL.
 * - publish: the products card opens the first product.
 */
export const TOUR_PREREQUISITES: Partial<Record<TourScope, TourPrerequisites>> = PREREQUISITES;

/** REQ-2010: what one run of the tour walks — a scope of TOUR_STEPS, or the named demo's track,
 * the steps after TOUR_STEPS in the runner's list. */
export type RunScope = TourScope | "demo";

export const TOUR_SCOPES: Record<TourScope, readonly string[]> = {
  // Registering a source and a table comes before querying it: the type picker, the tables page,
  // the table form and its preview are the core's, not a Deep Dive's.
  core: [
    "step0",
    "stepPolly",
    "step1",
    "step2",
    "step3",
    "stepBuildSource",
    "step4",
    "step5",
    "stepPreview",
    "stepQuery",
    "step14",
  ],
  relationships: [
    "step15",
    "stepRelsAgents",
    "step16",
    "stepRelsSuggest",
    "stepRelsList",
    "step17",
  ],
  model: ["step20", "step21", "stepMetrics", "stepCommands", "step22"],
  govern: ["step18", "stepRoleInherit", "step19", "stepDomains", "stepTags", "stepRequests"],
  query: [
    "step6",
    "step7",
    "step8",
    "step9",
    "step10",
    "step11",
    "step12",
    "stepPython",
    "stepNotebook",
    "step13",
  ],
  quality: [
    "stepQualityTable",
    "stepQualityPanel",
    "stepQualityProfile",
    "stepProfilerChecks",
    "stepQualityProduct",
  ],
  testdata: [
    "stepTestDataMap",
    "stepTestDataProfiler",
    "stepFakes",
    "stepFakesFill",
    "stepEnvKinds",
    "stepSynthetic",
    "stepSyntheticPrivacy",
  ],
  publish: [
    "stepDataProducts",
    "stepProductForm",
    "stepGlossary",
    "stepGlossaryTerm",
    "stepPublish",
  ],
  operate: ["stepReports", "stepObservability", "stepCache", "stepScheduled", "step23"],
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
export function stepRoute(
  index: number,
  steps: readonly TourStep[] = TOUR_STEPS,
): string | undefined {
  for (let i = index; i >= 0; i--) {
    const route = steps[i].route;
    if (route) return route;
  }
  return undefined;
}

/**
 * The steps of `itinerary` (indices into `steps`) this org can show. A step that opens Polly
 * (`pollyOpen`) to ask it live needs a model: without one Polly's launcher shows its
 * "not configured" notice instead of the panel, and the step's anchor never mounts. A step showing
 * a stored exchange (`pollyExchange`) asks nothing and stays.
 */
export function stepsTheOrgCanShow(
  itinerary: readonly number[],
  has: Pick<OrgHas, "pollyConfigured">,
  steps: readonly TourStep[] = TOUR_STEPS,
): number[] {
  if (has.pollyConfigured) return [...itinerary];
  return itinerary.filter((i) => !(steps[i].pollyOpen && steps[i].pollyExchange === undefined));
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
  scope?: RunScope,
  steps: readonly TourStep[] = TOUR_STEPS,
): number[] {
  // REQ-2010: the demo track is every step after TOUR_STEPS; a scope of TOUR_STEPS keeps its keys.
  const inScope = (step: TourStep, i: number) =>
    scope === undefined ||
    (scope === "demo"
      ? i >= TOUR_STEPS.length
      : i < TOUR_STEPS.length && TOUR_SCOPES[scope].includes(step.key));
  const out: number[] = [];
  // Whether the route currently in force is one this viewer may open. Steps before the first
  // routed step (there are none today) would inherit `true` — no gate to fail.
  let ownerAllowed = true;
  steps.forEach((step, i) => {
    if (step.route) {
      if (step.capability === undefined) {
        throw new Error(`Tour: step ${i} declares a route with no capability`);
      }
      ownerAllowed = meets(step.capability);
    }
    // The owner walk covers every step, so a scoped run whose first step is routeless still
    // resolves its owner's rights; the scope only filters what is kept.
    if (ownerAllowed && inScope(step, i)) out.push(i);
  });
  return out;
}
