// Copyright (c) 2026 Kenneth Stott
// Canary: fa9a9028-ab4a-45ba-990b-c9d20cca9b24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2010: a named demo tells its own story first. Its steps, written with the demo, become steps
// of the tour runner pointing at the demo's own objects; the tour button runs them before the
// product tour until they have been walked to their end, and the Deep Dives menu lists them first.

import * as React from "react";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, act, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";
import { useNavPayload } from "../hooks/useNavPayload";
import { MantineProvider } from "@mantine/core";
import {
  demoStepShown,
  demoTourSteps,
  demoTrackStamp,
  type DemoTourStepDef,
} from "../tour/demoTour";
import {
  NamedDemoContext,
  NO_NAMED_DEMO,
  namedDemoOf,
  type NamedDemo,
} from "../context/NamedDemoContext";
import { storageKey } from "../lib/browserStorage";
import type { OrgHas } from "../tour/tourPrerequisites";

// The counter's numbers ride along, so a test can read them off the Next button.
const t = (key: string, o?: { current?: number; total?: number }) =>
  o?.total !== undefined ? `${key} ${o.current}/${o.total}` : key;
// REQ-2013: the org has every object the product tour's steps show.
// REQ-2013: the org of each test: the weblogs demo's, unless a test names another.
const org = vi.hoisted(() => ({ current: null as unknown }));
const showExchange = vi.hoisted(() => vi.fn());
vi.mock("../hooks/useOrgHas", () => {
  // One function, as the hook's useCallback gives: a new one each render re-runs the tour's step.
  const read = () => Promise.resolve(org.current);
  return { useOrgHas: () => read };
});
vi.mock("react-i18next", () => ({ useTranslation: () => ({ t }) }));

let chunksResolve: () => void = () => {};
vi.mock("../pageChunks", () => ({
  prefetchAllPageChunks: () =>
    new Promise<void>((resolve) => {
      chunksResolve = resolve;
    }),
}));
vi.mock("../hooks/useAdminQueries", () => ({ useTourPrefetch: () => () => Promise.resolve() }));
vi.mock("../context/pollyState", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/pollyState")>()),
  usePolly: () => ({
    open: false,
    checkingConfig: false,
    unconfiguredReason: null,
    setUnconfiguredReason: () => {},
    openPolly: () => Promise.resolve(),
    closePolly: () => {},
    showExchange,
    shownExchange: null,
  }),
}));
// The viewer's rights; a test drops one mid-tour, as a role step does.
const ALL_RIGHTS = ["source_registration", "create_relationship", "query_development"];
const rights = vi.hoisted(() => ({ current: null as unknown as string[] }));
vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ loading: false, capabilities: rights.current }),
}));

const { TourProvider, useTour, resetTourStateForDemoSession } = await import("../tour/useTour");

beforeEach(() => {
  org.current = WEBLOGS_ORG;
  rights.current = ALL_RIGHTS;
});
const { TourMenu } = await import("../tour/TourMenu");

const EXAMPLES = [{ name: "1-slowest-pages-by-path", query: "query SlowestPagesByPath { a }" }];

describe("a demo's steps, as the runner walks them", () => {
  it("points a trigger step at the trigger's row on the Triggers page", () => {
    const [trigger] = demoTourSteps(
      [
        {
          key: "t",
          route: "/admin/scheduled-tasks",
          capability: "org_settings",
          trigger: "daily-review",
        },
      ],
      EXAMPLES,
    );
    expect(trigger).toEqual({
      key: "t",
      route: "/admin/scheduled-tasks",
      capability: "org_settings",
      element: '[data-testid="scheduled-task-row-daily-review"]',
    });
  });

  it("points each target at the demo's object on its page", () => {
    const defs: DemoTourStepDef[] = [
      { key: "a", route: "/sources", capability: "source_registration", source: "shop_access" },
      { key: "b", relationship: "LOGGED_FOR" },
      { key: "c", route: "/query", capability: "query_development", example: EXAMPLES[0].name },
      { key: "d", route: "/sql", capability: "query_development", sql: "SELECT 1" },
      { key: "e", role: "analyst" },
    ];
    const [source, relationship, example, sql, role] = demoTourSteps(defs, EXAMPLES);
    expect(source).toEqual({
      key: "a",
      route: "/sources",
      capability: "source_registration",
      element: '[data-testid="sources-row-shop_access"]',
    });
    expect(relationship).toEqual({ key: "b", element: '[data-tour-relationship="LOGGED_FOR"]' });
    // The step that opens the Relationships page searches it by the alias, so the row is on the
    // page's first page however many relationships the org holds.
    const [opened] = demoTourSteps(
      [
        {
          key: "j",
          route: "/relationships",
          capability: "create_relationship",
          relationship: "SCORED_AS",
        },
      ],
      EXAMPLES,
    );
    expect(opened).toEqual({
      key: "j",
      route: "/relationships?search=SCORED_AS",
      capability: "create_relationship",
      element: '[data-tour-relationship="SCORED_AS"]',
    });
    expect(example.openQuery).toEqual({ stateKey: "query", text: EXAMPLES[0].query });
    expect(example.element).toBe(".graphiql-response");
    expect(sql.openQuery).toEqual({ stateKey: "sql", text: "SELECT 1" });
    // The rows of the run, not the table an unrun tab already shows.
    expect(sql.readySelector).toContain("tbody tr:not(");
    // The picker is shown; Next opens it and picks the role.
    expect(role.element).toBe('[data-testid="sql-role"]');
    expect(role.chooseOnNext).toEqual({
      open: '[data-testid="sql-role"]',
      option: '[data-sql-role-option="analyst"]',
    });
  });

  it("shows a picture beside what the step before it showed, in its popover", () => {
    const [sql, picture] = demoTourSteps(
      [
        { key: "q", route: "/sql", capability: "query_development", sql: "SELECT 1" },
        { key: "shot", image: "app.png", image_src: "data:image/png;base64,AAAA" },
      ],
      EXAMPLES,
    );
    expect(picture.element).toBe(sql.element);
    expect(picture.image).toBe("data:image/png;base64,AAAA");
    expect(picture.route).toBeUndefined();
  });

  it("carries a link to another demo beside the step's own target", () => {
    const [linked, plain] = demoTourSteps(
      [
        {
          key: "a",
          route: "/sources",
          capability: "source_registration",
          source: "shop_access",
          demo: "churn",
        },
        { key: "b", source: "shop_db_log" },
      ],
      EXAMPLES,
    );
    expect(linked.element).toBe('[data-testid="sources-row-shop_access"]');
    expect(linked.demo).toBe("churn");
    expect(plain.demo).toBeUndefined();
  });

  it("refuses a picture with no step before it, or none sent", () => {
    expect(() =>
      demoTourSteps([{ key: "shot", image: "app.png", image_src: "data:," }], EXAMPLES),
    ).toThrow(/shot/);
    expect(() =>
      demoTourSteps(
        [
          { key: "q", route: "/sql", capability: "query_development", sql: "SELECT 1" },
          { key: "shot", image: "app.png" },
        ],
        EXAMPLES,
      ),
    ).toThrow(/shot/);
  });

  it("picks a role in the header's picker on any page but the SQL explorer", () => {
    const [onQuery, sql, onSql] = demoTourSteps(
      [
        { key: "c", route: "/query", capability: "query_development", role: "customer" },
        { key: "q", route: "/sql", capability: "query_development", sql: "SELECT 1" },
        { key: "a", role: "analyst" },
      ],
      EXAMPLES,
    );
    expect(onQuery.element).toBe('[data-testid="role-selector-trigger"]');
    expect(onQuery.chooseOnNext).toEqual({
      open: '[data-testid="role-selector-trigger"]',
      option: '[data-role-option="customer"]',
    });
    expect(sql.element).toBe('.sql-results-table, [data-testid="sql-results-error"]');
    expect(onSql.chooseOnNext?.option).toBe('[data-sql-role-option="analyst"]');
  });

  it("is ready on a statement's refusal as on its rows, and shows the refusal", () => {
    // The provisioning close, read outside its view's load window, is refused by design: the tour
    // waited for rows that never came and ended there.
    const [sql] = demoTourSteps(
      [{ key: "close", route: "/sql", capability: "query_development", sql: "SELECT 1" }],
      EXAMPLES,
    );
    document.body.innerHTML =
      '<pre data-testid="sql-results-error">The view view_close_by_category can be read only within its read window.</pre>';
    const refusal = document.querySelector('[data-testid="sql-results-error"]');
    expect(document.querySelector(sql.readySelector ?? "")).toBe(refusal);
    expect(document.querySelector(sql.element as string)).toBe(refusal);
    document.body.innerHTML =
      '<table class="sql-results-table"><tbody><tr data-testid="results-grid-empty"><td></td></tr></tbody></table>';
    expect(document.querySelector(sql.readySelector ?? "")).toBeNull();
    document.body.innerHTML = "";
  });

  it("opens a profiled table's runs and picks a graph itself, then explains it once drawn", () => {
    const [step] = demoTourSteps(
      [
        {
          key: "g",
          route: "/tables",
          capability: "table_registration",
          table: "crm.customers",
          open: "quality",
          graph: "driftStrata",
        },
      ],
      EXAMPLES,
    );
    expect(step.route).toBe("/tables?open=crm.customers&section=quality");
    expect(step.actions).toEqual([
      { do: "press", target: { testId: "table-read-view-profile-runs" } },
      { do: "press", target: { testId: "profile-tab-driftStrata" } },
    ]);
    // The drawn scene, never the loader: three.js loads on first use.
    expect(step.element).toBe('[data-testid="strata-stage"][data-ready]');
    expect(step.readySelector).toBe(step.element);
    expect(step.clickAfterNext).toBe('[data-testid="profile-runs-modal"] .mantine-Modal-close');
  });

  it("hands a set of roles, a derived role among them, to the SQL explorer", () => {
    // REQ-2057: reviewer,region__eu is no single option of the picker; the explorer offers it.
    const [set] = demoTourSteps([{ key: "s", role: "reviewer,region__eu" }], EXAMPLES);
    expect(set.openQuery).toEqual({ stateKey: "role", text: "reviewer,region__eu" });
    expect(set.element).toBe('[data-testid="sql-role"]');
    expect(set.chooseOnNext).toBeUndefined();
  });

  it("runs a statement whose relation says its own time, as written", () => {
    // REQ-1163 (amended 2026-10-25): the time is the relation's argument, in the statement.
    const sql = `SELECT * FROM "banking"."view_eod_totals"(as_recorded => '2026-10-01T17:00:00') AS e`;
    const [step] = demoTourSteps(
      [{ key: "a", route: "/sql", capability: "query_development", sql }],
      EXAMPLES,
    );
    expect(step.openQuery).toEqual({ stateKey: "sql", text: sql });
  });

  it("shows the environment switcher and picks the environment on Next", () => {
    // REQ-2053: the steps after it read that environment.
    const [env] = demoTourSteps([{ key: "e", env: "dev" }], EXAMPLES);
    expect(env.element).toBe('[data-testid="env-switcher-trigger"]');
    expect(env.chooseOnNext).toEqual({
      open: '[data-testid="env-switcher-trigger"]',
      option: '[data-env-option="dev"]',
    });
  });

  it("runs a call on each API surface's page and waits for its answer there", () => {
    const [api, jsonapi, grpc, cypher] = demoTourSteps(
      [
        { key: "a", route: "/openapi", api: "GET /data/rest/shop/pages?groupBy=path" },
        { key: "j", route: "/jsonapi", jsonapi: "/data/jsonapi/shop/pages" },
        { key: "g", route: "/grpc", grpc: "QuerySPagesGroupBy(by=[path])" },
        { key: "c", route: "/graph", cypher: "MATCH (p:S__Pages) RETURN p" },
      ],
      EXAMPLES,
    );
    // Each is the hand-off its page reads, and the mark it shows once that hand-off is answered.
    expect(api.openQuery).toEqual({
      stateKey: "openApiUrl",
      text: "GET /data/rest/shop/pages?groupBy=path",
    });
    expect(api.readySelector).toBe(".openapi-page[data-answered]");
    expect(jsonapi.openQuery).toEqual({ stateKey: "jsonapiUrl", text: "/data/jsonapi/shop/pages" });
    expect(jsonapi.readySelector).toBe('[data-testid="jsonapi-summary-tab"]');
    expect(grpc.openQuery).toEqual({
      stateKey: "grpcMethod",
      text: "QuerySPagesGroupBy(by=[path])",
    });
    expect(grpc.readySelector).toBe(".grpc-panel-right[data-answered]");
    expect(cypher.openQuery).toEqual({ stateKey: "query", text: "MATCH (p:S__Pages) RETURN p" });
    expect(cypher.readySelector).toBe('.graph-stream[data-handoff-status="done"]');
    for (const step of [api, jsonapi, grpc, cypher]) expect(step.waitMs).toBe(60000);
  });

  it("opens a data product on the Data Products page, its detail shown", () => {
    const [step] = demoTourSteps(
      [{ key: "p", route: "/data-products", product: "daily_sales" }],
      EXAMPLES,
    );
    expect(step.route).toBe("/data-products?product=daily_sales");
    expect(step.element).toBe('[data-testid="data-product-detail"]');
  });

  it("opens a statement's lineage on the Lineage page and waits for its graph", () => {
    const sql = 'SELECT address FROM "vault"."view_dim_customer"';
    const [step] = demoTourSteps([{ key: "l", route: "/lineage", lineage: sql }], EXAMPLES);
    expect(step.route).toBe(`/lineage?sql=${encodeURIComponent(sql)}`);
    expect(step.element).toBe('[data-testid="lineage-graph"]');
    expect(step.waitMs).toBe(60000);
  });

  it("groups the Graph page's frame on screen by a property, running no query", () => {
    const [run, grouped] = demoTourSteps(
      [
        { key: "c", route: "/graph", cypher: "MATCH (p:P) RETURN p" },
        { key: "g", group_by: "degTotal" },
      ],
      EXAMPLES,
    );
    expect(run.openQuery).toEqual({ stateKey: "query", text: "MATCH (p:P) RETURN p" });
    expect(grouped.navState).toEqual({ view: { groupBy: "degTotal" } });
    expect(grouped.openQuery).toBeUndefined();
    expect(grouped.element).toBe(".graph-stream .gf-frame");
  });

  it("points a table at its row on the Tables page", () => {
    const [table] = demoTourSteps([{ key: "t", table: "shop_access.shop_access" }], EXAMPLES);
    expect(table.element).toBe('[data-table-row="shop_access.shop_access"]');
  });

  it("opens Polly on a question with its stored answer, and needs that answer", () => {
    const answered = { key: "p", polly: "Which pages are slowest?", answer: "/checkout" };
    const [step] = demoTourSteps([answered], EXAMPLES);
    expect(step.pollyOpen).toBe(true);
    expect(step.pollyExchange).toEqual({ question: answered.polly, answer: "/checkout" });
    expect(step.element).toBe('[data-testid="chat-panel"]');
    // REQ-2012: nothing is asked live, so a question with no stored answer is left out.
    expect(demoStepShown(answered)).toBe(true);
    expect(demoStepShown({ key: "p", polly: answered.polly })).toBe(false);
    expect(() => demoTourSteps([{ key: "p", polly: answered.polly }], EXAMPLES)).toThrow(
      /no stored answer/,
    );
  });

  it("stamps a track by its demo and its steps' keys and routes", () => {
    const track: DemoTourStepDef[] = [{ key: "a", route: "/sources", source: "x" }];
    expect(demoTrackStamp("weblogs", track)).toBe(demoTrackStamp("weblogs", [...track]));
    expect(demoTrackStamp("weblogs", track)).not.toBe(demoTrackStamp("sonnets", track));
    expect(demoTrackStamp("weblogs", track)).not.toBe(
      demoTrackStamp("weblogs", [...track, { key: "b", source: "y" }]),
    );
    expect(demoTrackStamp("weblogs", track)).not.toBe(
      demoTrackStamp("weblogs", [{ ...track[0], route: "/tables" }]),
    );
  });

  it("hands the demo's notebook cells to the Notebook tab and waits for them to answer", () => {
    const cells = ["import provisa_client\nprovisa_client.sign_in()", "df.head()"];
    const [step] = demoTourSteps(
      [{ key: "n", route: "/notebook", capability: "query_development", notebook: true }],
      EXAMPLES,
      cells,
    );
    expect(step.navState).toEqual({ cells, autoRun: true });
    expect(step.openQuery).toBeUndefined();
    expect(step.readySelector).toBe('[data-testid="notebook-answer"]');
    expect(step.element).toBe('[data-testid="notebook-answer"]');
  });

  it("refuses a notebook step when the demo ships no notebook", () => {
    expect(() => demoTourSteps([{ key: "n", notebook: true }], EXAMPLES)).toThrow(
      /opens the demo's notebook, and it ships none/,
    );
  });

  it("opens a table's panel on the section its step describes", () => {
    const [onRow, opened] = demoTourSteps(
      [
        { key: "r", route: "/tables", table: "shop-db.customers" },
        { key: "o", route: "/tables", table: "shop-db.customers", open: "columns.testing" },
      ],
      EXAMPLES,
    );
    expect(onRow.element).toBe('[data-table-row="shop-db.customers"]');
    expect(opened.route).toBe("/tables?open=shop-db.customers&section=columns&tab=testing");
    expect(opened.element).toBe('[data-testid="table-section-columns-panel"]');
  });

  it("refuses a step naming an example the demo does not ship", () => {
    expect(() => demoTourSteps([{ key: "x", example: "9-missing" }], EXAMPLES)).toThrow(
      /names example "9-missing"/,
    );
  });
});

const WEBLOGS_ORG: OrgHas = {
  sources: new Set(["shop-db", "shop_access", "shop_db_log"]),
  tables: new Set(["shop-db.customers", "shop_access.shop_access"]),
  dataProducts: 0,
  pollyConfigured: true,
};

const WEBLOGS: NamedDemo = {
  name: "weblogs",
  exampleQueries: EXAMPLES,
  tour: [
    {
      key: "weblogsSources",
      route: "/sources",
      capability: "source_registration",
      source: "shop_access",
    },
    { key: "weblogsDbLog", source: "shop_db_log" },
  ],
  landing: null,
  notebook: [],
  orgFiles: [],
};

function Launcher() {
  const { startTour } = useTour();
  return (
    <>
      <button type="button" onClick={() => startTour()}>
        launch
      </button>
      {/* The welcome modal's Start, and ?tour=1. */}
      <button type="button" onClick={() => startTour({ restart: true })}>
        start
      </button>
    </>
  );
}

/** Where the app is: the page a tour's end leaves the visitor on. */
function Where() {
  const location = useLocation();
  return (
    <>
      <output data-testid="where">{location.pathname}</output>
      <output data-testid="where-state">{JSON.stringify(location.state ?? null)}</output>
    </>
  );
}

function renderTour(demo: NamedDemo) {
  return render(tourTree(demo));
}

function tourTree(demo: NamedDemo) {
  return (
    <MantineProvider>
      <MemoryRouter>
        <NamedDemoContext.Provider value={demo}>
          <TourProvider>
            <Launcher />
            <Where />
            {/* The demo's objects, as the Sources page paints them. */}
            <table>
              <tbody>
                <tr data-testid="sources-row-shop_access" />
                <tr data-testid="sources-row-shop_db_log" />
              </tbody>
            </table>
            {/* The Graph page's newest frame, as a Cypher step and a group_by step anchor on it. */}
            <div className="graph-stream" data-handoff-status="done">
              <div className="gf-frame" />
            </div>
          </TourProvider>
        </NamedDemoContext.Provider>
      </MemoryRouter>
    </MantineProvider>
  );
}

describe("the tour button in a named demo", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });
  afterEach(() => vi.useRealTimers());

  it("tells the demo's story first, and returns to the menu at its end", async () => {
    renderTour(WEBLOGS);
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();

    fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);
    expect(await screen.findByText("tour.steps.weblogsDbLog.title")).toBeInTheDocument();
    fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);

    // Walked to its end in this demo: the menu opens, the track marked done.
    expect(await screen.findByTestId("tour-menu")).toBeInTheDocument();
    expect(localStorage.getItem(storageKey("provisa_tour_demo_track_done"))).toBe(
      demoTrackStamp("weblogs", WEBLOGS.tour),
    );
  });

  for (const when of ["waiting", "stuck"] as const) {
    it(`skips a step whose object never appears, ${when}, to the track's next step`, async () => {
      const tour = [
        WEBLOGS.tour[0],
        // A source of the org the page here never paints (no sources-row-shop-db): its step waits, then is stuck.
        { key: "weblogsMissing", source: "shop-db" },
        WEBLOGS.tour[1],
      ];
      renderTour({ ...WEBLOGS, tour });
      fireEvent.click(screen.getByText("launch"));
      await act(async () => chunksResolve());
      expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
      fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);
      await act(async () => {
        await vi.advanceTimersByTimeAsync(when === "waiting" ? 2000 : 62000);
      });
      expect(screen.getByText(`tour.status.${when}`)).toBeInTheDocument();

      fireEvent.click(screen.getByText("tour.status.skip"));
      expect(await screen.findByText("tour.steps.weblogsDbLog.title")).toBeInTheDocument();
      expect(screen.queryByText("tour.status.waiting")).not.toBeInTheDocument();
      expect(screen.queryByText("tour.status.stuck")).not.toBeInTheDocument();
      // Nothing of the skipped step's wait is left to raise its status again.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(62000);
      });
      expect(screen.queryByText("tour.status.waiting")).not.toBeInTheDocument();
      expect(screen.queryByText("tour.status.stuck")).not.toBeInTheDocument();
      expect(screen.getByText("tour.steps.weblogsDbLog.title")).toBeInTheDocument();
    });
  }

  it("counts its steps as it started, though a role step drops one mid-tour", async () => {
    const tour = [
      WEBLOGS.tour[0],
      {
        key: "weblogsRels",
        route: "/relationships",
        capability: "create_relationship" as const,
        relationship: "HITS",
      },
      {
        key: "weblogsDbLogAgain",
        route: "/sources",
        capability: "source_registration" as const,
        source: "shop_db_log",
      },
      { key: "weblogsLast", source: "shop_access" },
    ];
    const demo = { ...WEBLOGS, tour };
    const view = renderTour(demo);
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
    const next = () => document.querySelector<HTMLElement>(".driver-popover-next-btn")!;
    expect(next().textContent).toBe("tour.nav.next 1/4");
    // The role picked leaves no right to the relationships page: its step is skipped, not recounted.
    rights.current = ["source_registration", "query_development"];
    view.rerender(tourTree(demo));
    fireEvent.click(next());
    expect(await screen.findByText("tour.steps.weblogsDbLogAgain.title")).toBeInTheDocument();
    expect(next().textContent).toBe("tour.nav.next 3/4");
  });

  it("goes on past a step whose role pick gives back the right the steps after it need", async () => {
    // As the churn tour: the analyst may open none of the pages after "back to every role", yet that
    // step is not the end; its Next picks All, which opens them again.
    const tour = [
      WEBLOGS.tour[0],
      {
        key: "weblogsAsAnalyst",
        route: "/query",
        capability: "query_development" as const,
        role: "analyst",
      },
      { key: "weblogsEveryRole", role: "all" },
      {
        key: "weblogsLast",
        route: "/sources",
        capability: "source_registration" as const,
        source: "shop_access",
      },
    ];
    const demo = { ...WEBLOGS, tour };
    // The header's role picker: a pick sets the rights every gate (and the itinerary) reads.
    function Picking() {
      const [, repaint] = React.useState(0);
      const pick = (to: string[]) => () => {
        rights.current = to;
        repaint((n) => n + 1);
      };
      return (
        <>
          {tourTree(demo)}
          <button type="button" data-testid="role-selector-trigger" />
          <button type="button" data-role-option="analyst" onClick={pick(["query_development"])} />
          <button type="button" data-role-option="all" onClick={pick(ALL_RIGHTS)} />
        </>
      );
    }
    render(<Picking />);
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    const next = () => document.querySelector<HTMLElement>(".driver-popover-next-btn")!;
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
    fireEvent.click(next());
    expect(await screen.findByText("tour.steps.weblogsAsAnalyst.title")).toBeInTheDocument();
    fireEvent.click(next());
    expect(await screen.findByText("tour.steps.weblogsEveryRole.title")).toBeInTheDocument();
    expect(rights.current).toEqual(["query_development"]);
    expect(next().textContent).toBe("tour.nav.next 3/4");
    fireEvent.click(next());
    // The All pick repaints the step's popover as it lands: read the title the popover shows now.
    await waitFor(() =>
      expect(document.querySelector(".driver-popover-title")?.textContent).toBe(
        "tour.steps.weblogsLast.title",
      ),
    );
    expect(rights.current).toEqual(ALL_RIGHTS);
  });

  it("offers a revised track again, though the one before it was walked", async () => {
    // Walked on this control plane before the track gained a step.
    const before = WEBLOGS.tour.slice(0, 1);
    localStorage.setItem(
      storageKey("provisa_tour_demo_track_done"),
      demoTrackStamp("weblogs", before),
    );
    renderTour(WEBLOGS);
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
  });

  it("shows a Polly question with its stored answer, asking nothing", async () => {
    showExchange.mockClear();
    const ask = { key: "weblogsAsk", polly: "Which pages?", answer: "/checkout" };
    renderTour({ ...WEBLOGS, tour: [WEBLOGS.tour[0], ask] });
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
    fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);
    await act(async () => {});
    expect(showExchange).toHaveBeenCalledWith({ question: "Which pages?", answer: "/checkout" });
  });

  it("regroups the graph a Cypher step drew, on the graph page, without leaving it", async () => {
    // reviewsBoughtWith then reviewsBoughtWithByDegree: the group_by step names no route and
    // stays on the Cypher step's page, handing it the view to apply to its newest frame.
    const cypher = {
      key: "weblogsGraph",
      route: "/graph",
      capability: "query_development" as const,
      cypher: "MATCH (p:Page) RETURN p",
    };
    const grouped = { key: "weblogsGrouped", group_by: "degTotal" };
    const after = {
      key: "weblogsAfter",
      route: "/sources",
      capability: "source_registration" as const,
      source: "shop_access",
    };
    renderTour({ ...WEBLOGS, tour: [WEBLOGS.tour[0], cypher, grouped, after] });
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
    const next = () => document.querySelector<HTMLElement>(".driver-popover-next-btn")!;
    fireEvent.click(next());
    expect(await screen.findByText("tour.steps.weblogsGraph.title")).toBeInTheDocument();
    fireEvent.click(next());
    expect(await screen.findByText("tour.steps.weblogsGrouped.title")).toBeInTheDocument();
    expect(screen.getByTestId("where").textContent).toBe("/graph");
    expect(JSON.parse(screen.getByTestId("where-state").textContent!)).toEqual({
      view: { groupBy: "degTotal" },
    });
  });

  it("waits for a page's answered mark, set on an element already on the page", async () => {
    const api = {
      key: "weblogsRest",
      route: "/sources",
      capability: "source_registration" as const,
      api: "GET /data/rest/shop/view_slowest_pages",
    };
    renderTour({ ...WEBLOGS, tour: [WEBLOGS.tour[0], api] });
    // The OpenAPI page's own elements, unanswered: its docs frame, and the page it marks.
    const page = document.createElement("div");
    page.className = "openapi-page";
    page.innerHTML = '<iframe class="openapi-frame"></iframe>';
    document.body.append(page);
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
    fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);
    // Past the "waiting" notice, the last change the document sees before the answer.
    expect(
      await screen.findByText("tour.status.waiting", {}, { timeout: 3000 }),
    ).toBeInTheDocument();
    await act(async () => new Promise((r) => setTimeout(r, 300)));
    expect(screen.queryByText("tour.steps.weblogsRest.title")).toBeNull();
    // The answer is in: the page gains its mark, and nothing else in the document changes.
    act(() => page.setAttribute("data-answered", ""));
    expect(await screen.findByText("tour.steps.weblogsRest.title")).toBeInTheDocument();
    page.remove();
  });

  it("leaves out a Polly question with no stored answer", async () => {
    renderTour({
      ...WEBLOGS,
      tour: [WEBLOGS.tour[0], { key: "weblogsAsk", polly: "Which pages?" }, WEBLOGS.tour[1]],
    });
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
    fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);
    expect(await screen.findByText("tour.steps.weblogsDbLog.title")).toBeInTheDocument();
    expect(screen.queryByText("tour.steps.weblogsAsk.title")).toBeNull();
  });

  it("opens the menu once the demo's track is done, the product tour not offered", async () => {
    // REQ-2013: the weblogs demo has none of the standard demo's pet store, which the product
    // tour's query card shows.
    localStorage.setItem(
      storageKey("provisa_tour_demo_track_done"),
      demoTrackStamp("weblogs", WEBLOGS.tour),
    );
    renderTour(WEBLOGS);
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByTestId("tour-menu")).toBeInTheDocument();
    expect(screen.queryByText("tour.steps.weblogsSources.title")).toBeNull();
    expect(screen.queryByTestId("tour-menu-core-tour")).toBeNull();
    expect(screen.getByTestId("tour-demo-track-done")).toBeInTheDocument();
  });

  it("runs the product tour where no named demo has a tour", async () => {
    const { STANDARD_DEMO_ORG } = await import("../test-utils/tourOrg");
    org.current = STANDARD_DEMO_ORG;
    renderTour({
      name: null,
      exampleQueries: [],
      tour: [],
      landing: null,
      notebook: [],
      orgFiles: [],
    });
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    await act(async () => vi.advanceTimersByTime(2000));
    expect(screen.queryByText("tour.steps.weblogsSources.title")).toBeNull();
    // The product tour's first step is under way instead (its page is not mounted here).
    expect(screen.getByText("tour.status.waiting")).toBeInTheDocument();
  });
});

describe("the end of a named demo's tour: where next?", () => {
  // A demo that names its landing page ends on a choice, never on a Done into another tour.
  const ENDING: NamedDemo = {
    ...WEBLOGS,
    landing: { route: "/sql", capability: "query_development" },
  };

  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });
  afterEach(() => vi.useRealTimers());

  async function toTheLastCard() {
    renderTour(ENDING);
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
    fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);
    expect(await screen.findByText("tour.steps.weblogsDbLog.title")).toBeInTheDocument();
    // The last card: the choice, its note, and no Done or second Deep Dives button beside it.
    expect(screen.getByText("tour.nav.whereNext")).toBeInTheDocument();
    expect(screen.getByText("tour.nav.anyTime")).toBeInTheDocument();
    expect(document.querySelector(".driver-popover-next-btn")).not.toBeVisible();
    expect(screen.queryByTestId("tour-deep-dives")).toBeNull();
  }

  it("Explore the Deep Dives opens the Deep Dives menu and marks the track walked", async () => {
    await toTheLastCard();
    fireEvent.click(screen.getByTestId("tour-where-next-deep-dives"));
    expect(await screen.findByTestId("tour-menu")).toBeInTheDocument();
    expect(localStorage.getItem(storageKey("provisa_tour_demo_track_done"))).toBe(
      demoTrackStamp("weblogs", ENDING.tour),
    );
  });

  it("Look around on my own ends the tour on the demo's landing page, the track walked", async () => {
    await toTheLastCard();
    fireEvent.click(screen.getByTestId("tour-where-next-own"));
    await act(async () => {});
    expect(screen.getByTestId("where")).toHaveTextContent("/sql");
    expect(screen.queryByTestId("tour-menu")).toBeNull(); // no menu: the visitor looks around
    expect(document.querySelector(".driver-popover")).toBeNull();
    expect(localStorage.getItem(storageKey("provisa_tour_demo_track_done"))).toBe(
      demoTrackStamp("weblogs", ENDING.tour),
    );
  });
});

describe("a demo step linking another demo", () => {
  // A demo that builds on another links back to it from its story.
  const LINKED: NamedDemo = {
    ...WEBLOGS,
    tour: [{ ...WEBLOGS.tour[0], demo: "churn" }, WEBLOGS.tour[1]],
  };

  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });
  afterEach(() => vi.useRealTimers());

  it("opens the other demo from its popover, leaving the tour without the menu", async () => {
    renderTour(LINKED);
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
    const open = screen.getByTestId("tour-open-demo");
    expect(open).toHaveTextContent("tour.nav.openDemo");
    fireEvent.click(open);
    await act(async () => {});
    expect(screen.getByTestId("where")).toHaveTextContent("/demos/churn");
    expect(screen.queryByTestId("tour-menu")).toBeNull();
    expect(document.querySelector(".driver-popover")).toBeNull();
  });

  it("offers no link on a step that names no other demo", async () => {
    renderTour(LINKED);
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
    fireEvent.click(document.querySelector<HTMLElement>(".driver-popover-next-btn")!);
    expect(await screen.findByText("tour.steps.weblogsDbLog.title")).toBeInTheDocument();
    expect(screen.queryByTestId("tour-open-demo")).toBeNull();
  });
});

describe("a freshly onboarded org, started as no named demo", () => {
  // The status of a deployment no named demo was started as: the onboard-org flow's.
  const FRESH = {
    named_demo: null,
    example_queries: [],
    demo_tour: [],
    demo_landing: null,
    demo_notebook: [],
    demo_org_files: [],
    orgFiles: [],
  };

  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
    vi.useFakeTimers({ shouldAdvanceTime: true });
  });
  afterEach(() => vi.useRealTimers());

  it("has no named demo: no demo track and no examples", () => {
    expect(namedDemoOf(FRESH)).toBe(NO_NAMED_DEMO);
  });

  it("opens the tour without a demo track", async () => {
    org.current = { sources: new Set(), tables: new Set(), dataProducts: 0, pollyConfigured: true };
    localStorage.setItem("provisa_tour_seen", "true"); // the tour button opens the menu
    renderTour(namedDemoOf(FRESH));
    fireEvent.click(screen.getByText("launch"));
    expect(await screen.findByTestId("tour-menu")).toBeInTheDocument();
    expect(screen.queryByTestId("tour-demo-track-done")).toBeNull();
    expect(screen.queryByText("tour.steps.weblogsSources.title")).toBeNull();
  });
});

describe("starting the tour from the top in a named demo", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });

  it("starts with the demo's story, even once it has been walked", async () => {
    localStorage.setItem(
      storageKey("provisa_tour_demo_track_done"),
      demoTrackStamp("weblogs", WEBLOGS.tour),
    );
    renderTour(WEBLOGS);
    fireEvent.click(screen.getByText("start"));
    await act(async () => chunksResolve());
    expect(await screen.findByText("tour.steps.weblogsSources.title")).toBeInTheDocument();
  });
});

describe("a fresh demo session", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });

  it("forgets the track walked by the visitor before", () => {
    const key = storageKey("provisa_tour_demo_track_done");
    localStorage.setItem(key, demoTrackStamp("weblogs", WEBLOGS.tour));
    resetTourStateForDemoSession();
    expect(localStorage.getItem(key)).toBeNull();
  });
});

describe("a demo step's statement", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });

  it("is handed to its explorer once for each visit to the step", async () => {
    const delivered: unknown[] = [];
    function Explorer() {
      useNavPayload<{ sql?: string }>((payload) => delivered.push(payload.sql));
      return <table className="sql-results-table" />;
    }
    render(
      <MantineProvider>
        {/* Entered from another page, as the tour enters an explorer: the move re-renders the
            router, which re-runs the step. */}
        <MemoryRouter initialEntries={["/sources"]}>
          <NamedDemoContext.Provider
            value={{
              name: "weblogs",
              exampleQueries: [],
              tour: [
                { key: "s1", route: "/sql", capability: "query_development", sql: "SELECT 1" },
              ],
              landing: null,
              notebook: [],
              orgFiles: [],
            }}
          >
            <TourProvider>
              <Launcher />
              <Routes>
                <Route path="/sql" element={<Explorer />} />
              </Routes>
            </TourProvider>
          </NamedDemoContext.Provider>
        </MemoryRouter>
      </MantineProvider>,
    );
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    await act(async () => new Promise((r) => setTimeout(r, 300)));
    // Handing it over moves the router, which re-runs the step: the statement opens once, not twice.
    expect(delivered).toEqual(["SELECT 1"]);
  });
});

describe("a demo step's anchor rendered again", () => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });

  it("is lit again where the page now shows it", async () => {
    // The SQL explorer renders its results again once its answer is in: the step lit a table no
    // longer on the page, and its popover sat in the window's corner.
    function Explorer() {
      const [n, setN] = React.useState(0);
      return (
        <>
          <button type="button" onClick={() => setN((v) => v + 1)}>
            again
          </button>
          <table className="sql-results-table" key={n} data-render={n}>
            <tbody>
              <tr>
                <td>1</td>
              </tr>
            </tbody>
          </table>
        </>
      );
    }
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/sql"]}>
          <NamedDemoContext.Provider
            value={{
              name: "weblogs",
              exampleQueries: [],
              tour: [
                { key: "s1", route: "/sql", capability: "query_development", sql: "SELECT 1" },
              ],
              landing: null,
              notebook: [],
              orgFiles: [],
            }}
          >
            <TourProvider>
              <Launcher />
              <Routes>
                <Route path="/sql" element={<Explorer />} />
              </Routes>
            </TourProvider>
          </NamedDemoContext.Provider>
        </MemoryRouter>
      </MantineProvider>,
    );
    fireEvent.click(screen.getByText("launch"));
    await act(async () => chunksResolve());
    await act(async () => new Promise((r) => setTimeout(r, 300)));
    expect(document.querySelector(".driver-active-element")?.getAttribute("data-render")).toBe("0");
    await act(async () => fireEvent.click(screen.getByText("again")));
    await act(async () => new Promise((r) => setTimeout(r, 100)));
    const lit = document.querySelector(".driver-active-element");
    expect(lit?.getAttribute("data-render")).toBe("1");
    expect(lit?.isConnected).toBe(true);
  });
});

describe("the Deep Dives menu in a named demo", () => {
  it("lists the demo's track first, marked done once walked", () => {
    const onDemo = vi.fn();
    render(
      <MantineProvider>
        <TourMenu
          opened
          topics={[{ id: "relationships", steps: 6 }]}
          completed={[]}
          demo={{ name: "weblogs", steps: 10, done: true }}
          onDemo={onDemo}
          onPick={() => {}}
          onCore={() => {}}
          onClose={() => {}}
        />
      </MantineProvider>,
    );
    const track = screen.getByTestId("tour-demo-track");
    expect(track.compareDocumentPosition(screen.getByTestId("tour-topic-relationships"))).toBe(
      Node.DOCUMENT_POSITION_FOLLOWING,
    );
    expect(screen.getByText("tour.demos.weblogs.title")).toBeInTheDocument();
    expect(screen.getByTestId("tour-demo-track-done")).toBeInTheDocument();
    fireEvent.click(track);
    expect(onDemo).toHaveBeenCalledOnce();
  });
});
