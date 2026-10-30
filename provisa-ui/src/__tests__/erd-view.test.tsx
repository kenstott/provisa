// Copyright (c) 2026 Kenneth Stott
// Canary: 6e2f4b18-93d7-4c05-b8a1-d47c0e95f3a2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2187: one ERD view hosted in place on the Tables and the Relationships page, each with its own
// panel, and a focus that is a link. Both real pages behind one router, drawn by the real cytoscape:
// the clicks are tap events on the diagram's own elements.

import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { Route, Routes, useLocation } from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render } from "../test-utils/render";
import type { RegisteredTable } from "../types/admin";

const drawn = vi.hoisted(() => ({
  cy: null as null | {
    getElementById(id: string): {
      length: number;
      emit(event: string): void;
      hasClass(c: string): boolean;
    };
  },
}));

// The real cytoscape, with the instance each ERD draws kept for the test to click on.
vi.mock("cytoscape", async (importOriginal) => {
  const real = (await importOriginal<{ default: (o: unknown) => unknown }>()).default;
  const keeping = Object.assign((o: unknown) => {
    drawn.cy = real(o) as typeof drawn.cy;
    return drawn.cy;
  }, real);
  return { default: keeping };
});

// jsdom has no canvas: the real cytoscape draws on a context that accepts every call.
HTMLCanvasElement.prototype.getContext = function (this: HTMLCanvasElement) {
  const state: Record<string, unknown> = {};
  return new Proxy(state, {
    get: (t, k: string) =>
      k in t
        ? t[k]
        : k === "measureText"
          ? () => ({ width: 10 })
          : k === "canvas"
            ? this
            : () => undefined,
    set: (t, k: string, v) => {
      t[k] = v;
      return true;
    },
  });
} as unknown as typeof HTMLCanvasElement.prototype.getContext;

function table(id: number, tableName: string, domainId: string): RegisteredTable {
  const serving = "cache" as const;
  return {
    id,
    sourceId: "warehouse",
    domainId,
    schemaName: "public",
    tableName,
    alias: null,
    description: null,
    cacheTtl: 60,
    roleTtl: [],
    pagingKind: null,
    pagination: null,
    pagingCeilingRows: null,
    replicate: null,
    region: null,
    draft: false,
    rowMaterialize: false,
    loadProtected: null,
    offPeakWindow: null,
    offPeakTz: null,
    refreshPolicySummary: { text: serving, serving, warning: null },
    gqlNamingConvention: null,
    watermarkColumn: null,
    changeSignal: null,
    probeQuery: null,
    probeType: null,
    columns: [],
    writeGrants: [],
    uniqueConstraints: [],
    graphqlFieldName: null,
    timeTravel: null,
    dqDataset: null,
    qualityCheckers: [],
    apiEndpoint: null,
    apiEndpointError: null,
    viewSql: null,
    dqContract: null,
    materialize: false,
    mvRefreshInterval: 0,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 0,
    pushDebounceQuiet: 0,
    pushDebounceMaxDelay: 5,
    mvConsistency: "shared",
    mvPreprocess: null,
    mvHoldOnFailingChecks: false,
    businessKey: [],
    validTime: null,
    recordedTime: null,
    mvPersist: "replace",
    mvPrimaryKey: [],
    mvIncremental: false,
    mvCalendar: null,
    mvGrain: null,
    mvAllowedLateness: 0,
    mvExpectedEvents: null,
    mvBusinessDayGrain: false,
    productId: null,
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    writeOps: ["delete", "insert", "update"],
    live: null,
    implicitMeasures: [],
    implicitDimensions: [],
  };
}

const relationship = (id: string, source: number, target: number) => ({
  id,
  sourceTableId: source,
  targetTableId: target,
  sourceTableName: `t${source}`,
  targetTableName: `t${target}`,
  sourceDomainId: "sales",
  sourceColumn: "a",
  targetColumn: "b",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: 300,
  targetFunctionName: null,
  functionArg: null,
  alias: null,
  graphqlAlias: null,
  computedCypherAlias: null,
  cypherType: null,
  disableCypher: false,
  sameEntity: false,
  viaTableId: null,
  viaTableName: null,
  viaSourceColumn: null,
  viaTargetColumn: null,
  viaTypeColumn: null,
  viaTypeValue: null,
  parameters: [],
});

const REFETCH = vi.fn();
const NONE: never[] = [];
const TABLES = {
  tables: [
    table(1, "orders", "sales"),
    table(2, "customers", "sales"),
    table(3, "ledger", "finance"),
    table(4, "invoices", "finance"),
    // A domain of orphan tables only: no relationship touches it.
    table(5, "audit_log", "ops"),
    table(6, "scratch", "ops"),
  ],
  loading: false,
  refetch: REFETCH,
};
const RELS = {
  relationships: [relationship("orders-customers", 1, 2), relationship("ledger-invoices", 3, 4)],
  loading: false,
  refetch: REFETCH,
};
const DOMAINS = {
  domains: [
    { id: "sales", description: "" },
    { id: "finance", description: "" },
    { id: "ops", description: "" },
  ],
  loading: false,
  refetch: REFETCH,
};
const AUTH = {
  role: "admin",
  selectedRoles: ["admin"],
  capabilities: ["admin", "table_registration", "create_relationship"],
  domainAccess: ["*"],
};
const FILTER = {
  checkedDomains: new Set<string>(),
  domains: ["sales", "finance", "ops"],
  domainsEnabled: true,
  ensureDomainChecked: vi.fn(),
};

vi.mock("../context/DomainFilterContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/DomainFilterContext")>()),
  useDomainFilter: () => FILTER,
}));
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => AUTH,
}));
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchSettings: vi.fn().mockResolvedValue({
    redirect: { enabled: false, threshold: 10000, default_format: "json", ttl: 3600 },
    sampling: { default_sample_size: 1000 },
    cache: { default_ttl: 300 },
    naming: { domain_prefix: false, convention: "none" },
  }),
  fetchCandidates: vi.fn().mockResolvedValue([]),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
}));
vi.mock("../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../components/TagControl", () => ({ TagControl: () => null }));
vi.mock("../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useRegionQueries")>()),
  useRegionChoices: () => ({ regions: [], connected: null }),
  useSetTableDraft: () => vi.fn(),
  useSetTableRegion: () => vi.fn(),
}));
vi.mock("../hooks/useAdminOpsQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminOpsQueries")>()),
  usePurgeCacheByTable: () => ({ purgeCacheByTable: vi.fn(), loading: false }),
  useUpdateTableRoleTtl: () => ({ updateTableRoleTtl: vi.fn(), loading: false }),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useTables: () => TABLES,
  useTableDetails: () => TABLES,
  useSources: () => ({ sources: NONE, loading: false, refetch: REFETCH }),
  useDomains: () => DOMAINS,
  useRoles: () => ({ roles: NONE, loading: false, refetch: REFETCH }),
  useDataProducts: () => ({ dataProducts: NONE, loading: false, refetch: REFETCH }),
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useDeleteRelationship: () => ({ deleteRelationship: vi.fn() }),
  useAvailableColumnsMetadataLazy: () => ({
    fetchColumns: async () => [],
    late: null,
    starting: false,
    startingTimedOut: false,
  }),
  useRefreshPolicyPreview: () => async () => null,
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { RelationshipsPage } from "../pages/RelationshipsPage";
import { TablesPage } from "../pages/TablesPage";

function Where() {
  const { pathname, search } = useLocation();
  return <output data-testid="where">{`${pathname}${search}`}</output>;
}

const open = (url: string) =>
  render(
    <>
      <Routes>
        <Route path="/tables" element={<TablesPage />} />
        <Route path="/relationships" element={<RelationshipsPage />} />
      </Routes>
      <Where />
    </>,
    { initialEntries: [url] },
  );

const where = () => screen.getByTestId("where").textContent as string;
const param = (key: string) => new URL(where(), "http://x").searchParams.get(key);

/** The element ids drawn, once the diagram holds `id`. */
const drawnHas = (id: string) => (drawn.cy?.getElementById(id).length ?? 0) > 0;
const diagramHolds = async (id: string) => waitFor(() => expect(drawnHas(id)).toBe(true));
const tap = (id: string) => act(() => drawn.cy?.getElementById(id).emit("tap"));
const highlighted = (id: string) => drawn.cy?.getElementById(id).hasClass("erd-focused");
const settled = (ids: string) =>
  waitFor(() => expect(screen.getByTestId("erd-panel")).toHaveAttribute("data-highlighted", ids));
const toggle = (page: "tables" | "relationships") =>
  fireEvent.click(screen.getByTestId(`${page}-erd-toggle`));

beforeEach(() => {
  drawn.cy = null;
  FILTER.checkedDomains = new Set();
});

describe("the requirement's scenario", () => {
  it("swaps in place, focuses from the panel, reloads the same, and Show all keeps the filter", async () => {
    FILTER.checkedDomains = new Set(["sales"]);
    const page = open("/tables");
    await screen.findByTestId("tables-list");

    // The ERD icon: the ERD replaces the list in place, and no modal opens.
    toggle("tables");
    expect(await screen.findByTestId("erd-panel")).toBeInTheDocument();
    expect(screen.queryByTestId("tables-list")).toBeNull();
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(within(screen.getByTestId("tables-split")).getByTestId("erd-panel")).toBeTruthy();
    await diagramHolds("t:1");
    expect(drawnHas("t:3")).toBe(false); // the page's filter stands

    // A table clicked: its panel opens, Focus in the head, and no Add to focus with no focus on.
    tap("t:1");
    const panel = await screen.findByTestId("tables-split-panel");
    expect(within(panel).getByTestId("tables-split-title")).toHaveTextContent("orders");
    expect(screen.queryByTestId("erd-add-to-focus")).toBeNull();

    // Focus: only the table's domain, the table highlighted, and the URL names both.
    fireEvent.click(screen.getByTestId("erd-focus-table"));
    await waitFor(() => expect(param("domains")).toBe("sales"));
    expect(param("objects")).toBe("sales.orders");
    expect(param("view")).toBe("erd");
    await settled("1");
    expect(highlighted("t:1")).toBe(true);
    expect(highlighted("t:2")).toBe(false);
    expect(screen.getByTestId("erd-focus-bar")).toHaveTextContent("sales.orders");

    // That URL opened afresh shows the same view.
    const url = where();
    page.unmount();
    drawn.cy = null;
    FILTER.checkedDomains = new Set();
    const again = open(url);
    await diagramHolds("t:1");
    await settled("1");
    expect(drawnHas("t:2")).toBe(true);
    expect(drawnHas("t:3")).toBe(false);
    expect(highlighted("t:1")).toBe(true);
    expect(screen.getByTestId("erd-focus-bar")).toHaveTextContent("sales.orders");
    again.unmount();

    // Show all: the focus clears, and the page's domain filter still stands.
    drawn.cy = null;
    FILTER.checkedDomains = new Set(["sales"]);
    open(url);
    await settled("1");
    fireEvent.click(screen.getByTestId("erd-show-all"));
    await waitFor(() => expect(param("objects")).toBeNull());
    expect(param("domains")).toBeNull();
    expect(param("view")).toBe("erd");
    await settled("");
    expect(screen.queryByTestId("erd-focus-bar")).toBeNull();
    expect(drawnHas("t:1")).toBe(true);
    expect(drawnHas("t:3")).toBe(false);
    expect(FILTER.checkedDomains).toEqual(new Set(["sales"]));
  });
});

describe("the toggle", () => {
  it("is pressed while the view is on, and off goes back to the list without the focus", async () => {
    open("/tables?view=erd&domains=sales&objects=sales.orders");
    await screen.findByTestId("erd-panel");
    expect(screen.getByTestId("tables-erd-toggle")).toHaveAttribute("aria-pressed", "true");
    toggle("tables");
    expect(await screen.findByTestId("tables-list")).toBeInTheDocument();
    expect(screen.queryByTestId("erd-panel")).toBeNull();
    expect(screen.getByTestId("tables-erd-toggle")).toHaveAttribute("aria-pressed", "false");
    expect(where()).toBe("/tables");
  });

  it("is the same on the Relationships page, hosted in its own split and no modal", async () => {
    open("/relationships");
    await screen.findByTestId("relationships-list");
    toggle("relationships");
    expect(await screen.findByTestId("erd-panel")).toBeInTheDocument();
    expect(screen.queryByTestId("relationships-list")).toBeNull();
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(within(screen.getByTestId("relationships-split")).getByTestId("erd-panel")).toBeTruthy();
    expect(param("view")).toBe("erd");
  });
});

describe("a focus link", () => {
  it("with only objects shows their own domains and highlights them", async () => {
    open("/tables?view=erd&objects=finance.ledger");
    await diagramHolds("t:3");
    await settled("3");
    expect(drawnHas("t:4")).toBe(true);
    expect(drawnHas("t:1")).toBe(false);
    expect(highlighted("t:3")).toBe(true);
    expect(highlighted("t:4")).toBe(false);
  });

  it("with only domains shows them and highlights nothing", async () => {
    open("/tables?view=erd&domains=finance");
    await diagramHolds("t:3");
    await settled("");
    expect(drawnHas("t:1")).toBe(false);
    expect(highlighted("t:3")).toBe(false);
  });

  it("with both shows only the domains, and overrides the filter without changing it", async () => {
    FILTER.checkedDomains = new Set(["sales"]);
    open("/relationships?view=erd&domains=finance&objects=finance.ledger");
    await diagramHolds("t:3");
    await settled("3");
    expect(drawnHas("t:1")).toBe(false);
    expect(FILTER.checkedDomains).toEqual(new Set(["sales"]));
    expect(FILTER.ensureDomainChecked).not.toHaveBeenCalled();
  });

  it("names what does not exist in the bar, and the rest still works", async () => {
    open("/tables?view=erd&domains=sales,atlantis&objects=sales.orders,sales.ghosts");
    await diagramHolds("t:1");
    await settled("1");
    const missing = screen.getByTestId("erd-focus-missing");
    expect(missing).toHaveTextContent("atlantis");
    expect(missing).toHaveTextContent("sales.ghosts");
    expect(missing).not.toHaveTextContent("sales.orders");
    expect(drawnHas("t:3")).toBe(false);
    expect(highlighted("t:1")).toBe(true);
  });

  it("naming nothing that exists draws nothing and still offers the way out", async () => {
    open("/tables?view=erd&objects=sales.ghosts");
    expect(await screen.findByTestId("erd-focus-missing")).toHaveTextContent("sales.ghosts");
    await waitFor(() => expect(drawn.cy).not.toBeNull());
    expect(drawnHas("t:1")).toBe(false);
    fireEvent.click(screen.getByTestId("erd-show-all"));
    await diagramHolds("t:1");
    expect(screen.queryByTestId("erd-focus-bar")).toBeNull();
  });

  it("opens the panel the link names, as Open in ERD's does", async () => {
    open("/tables?view=erd&objects=sales.customers&table=2");
    await settled("2");
    expect(await screen.findByTestId("tables-split-title")).toHaveTextContent("customers");
    expect(highlighted("t:2")).toBe(true);
  });

  it("the old parameter opens nothing", async () => {
    open("/relationships?erd=2");
    await screen.findByTestId("relationships-list");
    expect(screen.queryByTestId("erd-panel")).toBeNull();
  });
});

describe("Add to focus", () => {
  it("adds the table and its domain if new, written as the link is, commas plain", async () => {
    open("/tables?view=erd&domains=sales&objects=sales.orders&table=1");
    await settled("1");
    // The table is in the focus already.
    expect(await screen.findByTestId("erd-add-to-focus")).toBeDisabled();

    // Show all draws the other domain, and Focus starts a new focus on one of its tables.
    fireEvent.click(screen.getByTestId("erd-show-all"));
    await settled("");
    await diagramHolds("t:3");
    tap("t:3");
    await waitFor(() => expect(param("table")).toBe("3"));
    fireEvent.click(await screen.findByTestId("erd-focus-table"));
    await waitFor(() => expect(param("domains")).toBe("finance"));
    await settled("3");

    // The other table of the domain: no new domain.
    tap("t:4");
    await waitFor(() => expect(param("table")).toBe("4"));
    fireEvent.click(await screen.findByTestId("erd-add-to-focus"));
    await waitFor(() => expect(param("objects")).toBe("finance.ledger,finance.invoices"));
    expect(param("domains")).toBe("finance");
    await settled("3,4");
    expect(where()).toContain("objects=finance.ledger,finance.invoices");
  });

  it("is not on the Relationships page: its panel is the relationship's own", async () => {
    open(
      "/relationships?view=erd&domains=sales&objects=sales.orders&relationship=orders-customers",
    );
    await screen.findByTestId("relationships-split-title");
    expect(screen.queryByTestId("erd-focus-table")).toBeNull();
    expect(screen.queryByTestId("erd-add-to-focus")).toBeNull();
  });

  it("adds a table of a new domain to the domains list", async () => {
    open("/tables?view=erd&domains=finance&table=1");
    await screen.findByTestId("tables-split-title");
    fireEvent.click(await screen.findByTestId("erd-add-to-focus"));
    await waitFor(() => expect(param("domains")).toBe("finance,sales"));
    expect(param("objects")).toBe("sales.orders");
    await settled("1");
  });

  it("adds to a link of objects alone without starting a domains list", async () => {
    open("/tables?view=erd&objects=finance.ledger&table=1");
    await screen.findByTestId("tables-split-title");
    fireEvent.click(await screen.findByTestId("erd-add-to-focus"));
    await waitFor(() => expect(param("objects")).toBe("finance.ledger,sales.orders"));
    expect(param("domains")).toBeNull();
    await settled("3,1");
  });
});

describe("clicks in the diagram", () => {
  it("a line on Relationships opens its panel, and closing it drops it from the link", async () => {
    open("/relationships?view=erd");
    await diagramHolds("r:orders-customers");
    tap("r:orders-customers");
    expect(await screen.findByTestId("relationships-split-title")).toHaveTextContent(
      "orders-customers",
    );
    expect(param("relationship")).toBe("orders-customers");
    fireEvent.click(screen.getByTestId("relationships-split-close"));
    await waitFor(() => expect(param("relationship")).toBeNull());
    expect(screen.queryByTestId("relationships-split-panel")).toBeNull();
  });

  it("a table on Tables reopens after its panel is closed", async () => {
    open("/tables?view=erd");
    await diagramHolds("t:1");
    tap("t:1");
    await screen.findByTestId("tables-split-title");
    fireEvent.click(screen.getByTestId("tables-split-close"));
    await waitFor(() => expect(screen.queryByTestId("tables-split-panel")).toBeNull());
    expect(param("table")).toBeNull();
    tap("t:1");
    expect(await screen.findByTestId("tables-split-title")).toHaveTextContent("orders");
  });

  it("a line on Tables goes to the Relationships ERD with its panel open, the focus carried", async () => {
    open("/tables?view=erd&domains=sales&objects=sales.orders");
    await settled("1");
    tap("r:orders-customers");
    await waitFor(() => expect(where()).toMatch(/^\/relationships\?/));
    expect(param("view")).toBe("erd");
    expect(param("domains")).toBe("sales");
    expect(param("objects")).toBe("sales.orders");
    expect(param("relationship")).toBe("orders-customers");
    expect(await screen.findByTestId("relationships-split-title")).toHaveTextContent(
      "orders-customers",
    );
    await settled("1");
    expect(screen.getByTestId("erd-focus-bar")).toHaveTextContent("sales.orders");
  });

  it("a table on Relationships goes to the Tables ERD with its panel open, the focus carried", async () => {
    open("/relationships?view=erd&domains=sales&objects=sales.orders");
    await settled("1");
    tap("t:2");
    await waitFor(() => expect(where()).toMatch(/^\/tables\?/));
    expect(param("view")).toBe("erd");
    expect(param("domains")).toBe("sales");
    expect(param("objects")).toBe("sales.orders");
    expect(param("table")).toBe("2");
    expect(await screen.findByTestId("tables-split-title")).toHaveTextContent("customers");
    await settled("1");
    expect(screen.getByTestId("erd-focus-table")).toBeInTheDocument();
  });
});

describe("a domain of orphan tables only", () => {
  it.each(["tables", "relationships"] as const)(
    "is drawn on the %s ERD with only that domain ticked",
    async (page) => {
      FILTER.checkedDomains = new Set(["ops"]);
      open(`/${page}?view=erd`);
      await diagramHolds("t:5");
      expect(drawnHas("d:ops")).toBe(true);
      expect(drawnHas("t:6")).toBe(true);
      expect(drawnHas("t:1")).toBe(false);
      const node = (id: string) =>
        (drawn.cy as unknown as { getElementById(id: string): { style(k: string): string } }).getElementById(id);
      // Laid out and shown: the layout leaves the orphans displayed.
      await waitFor(() => expect(node("t:5").style("display")).toBe("element"));
      expect(node("d:ops").style("display")).toBe("element");
    },
  );
});

describe("the page's filter box on the ERD", () => {
  const dimmed = (id: string) =>
    (drawn.cy as unknown as { getElementById(id: string): { hasClass(c: string): boolean } })
      .getElementById(id)
      .hasClass("erd-dim");
  const type = (value: string) =>
    fireEvent.change(screen.getByTestId("filter-input"), { target: { value } });

  it("on Tables marks the tables that match and dims the rest, rebuilding and moving nothing", async () => {
    open("/tables?view=erd");
    await diagramHolds("t:1");
    await waitFor(() => expect(highlighted("t:1")).toBe(false));
    const cy = drawn.cy;
    const at = JSON.stringify(
      (drawn.cy as unknown as { getElementById(i: string): { position(): unknown } })
        .getElementById("t:3")
        .position(),
    );
    type("orders");
    await waitFor(() => expect(dimmed("t:3")).toBe(true));
    expect(dimmed("t:1")).toBe(false);
    expect(dimmed("d:sales")).toBe(false);
    expect(dimmed("d:finance")).toBe(true);
    expect(dimmed("r:ledger-invoices")).toBe(true);
    expect(drawn.cy).toBe(cy);
    expect(
      JSON.stringify(
        (drawn.cy as unknown as { getElementById(i: string): { position(): unknown } })
          .getElementById("t:3")
          .position(),
      ),
    ).toBe(at);
    // It is not in the link.
    expect(param("search")).toBeNull();
    // Clearing restores everything.
    type("");
    await waitFor(() => expect(dimmed("t:3")).toBe(false));
    expect(dimmed("d:finance")).toBe(false);
    expect(dimmed("r:ledger-invoices")).toBe(false);
  });

  it("on Relationships matches lines by source or target and dims the rest", async () => {
    open("/relationships?view=erd");
    await diagramHolds("r:orders-customers");
    type("customers");
    await waitFor(() => expect(dimmed("r:ledger-invoices")).toBe(true));
    expect(dimmed("r:orders-customers")).toBe(false);
    expect(dimmed("t:1")).toBe(false); // the line's source
    expect(dimmed("t:2")).toBe(false); // its target
    expect(dimmed("t:3")).toBe(true);
    type("t3"); // a source that is no table's name matches nothing
    await waitFor(() => expect(dimmed("r:orders-customers")).toBe(true));
  });
});
