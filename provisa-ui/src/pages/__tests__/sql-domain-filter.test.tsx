// Copyright (c) 2026 Kenneth Stott
// Canary: a2a5e20e-1b50-43ef-a501-c745564898dd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1723: the SCHEMA tree already filtered by the checked-domains selector, but the
// Metrics/Facts/Dimensions groups above it read the unfiltered table and metric lists, so
// unchecking a domain left its facts, dimensions, and derived metrics visible regardless.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import { Fragment } from "react";
import type { RegisteredTable, Metric } from "../../types/admin";

vi.mock("react-router-dom", () => ({
  MemoryRouter: ({ children }: { children: React.ReactNode }) => <Fragment>{children}</Fragment>,
  useNavigate: () => vi.fn(),
  useLocation: () => ({ state: null, pathname: "/sql", search: "", hash: "", key: "x" }),
}));

import { MemoryRouter } from "react-router-dom";

const idbStore = new Map<string, unknown>();
vi.mock("idb-keyval", () => ({
  get: vi.fn(async (k: string) => idbStore.get(k)),
  set: vi.fn(async (k: string, v: unknown) => void idbStore.set(k, v)),
  del: vi.fn(async (k: string) => void idbStore.delete(k)),
}));

vi.mock("@uiw/react-codemirror", () => ({
  default: ({ value, onChange }: { value: string; onChange?: (v: string) => void }) => (
    <textarea data-testid="sql-editor" value={value} onChange={(e) => onChange?.(e.target.value)} />
  ),
}));
vi.mock("@codemirror/lang-sql", () => ({ sql: () => [], PostgreSQL: {} }));
vi.mock("@codemirror/theme-one-dark", () => ({ oneDark: [] }));
vi.mock("@codemirror/view", () => ({ EditorView: { lineWrapping: [] } }));

// Only "sales" is checked — REQ-1723's regression is whether "ops-fact"/"ops-dim"/"ops-metric"
// (all in the "reporting" domain) leak into the panel anyway.
vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set(["sales"]) }),
}));

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "admin", capabilities: [] }, selectedRoles: [] }),
}));

vi.mock("../../hooks/useCapability", () => ({ useCapability: () => true }));

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  runSql: vi.fn(),
  nlToSql: vi.fn(),
}));

function table(overrides: Partial<RegisteredTable>): RegisteredTable {
  return {
    id: 1,
    sourceId: "src1",
    domainId: "sales",
    schemaName: "public",
    tableName: "t",
    alias: null,
    description: null,
    cacheTtl: null,
    preferMaterialized: null,
    loadProtected: null,
    offPeakWindow: null,
    offPeakTz: null,
    refreshPolicySummary: null,
    gqlNamingConvention: null,
    watermarkColumn: null,
    changeSignal: null,
    probeQuery: null,
    probeType: null,
    columns: [],
    columnPresets: [],
    implicitMeasures: [],
    implicitDimensions: [],
    apiEndpoint: null,
    viewSql: null,
    dqContract: null,
    materialize: false,
    mvRefreshInterval: 0,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 5,
    mvConsistency: "shared",
    mvPreprocess: null,
    mvBitemporalMode: null,
    mvBitemporalKey: [],
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
    live: null,
    uniqueConstraints: [],
    graphqlFieldName: null,
    dqDataset: null,
    modelingRole: null,
    ...overrides,
  } as RegisteredTable;
}

const salesFact = table({ id: 1, domainId: "sales", tableName: "orders", modelingRole: "fact" });
const salesDim = table({
  id: 2,
  domainId: "sales",
  tableName: "customers",
  modelingRole: "dimension",
});
const reportingFact = table({
  id: 3,
  domainId: "reporting",
  tableName: "shipments",
  modelingRole: "fact",
});
const reportingDim = table({
  id: 4,
  domainId: "reporting",
  tableName: "carriers",
  modelingRole: "dimension",
});

const salesMetric: Metric = {
  name: "revenue",
  expression: "SUM(orders.amount)",
  datatype: "numeric",
  description: null,
  aiContext: null,
  visibleTo: ["admin"],
  fromFact: "orders",
};
const reportingMetric: Metric = {
  name: "shipment_count",
  expression: "COUNT(shipments.id)",
  datatype: "integer",
  description: null,
  aiContext: null,
  visibleTo: ["admin"],
  fromFact: "shipments",
};
const handAuthoredMetric: Metric = {
  name: "constant_one",
  expression: "SELECT 1",
  datatype: "integer",
  description: null,
  aiContext: null,
  visibleTo: ["admin"],
  fromFact: null,
};

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [{ id: "admin" }], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({
    tables: [salesFact, salesDim, reportingFact, reportingDim],
    loading: false,
    refetch: vi.fn(),
  }),
  useRelationships: () => ({ relationships: [], loading: false, refetch: vi.fn() }),
  useMetrics: () => ({
    metrics: [salesMetric, reportingMetric, handAuthoredMetric],
    loading: false,
    refetch: vi.fn(),
  }),
  useRegisterTable: () => ({ registerTable: vi.fn(), loading: false }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { SqlPage } from "../SqlPage";

function renderPage() {
  return render(
    <MemoryRouter>
      <SqlPage />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  localStorage.clear();
  idbStore.clear();
  vi.clearAllMocks();
});

describe("Explore/SQL Metrics/Facts/Dimensions honor the checked-domains filter", () => {
  it("shows only the checked domain's facts, dimensions, and derived metric", async () => {
    renderPage();
    await waitFor(() => expect(screen.getByTestId("schema-group-facts")).toBeInTheDocument());
    screen.getByTestId("schema-group-facts").click();
    screen.getByTestId("schema-group-dimensions").click();

    expect(await screen.findByText("orders")).toBeInTheDocument();
    expect(screen.getByText("customers")).toBeInTheDocument();
    expect(screen.getByTestId("schema-metric-revenue")).toBeInTheDocument();

    expect(screen.queryByText("shipments")).not.toBeInTheDocument();
    expect(screen.queryByText("carriers")).not.toBeInTheDocument();
    expect(screen.queryByTestId("schema-metric-shipment_count")).not.toBeInTheDocument();
  });

  it("keeps a hand-authored metric (no fromFact) visible under any domain selection", async () => {
    renderPage();
    await waitFor(() =>
      expect(screen.getByTestId("schema-metric-constant_one")).toBeInTheDocument(),
    );
  });
});
