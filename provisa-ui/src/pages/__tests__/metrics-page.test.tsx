// Copyright (c) 2026 Kenneth Stott
// Canary: 3c8e5f1a-2b9d-4c7e-8a4f-6d0b1e9c5a72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Metrics page detail-then-edit pattern + fact-sourced expression builder:
// row click opens the detail panel (Edit/Delete live inside it, REQ-1317);
// the create/edit dialog composes AGG(fact.column) from the fact registry (REQ-1320).

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { Metric, RegisteredTable, TableColumn } from "../../types/admin";

const upsertMetric = vi.fn();
const deleteMetric = vi.fn();

function makeCol(columnName: string, id: number): TableColumn {
  return {
    id,
    columnName,
    computedSqlAlias: columnName,
    computedGqlAlias: columnName,
    visibleTo: [],
    writableBy: [],
    masks: [],
    alias: null,
    description: null,
    dataType: null,
    nativeFilterType: null,
    isPrimaryKey: false,
    isForeignKey: false,
    isAlternateKey: false,
    scope: "public",
    isImplicitMeasure: false,
    isImplicitDimension: false,
  } as TableColumn;
}

function makeTable(overrides: Partial<RegisteredTable>): RegisteredTable {
  return {
    id: 1,
    sourceId: "src1",
    domainId: "sales",
    schemaName: "public",
    tableName: "orders",
    alias: null,
    description: null,
    cacheTtl: null,
    replicate: null,
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
    materialize: false,
    mvRefreshInterval: 0,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 5,
    pushDebounceQuiet: 0,
    pushDebounceMaxDelay: 5,
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
    productId: null, // REQ-1634
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    writeOps: ["delete", "insert", "update"],
    live: null,
    uniqueConstraints: [],
    modelingRole: null,
    viewMetrics: null,
    ...overrides,
  } as RegisteredTable;
}

const METRICS: Metric[] = [
  {
    name: "revenue",
    expression: "SUM(orders.amount)",
    datatype: "numeric",
    description: "Total revenue",
    aiContext: null,
    visibleTo: ["*"],
    fromFact: "orders",
  },
];

const TABLES: RegisteredTable[] = [
  makeTable({
    id: 1,
    tableName: "orders",
    modelingRole: "fact",
    columns: [makeCol("amount", 1), makeCol("qty", 2)],
  }),
  makeTable({ id: 2, tableName: "customers", modelingRole: "dimension" }),
  makeTable({
    id: 3,
    tableName: "revenue_by_region",
    viewMetrics: { metrics: ["revenue"], dimensions: ["region"], filters: [] },
  }),
];

// The page's export panel writes a dbt sources file for the role in use (REQ-1967).
vi.mock("../../context/AuthContext", () => ({ useAuth: () => ({ role: { id: "analyst" } }) }));

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files and drops exports they need.
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useMetrics: () => ({ metrics: METRICS, loading: false, error: undefined, refetch: vi.fn() }),
  useTables: () => ({ tables: TABLES, loading: false, error: undefined, refetch: vi.fn() }),
  useUpsertMetric: () => ({ upsertMetric, loading: false }),
  useDeleteMetric: () => ({ deleteMetric, loading: false }),
}));

import { MetricsPage } from "../MetricsPage";

// Mantine Select portals its listbox; jsdom applies no layout so it reads as hidden.
async function selectOption(combobox: HTMLElement, name: string) {
  await userEvent.click(combobox);
  const listboxId = combobox.getAttribute("aria-controls");
  const listbox = listboxId ? document.getElementById(listboxId) : null;
  if (!listbox) throw new Error(`No listbox for combobox ${combobox.getAttribute("data-testid")}`);
  const option = await within(listbox).findByRole("option", { name, hidden: true });
  await userEvent.click(option);
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("MetricsPage detail-then-edit", () => {
  it("REQ-1940: renders its list through the shared list component", () => {
    const { container } = render(<MetricsPage />);
    expect(container.querySelector("[data-list-table].data-table")).not.toBeNull();
    expect(container.querySelectorAll("tbody tr.list-row").length).toBeGreaterThan(0);
  });

  it("row click opens the detail panel with source fact and dependent views", async () => {
    render(<MetricsPage />);
    expect(screen.queryByTestId("metric-detail-revenue")).toBeNull();
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    const detail = screen.getByTestId("metric-detail-revenue");
    expect(within(detail).getByText("orders")).toBeInTheDocument();
    expect(within(detail).getByText("revenue_by_region")).toBeInTheDocument();
  });

  it("edit swaps the detail panel for the inline form, prefilled with builder pickers", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metric-detail-edit-revenue"));
    expect(screen.getByTestId("metric-form")).toBeInTheDocument();
    expect(screen.queryByTestId("metric-detail-revenue")).toBeNull();
    expect(screen.getByTestId("metric-expression-input")).toHaveValue("SUM(orders.amount)");
    expect(screen.getByTestId("metric-builder-fact")).toHaveValue("orders");
    expect(screen.getByTestId("metric-builder-measure")).toHaveValue("amount");
    expect(screen.getByTestId("metric-builder-agg")).toHaveValue("SUM");
  });

  it("saves an edited metric as an update, never a create", async () => {
    upsertMetric.mockClear();
    upsertMetric.mockResolvedValue({ success: true, message: "ok" });
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metric-detail-edit-revenue"));
    await userEvent.click(screen.getByTestId("metric-save-button"));
    expect(upsertMetric).toHaveBeenCalledWith(expect.objectContaining({ name: "revenue" }), false);
  });

  it("delete opens the confirm dialog from within the detail panel", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metric-detail-delete-revenue"));
    expect(screen.getByTestId("metric-delete-modal")).toBeInTheDocument();
  });
});

describe("MetricsPage: the right-hand detail panel", () => {
  it("shows the chosen metric in the panel, not under its row, and marks the row", async () => {
    const { container } = render(<MetricsPage />);
    expect(screen.queryByTestId("metrics-split-panel")).toBeNull();
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    const panel = screen.getByTestId("metrics-split-panel");
    expect(screen.getByTestId("metrics-split-title")).toHaveTextContent("revenue");
    expect(within(panel).getByTestId("metric-detail-revenue")).toBeInTheDocument();
    // Its actions are in the panel too, and the list gained no row.
    expect(within(panel).getByTestId("metric-detail-edit-revenue")).toBeInTheDocument();
    expect(container.querySelectorAll("tbody tr.list-expand")).toHaveLength(0);
    expect(screen.getByTestId("metrics-row-revenue").className).toContain("row-selected");
  });

  it("closes from the row and from the panel's close", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    expect(screen.queryByTestId("metrics-split-panel")).toBeNull();
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metrics-split-close"));
    expect(screen.queryByTestId("metrics-split-panel")).toBeNull();
  });

  it("opens the new-metric form in the panel, in place of the open metric", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metrics-new-button"));
    expect(screen.getByTestId("metrics-split-title")).toHaveTextContent("New Metric");
    expect(
      within(screen.getByTestId("metrics-split-panel")).getByTestId("metric-create-card"),
    ).toBeInTheDocument();
    expect(screen.getByTestId("metrics-row-revenue").className).not.toContain("row-selected");
  });

  it("asks a new metric only for what it needs to exist, then opens it in edit", async () => {
    upsertMetric.mockResolvedValue({ success: true, message: "ok" });
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-new-button"));
    expect(screen.getByTestId("metric-name-input")).toBeInTheDocument();
    expect(screen.getByTestId("metric-expression-input")).toBeInTheDocument();
    expect(screen.queryByTestId("metric-description-input")).toBeNull();
    expect(screen.queryByTestId("metric-ai-context-input")).toBeNull();

    // "revenue" is in the list the mocked query answers, as a saved metric would be.
    await userEvent.type(screen.getByTestId("metric-name-input"), "revenue");
    await userEvent.type(screen.getByTestId("metric-expression-input"), "SUM(orders.amount)");
    await userEvent.click(screen.getByTestId("metric-save-button"));

    expect(screen.getByTestId("metrics-split-title")).toHaveTextContent("revenue");
    expect(screen.getByTestId("metrics-row-revenue").className).toContain("row-selected");
    // Its detail is open in edit, with what the first step left out.
    expect(screen.getByTestId("metric-form")).toBeInTheDocument();
    expect(screen.getByTestId("metric-description-input")).toBeInTheDocument();
    expect(screen.getByTestId("metric-ai-context-input")).toBeInTheDocument();
  });

  it("keeps Escape from dropping a metric's form, whose Save and Cancel take the panel's head", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metric-detail-edit-revenue"));
    fireEvent.keyDown(screen.getByTestId("metrics-split-panel"), { key: "Escape" });
    expect(screen.getByTestId("metric-form")).toBeInTheDocument();
    // While the form is open the panel's head is the form's: Save and Cancel in place of the
    // metric's own actions.
    expect(screen.queryByTestId("metric-detail-edit-revenue")).toBeNull();
    const head = screen.getByTestId("metrics-split-title").parentElement as HTMLElement;
    expect(within(head).getByTestId("metric-save-button")).toBeInTheDocument();
    expect(within(head).getByTestId("metric-cancel-button")).toBeInTheDocument();
  });
});

describe("MetricsPage fact-sourced builder", () => {
  it("composes AGG(fact.column) and derives the datatype from the aggregate", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-new-button"));
    expect(screen.getByTestId("metric-create-card")).toBeInTheDocument();
    await selectOption(screen.getByTestId("metric-builder-fact"), "orders");
    await selectOption(screen.getByTestId("metric-builder-measure"), "qty");
    await selectOption(screen.getByTestId("metric-builder-agg"), "AVG");
    expect(screen.getByTestId("metric-expression-input")).toHaveValue("AVG(orders.qty)");
    expect(screen.getByTestId("metric-datatype-input")).toHaveValue("numeric");
    await selectOption(screen.getByTestId("metric-builder-agg"), "COUNT");
    expect(screen.getByTestId("metric-datatype-input")).toHaveValue("bigint");
  });

  it("keeps the expression textarea as a free-text escape hatch", async () => {
    upsertMetric.mockResolvedValue({ success: true, message: "ok" });
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-new-button"));
    await userEvent.type(screen.getByTestId("metric-name-input"), "custom");
    await userEvent.type(
      screen.getByTestId("metric-expression-input"),
      "COUNT(DISTINCT orders.id)",
    );
    await userEvent.click(screen.getByTestId("metric-save-button"));
    expect(upsertMetric).toHaveBeenCalledWith(
      expect.objectContaining({ name: "custom", expression: "COUNT(DISTINCT orders.id)" }),
      true, // a new metric is created
    );
  });
});
