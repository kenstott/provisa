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
import { render, screen, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";
import type { Metric, RegisteredTable, TableColumn } from "../../types/admin";

const upsertMetric = vi.fn();
const deleteMetric = vi.fn();

function makeCol(columnName: string, id: number): TableColumn {
  return {
    id,
    columnName,
    computedSqlAlias: columnName,
    visibleTo: [],
    writableBy: [],
    unmaskedTo: [],
    maskType: null,
    maskPattern: null,
    maskReplace: null,
    maskValue: null,
    maskPrecision: null,
    alias: null,
    description: null,
    dataType: null,
    nativeFilterType: null,
    isPrimaryKey: false,
    isForeignKey: false,
    isAlternateKey: false,
    scope: "public",
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
    apiEndpoint: null,
    viewSql: null,
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
    dataProduct: false,
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
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

  it("delete opens the confirm dialog from within the detail panel", async () => {
    render(<MetricsPage />);
    await userEvent.click(screen.getByTestId("metrics-row-revenue"));
    await userEvent.click(screen.getByTestId("metric-detail-delete-revenue"));
    expect(screen.getByTestId("metric-delete-modal")).toBeInTheDocument();
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

  // 15s: long userEvent sequence; under full-suite load the 5s default trips.
  it("keeps the expression textarea as a free-text escape hatch", { timeout: 15000 }, async () => {
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
    );
  });
});
