// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2c9e4b-5d1f-4b8a-9c3e-0e6d2f8a4b17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1318: the Views page definition-mode toggle — SQL editor vs metric/dimension
// picker. A metric view opens in Metrics mode prefilled; a free-hand view opens in
// SQL mode; mode changes only through the explicit control; Metrics mode submits
// registerTable with viewMetrics and NO viewSql.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { MemoryRouter } from "react-router-dom";
import { render, screen, waitFor, within } from "../../test-utils/render";
import userEvent from "@testing-library/user-event";

// Mantine Select/MultiSelect mount options into a portal listbox (referenced by the
// input's aria-controls) only once the dropdown is open; jsdom applies no layout so
// the dropdown reads as "hidden" to Testing Library — hence { hidden: true }.
async function selectOption(combobox: HTMLElement, name: string) {
  await userEvent.click(combobox);
  const listboxId = combobox.getAttribute("aria-controls");
  const listbox = listboxId ? document.getElementById(listboxId) : null;
  if (!listbox) throw new Error(`No listbox for combobox ${combobox.getAttribute("data-testid")}`);
  const option = await within(listbox).findByRole("option", { name, hidden: true });
  await userEvent.click(option);
}
import type { Metric, RegisteredTable, Relationship, TableColumn } from "../../types/admin";

const METRICS: Metric[] = [
  {
    name: "revenue",
    expression: "SUM(orders.amount)",
    datatype: "numeric",
    description: null,
    aiContext: null,
    visibleTo: ["*"],
    fromFact: null,
  },
];

// Spread the real module: vmThreads + fileParallelism:false share one module registry, so a
// replace-everything factory here leaks into other files and drops exports they need.
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useMetrics: () => ({ metrics: METRICS, loading: false, error: undefined, refetch: vi.fn() }),
}));

import { ViewDefinitionForm } from "../tables/ViewDefinitionForm";

function makeCol(overrides: Partial<TableColumn> = {}): TableColumn {
  return {
    id: 1,
    columnName: "col",
    computedSqlAlias: "col",
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
    isImplicitMeasure: false,
    isImplicitDimension: false,
    ...overrides,
  };
}

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
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
    dataProduct: false,
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    live: null,
    uniqueConstraints: [],
    modelingRole: null,
    viewMetrics: null,
    ...overrides,
  };
}

function makeRel(overrides: Partial<Relationship> = {}): Relationship {
  return {
    id: 1,
    sourceTableId: 1,
    targetTableId: 2,
    sourceTableName: "orders",
    sourceDomainId: "sales",
    targetTableName: "customers",
    sourceColumn: "customer_id",
    targetColumn: "id",
    cardinality: "many-to-one",
    materialize: false,
    refreshInterval: 0,
    targetFunctionName: null,
    functionArg: null,
    alias: null,
    graphqlAlias: null,
    physicalName: null,
    computedCypherAlias: null,
    autoSuggested: false,
    disableCypher: false,
    // REQ-1586: an FK/PK relationship declares no junction.
    viaTableId: null,
    viaTableName: null,
    viaSourceColumn: null,
    viaTargetColumn: null,
    viaTypeColumn: null,
    viaTypeValue: null,
    viaLabelSource: null,
    ownerDomainId: null,
    ...overrides,
  };
}

const ordersTable = makeTable({
  id: 1,
  tableName: "orders",
  columns: [makeCol({ id: 1, columnName: "amount", computedSqlAlias: "amount" })],
});
const customersTable = makeTable({
  id: 2,
  tableName: "customers",
  columns: [makeCol({ id: 2, columnName: "region", computedSqlAlias: "region" })],
});

const registerTable = vi.fn();
const updateTable = vi.fn();

function renderForm(editing: RegisteredTable | null) {
  const onSuccess = vi.fn();
  render(
    <MemoryRouter>
      <ViewDefinitionForm
        editing={editing}
        tables={[ordersTable, customersTable]}
        relationships={[makeRel()]}
        domainHints={["sales", "ops"]}
        registerTable={registerTable}
        updateTable={updateTable}
        onSuccess={onSuccess}
        onCancel={vi.fn()}
      />
    </MemoryRouter>,
  );
  return { onSuccess };
}

beforeEach(() => {
  vi.clearAllMocks();
  registerTable.mockResolvedValue({ success: true, message: "ok" });
  updateTable.mockResolvedValue({ success: true, message: "ok" });
});

describe("Views definition-mode toggle (REQ-1318)", () => {
  it("creating a view starts in SQL mode with the existing editor entry point", () => {
    renderForm(null);
    expect(screen.getByTestId("view-definition-mode")).toBeInTheDocument();
    expect(screen.getByTestId("view-definition-sql-panel")).toBeInTheDocument();
    expect(screen.getByTestId("view-definition-open-sql")).toHaveTextContent("Open SQL editor");
    expect(screen.queryByTestId("view-definition-metrics-panel")).not.toBeInTheDocument();
  });

  it("switching to Metrics mode shows the metric/dimension picker", async () => {
    const user = userEvent.setup();
    renderForm(null);
    await user.click(screen.getByRole("radio", { name: "Metrics" }));
    expect(screen.getByTestId("view-definition-metrics-panel")).toBeInTheDocument();
    expect(screen.queryByTestId("view-definition-sql-panel")).not.toBeInTheDocument();
  });

  // 15s: long userEvent sequence; under full-suite load the 5s default trips.
  it(
    "Metrics mode submits registerTable with viewMetrics and no viewSql",
    { timeout: 15000 },
    async () => {
      const user = userEvent.setup();
      const { onSuccess } = renderForm(null);
      await user.click(screen.getByRole("radio", { name: "Metrics" }));

      await user.type(screen.getByTestId("view-definition-name"), "rev_by_region");
      await selectOption(screen.getByTestId("view-definition-domain"), "sales");

      // Pick the metric.
      await selectOption(
        screen.getByPlaceholderText("Pick one or more registered metrics"),
        "revenue",
      );

      // Dimensions: columns of the metric's expression tables plus one relationship
      // hop (orders.amount, customers.region) — pick region.
      await selectOption(
        screen.getByPlaceholderText(
          "Columns of the metrics' tables (one relationship hop away included)",
        ),
        "customers.region",
      );

      await user.type(screen.getByTestId("view-definition-filters"), "status = 'complete'");
      await user.click(screen.getByTestId("view-definition-save"));

      await waitFor(() => expect(onSuccess).toHaveBeenCalled());
      expect(registerTable).toHaveBeenCalledTimes(1);
      const input = registerTable.mock.calls[0][0] as Record<string, unknown>;
      expect(input).toMatchObject({
        sourceId: "__derived__",
        domainId: "sales",
        schemaName: "views",
        tableName: "rev_by_region",
        alias: "rev_by_region",
        viewMetrics: {
          metrics: ["revenue"],
          dimensions: ["region"],
          filters: ["status = 'complete'"],
        },
      });
      // Declarative definition only — the UI never sends free-hand SQL here.
      expect(input.viewSql).toBeUndefined();
      // View output columns = dimensions + one column per metric.
      expect(input.columns).toEqual([
        { name: "region", visibleTo: ["*"] },
        { name: "revenue", visibleTo: ["*"] },
      ]);
      expect(updateTable).not.toHaveBeenCalled();
    },
  );

  it("editing a metric view opens in Metrics mode prefilled", () => {
    renderForm(
      makeTable({
        id: 9,
        sourceId: "__derived__",
        schemaName: "views",
        tableName: "rev_by_region",
        viewSql: "SELECT ...generated...",
        viewMetrics: { metrics: ["revenue"], dimensions: ["region"], filters: [] },
      }),
    );
    expect(screen.getByTestId("view-definition-metrics-panel")).toBeInTheDocument();
    expect(screen.queryByTestId("view-definition-sql-panel")).not.toBeInTheDocument();
    // Prefilled pills for the saved spec (the dimension pill shows its table-qualified label).
    expect(screen.getAllByText("revenue").length).toBeGreaterThan(0);
    expect(screen.getAllByText("customers.region").length).toBeGreaterThan(0);
  });

  it("editing a free-hand view opens in SQL mode with the existing editor entry", () => {
    renderForm(
      makeTable({
        id: 9,
        sourceId: "__derived__",
        schemaName: "views",
        tableName: "freehand_view",
        viewSql: "SELECT 1",
        viewMetrics: null,
      }),
    );
    expect(screen.getByTestId("view-definition-sql-panel")).toBeInTheDocument();
    expect(screen.getByTestId("view-definition-open-sql")).toHaveTextContent("Edit in SQL editor");
    expect(screen.queryByTestId("view-definition-metrics-panel")).not.toBeInTheDocument();
  });

  it("saving an edited metric view sends updateTable with the spec and viewSql omitted", async () => {
    const user = userEvent.setup();
    const { onSuccess } = renderForm(
      makeTable({
        id: 9,
        sourceId: "__derived__",
        schemaName: "views",
        tableName: "rev_by_region",
        viewSql: "SELECT ...generated...",
        viewMetrics: { metrics: ["revenue"], dimensions: ["region"], filters: [] },
      }),
    );
    await user.type(screen.getByTestId("view-definition-filters"), "status = 'complete'");
    await user.click(screen.getByTestId("view-definition-save"));

    await waitFor(() => expect(onSuccess).toHaveBeenCalled());
    expect(updateTable).toHaveBeenCalledTimes(1);
    const input = updateTable.mock.calls[0][0] as Record<string, unknown>;
    expect(input.viewMetrics).toEqual({
      metrics: ["revenue"],
      dimensions: ["region"],
      filters: ["status = 'complete'"],
    });
    // Mutually exclusive with free-hand SQL: the generated SQL is never echoed back.
    expect(input.viewSql).toBeUndefined();
    expect(registerTable).not.toHaveBeenCalled();
  });
});
