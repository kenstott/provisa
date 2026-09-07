// Copyright (c) 2026 Kenneth Stott
// Canary: 5b1e8d27-9c3a-4f6e-b2d0-7a4c1e9f3b58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1443 clause 10: a checker table's data-product membership is inherited from the table its
// contract scans — the form shows it read-only and the update payload never carries it.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";

vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
}));

// The DQ panel mounts on a checker table and parses the contract on the server; that panel has
// its own tests, so its server calls resolve to the parsed contract here.
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useDqContract: () => ({
    parseContract: vi.fn().mockResolvedValue({
      dataset: "provisa/sales/orders",
      checks: [],
      error: null,
    }),
    buildContract: vi.fn(),
    checkCatalog: vi.fn().mockResolvedValue({ checks: [] }),
    buildCheck: vi.fn(),
    dryRunContract: vi.fn(),
    runCheckNow: vi.fn(),
  }),
  useTables: () => ({
    tables: [{ schemaName: "sales", tableName: "orders", dqDataset: "provisa/sales/orders" }],
    loading: false,
    error: undefined,
    refetch: vi.fn(),
  }),
}));

import { TableEditForm } from "../TableEditForm";
import { buildTableUpdateInput } from "../helpers";
import type { DataProduct, RegisteredTable, Source } from "../../../types/admin";
import i18n from "../../../i18n";

const t = i18n.getFixedT("en");

const CONTRACT = "dataset: provisa/sales/orders\nchecks:\n  - row_count:\n";

function makeTable(overrides: Partial<RegisteredTable> = {}): RegisteredTable {
  return {
    id: 2,
    sourceId: "dq",
    domainId: "sales",
    schemaName: "quality",
    tableName: "orders_scans",
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
    dqContract: CONTRACT,
    materialize: false,
    mvRefreshInterval: 300,
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
    productId: "orders-product", // server-derived from the scanned table
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    live: null,
    uniqueConstraints: [],
    graphqlFieldName: null,
    ...overrides,
  };
}

const CHECKER_SOURCE = { id: "dq", type: "soda" } as unknown as Source;
const PLAIN_SOURCE = { id: "wh", type: "postgresql" } as unknown as Source;
const PRODUCTS = [
  { id: "orders-product", name: "Orders", domainId: "sales" } as unknown as DataProduct,
];

function renderForm(table: RegisteredTable, sources: Source[]) {
  render(
    <TableEditForm
      editingTable={table}
      setEditingTable={vi.fn()}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={sources}
      roles={[]}
      dataProducts={PRODUCTS}
      settings={null}
      saving={false}
      generatingDesc={false}
      setGeneratingDesc={vi.fn()}
      generatingColDesc={null}
      setGeneratingColDesc={vi.fn()}
      generateTableDescription={vi.fn()}
      generateColumnDescription={vi.fn()}
      cancelEditing={vi.fn()}
      handleSaveEdit={vi.fn()}
      updateEditCol={vi.fn()}
    />,
  );
}

describe("TableEditForm — checker table product membership (REQ-1443 clause 10)", () => {
  it("shows the inherited product read-only on a checker table", () => {
    renderForm(makeTable(), [CHECKER_SOURCE]);
    const control = screen.getByTestId("table-edit-product-id") as HTMLInputElement;
    expect(control.value).toBe("Orders");
    expect(control).toBeDisabled();
    expect(control).toHaveAttribute("readonly");
    expect(screen.getByText(t("tableEditForm.dataProductInherited"))).toBeTruthy();
  });

  it("names no product when the scanned table has none", () => {
    renderForm(makeTable({ productId: null }), [CHECKER_SOURCE]);
    const control = screen.getByTestId("table-edit-product-id") as HTMLInputElement;
    expect(control.value).toBe(t("tableEditForm.dataProductNone"));
  });

  it("keeps the editable picker for an ordinary table", () => {
    renderForm(
      makeTable({ sourceId: "wh", dqContract: null, productId: null }),
      [PLAIN_SOURCE],
    );
    const control = screen.getByTestId("table-edit-product-id") as HTMLInputElement;
    expect(control).not.toBeDisabled();
    expect(screen.getByText(t("tableEditForm.dataProductDesc"))).toBeTruthy();
  });

  it("never sends a productId for a checker table", () => {
    expect(buildTableUpdateInput(makeTable()).productId).toBeNull();
  });

  it("sends the chosen productId for an ordinary table", () => {
    expect(buildTableUpdateInput(makeTable({ dqContract: null })).productId).toBe("orders-product");
  });
});
