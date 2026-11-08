// Copyright (c) 2026 Kenneth Stott
// Canary: 5b1e8d27-9c3a-4f6e-b2d0-7a4c1e9f3b58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1634: a table belongs to any number of data products, set on the product only: the form lists
// them read-only, each a link that opens the product, and the update payload never carries them.

import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";

// The table's class term reads the meta domain as the user: its own tests (termGrounding) hold it.
vi.mock("../TableClassTerm", () => ({ TableClassTerm: () => <span>class term</span> }));
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
    refreshPolicySummary: null,
    gqlNamingConvention: null,
    watermarkColumn: null,
    changeSignal: null,
    probeQuery: null,
    probeType: null,
    columns: [],
    implicitMeasures: [],
    implicitDimensions: [],
    apiEndpoint: null,
    apiEndpointError: null,
    viewSql: null,
    dqContract: CONTRACT,
    materialize: false,
    mvRefreshInterval: 300,
    mvDebounceQuiet: 0,
    mvDebounceMaxDelay: 5,
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
    productIds: ["orders-product", "returns-product"], // the products the checker table is in
    enableAggregates: false,
    enableGroupBy: false,
    canDeployToDb: false,
    writeOps: ["delete", "insert", "update"],
    live: null,
    writeGrants: [],
    uniqueConstraints: [],
    graphqlFieldName: null,
    timeTravel: null,
    dqDataset: null,
    qualityCheckers: [],
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
      // What this file tests is on the associations section.
      relationships={[]}
      section="associations"
      onSectionChange={vi.fn()}
      savedProfilerId={null}
      editingTable={table}
      savedTable={table}
      setEditingTable={vi.fn()}
      editingColumnTypes={{}}
      cacheTtlEdits={{}}
      setCacheTtlEdits={vi.fn()}
      sources={sources}
      roles={[]}
      dataProducts={PRODUCTS}
      settings={null}
      saving={false}
      generateTableDescription={vi.fn()}
      generateColumnDescription={vi.fn()}
      cancelEditing={vi.fn()}
      handleSaveEdit={vi.fn()}
      updateEditCol={vi.fn()}
    />,
  );
}

describe("TableEditForm — the products a table belongs to are read-only (REQ-1634)", () => {
  it("lists each product of a checker table, a link that opens it, with no picker", () => {
    renderForm(makeTable(), [CHECKER_SOURCE]);
    const field = screen.getByTestId("table-read-view-product");
    const links = within(field).getAllByRole("link");
    expect(links.map((l) => l.textContent)).toEqual(["Orders", "returns-product"]);
    expect(links.map((l) => l.getAttribute("href"))).toEqual([
      "/data-products?product=orders-product",
      "/data-products?product=returns-product",
    ]);
    expect(screen.queryByTestId("table-edit-product-id")).toBeNull();
  });

  it("says a table that is in no product belongs to none", () => {
    renderForm(makeTable({ productIds: [], sourceId: "wh", dqContract: null }), [PLAIN_SOURCE]);
    const field = screen.getByTestId("table-read-view-product");
    expect(within(field).queryAllByRole("link")).toEqual([]);
    expect(field).toHaveTextContent(t("tableEditForm.dataProductNone"));
  });

  it("sends no product with a table's update: membership is set on the product", () => {
    const input = buildTableUpdateInput(makeTable()) as Record<string, unknown>;
    expect(input).not.toHaveProperty("productId");
    expect(input).not.toHaveProperty("productIds");
  });
});
