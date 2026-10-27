// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2f4a91-3e8d-4b6a-9f05-2d1e8c7b4a63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1443 clause 10: a checker table is listed under Data Quality on the product detail, never
// among the output ports it scans.

import { describe, it, expect, vi } from "vitest";
import { render, screen, within, fireEvent } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { DataProduct } from "../../../types/admin";
import type { CoreTable } from "../productCore";
import type { ProductDq, ProductDqEntry } from "../../../api/lineage";
import i18n from "../../../i18n";
import { lifecycleProps } from "./lifecycleProps";

// The panel parses nothing: a checker table's rules come with it, as meta's dq_rules rows.
const parseContract = vi.fn();

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useDqContract: () => ({ parseContract }),
}));

// The panel's 3D graph is its own component, held by its own tests.
vi.mock("../../../components/graph3d/GraphPanel", () => ({
  GraphPanel: () => null,
}));

const t = i18n.getFixedT("en");

const PRODUCT = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
} as unknown as DataProduct;

// Member tables as data_products_meta's memberTables answers them.
function makeTable(overrides: Partial<CoreTable>): CoreTable {
  return {
    id: 1,
    source_id: "wh",
    domain_id: "pet-store",
    schema_name: "pet_store",
    table_name: "vets",
    alias: null,
    description: null,
    dq_contract: null,
    ...overrides,
  };
}

const VETS = makeTable({});
const VETS_SCAN = makeTable({
  id: 2,
  source_id: "dq-soda",
  schema_name: "quality",
  table_name: "vets_scan",
  dq_contract: "dataset: provisa/pet_store/vets\nchecks: []\n",
  description: "Soda contract over vets",
  checks: { id: 1, domain_id: "pet-store", table_name: "vets", alias: null },
  dq_rules: [
    { position: 1, column_name: "", check_type: "row_count", definition: "row_count > 0" },
    {
      position: 2,
      column_name: "email",
      check_type: "missing_count",
      definition: "missing_count(email) = 0",
    },
  ],
});

// The product lineage endpoint's data quality (REQ-2157): vets_scan, the product's own check on its
// member vets, is a promise.
const VETS_PROMISE: ProductDqEntry = {
  checker_table_id: 2,
  domain_id: "pet-store",
  table_name: "vets_scan",
  alias: null,
  description: "Soda contract over vets",
  owner_product_id: "pet_health",
  checked_table_id: 1,
  checked_table: { id: 1, domain_id: "pet-store", table_name: "vets", alias: null },
  latest_outcome: "pass",
  latest_run_at: "2026-10-20T10:00:00Z",
};
const NO_DQ: ProductDq = { expectations: [], promises: [], related: [], unattached: [] };

function renderPanel(tables: CoreTable[], dq: ProductDq = { ...NO_DQ, promises: [VETS_PROMISE] }) {
  render(
    <DataProductDetailPanel
      p={PRODUCT}
      core={{
        product: { ...PRODUCT, customProperties: {} },
        tables,
        queries: { graphql: "", sql: "", jsonapi: "" },
      }}
      coreError={null}
      reader={null}
      canEdit={false}
      canDelete={false}
      owner
      lineageLoading={false}
      lineageError=""
      lineageGraph={{
        nodes: [],
        edges: [],
        outputs: [],
        members: [],
        consumers: [],
        inputs: [],
        dq,
      }}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
      {...lifecycleProps()}
    />,
  );
}

describe("DataProductDetailPanel — Data Quality panel (REQ-1443 clause 10)", () => {
  it("moves checker tables out of Output Ports into Data Quality", () => {
    renderPanel([VETS, VETS_SCAN]);
    const ports = screen.getByTestId("data-product-detail-tables-pet_health");
    expect(within(ports).getByText("pet-store.vets")).toBeTruthy();
    expect(within(ports).queryByText("pet-store.vets_scan")).toBeNull();

    const dq = screen.getByTestId("data-product-detail-dq-tables-pet_health");
    expect(within(dq).getByText("pet-store.vets_scan")).toBeTruthy();
    expect(within(dq).getByText(/Soda contract over vets/)).toBeTruthy();
    expect(screen.getByText(t("dataProductsTab.detail.field.dataQuality"))).toBeTruthy();
  });

  it("keeps checker tables out of Access Patterns and offers only SQL on the scan", () => {
    renderPanel([VETS, VETS_SCAN]);
    const patterns = screen.getByTestId("data-product-detail-graphql-queries-pet_health");
    expect(within(patterns).getByText("pet-store.vets")).toBeTruthy();
    expect(within(patterns).queryByText("pet-store.vets_scan")).toBeNull();
    expect(screen.queryByTestId("data-product-detail-sql-query-2")).toBeNull();

    const dq = screen.getByTestId("data-product-detail-dq-tables-pet_health");
    expect(within(dq).getByTestId("data-product-detail-dq-sql-query-2")).toBeTruthy();
    expect(within(dq).queryByTestId("data-product-detail-graphql-query-2")).toBeNull();
  });

  it("shows the Access Patterns empty state when the only member is a checker table", () => {
    renderPanel([VETS_SCAN]);
    expect(screen.getByText(t("dataProductsTab.graphqlQueriesEmpty"))).toBeTruthy();
  });

  it("shows the empty state when no checker scans the product", () => {
    renderPanel([VETS], NO_DQ);
    expect(screen.queryByTestId("data-product-detail-dq-tables-pet_health")).toBeNull();
    expect(screen.getByText(t("dataProductsTab.detail.dataQualityEmpty"))).toBeTruthy();
  });

  it("opens the scan's rules in a modal from the icon beside SQL, as the core read them", async () => {
    renderPanel([VETS, VETS_SCAN]);
    const dq = screen.getByTestId("data-product-detail-dq-tables-pet_health");
    const sql = within(dq).getByTestId("data-product-detail-dq-sql-query-2");
    const rules = within(dq).getByTestId("data-product-detail-dq-rules-2");
    expect(sql.compareDocumentPosition(rules) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.queryByRole("dialog")).toBeNull();

    fireEvent.click(rules);
    const dialog = await screen.findByRole("dialog");
    const rows = await within(dialog).findByTestId("dq-rules-rows");
    expect(within(rows).getByText("row_count > 0")).toBeTruthy();
    expect(within(rows).getByText("email")).toBeTruthy();
    expect(within(rows).getByText(t("dataQualityPanel.datasetScope"))).toBeTruthy();
    expect(within(dialog).getByTestId("dq-rules-dataset").textContent).toContain("pet-store.vets");
    expect(parseContract).not.toHaveBeenCalled();
  });

  it("names the table each checker scans (REQ-2125's checks)", () => {
    renderPanel([VETS, VETS_SCAN]);
    expect(screen.getByTestId("data-product-detail-dq-checks-2").textContent).toBe(
      "scans pet-store.vets",
    );
  });

  it("names a checker by its alias, as the model does", () => {
    renderPanel(
      [VETS, { ...VETS_SCAN, table_name: "vets_quality_dq_soda_dq", alias: "vets_quality" }],
      {
        ...NO_DQ,
        promises: [
          { ...VETS_PROMISE, table_name: "vets_quality_dq_soda_dq", alias: "vets_quality" },
        ],
      },
    );
    const dq = screen.getByTestId("data-product-detail-dq-tables-pet_health");
    expect(dq).toHaveTextContent("pet-store.vets_quality");
    expect(dq).not.toHaveTextContent("vets_quality_dq_soda_dq");
  });

  it("offers the rules at once, with no read of the scan's source", () => {
    renderPanel([VETS_SCAN]);
    expect(screen.getByTestId("data-product-detail-dq-rules-2")).toBeEnabled();
  });

  it("shows no output ports when the only member is a checker table", () => {
    renderPanel([VETS_SCAN]);
    expect(screen.queryByTestId("data-product-detail-tables-pet_health")).toBeNull();
    expect(screen.getByText(t("dataProductsTab.tablesEmpty"))).toBeTruthy();
    expect(screen.getByTestId("data-product-detail-dq-tables-pet_health")).toBeTruthy();
  });
});
