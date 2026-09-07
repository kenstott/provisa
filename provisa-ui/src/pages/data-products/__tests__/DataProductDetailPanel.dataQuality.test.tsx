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
import { render, screen, within, fireEvent, waitFor } from "../../../test-utils/render";
import { DataProductDetailPanel } from "../DataProductDetailPanel";
import type { DataProduct, RegisteredTable, Source } from "../../../types/admin";
import i18n from "../../../i18n";

const parseContract = vi.fn().mockResolvedValue({
  dataset: "provisa/pet_store/vets",
  checker: "soda",
  checks: [
    { columnName: "", checkType: "row_count", definition: "row_count > 0", extra: "" },
    { columnName: "email", checkType: "missing_count", definition: "missing_count(email) = 0", extra: "" },
  ],
  error: null,
});

vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useDqContract: () => ({ parseContract }),
}));

const t = i18n.getFixedT("en");

const SOURCES = [
  { id: "wh", type: "postgres" },
  { id: "dq-soda", type: "soda" },
] as unknown as Source[];

const PRODUCT = { id: "pet_health", domainId: "pet-store", name: "Pet Health" } as unknown as DataProduct;

function makeTable(overrides: Partial<RegisteredTable>): RegisteredTable {
  return {
    id: 1,
    sourceId: "wh",
    domainId: "pet-store",
    schemaName: "pet_store",
    tableName: "vets",
    alias: null,
    description: null,
    columns: [],
    dqContract: null,
    graphqlFieldName: "vets",
    ...overrides,
  } as RegisteredTable;
}

const VETS = makeTable({});
const VETS_SCAN = makeTable({
  id: 2,
  sourceId: "dq-soda",
  schemaName: "quality",
  tableName: "vets_scan",
  dqContract: "dataset: provisa/pet_store/vets\nchecks: []\n",
  description: "Soda contract over vets",
});

function renderPanel(tables: RegisteredTable[], sources: Source[] = SOURCES) {
  render(
    <DataProductDetailPanel
      p={PRODUCT}
      tables={tables}
      sources={sources}
      functions={[]}
      relatedTerms={[]}
      relatedTermsLoading={false}
      relatedTables={[]}
      internalRelationships={[]}
      canEdit={false}
      canSeeLineage={false}
      lineageLoading={false}
      lineageError=""
      lineageGraph={null}
      inputPorts={[]}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
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
    renderPanel([VETS]);
    expect(screen.queryByTestId("data-product-detail-dq-tables-pet_health")).toBeNull();
    expect(screen.getByText(t("dataProductsTab.detail.dataQualityEmpty"))).toBeTruthy();
  });

  it("opens the scan's rules in a modal from the icon beside SQL, parsed with the source's checker", async () => {
    renderPanel([VETS, VETS_SCAN]);
    const dq = screen.getByTestId("data-product-detail-dq-tables-pet_health");
    const sql = within(dq).getByTestId("data-product-detail-dq-sql-query-2");
    const rules = within(dq).getByTestId("data-product-detail-dq-rules-2");
    expect(sql.compareDocumentPosition(rules) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.queryByRole("dialog")).toBeNull();

    fireEvent.click(rules);
    const dialog = await screen.findByRole("dialog");
    await waitFor(() =>
      expect(parseContract).toHaveBeenCalledWith({
        checker: "soda",
        contractText: VETS_SCAN.dqContract,
      }),
    );
    const rows = await within(dialog).findByTestId("dq-rules-rows");
    expect(within(rows).getByText("row_count > 0")).toBeTruthy();
    expect(within(rows).getByText("email")).toBeTruthy();
    expect(within(rows).getByText(t("dataQualityPanel.datasetScope"))).toBeTruthy();
    expect(within(dialog).getByTestId("dq-rules-dataset").textContent).toContain(
      "provisa/pet_store/vets",
    );
  });

  it("disables the rules icon until the scan's source is known", () => {
    renderPanel([VETS_SCAN], []);
    expect(screen.getByTestId("data-product-detail-dq-rules-2")).toBeDisabled();
  });

  it("shows no output ports when the only member is a checker table", () => {
    renderPanel([VETS_SCAN]);
    expect(screen.queryByTestId("data-product-detail-tables-pet_health")).toBeNull();
    expect(screen.getByText(t("dataProductsTab.tablesEmpty"))).toBeTruthy();
    expect(screen.getByTestId("data-product-detail-dq-tables-pet_health")).toBeTruthy();
  });
});
