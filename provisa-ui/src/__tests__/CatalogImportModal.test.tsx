// Copyright (c) 2026 Kenneth Stott
// Canary: 4f8a2d61-3c97-4e15-b0d4-7a1e9c5b2f38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2122: a catalog's Import review. What changed is shown beside Provisa's values, a local edit
// is marked, the steward accepts all or one table, and an unlinked table is listed, never guessed.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { CatalogImportModal } from "../components/admin/CatalogImportModal";
import type { ImportReview } from "../api/catalogs";

vi.mock("../api/catalogs", () => ({ fetchImportReview: vi.fn(), acceptImport: vi.fn() }));

import { acceptImport, fetchImportReview } from "../api/catalogs";

const mockReview = vi.mocked(fetchImportReview);
const mockAccept = vi.mocked(acceptImport);

const REVIEW: ImportReview = {
  catalog: "om",
  linked: [
    {
      table_id: 1,
      table: "orders",
      asset: "pg.sales.public.orders",
      url: null,
      locked: false,
      changes: [
        {
          field: "description",
          column: null,
          current: "Orders",
          catalog: "Every order",
          local_edit: true,
        },
        { field: "column", column: "amount", current: "", catalog: "Total", local_edit: false },
      ],
    },
    { table_id: 2, table: "customers", asset: "pg.sales.public.customers", changes: [] },
  ],
  not_linked: [{ table_id: 3, table: "returns" }],
};

describe("CatalogImportModal", () => {
  beforeEach(() => {
    mockReview.mockReset();
    mockAccept.mockReset();
  });

  it("lays each change beside Provisa's value and marks a local edit", async () => {
    mockReview.mockResolvedValue(REVIEW);
    render(<CatalogImportModal catalog="om" onClose={vi.fn()} />);

    const orders = await screen.findByTestId("catalog-import-table-1");
    expect(orders).toHaveTextContent("Every order");
    expect(orders).toHaveTextContent("Edited in Provisa since the last import");
    expect(orders).toHaveTextContent("Column amount description");
    expect(screen.getByTestId("catalog-import-table-2")).toHaveTextContent(
      "In step with the catalog",
    );
    expect(screen.queryByTestId("catalog-import-accept-2")).toBeNull();
    expect(screen.getByTestId("catalog-import-not-linked")).toHaveTextContent("returns");
  });

  it("accepts one table, or every changed table at once", async () => {
    mockReview.mockResolvedValue(REVIEW);
    mockAccept.mockResolvedValue(undefined);
    render(<CatalogImportModal catalog="om" onClose={vi.fn()} />);

    fireEvent.click(await screen.findByTestId("catalog-import-accept-1"));
    await waitFor(() => expect(mockAccept).toHaveBeenCalledWith("om", [1]));
    fireEvent.click(await screen.findByTestId("catalog-import-accept-all"));
    await waitFor(() => expect(mockAccept).toHaveBeenLastCalledWith("om", [1]));
    expect(mockReview).toHaveBeenCalledTimes(3);
  });
});
