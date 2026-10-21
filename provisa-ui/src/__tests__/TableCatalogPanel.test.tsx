// Copyright (c) 2026 Kenneth Stott
// Canary: 2a7d9f40-5e13-4c86-b2f1-9d4c0e6a3b57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2122: a table's catalogs in its editor. It states where its descriptions were last imported
// from and when, offers Refresh only where the table is linked, offers the lock only on a linked
// catalog, and, once locked, says which catalog is the authority and links to edit there.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../test-utils/render";
import { TableCatalogPanel } from "../pages/tables/TableCatalogPanel";
import type { TableCatalog } from "../api/catalogLinks";

const OM: TableCatalog = {
  id: "om",
  provider: "openmetadata",
  link: { ref: "o", name: "pg.sales.public.orders", url: "https://om.example/table/x" },
  imported_at: "2026-10-21T09:30:00Z",
  locked: false,
};
const COLLIBRA: TableCatalog = {
  id: "collibra",
  provider: "collibra",
  link: null,
  imported_at: null,
  locked: false,
};

describe("TableCatalogPanel", () => {
  it("states the provenance of the imported descriptions and offers refresh where linked", () => {
    const onRefresh = vi.fn();
    render(
      <TableCatalogPanel
        catalogs={[OM, COLLIBRA]}
        editUrl={null}
        locking={false}
        onRefresh={onRefresh}
        onLock={vi.fn()}
      />,
    );

    expect(screen.getByTestId("table-catalog-status-om")).toHaveTextContent(
      "Descriptions last imported from om (pg.sales.public.orders) on",
    );
    expect(screen.getByTestId("table-catalog-status-collibra")).toHaveTextContent("Not linked");
    expect(screen.queryByTestId("table-catalog-refresh-collibra")).toBeNull();
    fireEvent.click(screen.getByTestId("table-catalog-refresh-om"));
    expect(onRefresh).toHaveBeenCalledWith("om");
    expect(screen.queryByTestId("table-catalog-locked")).toBeNull();
  });

  it("offers the lock on a linked catalog alone", async () => {
    const onLock = vi.fn();
    render(
      <TableCatalogPanel
        catalogs={[OM, COLLIBRA]}
        editUrl={null}
        locking={false}
        onRefresh={vi.fn()}
        onLock={onLock}
      />,
    );

    fireEvent.click(screen.getByTestId("table-catalog-lock"));
    expect(screen.queryByRole("option", { name: "collibra · collibra", hidden: true })).toBeNull();
    fireEvent.click(await screen.findByRole("option", { name: "om · openmetadata", hidden: true }));
    expect(onLock).toHaveBeenCalledWith("om");
  });

  it("names the authority of a locked table and links to edit there", () => {
    render(
      <TableCatalogPanel
        catalogs={[{ ...OM, locked: true }, COLLIBRA]}
        editUrl="https://om.example/table/x"
        locking={false}
        onRefresh={vi.fn()}
        onLock={vi.fn()}
      />,
    );

    const locked = screen.getByTestId("table-catalog-locked");
    expect(locked).toHaveTextContent("om is their authority");
    expect(screen.getByRole("link", { name: /Edit in om/ })).toHaveAttribute(
      "href",
      "https://om.example/table/x",
    );
  });
});
