// Copyright (c) 2026 Kenneth Stott
// Canary: 9b3e7f12-4c58-4a6d-8e20-d61f0a5c3b94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2361: the SQL page inserts a table by the name the server's names table gives it (the
// table's sqlSchema and sqlName), never one built from its domain id and alias; a table the
// published build does not name inserts nothing.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { SchemaBrowser } from "../SchemaBrowser";
import type { ListedTable } from "../../../types/admin";

function table(sqlSchema: string | null, sqlName: string | null): ListedTable {
  return {
    id: 1,
    sourceId: "s",
    domainId: "sales-eu",
    schemaName: "public",
    tableName: "orders",
    alias: "orders_alias",
    sqlSchema,
    sqlName,
    description: null,
    columns: [],
  } as unknown as ListedTable;
}

function renderWith(tbl: ListedTable, insertAtCursor: (s: string) => void) {
  render(
    <SchemaBrowser
      sidebarOpen
      setSidebarOpen={vi.fn()}
      domainGroups={{ "sales-eu": [tbl] }}
      domainMap={{ "sales-eu": { id: "sales-eu", description: "" } as never }}
      expandedDomains={new Set(["sales-eu"])}
      expandedTables={new Set()}
      domainPages={{}}
      topTab="sql"
      insertAtCursor={insertAtCursor}
      toggleDomain={vi.fn()}
      toggleTable={vi.fn()}
      setDomainPages={vi.fn()}
      metrics={[]}
      tables={[tbl]}
    />,
  );
}

describe("SchemaBrowser inserts a table by its served SQL name", () => {
  it("inserts the names table's schema and name, quoted", () => {
    const insert = vi.fn();
    renderWith(table("<served_schema>", "<served_name>"), insert);
    fireEvent.click(screen.getByTestId("schema-insert-table-orders"));
    expect(insert).toHaveBeenCalledWith('"<served_schema>"."<served_name>"');
  });

  it("inserts nothing for a table the published build does not name", () => {
    const insert = vi.fn();
    renderWith(table(null, null), insert);
    fireEvent.click(screen.getByTestId("schema-insert-table-orders"));
    expect(insert).not.toHaveBeenCalled();
  });
});
