// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8a1d6e-52c4-4b0a-9e7d-61b2a8c04d93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Hovering and focusing the tree's rows opens their hints without React's "Maximum update depth
// exceeded": a Tip nested in another Tip's target must not re-attach its ref on every render.

import { describe, it, expect, vi, afterEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen } from "../../../test-utils/render";
import { SchemaBrowser } from "../SchemaBrowser";
import type { ListedTable } from "../../../types/admin";

const table = {
  id: 1,
  sourceId: "s",
  domainId: "sales",
  schemaName: "public",
  tableName: "orders",
  alias: null,
  description: "All orders",
  columns: [
    { id: 1, columnName: "id", computedSqlAlias: "id", alias: null, description: "Key", visibleTo: [] },
  ],
} as unknown as ListedTable;

afterEach(() => vi.restoreAllMocks());

describe("SchemaBrowser hints", () => {
  it("hovering every row with a hint does not loop", async () => {
    const errors = vi.spyOn(console, "error").mockImplementation(() => {});
    const user = userEvent.setup();
    render(
      <SchemaBrowser
        sidebarOpen
        setSidebarOpen={vi.fn()}
        domainGroups={{ sales: [table] }}
        domainMap={{ sales: { id: "sales", description: "Sales data" } as never }}
        expandedDomains={new Set(["sales"])}
        expandedTables={new Set(["orders"])}
        domainPages={{}}
        topTab="sql"
        insertAtCursor={vi.fn()}
        toggleDomain={vi.fn()}
        toggleTable={vi.fn()}
        setDomainPages={vi.fn()}
        metrics={[]}
        tables={[table]}
      />,
    );
    for (const id of ["schema-domain-sales", "schema-table-orders", "schema-insert-table-orders", "schema-column-orders-id"]) {
      await user.hover(screen.getByTestId(id));
      await new Promise((r) => setTimeout(r, 250));
      await user.keyboard("{Tab}");
    }
    const loops = errors.mock.calls.filter((c) => String(c[0]).includes("Maximum update depth"));
    expect(loops).toEqual([]);
    expect(screen.getByTestId("schema-domain-sales")).toBeTruthy();
  });
});
