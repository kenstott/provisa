// Copyright (c) 2026 Kenneth Stott
// Canary: 7c1e5a42-93b0-4f6d-8a1e-2d5b0c9f4e17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The SCHEMA tree lists schemas and the tables under each alphabetically (case-insensitive, by the
// displayed name); column order within a table stays as declared.

import { describe, it, expect, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import { SchemaBrowser } from "../SchemaBrowser";
import type { ListedTable } from "../../../types/admin";

function tbl(tableName: string, alias: string | null = null, columns: string[] = []): ListedTable {
  return {
    id: 1,
    sourceId: "src1",
    domainId: "d",
    schemaName: "public",
    tableName,
    alias,
    columns: columns.map((c, i) => ({ id: i, columnName: c, alias: null, visibleTo: [] })),
  } as unknown as ListedTable;
}

function renderBrowser(
  domainGroups: Record<string, ListedTable[]>,
  expandedDomains: Set<string>,
  expandedTables = new Set<string>(),
) {
  render(
    <SchemaBrowser
      sidebarOpen
      setSidebarOpen={vi.fn()}
      domainGroups={domainGroups}
      domainMap={{}}
      expandedDomains={expandedDomains}
      expandedTables={expandedTables}
      domainPages={{}}
      topTab="sql"
      insertAtCursor={vi.fn()}
      toggleDomain={vi.fn()}
      toggleTable={vi.fn()}
      setDomainPages={vi.fn()}
      metrics={[]}
      tables={[]}
    />,
  );
}

const ids = (prefix: string) =>
  screen
    .getAllByTestId(new RegExp(`^${prefix}`))
    .map((el) => el.getAttribute("data-testid"));

describe("SchemaBrowser alphabetical order", () => {
  it("lists schemas alphabetically, ignoring case", () => {
    renderBrowser(
      { registered_tables: [], meta: [], Domains: [], analytics: [] },
      new Set(),
    );
    expect(ids("schema-domain-")).toEqual([
      "schema-domain-analytics",
      "schema-domain-Domains",
      "schema-domain-meta",
      "schema-domain-registered_tables",
    ]);
  });

  it("lists tables alphabetically by displayed name (alias, else table name)", () => {
    renderBrowser(
      {
        meta: [
          tbl("registered_tables"),
          tbl("table_columns"),
          tbl("domains"),
          tbl("zeta", "Alpha"),
          tbl("Relationships"),
        ],
      },
      new Set(["meta"]),
    );
    expect(ids("schema-table-")).toEqual([
      "schema-table-zeta",
      "schema-table-domains",
      "schema-table-registered_tables",
      "schema-table-Relationships",
      "schema-table-table_columns",
    ]);
  });

  it("keeps columns in declared order", () => {
    renderBrowser(
      { meta: [tbl("t", null, ["zz", "aa", "mm"])] },
      new Set(["meta"]),
      new Set(["t"]),
    );
    const text = document.body.textContent ?? "";
    expect(text.indexOf("zz")).toBeLessThan(text.indexOf("aa"));
    expect(text.indexOf("aa")).toBeLessThan(text.indexOf("mm"));
  });
});
