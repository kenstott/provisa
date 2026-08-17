// Copyright (c) 2026 Kenneth Stott
// Canary: 87c5c43d-ece0-45f2-abdc-1c5de0f6401e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1484: the save-time diff that decides whether to ask the server what depends on the columns
// this edit is about to rename or drop.

import { describe, it, expect } from "vitest";
import { diffEditedColumns } from "../pages/tables/columnDiff";
import type { TableColumn } from "../types/admin";

function column(columnName: string, alias: string | null): TableColumn {
  return { columnName, alias } as TableColumn;
}

describe("diffEditedColumns (REQ-1484)", () => {
  it("reports nothing when nothing changed", () => {
    const stored = [column("order_total", "total"), column("cust_id", null)];
    expect(diffEditedColumns(stored, [...stored])).toEqual({ renamed: [], removed: [] });
  });

  it("reports a changed alias by its physical column name", () => {
    const stored = [column("order_total", "total")];
    const edited = [column("order_total", "amount")];
    expect(diffEditedColumns(stored, edited)).toEqual({ renamed: ["order_total"], removed: [] });
  });

  it("treats setting an alias on an unaliased column as a rename", () => {
    // The column already had an exposed name — the snake_case default — that views may use.
    const stored = [column("order_total", null)];
    const edited = [column("order_total", "total")];
    expect(diffEditedColumns(stored, edited)).toEqual({ renamed: ["order_total"], removed: [] });
  });

  it("treats clearing an alias as a rename", () => {
    const stored = [column("order_total", "total")];
    const edited = [column("order_total", null)];
    expect(diffEditedColumns(stored, edited)).toEqual({ renamed: ["order_total"], removed: [] });
  });

  it("does not treat an empty-string alias as different from no alias", () => {
    const stored = [column("order_total", null)];
    const edited = [column("order_total", "")];
    expect(diffEditedColumns(stored, edited)).toEqual({ renamed: [], removed: [] });
  });

  it("reports a dropped column", () => {
    const stored = [column("order_total", "total"), column("cust_id", null)];
    const edited = [column("order_total", "total")];
    expect(diffEditedColumns(stored, edited)).toEqual({ renamed: [], removed: ["cust_id"] });
  });

  it("ignores an added column, which nothing can depend on yet", () => {
    const stored = [column("order_total", "total")];
    const edited = [column("order_total", "total"), column("discount", "disc")];
    expect(diffEditedColumns(stored, edited)).toEqual({ renamed: [], removed: [] });
  });

  it("reports a rename and a drop together", () => {
    const stored = [column("order_total", "total"), column("cust_id", null)];
    const edited = [column("order_total", "amount")];
    expect(diffEditedColumns(stored, edited)).toEqual({
      renamed: ["order_total"],
      removed: ["cust_id"],
    });
  });
});
