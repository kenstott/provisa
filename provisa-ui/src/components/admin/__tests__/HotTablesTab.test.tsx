// Copyright (c) 2026 Kenneth Stott
// Canary: b4e91bd9-0fcc-4b62-a0c1-456c9796948d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-826: the cache manager lists, beside the hot tier, the tables replicated because they are
// busy — served from their replica, or still read live while the replica is built (no row count
// to report yet).

import { describe, it, expect, vi } from "vitest";
import { render, screen, within } from "../../../test-utils/render";
import type { HotTableStat } from "../../../api/admin";

const rows: HotTableStat[] = [
  { tableName: "currencies", catalog: "pg", schemaName: "public", rowCount: 40, kind: "hot", region: null },
  { tableName: "orders", catalog: "pg", schemaName: "public", rowCount: 5000, kind: "replica", region: null },
  {
    tableName: "items",
    catalog: "pg",
    schemaName: "public",
    rowCount: 0,
    kind: "replica_building",
    region: null,
  },
];

vi.mock("../../../hooks/useAdminOpsQueries", async (orig) => ({
  ...(await orig<typeof import("../../../hooks/useAdminOpsQueries")>()),
  useHotTables: () => ({ hotTables: rows, loading: false, error: undefined, refetch: vi.fn() }),
}));

import { HotTablesTab } from "../CacheManager";

function row(table: string): HTMLElement {
  return screen.getByText(table).closest("tr") as HTMLElement;
}

describe("HotTablesTab", () => {
  it("counts and labels the tables replicated because they are busy", () => {
    render(<HotTablesTab platform={false} />);
    const stat = screen.getByText("Replicated When Busy").parentElement as HTMLElement;
    expect(within(stat).getByText("2")).toBeInTheDocument();
    expect(within(row("orders")).getByText("replicated (busy)")).toBeInTheDocument();
    expect(within(row("orders")).getByText("5000")).toBeInTheDocument();
    expect(within(row("currencies")).getByText("hot")).toBeInTheDocument();
  });

  it("shows a replica still being built with no row count", () => {
    render(<HotTablesTab platform={false} />);
    const building = row("items");
    expect(within(building).getByText("replica being built")).toBeInTheDocument();
    expect(within(building).getByText("—")).toBeInTheDocument();
    expect(within(building).queryByText("0")).toBeNull();
  });

  it("explains when a table is replicated for being busy", () => {
    render(<HotTablesTab platform={false} />);
    expect(
      screen.getByText(/replicated once the statements that read it pass its threshold/),
    ).toBeInTheDocument();
  });
});
