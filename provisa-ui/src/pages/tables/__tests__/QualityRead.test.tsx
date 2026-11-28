// Copyright (c) 2026 Kenneth Stott
// Canary: 77950ea5-3d49-43ba-929b-9c827d40b276
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table's Quality tab reads what its edit tab stores: a checker results table's contract (the
// table it observes and its checks), and the table's declared unique keys.

import { describe, it, expect, vi } from "vitest";
// REQ-2277 rule 6: names link to their panels for a viewer who opens them (EntityLink).
vi.mock("../../../context/AuthContext", () => ({
  useAuth: () => ({
    capabilities: ["access_config", "org_settings", "source_registration", "table_registration"],
  }),
}));
import { render, screen, within } from "../../../test-utils/render";
import type { RegisteredTable, Source } from "../../../types/admin";

const parseContract = vi.fn(async () => ({
  dataset: "provisa/sales/orders",
  checker: "soda",
  checks: [
    { columnName: "", checkType: "row_count", definition: "must_be_greater_than: 0", extra: "" },
    { columnName: "total", checkType: "missing", definition: "", extra: "" },
  ],
  error: null,
}));

// vmThreads shares the module registry, so pass the module through and replace only the one hook.
vi.mock("../../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../hooks/useAdminQueries")>()),
  useDqContract: () => ({ parseContract }),
}));

import { QualityRead } from "../TableSectionsRead";

const TABLE = {
  id: 7,
  dqContract: "dataset: provisa/sales/orders\nchecks: []",
  profilerSourceId: null,
  uniqueConstraints: [{ name: "order_no", columns: ["id", "line"] }],
  qualityCheckers: [],
} as unknown as RegisteredTable;

describe("QualityRead", () => {
  it("reads a checker results table's contract the way its builder lists it", async () => {
    render(<QualityRead table={TABLE} source={{ id: "dq", type: "Soda" } as Source} />);
    const checks = await screen.findByTestId("table-read-dq-checks");
    expect(parseContract).toHaveBeenCalledWith({ checker: "soda", contractText: TABLE.dqContract });
    expect(
      within(checks)
        .getAllByRole("columnheader")
        .map((h) => h.textContent),
    ).toEqual(["Column", "Check", "Args"]);
    expect(await within(checks).findByText("whole table")).toBeInTheDocument();
    expect(checks).toHaveTextContent("must_be_greater_than: 0");
    expect(screen.getByTestId("table-quality")).toHaveTextContent("sales.orders");
  });

  it("lists the unique keys under the uniques editor's headers", () => {
    render(<QualityRead table={TABLE} source={{ id: "pg", type: "postgresql" } as Source} />);
    const uniques = screen.getByTestId("table-read-uniques");
    expect(
      within(uniques)
        .getAllByRole("columnheader")
        .map((h) => h.textContent),
    ).toEqual(["Name", "Columns"]);
    expect(uniques).toHaveTextContent("id, line");
    expect(screen.queryByTestId("table-read-dq-checks")).not.toBeInTheDocument();
  });

  it("lists the checkers whose contracts scan the table, each linked, with its last scan", () => {
    const scanned = {
      ...TABLE,
      qualityCheckers: [
        {
          tableId: 21,
          tableName: "orders_rows",
          sourceId: "soda-dq",
          outcome: "fail",
          scanTime: "2026-10-12T09:00:00+00:00",
        },
        {
          tableId: 22,
          tableName: "orders_nulls",
          sourceId: "gx-dq",
          outcome: null,
          scanTime: null,
        },
      ],
    } as RegisteredTable;
    render(<QualityRead table={scanned} source={{ id: "pg", type: "postgresql" } as Source} />);
    const first = screen.getByTestId("table-read-checker-21");
    expect(within(first).getByRole("link", { name: "orders_rows" })).toHaveAttribute(
      "href",
      "/tables?table=21",
    );
    expect(first).toHaveTextContent("soda-dq");
    expect(first).toHaveTextContent("Fail");
    expect(screen.getByTestId("table-read-checker-22")).toHaveTextContent("—");
  });

  it("links the profiler to its source on the Sources page", () => {
    const profiled = { ...TABLE, profilerSourceId: "demo-profiler" } as RegisteredTable;
    render(<QualityRead table={profiled} source={{ id: "pg", type: "postgresql" } as Source} />);
    expect(screen.getByRole("link", { name: "demo-profiler" })).toHaveAttribute(
      "href",
      "/sources?expanded=demo-profiler",
    );
  });

  it("says no checker scans a table none does", () => {
    render(<QualityRead table={TABLE} source={{ id: "pg", type: "postgresql" } as Source} />);
    expect(screen.getByTestId("table-read-checkers")).toHaveTextContent("—");
  });
});
