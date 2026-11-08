// Copyright (c) 2026 Kenneth Stott
// Canary: 9d2e6f3a-41b8-4c75-8e0d-7a1c53b9f624
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The report list filter: the shared FilterInput narrows the list by name or description,
// case-insensitive; clearing restores the list; no match shows a line.

import { afterEach, beforeEach, describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, within } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", () => ({
  useAuth: () => ({
    actingRoles: "analyst",
    loading: false,
    capabilities: ["table_registration"],
    demonstrated: [],
  }),
}));

const row = (id: number, tableName: string, alias: string | null, description: string | null) => ({
  id,
  tableName,
  alias,
  description,
  domainId: "ops",
  sourceId: "pg",
  schemaName: "views",
});
const TABLES = [
  row(1, "zeta_usage", null, "Credits consumed per tenant"),
  row(2, "mid", "beta report", "Revenue by REGION"),
  row(3, "alpha_view", "Alpha report", "short"),
];

vi.mock("../../../hooks/useAdminQueries", () => ({
  useTables: () => ({ tables: TABLES, loading: false, refetch: vi.fn() }),
  useRegisterTable: () => ({ registerTable: vi.fn() }),
  useDeleteTable: () => ({ deleteTable: vi.fn() }),
}));
vi.mock("../../GovernedTableViewer", () => ({
  GovernedTableViewer: ({ table }: { table: { tableName: string } }) => (
    <div data-testid="viewer">{table.tableName}</div>
  ),
}));

import { ReportsTab } from "../ReportsTab";

const shown = () =>
  within(screen.getByTestId("reports-list"))
    .queryAllByTestId(/^report-item-/)
    .map((el) => el.getAttribute("data-testid"));
const type = (v: string) =>
  fireEvent.change(screen.getByTestId("filter-input"), { target: { value: v } });

beforeEach(() => window.localStorage.clear());
afterEach(() => vi.restoreAllMocks());

describe("ReportsTab filter", () => {
  it("filters by name, case-insensitive, keeping the alphabetical order", () => {
    render(<ReportsTab />);
    expect(shown()).toHaveLength(3);
    type("REPORT");
    expect(shown()).toEqual(["report-item-alpha_view", "report-item-mid"]);
    type("zeta");
    expect(shown()).toEqual(["report-item-zeta_usage"]);
  });

  it("filters by description, case-insensitive", () => {
    render(<ReportsTab />);
    type("revenue by region");
    expect(shown()).toEqual(["report-item-mid"]);
    type("TENANT");
    expect(shown()).toEqual(["report-item-zeta_usage"]);
  });

  it("restores the full list when the filter is cleared", () => {
    render(<ReportsTab />);
    type("tenant");
    expect(shown()).toHaveLength(1);
    fireEvent.click(screen.getByTestId("filter-input-clear"));
    expect(shown()).toEqual(["report-item-alpha_view", "report-item-mid", "report-item-zeta_usage"]);
  });

  it("shows a No reports match line when nothing matches, and no list rows", () => {
    render(<ReportsTab />);
    type("nothing like this");
    expect(shown()).toEqual([]);
    expect(screen.getByTestId("reports-no-match")).toHaveTextContent("No reports match");
    expect(screen.queryByText("No reports registered in the ops domain.")).toBeNull();
    type("");
    expect(screen.queryByTestId("reports-no-match")).toBeNull();
  });
});
