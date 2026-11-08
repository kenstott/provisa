// Copyright (c) 2026 Kenneth Stott
// Canary: 5b0c7a1e-3f52-4d0a-9a53-2c6f1d8e4b97
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The report list: sorted by name, each description clamped to two lines and unfolded by a click
// that does not select the report.

import { afterEach, describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, within } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", () => ({
  useAuth: () => ({
    actingRoles: "analyst",
    loading: false,
    capabilities: ["table_registration"],
    demonstrated: [],
  }),
}));

const LONG =
  "A very long description of this report that goes on and on, naming every column it shows and every rule it applies, so that it cannot fit on two lines of a narrow list.";

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
  row(1, "zeta_usage", null, null),
  row(2, "mid", "beta report", LONG),
  row(3, "alpha_view", "Alpha report", "short"),
  row(4, "other", "Not ops", null),
];
TABLES[3].domainId = "sales";

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

function laidOut(lines: number) {
  vi.spyOn(HTMLElement.prototype, "scrollHeight", "get").mockReturnValue(lines * 20);
  vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockReturnValue(Math.min(lines, 2) * 20);
}

afterEach(() => vi.restoreAllMocks());

describe("ReportsTab list", () => {
  it("lists the ops reports alphabetically by the name shown, case ignored", () => {
    laidOut(1);
    render(<ReportsTab />);
    const list = screen.getByTestId("reports-list");
    const names = within(list)
      .getAllByTestId(/^report-item-/)
      .map((el) => el.getAttribute("data-testid"));
    expect(names).toEqual(["report-item-alpha_view", "report-item-mid", "report-item-zeta_usage"]);
  });

  it("clamps a long description to two lines and expands it on click, then folds it", () => {
    laidOut(6);
    render(<ReportsTab />);
    const text = screen.getByTestId("report-description-mid-text");
    expect(text.style.webkitLineClamp).toBe("2");
    expect(text.style.overflow).toBe("hidden");
    const d = screen.getByTestId("report-description-mid");
    expect(d).toHaveAttribute("aria-expanded", "false");

    fireEvent.click(d);
    expect(d).toHaveAttribute("aria-expanded", "true");
    expect(text.style.webkitLineClamp).toBe("");
    expect(text.style.overflow).toBe("");
    expect(text.textContent).toBe(LONG);

    fireEvent.click(d);
    expect(d).toHaveAttribute("aria-expanded", "false");
    expect(text.style.webkitLineClamp).toBe("2");
  });

  it("shows a description that fits in two lines as it is, with nothing to expand", () => {
    laidOut(2);
    render(<ReportsTab />);
    expect(screen.queryByTestId("report-description-alpha_view")).toBeNull();
    expect(screen.getByTestId("report-description-alpha_view-text")).toHaveTextContent("short");
  });

  it("does not select the report when its description is clicked; the row still does", () => {
    laidOut(6);
    render(<ReportsTab />);
    // The first report in order is the fallback selection.
    expect(screen.getByTestId("report-pane")).toHaveAttribute("data-report", "alpha_view");

    fireEvent.click(screen.getByTestId("report-description-mid"));
    expect(screen.getByTestId("report-pane")).toHaveAttribute("data-report", "alpha_view");

    fireEvent.click(screen.getByTestId("report-item-mid"));
    expect(screen.getByTestId("report-pane")).toHaveAttribute("data-report", "mid");
  });
});
