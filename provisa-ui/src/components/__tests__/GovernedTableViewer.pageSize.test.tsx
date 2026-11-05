// Copyright (c) 2026 Kenneth Stott
// Canary: 6c375cad-c127-4d1e-8048-e653be1d59c7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The viewer shows one page of the relation. Choosing another page size reads that many rows and
// shows that many; a relation that fits on the page offers no next page.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../../test-utils/render";
import type { RegisteredTable } from "../../types/admin";
import { runSql } from "../../api/admin";

vi.mock("../../api/admin", () => ({ runSql: vi.fn() }));

import { GovernedTableViewer } from "../GovernedTableViewer";

const TABLE = {
  id: 1,
  sourceId: "src",
  domainId: "ops",
  schemaName: "public",
  tableName: "events",
  alias: null,
  description: null,
  apiEndpoint: null,
  viewSql: null,
  columns: [],
} as unknown as RegisteredTable;

/** Answers a statement as the server would: the rows its LIMIT and OFFSET name, out of `total`. */
function relationOf(total: number) {
  vi.mocked(runSql).mockImplementation(async (statement: unknown) => {
    const sql = (statement as { sql: string }).sql;
    const limit = Number(/LIMIT (\d+)/.exec(sql)?.[1]);
    const offset = Number(/OFFSET (\d+)/.exec(sql)?.[1]);
    const count = Math.max(0, Math.min(limit, total - offset));
    return {
      columns: ["id"],
      rows: Array.from({ length: count }, (_, i) => ({ id: offset + i })),
    } as never;
  });
}

const bodyRows = () => document.querySelectorAll(".sql-results-table tbody tr").length;

async function choosePageSize(n: number) {
  fireEvent.click(screen.getByTestId("page-size-select"));
  const option = await screen.findByRole("option", { name: String(n), hidden: true });
  fireEvent.click(option);
}

describe("GovernedTableViewer page size", () => {
  beforeEach(() => {
    vi.mocked(runSql).mockReset();
    localStorage.clear();
  });

  it("shows as many rows as the chosen page size", async () => {
    relationOf(300);
    render(<GovernedTableViewer table={TABLE} role="analyst" />);
    await waitFor(() => expect(bodyRows()).toBeGreaterThan(0));

    await choosePageSize(100);
    await waitFor(() => expect(bodyRows()).toBe(100));

    await choosePageSize(25);
    await waitFor(() => expect(bodyRows()).toBe(25));
    const last = vi.mocked(runSql).mock.calls.at(-1)![0] as { sql: string };
    expect(last.sql).toContain("LIMIT 26 OFFSET 0");
  });

  it("offers no next or previous page when the relation fits on one", async () => {
    relationOf(10);
    render(<GovernedTableViewer table={TABLE} role="analyst" />);
    await waitFor(() => expect(bodyRows()).toBe(10));

    const pager = within(document.body);
    expect((pager.getByLabelText("Next page") as HTMLButtonElement).disabled).toBe(true);
    expect((pager.getByLabelText("Previous page") as HTMLButtonElement).disabled).toBe(true);
    expect((pager.getByLabelText("First page") as HTMLButtonElement).disabled).toBe(true);
  });
});
