// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4a6c1e-3b7f-4e52-8a90-1f2c5e7b3d68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// SELECT 1 AS id, 2 AS id: two columns of one name each keep their own value -- in the grid, the
// CSV, the XLSX and a copy -- and both headers read "id".

import { describe, it, expect, vi, afterEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../../../test-utils/render";
import { ResultsGrid } from "../ResultsGrid";
import { useResultsGrid } from "../useResultsGrid";
import { downloadXlsx, dataSheet } from "../exportXlsx";
import { runSqlEach } from "../../../api/admin";
import { columnKeys, columnLabel, rowByKeys } from "../../../lib/columnKeys";

vi.mock("../exportXlsx", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../exportXlsx")>()),
  downloadXlsx: vi.fn(),
}));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

afterEach(() => vi.unstubAllGlobals());

const KEYS = columnKeys(["id", "id"]);
const ROWS = [rowByKeys(KEYS, [1, 2])];

function Grid() {
  const grid = useResultsGrid(ROWS, KEYS);
  return <ResultsGrid grid={grid} totalRowCount={ROWS.length} />;
}

describe("columns of one name", () => {
  it("are keyed apart, each named as the result named it", () => {
    expect(KEYS).toHaveLength(2);
    expect(new Set(KEYS).size).toBe(2);
    expect(KEYS.map(columnLabel)).toEqual(["id", "id"]);
    expect(KEYS[0]).toBe("id");
  });

  it("read from the server's positional rows, keep both values", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            results: [
              {
                statement: "SELECT 1 AS id, 2 AS id",
                warnings: [],
                data: [[1, 2]],
                columns: ["id", "id"],
                column_types: ["integer", "integer"],
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );
    const { answers } = await runSqlEach("SELECT 1 AS id, 2 AS id", "analyst", false);
    const [a] = answers;
    expect(a.columns.map(columnLabel)).toEqual(["id", "id"]);
    expect(a.columns.map((c) => a.rows[0][c])).toEqual([1, 2]);
  });

  it("show 1 and 2 in the grid under two id headers", () => {
    render(<Grid />);
    const table = screen.getByRole("table");
    const headers = within(table)
      .getAllByRole("columnheader")
      .map((th) => th.querySelector(".th-label span")?.textContent);
    expect(headers.filter((h) => h === "id")).toHaveLength(2);
    const cells = within(table)
      .getAllByRole("cell")
      .map((td) => td.textContent);
    expect(cells).toEqual(expect.arrayContaining(["1", "2"]));
  });

  it("write both values to the CSV under two id headers", async () => {
    const blobs: Blob[] = [];
    vi.stubGlobal("URL", {
      ...URL,
      createObjectURL: (b: Blob) => {
        blobs.push(b);
        return "blob:csv";
      },
      revokeObjectURL: vi.fn(),
    });
    const click = vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
    render(<Grid />);
    fireEvent.click(screen.getByTestId("download-csv-btn"));
    await waitFor(() => expect(click).toHaveBeenCalled());
    expect(await blobs[0].text()).toBe("id,id\n1,2");
    click.mockRestore();
  });

  it("write both values to the XLSX under two id headers", async () => {
    render(<Grid />);
    fireEvent.click(screen.getByTestId("download-xlsx-btn"));
    await waitFor(() => expect(downloadXlsx).toHaveBeenCalled());
    const [, columns, rows] = vi.mocked(downloadXlsx).mock.calls[0];
    const [header, body] = dataSheet(columns, rows);
    expect(header.map((c) => c?.value)).toEqual(["id", "id"]);
    expect(body.map((c) => c?.value)).toEqual([1, 2]);
  });
});
