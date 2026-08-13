// Copyright (c) 2026 Kenneth Stott
// Canary: 0414d12a-f1bb-4f02-94cc-223a7673b271
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1444: the viewer never holds more than a page, so an export reads the relation itself — the
// same governed statement, one call per chunk with an advancing OFFSET, until a chunk comes back
// short. The workbook gets every row, not the page the reader happens to be on.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../test-utils/render";
import type { RegisteredTable } from "../../types/admin";
import { runSql } from "../../api/admin";
import { downloadXlsx } from "../../pages/sql/exportXlsx";

vi.mock("../../api/admin", () => ({ runSql: vi.fn() }));
vi.mock("../../pages/sql/exportXlsx", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../pages/sql/exportXlsx")>()),
  downloadXlsx: vi.fn(),
}));

import { GovernedTableViewer } from "../GovernedTableViewer";

const CHUNK = 5000;
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

function rows(from: number, count: number) {
  return Array.from({ length: count }, (_, i) => ({ id: from + i }));
}

describe("GovernedTableViewer XLSX export", () => {
  beforeEach(() => {
    vi.mocked(runSql).mockReset();
    vi.mocked(downloadXlsx).mockClear();
    localStorage.clear();
  });

  it("pages through the relation until a chunk comes back short", async () => {
    // The grid's own page, then the export's chunks: a full one (plus the +1 probe row) and a short one.
    vi.mocked(runSql)
      .mockResolvedValueOnce({ columns: ["id"], rows: rows(0, 26) })
      .mockResolvedValueOnce({ columns: ["id"], rows: rows(0, CHUNK + 1) })
      .mockResolvedValueOnce({ columns: ["id"], rows: rows(CHUNK, 3) });

    render(<GovernedTableViewer table={TABLE} />);
    await waitFor(() => expect(screen.getByTestId("download-xlsx-btn")).toBeTruthy());
    fireEvent.click(screen.getByTestId("download-xlsx-btn"));

    await waitFor(() => expect(downloadXlsx).toHaveBeenCalled());
    const exportCalls = vi.mocked(runSql).mock.calls.slice(1);
    expect(exportCalls[0][0]).toContain(`LIMIT ${CHUNK + 1} OFFSET 0`);
    expect(exportCalls[1][0]).toContain(`LIMIT ${CHUNK + 1} OFFSET ${CHUNK}`);
    expect(exportCalls).toHaveLength(2);

    const [, , exported] = vi.mocked(downloadXlsx).mock.calls[0];
    expect(exported).toHaveLength(CHUNK + 3);
    expect(exported[CHUNK + 2]).toEqual({ id: CHUNK + 2 });
  });

  it("does not write a workbook when a chunk fails", async () => {
    vi.mocked(runSql)
      .mockResolvedValueOnce({ columns: ["id"], rows: rows(0, 2) })
      .mockResolvedValueOnce({ columns: [], rows: [], error: "permission denied" });

    render(<GovernedTableViewer table={TABLE} />);
    await waitFor(() => expect(screen.getByTestId("download-xlsx-btn")).toBeTruthy());
    fireEvent.click(screen.getByTestId("download-xlsx-btn"));

    await waitFor(() => expect(vi.mocked(runSql).mock.calls).toHaveLength(2));
    expect(downloadXlsx).not.toHaveBeenCalled();
  });
});
