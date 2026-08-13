// Copyright (c) 2026 Kenneth Stott
// Canary: 4f9bfc02-7a09-4166-8783-d48947f33161
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1444: the XLSX export is the whole relation, not the page on screen. A server-paged caller
// hands the grid a reader for the rest; without one the grid already holds every row.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../../test-utils/render";
import { ResultsGrid } from "../ResultsGrid";
import { useResultsGrid } from "../useResultsGrid";
import { downloadXlsx } from "../exportXlsx";
import { notifications } from "@mantine/notifications";

vi.mock("../exportXlsx", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../exportXlsx")>()),
  downloadXlsx: vi.fn(),
}));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

const COLS = ["id"];
const PAGE = [{ id: 1 }];
const ALL = [{ id: 1 }, { id: 2 }, { id: 3 }];

function ServerPaged({
  fetchAllRows,
}: {
  fetchAllRows?: () => Promise<Record<string, unknown>[]>;
}) {
  const grid = useResultsGrid(PAGE, COLS, undefined, { hasMore: true });
  return <ResultsGrid grid={grid} totalRowCount={PAGE.length} fetchAllRows={fetchAllRows} />;
}

function scopeOf(entries: { label: string; value: string }[]): string {
  const entry = entries.find((e) => e.label === "Scope");
  if (!entry) throw new Error(`no Scope entry in ${JSON.stringify(entries)}`);
  return entry.value;
}

describe("ResultsGrid XLSX export scope", () => {
  beforeEach(() => {
    vi.mocked(downloadXlsx).mockClear();
    vi.mocked(notifications.show).mockClear();
  });

  it("exports every row the reader returns, not the page on screen", async () => {
    render(<ServerPaged fetchAllRows={() => Promise.resolve(ALL)} />);
    fireEvent.click(screen.getByTestId("download-xlsx-btn"));

    await waitFor(() => expect(downloadXlsx).toHaveBeenCalled());
    const [, , rows, entries] = vi.mocked(downloadXlsx).mock.calls[0];
    expect(rows).toEqual(ALL);
    expect(scopeOf(entries)).toBe("Every row matching the choices recorded here");
    expect(entries.find((e) => e.label === "Rows exported")?.value).toBe("3");
  });

  it("says so in the provenance when a server-paged caller offers no reader", async () => {
    render(<ServerPaged />);
    fireEvent.click(screen.getByTestId("download-xlsx-btn"));

    await waitFor(() => expect(downloadXlsx).toHaveBeenCalled());
    const [, , rows, entries] = vi.mocked(downloadXlsx).mock.calls[0];
    expect(rows).toEqual(PAGE);
    expect(scopeOf(entries)).toContain("Page 1");
  });

  it("surfaces a failed read instead of writing a short workbook", async () => {
    render(<ServerPaged fetchAllRows={() => Promise.reject(new Error("read denied"))} />);
    fireEvent.click(screen.getByTestId("download-xlsx-btn"));

    await waitFor(() =>
      expect(notifications.show).toHaveBeenCalledWith({ color: "red", message: "read denied" }),
    );
    expect(downloadXlsx).not.toHaveBeenCalled();
  });
});
