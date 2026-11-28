// Copyright (c) 2026 Kenneth Stott
// Canary: 8a3e1f57-2b90-4c64-9d1e-7f5a0c3b6e28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234: Review samples shows each registered table's first rows as a tab, read-only, with no
// export, filters, paging or row count; done once every tab has been opened; a Query it link.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import type { ListedTable } from "../../../types/admin";

const runSql = vi.fn();
const previewTable = vi.fn();
vi.mock("../../../api/admin", () => ({
  runSql: (...a: unknown[]) => runSql(...a),
  previewTable: (...a: unknown[]) => previewTable(...a),
  PreviewRefused: class PreviewRefused extends Error {},
}));
const listed = (id: number, tableName: string): ListedTable =>
  ({
    id,
    tableName,
    alias: null,
    schemaName: "public",
    domainId: "sales",
    columns: [{ columnName: "id", alias: null, dataType: "integer" }],
    withdrawn: false,
    sourceWithdrawn: false,
  }) as unknown as ListedTable;
const tables = [listed(1, "orders"), listed(2, "lines")];
vi.mock("../../../hooks/useAdminQueries", () => ({
  useTables: () => ({ tables }),
  useTableAccess: () => ({ access: { graphql: "query { orders { id } }" }, error: null }),
}));

const { ReviewSamplesStep } = await import("../ReviewSamplesStep");

beforeEach(() => {
  runSql.mockReset();
  previewTable.mockReset();
  runSql.mockResolvedValue({
    columns: ["id"],
    rows: Array.from({ length: 21 }, (_, i) => ({ id: i })),
  });
});

function renderStep(onOpenQuery = vi.fn(), onAllOpened = vi.fn()) {
  render(
    <ReviewSamplesStep
      registered={{ tableIds: [1, 2], refused: [] }}
      role="analyst"
      onOpenQuery={onOpenQuery}
      onAllOpened={onAllOpened}
    />,
  );
  return { onOpenQuery, onAllOpened };
}

describe("ReviewSamplesStep", () => {
  it("shows about twenty rows, read-only: no export, filter, paging or count", async () => {
    renderStep();
    await waitFor(() => expect(screen.getAllByRole("row")).toHaveLength(21));
    expect(runSql.mock.calls[0][1]).toBe("analyst");
    expect(screen.queryByText(/CSV|XLSX|Copy/)).toBeNull();
    expect(screen.queryByRole("textbox")).toBeNull();
    expect(screen.queryByText(/rows\)/)).toBeNull();
  });

  it("done once every tab has been opened, not before", async () => {
    const { onAllOpened } = renderStep();
    await waitFor(() => expect(runSql).toHaveBeenCalledTimes(1));
    expect(onAllOpened).not.toHaveBeenCalled();
    fireEvent.mouseDown(screen.getByTestId("review-samples-tab-lines"));
    fireEvent.click(screen.getByTestId("review-samples-tab-lines"));
    await waitFor(() => expect(onAllOpened).toHaveBeenCalledTimes(1));
  });

  it("Query it opens the explorer on the table's query", async () => {
    const { onOpenQuery } = renderStep();
    fireEvent.click(await screen.findByTestId("review-samples-query-it"));
    expect(onOpenQuery).toHaveBeenCalledWith("query { orders { id } }");
  });

  it("a read the server refused is said as the error", async () => {
    runSql.mockResolvedValue({ columns: [], rows: [], error: "denied" });
    renderStep();
    expect(await screen.findByTestId("review-samples-error")).toHaveTextContent("denied");
  });
});
