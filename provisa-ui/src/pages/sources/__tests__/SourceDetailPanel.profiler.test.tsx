// Copyright (c) 2026 Kenneth Stott
// Canary: 8e9fd7a8-93f0-464c-babd-7c2402acc6fd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1934: a profiler's detail view has a button that runs it at once for all its members, among its actions.
import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../../test-utils/render";
import type { Source } from "../../../types/admin";

const runProfiler = vi.fn();
vi.mock("../../../api/profiler", () => ({ runProfiler: (id: string) => runProfiler(id) }));
vi.mock("../../../hooks/useAdminQueries", () => ({
  useRefreshKaggleSource: () => ({ refreshKaggleSource: vi.fn(), loading: false }),
}));

import { SourceDetailPanel } from "../SourceDetailPanel";

function panel(type: string) {
  const s = { id: "nightly", type, mappingJson: null } as unknown as Source;
  return render(
    <SourceDetailPanel
      s={s}
      fields={null}
      onEdit={vi.fn()}
      onDuplicate={vi.fn()}
      onNavigate={vi.fn()}
      onDelete={vi.fn()}
      onWithdraw={vi.fn()}
      onPutBack={vi.fn()}
    />,
  );
}

describe("SourceDetailPanel run profiler", () => {
  it("shows the run button among the source's actions, and it runs the profiler", async () => {
    runProfiler.mockResolvedValue([{ table: "orders", error: null }]);
    panel("data_profiler");
    const button = screen.getByTestId("source-detail-run-profiler");
    // Beside Edit and Delete, in the same row of borderless icon buttons.
    expect(button.parentElement).toBe(screen.getByTestId("source-detail-edit").parentElement);
    expect(button.parentElement).toBe(screen.getByTestId("source-detail-delete").parentElement);
    expect(button.getAttribute("data-variant")).toBe("subtle");
    fireEvent.click(button);
    expect(runProfiler).toHaveBeenCalledWith("nightly");
    expect(await screen.findByTestId("source-detail-profiler-outcomes")).toBeInTheDocument();
  });

  it("lists a member that requires parameters as not read, with what it requires", async () => {
    // REQ-1986: the run asks the table's metadata and does not read it.
    runProfiler.mockResolvedValue([
      { table: "orders", run_id: "r1", error: null, not_read: null },
      {
        table: "get_order_by_id",
        run_id: null,
        error: null,
        not_read: "cannot be counted or profiled: it requires orderId",
        requires: ["orderId"],
      },
    ]);
    panel("data_profiler");
    fireEvent.click(screen.getByTestId("source-detail-run-profiler"));
    const outcomes = await screen.findByTestId("source-detail-profiler-outcomes");
    expect(outcomes).toHaveTextContent("orders: profiled");
    expect(outcomes).toHaveTextContent(
      "get_order_by_id: not read. It requires orderId, so it cannot be counted or profiled.",
    );
    expect(outcomes).not.toHaveTextContent("get_order_by_id: profiled");
  });

  it("has no run button on any other source", () => {
    panel("postgresql");
    expect(screen.queryByTestId("source-detail-run-profiler")).toBeNull();
  });
});
