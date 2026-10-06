// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1934: a profiler's detail view ends with a button that runs it at once for all its members.
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
      domainsEnabled={false}
      getEffectiveTtl={() => ""}
      onEdit={vi.fn()}
      onNavigate={vi.fn()}
      onDelete={vi.fn()}
    />,
  );
}

describe("SourceDetailPanel run profiler", () => {
  it("ends a profiler's detail view with the run button, which runs the profiler", async () => {
    runProfiler.mockResolvedValue([{ table: "orders", error: null }]);
    const { container } = panel("data_profiler");
    const button = screen.getByTestId("source-detail-run-profiler");
    const buttons = container.querySelectorAll("button");
    expect(buttons[buttons.length - 1]).toBe(button);
    fireEvent.click(button);
    expect(runProfiler).toHaveBeenCalledWith("nightly");
    expect(await screen.findByTestId("source-detail-profiler-outcomes")).toBeInTheDocument();
  });

  it("has no run button on any other source", () => {
    panel("postgresql");
    expect(screen.queryByTestId("source-detail-run-profiler")).toBeNull();
  });
});
