// Copyright (c) 2026 Kenneth Stott
// Canary: 9e4a1c72-5b38-4d06-a2f9-c7e3b8d0f164
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1934 DRIFT ACROSS RUNS: a run's comparison and drift rows, a drifting measure marked; picking
// a measure shows its history across the window, fetched as the viewer's role.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";

const api = vi.hoisted(() => ({ fetchMeasureHistory: vi.fn() }));
vi.mock("../../../api/profiler", () => api);

import { ProfileDriftPanel } from "../ProfileDriftPanel";

const ROWS = [
  {
    run_id: "r9",
    scope: "table",
    column_name: null,
    subject: null,
    measure: "row_count",
    previous: 100,
    current: 130,
    change: 30,
    window_runs: 4,
    baseline: 100.5,
    spread: 1,
    distance: 29.5,
    drifting: true,
    drift_reason: "distance",
  },
  {
    run_id: "r9",
    scope: "column",
    column_name: "amount",
    subject: null,
    measure: "null_share",
    previous: 0,
    current: 0,
    change: 0,
    window_runs: 4,
    drifting: false,
    drift_reason: null,
  },
];

beforeEach(() => api.fetchMeasureHistory.mockReset());

describe("ProfileDriftPanel", () => {
  it("marks a drifting measure and shows its history across the window", async () => {
    api.fetchMeasureHistory.mockResolvedValue({
      drift: ROWS[0],
      points: [
        { runId: "r1", runTime: "2026-09-01T03:00:00Z", value: 100, current: false },
        { runId: "r9", runTime: "2026-09-05T03:00:00Z", value: 130, current: true },
      ],
    });
    render(<ProfileDriftPanel tableId={7} runId="r9" role="analyst" rows={ROWS} />);
    expect(screen.getByTestId("profile-drift-flag-0")).toHaveTextContent("distance");
    expect(screen.queryByTestId("profile-drift-flag-1")).toBeNull();
    fireEvent.click(screen.getByTestId("profile-drift-row-0"));
    expect(await screen.findByTestId("profile-drift-point-r9")).toHaveTextContent("130");
    expect(screen.getByTestId("profile-drift-point-r1")).toHaveTextContent("100");
    expect(api.fetchMeasureHistory).toHaveBeenCalledWith(7, "r9", "analyst", {
      scope: "table",
      measure: "row_count",
      column: null,
      subject: null,
    });
  });

  it("says when a run records no comparison", () => {
    render(<ProfileDriftPanel tableId={7} runId="r9" role="analyst" rows={[]} />);
    expect(screen.getByTestId("profile-rows-drift-empty")).toBeInTheDocument();
  });
});
