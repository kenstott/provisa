// Copyright (c) 2026 Kenneth Stott
// Canary: 2c7e9a41-5b08-4f63-8d1e-a9f3c6b2e075
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1934: a table joins a Data Profiler from its editor; a member can be removed, run now, and
// its runs viewed — the runs and Run Now act only on the saved membership.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";

const api = vi.hoisted(() => ({
  fetchProfilers: vi.fn(),
  runProfileNow: vi.fn(),
  fetchProfileRuns: vi.fn(),
  fetchProfileRun: vi.fn(),
}));
vi.mock("../../../api/profiler", () => api);
// The runs view is fetched as the viewer's acting role, which the server makes it safe for.
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ role: { id: "analyst" } }) }));

import { ProfilerPanel } from "../ProfilerPanel";
import type { RegisteredTable } from "../../../types/admin";
import i18n from "../../../i18n";

const t = i18n.getFixedT("en");
const PROFILERS = [
  { id: "nightly", cron: "0 3 * * *", sampleAboveCells: null, lowCardinalityMax: 100, members: [] },
  {
    id: "hourly",
    cron: "0 * * * *",
    sampleAboveCells: 50000,
    lowCardinalityMax: 100,
    members: ["orders"],
  },
];

function table(profilerSourceId: string | null): RegisteredTable {
  return { id: 7, tableName: "orders", profilerSourceId } as unknown as RegisteredTable;
}

beforeEach(() => {
  Object.values(api).forEach((fn) => fn.mockReset());
  api.fetchProfilers.mockResolvedValue(PROFILERS);
});

describe("ProfilerPanel", () => {
  it("adds the table to a picked profiler, listed with its schedule, as an unsaved edit", async () => {
    const setEditingTable = vi.fn();
    render(
      <ProfilerPanel
        editingTable={table(null)}
        savedProfilerId={null}
        setEditingTable={setEditingTable}
      />,
    );
    fireEvent.click(screen.getByTestId("profiler-panel-toggle"));
    // Nothing is asked of the server until the picker opens.
    expect(api.fetchProfilers).not.toHaveBeenCalled();
    fireEvent.click(screen.getByTestId("profiler-add"));
    const option = await screen.findByText(
      `hourly — ${t("profilerPanel.scheduleSample", { cron: "0 * * * *", cells: 50000 })}`,
    );
    fireEvent.click(option);
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ profilerSourceId: "hourly" }),
    );
  });

  it("offers Remove, Run Now and View Runs to a saved member", async () => {
    const setEditingTable = vi.fn();
    api.runProfileNow.mockResolvedValue({ runId: "r1", rowCount: 10, profiledRows: 10 });
    render(
      <ProfilerPanel
        editingTable={table("nightly")}
        savedProfilerId="nightly"
        setEditingTable={setEditingTable}
      />,
    );
    expect(await screen.findByTestId("profiler-member")).toHaveTextContent(
      t("profilerPanel.scheduleWhole", { cron: "0 3 * * *" }),
    );
    fireEvent.click(screen.getByTestId("profiler-run-now"));
    expect(await screen.findByTestId("profiler-run-message")).toHaveTextContent(
      t("profilerPanel.runDone", { rows: 10 }),
    );
    expect(api.runProfileNow).toHaveBeenCalledWith(7);
    fireEvent.click(screen.getByTestId("profiler-remove"));
    expect(setEditingTable).toHaveBeenCalledWith(
      expect.objectContaining({ profilerSourceId: null }),
    );
  });

  it("acts on nothing the server does not hold yet", () => {
    render(
      <ProfilerPanel
        editingTable={table("nightly")}
        savedProfilerId={null}
        setEditingTable={vi.fn()}
      />,
    );
    expect(screen.getByTestId("profiler-run-now")).toBeDisabled();
    expect(screen.getByTestId("profiler-view-runs")).toBeDisabled();
    expect(screen.getAllByTestId("profiler-unsaved").length).toBeGreaterThan(0);
  });

  it("shows a member's run history and the latest successful run's results", async () => {
    api.fetchProfileRuns.mockResolvedValue([
      {
        run_id: "r2",
        run_time: "2026-10-05T03:00:00Z",
        status: "failed",
        error: "boom",
        row_count: null,
        profiled_rows: null,
        sampled: null,
        sample_method: null,
        target_fraction: null,
        sample_fraction: null,
        sample_attempts: null,
        duration_ms: 4,
        region: null,
        profiled_table: "orders",
      },
      {
        run_id: "r1",
        run_time: "2026-10-04T03:00:00Z",
        status: "succeeded",
        error: null,
        row_count: 3,
        profiled_rows: 3,
        sampled: false,
        sample_method: "whole",
        target_fraction: 1,
        sample_fraction: 1,
        sample_attempts: "[]",
        duration_ms: 12,
        region: null,
        profiled_table: "orders",
      },
      {
        run_id: "r0",
        run_time: "2026-10-03T03:00:00Z",
        status: "succeeded",
        error: null,
        row_count: 200000,
        profiled_rows: 9800,
        sampled: true,
        sample_method: "block",
        target_fraction: 0.05,
        sample_fraction: 0.049,
        sample_attempts: '[{"percent": 5.0, "rows": 9800}]',
        duration_ms: 30,
        region: null,
        profiled_table: "orders",
      },
    ]);
    api.fetchProfileRun.mockResolvedValue({
      runs: [{ run_id: "r1" }],
      columns: [{ run_id: "r1", run_time: "x", column_name: "amount", null_count: 0, mean: 20.5 }],
    });
    render(
      <ProfilerPanel
        editingTable={table("nightly")}
        savedProfilerId="nightly"
        setEditingTable={vi.fn()}
      />,
    );
    fireEvent.click(screen.getByTestId("profiler-view-runs"));
    expect(await screen.findByTestId("profile-run-r2")).toHaveTextContent("boom");
    expect(screen.getByTestId("profile-run-method-r1")).toHaveTextContent("Whole table");
    expect(screen.getByTestId("profile-run-method-r0")).toHaveTextContent(
      "Block sample · 4.9% of rows",
    );
    expect(screen.getByTestId("profile-run-method-r2")).toBeEmptyDOMElement();
    await waitFor(() => expect(api.fetchProfileRun).toHaveBeenCalledWith(7, "r1", "analyst"));
    const rows = await screen.findByTestId("profile-rows-columns");
    expect(rows).toHaveTextContent("amount");
    expect(rows).toHaveTextContent("20.5000");
    expect(rows).not.toHaveTextContent("run_id");
  });
});
