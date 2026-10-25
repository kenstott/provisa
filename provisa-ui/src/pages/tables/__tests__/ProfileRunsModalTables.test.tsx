// Copyright (c) 2026 Kenneth Stott
// Canary: 2c338b6b-2e03-4ba6-9100-74e116c87552
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123, amended 2026-10-21: every table of the profile runs modal is the explorer's results
// grid (sort, group, column filters, xlsx download); a result table expands into the 95% modal;
// the result tabs read down the side; a click picks a run, or a measure's history; and View
// Profile Runs is in the table's read-only panel too.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ role: { id: "admin" } }) }));
vi.mock("../../sql/exportXlsx", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../sql/exportXlsx")>()),
  downloadXlsx: vi.fn(),
}));
vi.mock("../../../api/profiler", () => ({
  fetchProfileRuns: vi.fn(async () => [
    run("r1", "2026-10-20T10:00:00Z"),
    run("r2", "2026-10-21T10:00:00Z"),
  ]),
  fetchProfileRun: vi.fn(async () => ({
    columns: [
      { run_id: "r2", column_name: "plan", data_type: "text", nulls: 0 },
      { run_id: "r2", column_name: "tenure", data_type: "integer", nulls: 4 },
      { run_id: "r2", column_name: "region", data_type: "text", nulls: 1 },
    ],
    drift: [
      {
        scope: "column",
        measure: "mean",
        column_name: "tenure",
        subject: null,
        previous: 11,
        current: 14,
        drifting: true,
        drift_reason: "psi",
      },
    ],
  })),
  fetchProfileChecks: vi.fn(async () => ({ drift: [], expectations: [] })),
  createDriftCheck: vi.fn(),
  createExpectationCheck: vi.fn(),
  fetchMeasureHistory: vi.fn(async () => ({
    points: [
      { runId: "r1", runTime: "2026-10-20T10:00:00Z", value: 11, current: false },
      { runId: "r2", runTime: "2026-10-21T10:00:00Z", value: 14, current: true },
    ],
    drift: { drifting: true },
  })),
}));

function run(run_id: string, run_time: string) {
  return {
    run_id,
    run_time,
    region: null,
    profiled_table: "customers",
    row_count: 100,
    sampled: false,
    sample_method: "whole",
    target_fraction: null,
    sample_fraction: null,
    sample_attempts: null,
    profiled_rows: 100,
    previous_run_id: null,
    duplicate_rows: 0,
    duplicate_share: 0,
    key_duplicates: null,
    duration_ms: 12,
    status: "succeeded",
    error: null,
  };
}

import { fetchMeasureHistory } from "../../../api/profiler";
import { downloadXlsx } from "../../sql/exportXlsx";
import { ProfileRunsModal } from "../ProfileRunsModal";
import { ProfileRunsButton } from "../ProfileRunsButton";

const open = () =>
  render(<ProfileRunsModal tableId={1} tableName="customers" onClose={() => {}} />);
const rowWith = (scope: HTMLElement, text: string) =>
  within(scope).getByText(text).closest("tr") as HTMLTableRowElement;

describe("the profile runs modal", () => {
  it("reads its results down the side, in the app's one vertical menu", async () => {
    open();
    await screen.findByTestId("profile-rows-columns");
    const menu = screen.getByRole("navigation", { name: "Results" });
    expect(menu).toHaveClass("vertical-nav");
    expect(screen.getByTestId("profile-tab-columns")).toHaveAttribute("aria-current", "true");
    fireEvent.click(screen.getByTestId("profile-tab-quantiles"));
    expect(screen.getByTestId("profile-tab-quantiles")).toHaveAttribute("aria-current", "true");
    expect(screen.getByTestId("profile-tab-columns")).not.toHaveAttribute("aria-current");
    expect(screen.getByTestId("profile-panel-quantiles")).toBeInTheDocument();
  });

  it("filters a result table by column", async () => {
    open();
    const table = await screen.findByTestId("profile-rows-columns");
    fireEvent.change(within(table).getByLabelText("Filter rows… data_type"), {
      target: { value: "integer" },
    });
    expect(within(table).queryByText("plan")).toBeNull();
    expect(within(table).getByText("tenure")).toBeTruthy();
  });

  it("downloads a result table as xlsx, named after the table and the result", async () => {
    open();
    const table = await screen.findByTestId("profile-rows-columns");
    fireEvent.click(within(table).getByTestId("download-xlsx-btn"));
    await waitFor(() => expect(downloadXlsx).toHaveBeenCalled());
    const [name, columns, rows] = vi.mocked(downloadXlsx).mock.calls[0];
    expect(name).toBe("customers · Columns.xlsx");
    expect(columns).toEqual(["column_name", "data_type", "nulls"]);
    expect(rows).toHaveLength(3);
  });

  it("expands a result table into the 95% modal", async () => {
    open();
    await screen.findByTestId("profile-rows-columns");
    fireEvent.click(screen.getByTestId("profile-rows-columns-expand"));
    const expanded = await screen.findByTestId("profile-rows-columns-expanded");
    expect(within(expanded).getByText("tenure")).toBeTruthy();
  });

  it("filters, downloads and picks a run from its list of runs", async () => {
    open();
    const list = await screen.findByTestId("profile-runs-list");
    const first = new Date("2026-10-20T10:00:00Z").toLocaleString();
    const second = new Date("2026-10-21T10:00:00Z").toLocaleString();
    // The first succeeded run is picked when the list comes back.
    await waitFor(() => expect(rowWith(list, first).className).toContain("row-selected"));
    fireEvent.click(rowWith(list, second));
    expect(rowWith(list, second).className).toContain("row-selected");
    expect(rowWith(list, first).className).not.toContain("row-selected");
    expect(within(list).getByTestId("download-xlsx-btn")).toBeTruthy();
    expect(within(list).getByLabelText("Filter rows… Status")).toBeTruthy();
  });

  it("opens a measure's history from the drift table, itself a grid", async () => {
    open();
    await screen.findByTestId("profile-rows-columns");
    fireEvent.click(screen.getByTestId("profile-tab-drift"));
    const drift = await screen.findByTestId("profile-rows-drift");
    fireEvent.click(rowWith(drift, "column · tenure · mean"));
    await waitFor(() => expect(fetchMeasureHistory).toHaveBeenCalled());
    const history = await screen.findByTestId("profile-drift-history");
    expect(within(history).getByText("14")).toBeTruthy();
    expect(within(history).getByTestId("download-xlsx-btn")).toBeTruthy();
  });
});

describe("View Profile Runs in the read-only panel", () => {
  it("opens the table's profile runs", async () => {
    render(
      <ProfileRunsButton
        table={
          { id: 1, tableName: "customers" } as Parameters<typeof ProfileRunsButton>[0]["table"]
        }
      />,
    );
    fireEvent.click(screen.getByTestId("table-read-view-profile-runs"));
    expect(await screen.findByTestId("profile-runs-modal")).toBeTruthy();
  });
});
