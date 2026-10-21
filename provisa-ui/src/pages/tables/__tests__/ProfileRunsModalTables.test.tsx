// Copyright (c) 2026 Kenneth Stott
// Canary: a5e800b9-4c6e-47be-a26a-40e09dbb9372
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123, amended 2026-10-21: in the profile runs modal every table sorts and groups as the app's
// other lists do, every result table has an expand button (the 3D views' 95% modal), the result
// tabs read down the side, and View Profile Runs is in the table's read-only panel too.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ role: { id: "admin" } }) }));
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
  })),
  fetchProfileChecks: vi.fn(async () => ({ drift: [], expectations: [] })),
  createDriftCheck: vi.fn(),
  createExpectationCheck: vi.fn(),
  fetchMeasureHistory: vi.fn(),
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

import { ProfileRunsModal } from "../ProfileRunsModal";
import { ProfileRunsButton } from "../ProfileRunsButton";

const cellsOf = (table: HTMLElement, col: number) =>
  [...table.querySelectorAll("tbody tr.list-row")].map((tr) => tr.children[col]?.textContent);

describe("the profile runs modal", () => {
  it("reads its result tabs down the side", async () => {
    render(<ProfileRunsModal tableId={1} tableName="customers" onClose={() => {}} />);
    await screen.findByTestId("profile-rows-columns");
    expect(screen.getByRole("tablist").getAttribute("aria-orientation")).toBe("vertical");
  });

  it("sorts and groups a result table as the app's other lists do", async () => {
    render(<ProfileRunsModal tableId={1} tableName="customers" onClose={() => {}} />);
    const table = await screen.findByTestId("profile-rows-columns");
    expect(screen.queryByTestId("profile-rows-columns-group-nulls")).toBeNull(); // a number
    fireEvent.click(screen.getByTestId("profile-rows-columns-sort-column_name"));
    expect(cellsOf(table, 0)).toEqual(["plan", "region", "tenure"]);
    fireEvent.click(screen.getByTestId("profile-rows-columns-group-data_type"));
    const first = table.querySelector("tbody tr");
    expect(first?.textContent).toContain("data_type: integer");
  });

  it("expands a result table into the 95% modal", async () => {
    render(<ProfileRunsModal tableId={1} tableName="customers" onClose={() => {}} />);
    await screen.findByTestId("profile-rows-columns");
    fireEvent.click(screen.getByTestId("profile-rows-columns-expand"));
    const expanded = await screen.findByTestId("profile-rows-columns-expanded");
    expect(cellsOf(expanded, 0)).toHaveLength(3);
  });

  it("sorts its list of runs, and a click still picks a run", async () => {
    render(<ProfileRunsModal tableId={1} tableName="customers" onClose={() => {}} />);
    const list = await screen.findByTestId("profile-runs-list");
    // The first succeeded run is picked when the list comes back.
    await waitFor(() =>
      expect(screen.getByTestId("profile-run-r1").className).toContain("row-selected"),
    );
    fireEvent.click(within(list).getByTestId("profile-runs-list-sort-run_time"));
    const order = () =>
      [...list.querySelectorAll("tbody tr.list-row")].map((tr) => tr.getAttribute("data-testid"));
    expect(order()).toEqual(["profile-run-r1", "profile-run-r2"]);
    fireEvent.click(screen.getByTestId("profile-run-r2"));
    expect(screen.getByTestId("profile-run-r2").className).toContain("row-selected");
    expect(screen.getByTestId("profile-run-r1").className).not.toContain("row-selected");
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
