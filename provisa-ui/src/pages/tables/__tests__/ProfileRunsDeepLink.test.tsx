// Copyright (c) 2026 Kenneth Stott
// Canary: 8934a09d-f9d5-4871-ac95-6dc9d4c02453
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the profile runs dialog is a deep link: `?profile=<runId|latest>` and
// `?profileView=<view>`, read into the dialog and written back from it.

import { describe, expect, it, vi } from "vitest";
import { useLocation, useNavigationType } from "react-router-dom";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "admin" }) }));
vi.mock("../../../api/profiler", () => ({
  fetchProfileRuns: vi.fn(async () => [
    run("r1", "2026-10-20T10:00:00Z"),
    run("r2", "2026-10-21T10:00:00Z"),
  ]),
  fetchProfileRun: vi.fn(async (_table: number, runId: string) => ({
    columns: [{ run_id: runId, column_name: "plan", data_type: "text", nulls: 0 }],
    quantiles: [{ run_id: runId, column_name: "age", p50: 3 }],
  })),
  fetchProfileChecks: vi.fn(async () => ({ drift: [], expectations: [] })),
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

import { fetchProfileRun } from "../../../api/profiler";
import { ProfileRunsButton } from "../ProfileRunsButton";

function Probe() {
  const { search } = useLocation();
  return (
    <>
      <span data-testid="search">{search}</span>
      <span data-testid="nav-type">{useNavigationType()}</span>
    </>
  );
}

const table = { id: 1, tableName: "customers" } as Parameters<typeof ProfileRunsButton>[0]["table"];
const open = (entry: string) =>
  render(
    <>
      <ProfileRunsButton table={table} />
      <Probe />
    </>,
    { initialEntries: [entry] },
  );
const searchOf = () => new URLSearchParams(screen.getByTestId("search").textContent ?? "");
const rowWith = (scope: HTMLElement, text: string) =>
  within(scope).getByText(text).closest("tr") as HTMLTableRowElement;
const first = new Date("2026-10-20T10:00:00Z").toLocaleString();
const second = new Date("2026-10-21T10:00:00Z").toLocaleString();

describe("the profile runs link, into the dialog", () => {
  it("opens on the run and the result the URL names", async () => {
    open("/tables?table=1&section=quality&profile=r1&profileView=quantiles");
    await screen.findByTestId("profile-panel-quantiles");
    expect(screen.getByTestId("profile-tab-quantiles")).toHaveAttribute("aria-current", "true");
    const list = await screen.findByTestId("profile-runs-list");
    await waitFor(() => expect(rowWith(list, first).className).toContain("row-selected"));
    expect(rowWith(list, second).className).not.toContain("row-selected");
    expect(fetchProfileRun).toHaveBeenCalledWith(1, "r1", "admin");
  });

  it("opens on the latest run and the first result when the URL names neither", async () => {
    open("/tables?table=1&profile=latest");
    await screen.findByTestId("profile-panel-columns");
    const list = await screen.findByTestId("profile-runs-list");
    await waitFor(() => expect(rowWith(list, first).className).toContain("row-selected"));
  });

  it("stays closed without the parameter", () => {
    open("/tables?table=1&section=quality");
    expect(screen.queryByTestId("profile-runs-modal")).toBeNull();
  });
});

describe("the dialog, into the profile runs link", () => {
  it("writes its opening, its view, its run and its closing to the URL", async () => {
    open("/tables?table=1&section=quality");
    fireEvent.click(screen.getByTestId("table-read-view-profile-runs"));
    await screen.findByTestId("profile-panel-columns");
    expect(searchOf().get("profile")).toBe("latest");
    expect(searchOf().has("profileView")).toBe(false);
    expect(screen.getByTestId("nav-type")).toHaveTextContent("PUSH");

    fireEvent.click(screen.getByTestId("profile-tab-quantiles"));
    expect(searchOf().get("profileView")).toBe("quantiles");
    expect(screen.getByTestId("nav-type")).toHaveTextContent("REPLACE");
    await screen.findByTestId("profile-panel-quantiles");

    const list = screen.getByTestId("profile-runs-list");
    fireEvent.click(rowWith(list, second));
    expect(searchOf().get("profile")).toBe("r2");
    expect(screen.getByTestId("nav-type")).toHaveTextContent("REPLACE");
    await waitFor(() => expect(fetchProfileRun).toHaveBeenCalledWith(1, "r2", "admin"));
    await waitFor(() => expect(rowWith(list, second).className).toContain("row-selected"));

    // The first result is the page without a view named.
    fireEvent.click(screen.getByTestId("profile-tab-columns"));
    expect(searchOf().has("profileView")).toBe(false);

    fireEvent.keyDown(screen.getByTestId("profile-runs-modal"), { key: "Escape" });
    await waitFor(() => expect(screen.queryByTestId("profile-runs-modal")).toBeNull());
    expect(searchOf().has("profile")).toBe(false);
    expect(searchOf().has("profileView")).toBe(false);
    expect(searchOf().get("table")).toBe("1");
    expect(searchOf().get("section")).toBe("quality");
  });
});
