// Copyright (c) 2026 Kenneth Stott
// Canary: 4717c109-9b34-47e2-af74-f8af6bc527e9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: a product's Profile section opens a dataset's profile runs dialog from the same link
// the tables page does: `?table=<id>` names the dataset, `?profile=<runId|latest>` the dialog and
// its run, `?profileView=<view>` its result.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { useLocation, useNavigate, useNavigationType } from "react-router-dom";
import { fireEvent, render, screen, waitFor, within } from "../../../../test-utils/render";
import type { ProductProfile } from "../../../../api/dataProductOffers";

vi.mock("../../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "admin" }) }));
const api = vi.hoisted(() => ({
  profiles: vi.fn<(id: string, role: string) => Promise<ProductProfile[]>>(),
}));
vi.mock("../../../../api/dataProductOffers", () => ({
  fetchProductProfiles: (id: string, role: string) => api.profiles(id, role),
}));
vi.mock("../../../tables/profile3d/terrain/ProfileTerrain", () => ({
  ProfileTerrain: () => <div data-testid="terrain" />,
}));
vi.mock("../../../../api/profiler", () => ({
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

import { fetchProfileRun, fetchProfileRuns } from "../../../../api/profiler";
import { ProfileTab } from "../ProfileTab";

const PROFILES: ProductProfile[] = [
  {
    table_id: 7,
    table: "sales.customers",
    run: { run_id: "r2", run_time: "2026-10-21T10:00:00Z" } as ProductProfile["run"],
    results: { columns: [{ column_name: "id", null_rate: 0, distinct_count: 100 }] },
  },
  { table_id: 8, table: "sales.orders", run: null, results: null },
];

function Probe() {
  const { search } = useLocation();
  const navigate = useNavigate();
  return (
    <>
      <span data-testid="search">{search}</span>
      <span data-testid="nav-type">{useNavigationType()}</span>
      <button data-testid="back" onClick={() => navigate(-1)} />
      <button data-testid="forward" onClick={() => navigate(1)} />
    </>
  );
}

const open = (entry: string, owner = true) =>
  render(
    <>
      <ProfileTab productId="c360" owner={owner} role="analyst" />
      <Probe />
    </>,
    { initialEntries: [entry] },
  );
const searchOf = () => new URLSearchParams(screen.getByTestId("search").textContent ?? "");
const rowWith = (scope: HTMLElement, text: string) =>
  within(scope).getByText(text).closest("tr") as HTMLTableRowElement;
const first = new Date("2026-10-20T10:00:00Z").toLocaleString();

beforeEach(() => {
  window.localStorage.clear();
  api.profiles.mockReset().mockResolvedValue(PROFILES);
  vi.mocked(fetchProfileRuns).mockClear();
  vi.mocked(fetchProfileRun).mockClear();
});

describe("the profile runs link, into a product's Profile section", () => {
  it("opens the dataset it names, on the run and the result it names", async () => {
    open("/data-products?product=c360&table=7&profile=r1&profileView=quantiles");
    await screen.findByTestId("profile-panel-quantiles");
    expect(screen.getByTestId("data-product-profile-7")).toBeInTheDocument();
    expect(screen.getByTestId("profile-tab-quantiles")).toHaveAttribute("aria-current", "true");
    const list = await screen.findByTestId("profile-runs-list");
    await waitFor(() => expect(rowWith(list, first).className).toContain("row-selected"));
    expect(fetchProfileRuns).toHaveBeenCalledWith(7);
    expect(fetchProfileRun).toHaveBeenCalledWith(7, "r1", "admin");
  });

  it("opens the dialog of the dataset it names, not another's", async () => {
    open("/data-products?product=c360&table=8&profile=latest");
    await screen.findByTestId("profile-runs-modal");
    expect(screen.getAllByTestId("profile-runs-modal")).toHaveLength(1);
    expect(fetchProfileRuns).toHaveBeenCalledWith(8);
    expect(fetchProfileRuns).not.toHaveBeenCalledWith(7);
  });

  it("stays closed without the parameter", async () => {
    open("/data-products?product=c360&table=7");
    await screen.findByTestId("data-product-profile-7-toggle");
    expect(screen.queryByTestId("profile-runs-modal")).toBeNull();
  });

  it("opens nothing for a reader who is not the owner", async () => {
    open("/data-products?product=c360&table=7&profile=latest", false);
    await screen.findByTestId("data-product-profile-7");
    expect(screen.queryByTestId("profile-runs-modal")).toBeNull();
  });
});

describe("a product's dialog, into the profile runs link", () => {
  it("writes its dataset, its opening, its view and its closing to the URL", async () => {
    open("/data-products?product=c360");
    fireEvent.click(await screen.findByTestId("data-product-profile-7-toggle"));
    fireEvent.click(await screen.findByTestId("data-product-profile-7-view-runs"));
    await screen.findByTestId("profile-panel-columns");
    expect(searchOf().get("table")).toBe("7");
    expect(searchOf().get("profile")).toBe("latest");
    expect(screen.getByTestId("nav-type")).toHaveTextContent("PUSH");

    fireEvent.click(screen.getByTestId("profile-tab-quantiles"));
    expect(searchOf().get("profileView")).toBe("quantiles");
    expect(screen.getByTestId("nav-type")).toHaveTextContent("REPLACE");

    fireEvent.keyDown(screen.getByTestId("profile-runs-modal"), { key: "Escape" });
    await waitFor(() => expect(screen.queryByTestId("profile-runs-modal")).toBeNull());
    expect(searchOf().has("table")).toBe(false);
    expect(searchOf().has("profile")).toBe(false);
    expect(searchOf().has("profileView")).toBe(false);
    expect(searchOf().get("product")).toBe("c360");
  });

  it("closes on Back and opens again on Forward", async () => {
    open("/data-products?product=c360");
    fireEvent.click(await screen.findByTestId("data-product-profile-7-toggle"));
    fireEvent.click(await screen.findByTestId("data-product-profile-7-view-runs"));
    await screen.findByTestId("profile-panel-columns");
    fireEvent.click(screen.getByTestId("profile-tab-quantiles"));
    await screen.findByTestId("profile-panel-quantiles");

    fireEvent.click(screen.getByTestId("back"));
    await waitFor(() => expect(screen.queryByTestId("profile-runs-modal")).toBeNull());
    expect(searchOf().toString()).toBe("product=c360");

    fireEvent.click(screen.getByTestId("forward"));
    await screen.findByTestId("profile-panel-quantiles");
    expect(searchOf().get("table")).toBe("7");
    expect(searchOf().get("profile")).toBe("latest");
  });
});
