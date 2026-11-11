// Copyright (c) 2026 Kenneth Stott
// Canary: 1b9e4d72-c3a5-4e80-8f16-a2d7b0c5e963
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180, A PROFILE SHOWN IS SANITIZED: a product's Profile tab shows each dataset's profile as
// the server sanitized it for the reader's role, through the tables page's profile panel
// (REQ-2239): the owner's in full (runs and drift, constraints, classifications), anyone else's
// read-only; each toggles between its table and its terrain.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, within } from "../../../../test-utils/render";
import type { ProductProfile } from "../../../../api/dataProductOffers";

const api = vi.hoisted(() => ({
  profiles: vi.fn<(id: string, role: string) => Promise<ProductProfile[]>>(),
}));
vi.mock("../../../../api/dataProductOffers", () => ({
  fetchProductProfiles: (id: string, role: string) => api.profiles(id, role),
}));
vi.mock("../../../tables/profile3d/terrain/ProfileTerrain", () => ({
  ProfileTerrain: ({ tableName }: { tableName: string }) => (
    <div data-testid="terrain">{tableName}</div>
  ),
}));
vi.mock("../../../tables/ProfileRunsModal", () => ({
  ProfileRunsModal: () => <div data-testid="runs-modal" />,
}));
vi.mock("../../../tables/ProfileConstraintsModal", () => ({
  ProfileConstraintsModal: () => <div data-testid="constraints-modal" />,
}));
vi.mock("../../../tables/ProfileClassificationsModal", () => ({
  ProfileClassificationsModal: () => <div data-testid="classifications-modal" />,
}));

import { ProfileTab } from "../ProfileTab";

const PROFILES: ProductProfile[] = [
  {
    table_id: 7,
    table: "sales.customers",
    run: { run_id: "r1", run_time: "2026-11-10T00:00:00Z" } as ProductProfile["run"],
    results: {
      columns: [
        { column_name: "id", null_rate: 0, distinct_count: 100, min: 1 },
        // A masked column: its statistics, none of its values (the server's sanitizing).
        { column_name: "email", null_rate: 0.1, distinct_count: 98 },
      ],
    },
  },
  { table_id: 8, table: "sales.orders", run: null, results: null },
];

function tab(owner: boolean) {
  render(<ProfileTab productId="c360" owner={owner} role="analyst" />);
}

beforeEach(() => {
  window.localStorage.clear();
  api.profiles.mockReset().mockResolvedValue(PROFILES);
});

describe("a product's Profile tab", () => {
  it("reads the profiles as the reader's role, one per dataset", async () => {
    tab(false);
    expect(await screen.findByTestId("data-product-profile-7-toggle")).toHaveTextContent(
      "sales.customers",
    );
    expect(screen.getByTestId("data-product-profile-8-toggle")).toHaveTextContent("sales.orders");
    expect(api.profiles).toHaveBeenCalledWith("c360", "analyst");
  });

  it("shows a dataset's columns as the server sent them, and says when there is no run", async () => {
    tab(false);
    fireEvent.click(await screen.findByTestId("data-product-profile-7-toggle"));
    const panel = await screen.findByTestId("data-product-profile-7");
    expect(within(panel).getByTestId("latest-profile-panel")).toHaveTextContent("email");
    fireEvent.click(screen.getByTestId("data-product-profile-8-toggle"));
    expect(await screen.findByTestId("data-product-profile-8-none")).toBeInTheDocument();
  });

  it("is read-only for a consumer: no runs, drift, constraints or classifications", async () => {
    tab(false);
    fireEvent.click(await screen.findByTestId("data-product-profile-7-toggle"));
    await screen.findByTestId("data-product-profile-7");
    expect(screen.queryByTestId("data-product-profile-7-owner")).toBeNull();
  });

  it("is in full for the owner", async () => {
    tab(true);
    fireEvent.click(await screen.findByTestId("data-product-profile-7-toggle"));
    const owner = await screen.findByTestId("data-product-profile-7-owner");
    fireEvent.click(within(owner).getByText("Constraints"));
    expect(screen.getByTestId("constraints-modal")).toBeInTheDocument();
  });

  it("toggles between the table and the terrain", async () => {
    tab(false);
    fireEvent.click(await screen.findByTestId("data-product-profile-7-toggle"));
    await screen.findByTestId("data-product-profile-7");
    expect(screen.queryByTestId("terrain")).toBeNull();
    fireEvent.click(screen.getByText("Terrain"));
    expect(screen.getByTestId("terrain")).toHaveTextContent("sales.customers");
    expect(screen.queryByTestId("latest-profile-panel")).toBeNull();
  });
});
