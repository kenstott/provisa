// Copyright (c) 2026 Kenneth Stott
// Canary: 5d2c8e14-7a3b-4f0e-9c61-2b8e4f7a1d90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-259: the org's Apollo Federation subgraph is served as one declared role. The card saves
// the org's `federation` block alone.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../../test-utils/render";
import { ApolloSubgraphCard } from "../settingsCards";
import * as api from "../../../api/admin";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchSettings: vi.fn(),
  updateSettings: vi.fn(),
}));

vi.mock("../../../hooks/useAdminQueries", async (orig) => ({
  ...(await orig<typeof import("../../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [{ id: "partner" }, { id: "analyst" }] }),
}));

const SETTINGS = {
  naming: { use_domains: true, default_domain: "default" },
  federation: { enabled: false, subgraph_name: "provisa", role: "partner" },
  redirect: { enabled: true, threshold: 10, default_format: "parquet", ttl: 60 },
};

describe("ApolloSubgraphCard", () => {
  beforeEach(() => {
    vi.mocked(api.fetchSettings).mockResolvedValue(
      SETTINGS as unknown as Awaited<ReturnType<typeof api.fetchSettings>>,
    );
    vi.mocked(api.updateSettings).mockResolvedValue({
      success: true,
      updated: ["federation.enabled"],
      restart_required: false,
    } as unknown as Awaited<ReturnType<typeof api.updateSettings>>);
  });

  it("saves the org's federation block alone, with its declared role", async () => {
    render(<ApolloSubgraphCard />);
    fireEvent.click(await screen.findByTestId("apollo-subgraph-enabled"));
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() =>
      expect(api.updateSettings).toHaveBeenCalledWith({
        federation: { enabled: true, subgraph_name: "provisa", role: "partner" },
      }),
    );
  });
});
