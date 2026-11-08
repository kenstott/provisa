// Copyright (c) 2026 Kenneth Stott
// Canary: 8b4d1f6e-2a95-4c37-b0e8-7d3a5c9e1f24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-194 (CLASH POLICY): the org's clash policy, refuse (the default) or disambiguate. The card
// shows the org's policy and saves its `names` block alone.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, fireEvent, waitFor } from "../../../test-utils/render";
import { NameClashPolicyCard } from "../settingsCards";
import * as api from "../../../api/admin";

vi.mock("../../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../../api/admin")>()),
  fetchSettings: vi.fn(),
  updateSettings: vi.fn(),
}));

const SETTINGS = {
  naming: { use_domains: true, default_domain: "default", convention: "apollo_graphql" },
  names: { clash_policy: "refuse" },
  federation: { enabled: false, subgraph_name: "provisa", role: null },
};

describe("NameClashPolicyCard", () => {
  beforeEach(() => {
    vi.mocked(api.fetchSettings).mockResolvedValue(
      SETTINGS as unknown as Awaited<ReturnType<typeof api.fetchSettings>>,
    );
    vi.mocked(api.updateSettings).mockResolvedValue({
      success: true,
      updated: ["names.clash_policy"],
      restart_required: false,
    } as unknown as Awaited<ReturnType<typeof api.updateSettings>>);
  });

  it("shows the org's policy, refuse by default", async () => {
    render(<NameClashPolicyCard />);
    expect(await screen.findByTestId("name-clash-policy")).toHaveValue(
      "Refuse, naming both objects",
    );
  });

  it("saves the org's names block alone", async () => {
    const user = userEvent.setup();
    render(<NameClashPolicyCard />);
    await user.click(await screen.findByTestId("name-clash-policy"));
    await user.click(
      await screen.findByRole("option", { name: "Disambiguate and keep as alias", hidden: true }),
    );
    fireEvent.click(screen.getByRole("button", { name: /save/i }));
    await waitFor(() =>
      expect(api.updateSettings).toHaveBeenCalledWith({
        names: { clash_policy: "disambiguate" },
      }),
    );
  });
});
