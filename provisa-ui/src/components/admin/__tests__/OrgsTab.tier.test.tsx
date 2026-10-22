// Copyright (c) 2026 Kenneth Stott
// Canary: 1e5c8a37-4d92-4f60-b7e1-9a2c3d0f6b48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1066: an org's subscription tier is stored with the org and set by a platform administrator
// in Admin > Orgs; every tier-gated feature reads it.

import { describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";

vi.mock("../../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/admin")>()),
  fetchOrgs: vi.fn().mockResolvedValue([
    {
      id: "acme",
      name: "Acme",
      created_by: null,
      created_at: "",
      isolated_engine: false,
      tier: null,
    },
    {
      id: "globex",
      name: "Globex",
      created_by: null,
      created_at: "",
      isolated_engine: false,
      tier: "premium",
    },
  ]),
  fetchInvites: vi.fn().mockResolvedValue([]),
  setOrgTier: vi.fn().mockResolvedValue(undefined),
}));

import { setOrgTier } from "../../../api/admin";
import { OrgsTab } from "../OrgsTab";

describe("an org's tier in Admin > Orgs", () => {
  it("lists the orgs matching any word of the search", async () => {
    render(<OrgsTab />);
    await screen.findByTestId("org-tier-globex");
    // The first search bar is the orgs'; the second, the invites'.
    fireEvent.change(screen.getAllByTestId("filter-input")[0], {
      target: { value: "acme initech" },
    });
    expect(screen.getByTestId("org-tier-acme")).toBeInTheDocument();
    expect(screen.queryByTestId("org-tier-globex")).toBeNull();
  });

  it("shows each org's stored tier, and none as not set", async () => {
    render(<OrgsTab />);
    await waitFor(() => expect(screen.getByTestId("org-tier-globex")).toHaveValue("Premium"));
    expect(screen.getByTestId("org-tier-acme")).toHaveValue("");
    expect(screen.getByTestId("org-tier-acme")).toHaveAttribute("placeholder", "Not set");
  });

  it("stores the tier a platform administrator picks", async () => {
    render(<OrgsTab />);
    const acme = await screen.findByTestId("org-tier-acme");
    fireEvent.click(acme);
    // Every row's select keeps its options mounted; Acme's are the list named for it.
    const options = await screen.findByRole("listbox", { name: "Tier of Acme", hidden: true });
    fireEvent.click(within(options).getByRole("option", { name: "Standard", hidden: true }));
    await waitFor(() => expect(setOrgTier).toHaveBeenCalledWith("acme", "standard"));
    await waitFor(() => expect(screen.getByTestId("org-tier-acme")).toHaveValue("Standard"));
  });
});
