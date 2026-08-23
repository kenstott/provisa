// Copyright (c) 2026 Kenneth Stott
// Canary: 6f0c1a92-2b74-4d1e-9a53-1d84c2f7e0b5
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";
import { OrgSwitcher } from "../components/OrgSwitcher";

vi.mock("../context/AuthContext", () => ({ useAuth: vi.fn() }));
vi.mock("../api/admin", () => ({ fetchOrgs: vi.fn() }));

import { useAuth } from "../context/AuthContext";
import { fetchOrgs } from "../api/admin";

const mockUseAuth = vi.mocked(useAuth);
const mockFetchOrgs = vi.mocked(fetchOrgs);

function auth(overrides: Record<string, unknown> = {}) {
  return {
    capabilities: [],
    orgMemberships: [{ org_id: "acme", org_name: "Enterprise", roles: [] }],
    activeOrgId: "acme",
    selectOrg: vi.fn(),
    multitenancy: true,
    ...overrides,
  } as unknown as ReturnType<typeof useAuth>;
}

describe("OrgSwitcher", () => {
  beforeEach(() => {
    mockUseAuth.mockReset();
    mockFetchOrgs.mockReset();
    mockFetchOrgs.mockResolvedValue([]);
  });

  it("names the org when there is more than one tenant in the deployment", () => {
    mockUseAuth.mockReturnValue(auth());
    render(<OrgSwitcher />);
    expect(screen.getByTestId("org-switcher-static")).toHaveTextContent("Enterprise");
  });

  it("renders nothing in a single-tenant deployment", () => {
    // One org means nothing to switch between, and naming it is noise in the navbar.
    mockUseAuth.mockReturnValue(auth({ multitenancy: false }));
    render(<OrgSwitcher />);
    expect(screen.queryByTestId("org-switcher-static")).not.toBeInTheDocument();
    expect(screen.queryByTestId("org-switcher-trigger")).not.toBeInTheDocument();
  });

  it("does not list every org in a single-tenant deployment even for cross_org", () => {
    mockUseAuth.mockReturnValue(auth({ multitenancy: false, capabilities: ["cross_org"] }));
    render(<OrgSwitcher />);
    expect(screen.queryByTestId("org-switcher-trigger")).not.toBeInTheDocument();
  });

  it("offers the switch when the viewer may see every org", async () => {
    mockFetchOrgs.mockResolvedValue([
      { id: "acme", name: "Enterprise" },
      { id: "globex", name: "Globex" },
    ] as never);
    mockUseAuth.mockReturnValue(auth({ capabilities: ["cross_org"] }));
    render(<OrgSwitcher />);
    await waitFor(() =>
      expect(screen.getByTestId("org-switcher-trigger")).toHaveTextContent("Enterprise"),
    );
  });
});
