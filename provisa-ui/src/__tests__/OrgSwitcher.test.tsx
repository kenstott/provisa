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
import { render, screen } from "../test-utils/render";
import { OrgSwitcher } from "../components/OrgSwitcher";

vi.mock("../context/AuthContext", () => ({ useAuth: vi.fn() }));

import { useAuth } from "../context/AuthContext";

const mockUseAuth = vi.mocked(useAuth);

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
  });

  it("names the org when there is only one membership", () => {
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

  it("lists only orgs the caller holds a membership in, even for cross_org", () => {
    // REQ-1605: cross_org (platform_admin) lets an identity ACT in any org via dedicated admin
    // surfaces, but this switcher must never list an org the caller has no admin-plane
    // membership row in — that membership is exactly what the data-plane endpoints require.
    mockUseAuth.mockReturnValue(
      auth({
        capabilities: ["cross_org"],
        orgMemberships: [
          { org_id: "acme", org_name: "Enterprise", roles: [] },
          { org_id: "sandbox", org_name: "Sandbox", roles: [] },
        ],
      }),
    );
    render(<OrgSwitcher />);
    expect(screen.getByTestId("org-switcher-trigger")).toHaveTextContent("Enterprise");
    expect(screen.queryByText("Globex")).not.toBeInTheDocument();
  });
});
