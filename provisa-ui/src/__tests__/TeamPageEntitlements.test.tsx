// Copyright (c) 2026 Kenneth Stott
// Canary: dfd89b84-d3c1-4744-bcb0-44133b97d94e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the Team page's entitlement parts are assign_entitlements. With it, a member's panel
// has an Access tab beside Attributes, and the page has an Access grants section; without it,
// neither is offered and the entitlements API is never called.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen } from "../test-utils/render";
import { TeamPage } from "../pages/TeamPage";

let mockCapabilities: string[] = [];

vi.mock("../api/admin", () => ({
  fetchOrgSuspensions: () => Promise.resolve({ tokens: [], principals: [], events: [] }),
  liftOrgSuspension: () => Promise.resolve(),
  fetchAuditedStatement: () => Promise.resolve(""),
  ENV_POLICY_NONE: "none",
  ENV_POLICY_PER_VISITOR: "per_visitor",
  ENV_POLICY_SHARED: "shared",
  fetchInvites: () => Promise.resolve([]),
  createInvite: vi.fn(),
  revokeInvite: vi.fn(),
  fetchOrgMembers: () =>
    Promise.resolve([
      {
        user_id: "okta|31",
        email: "kenji@acme.example",
        display_name: "Kenji Sato",
        provider: "okta",
        is_org_admin: false,
      },
    ]),
  removeOrgMember: vi.fn(),
  grantOrgAdmin: vi.fn(),
  revokeOrgAdmin: vi.fn(),
  deleteOrg: vi.fn(),
  exportOrgConfig: vi.fn(),
  fetchOrgSettings: () =>
    Promise.resolve({ id: "acme", email_rule: null, auto_join: false, auto_join_role: null }),
  updateOrgSettings: vi.fn(),
  fetchMemberAttributes: () => Promise.resolve([]),
  setMemberAttribute: vi.fn(),
  deleteMemberAttribute: vi.fn(),
  OrgError: class OrgError extends Error {},
}));

vi.mock("../api/entitlements", async (orig) => ({
  ...(await orig<typeof import("../api/entitlements")>()),
  fetchPending: vi.fn(() => Promise.resolve([])),
  fetchHeldLists: vi.fn(() => Promise.resolve({ principal: "okta|31", lists: [] })),
  fetchEffective: vi.fn(),
  searchPeople: vi.fn(),
}));

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    activeOrgId: "acme",
    userId: "uid-me",
    multitenancy: true,
    capabilities: mockCapabilities,
  }),
}));

vi.mock("../hooks/useAdminQueries", () => ({
  useRoles: () => ({ roles: [{ id: "viewer" }] }),
  useDomains: () => ({ domains: [] }),
}));
vi.mock("../hooks/useTableQueries", () => ({ useTables: () => ({ tables: [] }) }));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

import { fetchHeldLists, fetchPending } from "../api/entitlements";

beforeEach(() => {
  vi.clearAllMocks();
  mockCapabilities = [];
});

describe("the Team page's entitlements", () => {
  it("offers a member's access and the grants section with assign_entitlements", async () => {
    mockCapabilities = ["user_management", "assign_entitlements"];
    render(<TeamPage />);
    expect(await screen.findByTestId("team-section-entitlements")).toBeInTheDocument();
    await userEvent.click(await screen.findByTestId("team-member-okta|31"));
    await userEvent.click(await screen.findByTestId("team-member-tab-access"));
    expect(await screen.findByTestId("person-entitlements")).toBeInTheDocument();
    expect(fetchHeldLists).toHaveBeenCalledWith("okta|31");
  });

  it("offers neither without assign_entitlements, and reads nothing of them", async () => {
    mockCapabilities = ["user_management"];
    render(<TeamPage />);
    await userEvent.click(await screen.findByTestId("team-member-okta|31"));
    expect(await screen.findByTestId("member-attributes")).toBeInTheDocument();
    expect(screen.queryByTestId("team-member-tab-access")).toBeNull();
    expect(screen.queryByTestId("team-section-entitlements")).toBeNull();
    expect(fetchPending).not.toHaveBeenCalled();
    expect(fetchHeldLists).not.toHaveBeenCalled();
  });
});
