// Copyright (c) 2026 Kenneth Stott
// Canary: a4d33526-ebc8-4360-b9b7-d730674c4fa2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1696: how long an invitation stays redeemable is the org_admin's call, made on the form. A
// fixed week -- never shown, never asked -- expired the public sandbox link while it was still on
// the website. These tests pin that the field exists, that its value is what the page sends, and
// that a value no invitation can carry disables minting rather than sending it.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../test-utils/render";
import { TeamPage } from "../pages/TeamPage";

const createInviteSpy = vi.fn();

vi.mock("../api/admin", () => ({
  ENV_POLICY_NONE: "none",
  ENV_POLICY_PER_VISITOR: "per_visitor",
  ENV_POLICY_SHARED: "shared",
  fetchInvites: () => Promise.resolve([]),
  createInvite: (...a: unknown[]) => createInviteSpy(...(a as [])),
  revokeInvite: vi.fn(),
  fetchOrgMembers: () => Promise.resolve([]),
  removeOrgMember: vi.fn(),
  grantOrgAdmin: vi.fn(),
  revokeOrgAdmin: vi.fn(),
  deleteOrg: vi.fn(),
  exportOrgConfig: vi.fn(),
  fetchOrgSettings: () =>
    Promise.resolve({ id: "acme", email_rule: null, auto_join: false, auto_join_role: null }),
  updateOrgSettings: vi.fn(),
  OrgError: class OrgError extends Error {},
}));

vi.mock("../api/branding", () => ({
  fetchOrgBranding: () => Promise.resolve({ branding: {}, logo_media_type: null }),
  saveOrgBranding: vi.fn(),
  uploadOrgLogo: vi.fn(),
  deleteOrgLogo: vi.fn(),
  previewInviteMessage: vi.fn(),
  publicLogoUrl: () => "",
}));

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ activeOrgId: "acme", userId: "uid-me", multitenancy: true }),
}));
vi.mock("../hooks/useAdminQueries", () => ({
  useRoles: () => ({ roles: [{ id: "analyst" }] }),
}));
vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  createInviteSpy.mockResolvedValue({
    token: "tok",
    org_id: "acme",
    role_id: "analyst",
    email: null,
    expires_at: "2100-01-01T00:00:00Z",
    used_at: null,
    uses: 0,
    max_uses: 1,
    env_policy: "none",
    env_ttl_seconds: null,
    env_name: null,
    delivery: "not_addressed",
  });
  Object.defineProperty(navigator, "clipboard", {
    configurable: true,
    value: { writeText: vi.fn(async () => undefined) },
  });
});

async function pickAnalyst(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByTestId("team-invite-role"));
  await user.click(await screen.findByText("analyst"));
}

describe("choosing how long an invitation stays redeemable", () => {
  it("opens on a week and sends what the org_admin types instead", async () => {
    const user = userEvent.setup();
    render(<TeamPage />);
    await pickAnalyst(user);

    const days = screen.getByTestId("team-invite-expires-days");
    expect(days).toHaveValue("7");
    await user.clear(days);
    await user.type(days, "36500");
    await user.click(screen.getByTestId("team-invite-create"));

    await waitFor(() => expect(createInviteSpy).toHaveBeenCalled());
    expect(createInviteSpy.mock.calls[0][1]).toMatchObject({ expiresInDays: 36500 });
  });

  it("will not mint a link that expires before it is made", async () => {
    const user = userEvent.setup();
    render(<TeamPage />);
    await pickAnalyst(user);

    const days = screen.getByTestId("team-invite-expires-days");
    await user.clear(days);
    expect(screen.getByTestId("team-invite-create")).toBeDisabled();

    await user.type(days, "30");
    expect(screen.getByTestId("team-invite-create")).toBeEnabled();
  });
});
