// Copyright (c) 2026 Kenneth Stott
// Canary: 3f7a2d18-64c9-4e05-b8d1-27ac5f0e93b6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1601: the open link and the per-visitor environment belong to every org_admin, not to the
// sandbox that first used them. From their own team page an org_admin hands someone a link that
// seats them in a private environment deployed from production -- the real model, none of the org --
// and takes it back once it has gone unused. These tests pin what the page sends and what it reports
// back: the ceiling, the policy, the span of disuse, and when a link has nothing left to give.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../test-utils/render";
import { MemoryRouter } from "react-router-dom";
import { TeamPage } from "../pages/TeamPage";

const createInviteSpy = vi.fn();
const revokeSpy = vi.fn();
let listed: unknown[] = [];

vi.mock("../api/admin", () => ({
  ENV_POLICY_NONE: "none",
  ENV_POLICY_PER_VISITOR: "per_visitor",
  ENV_POLICY_SHARED: "shared",
  fetchInvites: () => Promise.resolve(listed),
  createInvite: (...a: unknown[]) => createInviteSpy(...(a as [])),
  revokeInvite: (...a: unknown[]) => revokeSpy(...(a as [])),
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

/** An invitation as the API returns one, at the defaults every field of these tests varies from. */
function invite(over: Record<string, unknown> = {}) {
  return {
    token: "tok-1",
    org_id: "acme",
    org_name: "Acme",
    role_id: "analyst",
    email: null,
    created_by: "uid-me",
    expires_at: new Date("2026-09-01T00:00:00Z").toISOString(),
    used_at: null,
    used_by: null,
    uses: 0,
    max_uses: 1,
    env_policy: "none",
    env_ttl_seconds: null,
    env_name: null,
    ...over,
  };
}

function renderTeamPage() {
  return render(
    <MemoryRouter>
      <TeamPage />
    </MemoryRouter>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  listed = [];
  createInviteSpy.mockResolvedValue(invite());
  Object.defineProperty(navigator, "clipboard", {
    configurable: true,
    value: { writeText: vi.fn(async () => undefined) },
  });
});

async function pickAnalyst(user: ReturnType<typeof userEvent.setup>) {
  await user.click(await screen.findByTestId("team-invite-role"));
  await user.click(await screen.findByText("analyst"));
}

describe("minting an environment-provisioning invitation", () => {
  it("asks for one environment per redeemer, reaped after an hour of disuse", async () => {
    const user = userEvent.setup();
    renderTeamPage();
    await pickAnalyst(user);

    await user.click(screen.getByTestId("team-invite-open-link"));
    await user.click(screen.getByTestId("team-invite-env-policy"));
    await user.click(await screen.findByText("A fresh environment of their own"));
    await user.click(screen.getByTestId("team-invite-create"));

    await waitFor(() => expect(createInviteSpy).toHaveBeenCalled());
    expect(createInviteSpy.mock.calls[0][1]).toMatchObject({
      roleId: "analyst",
      // REQ-1594: null is unlimited, the only ceiling an open link can carry.
      maxUses: null,
      envPolicy: "per_visitor",
      // REQ-1600: an hour of DISUSE, and the default the control opens on.
      envTtlSeconds: 3600,
    });
  });

  it("offers the span of disuse only for the policy it means anything under", async () => {
    const user = userEvent.setup();
    renderTeamPage();
    await pickAnalyst(user);

    expect(screen.queryByTestId("team-invite-env-ttl")).toBeNull();
    await user.click(screen.getByTestId("team-invite-env-policy"));
    await user.click(await screen.findByText("A fresh environment of their own"));
    expect(await screen.findByTestId("team-invite-env-ttl")).toBeTruthy();
  });

  it("sends no span when the redeemer is seated in the org itself", async () => {
    const user = userEvent.setup();
    renderTeamPage();
    await pickAnalyst(user);
    await user.click(screen.getByTestId("team-invite-create"));

    await waitFor(() => expect(createInviteSpy).toHaveBeenCalled());
    expect(createInviteSpy.mock.calls[0][1]).toMatchObject({
      maxUses: 1,
      envPolicy: "none",
      envTtlSeconds: null,
    });
  });
});

describe("what the invite list reports", () => {
  it("counts redemptions against the ceiling, and says unlimited when there is none", async () => {
    listed = [
      invite({ token: "tok-open", uses: 12, max_uses: null }),
      invite({ token: "tok-one", uses: 0, max_uses: 1 }),
    ];
    renderTeamPage();
    expect(await screen.findByText("12 of unlimited")).toBeTruthy();
    expect(screen.getByText("0 of 1")).toBeTruthy();
  });

  it("says what each link hands its redeemer", async () => {
    listed = [
      invite({ token: "tok-env", env_policy: "per_visitor", env_ttl_seconds: 3600, max_uses: null }),
    ];
    renderTeamPage();
    expect(await screen.findByText("Per visitor, 1 hour of disuse")).toBeTruthy();
  });

  it("reads an exhausted ceiling as spent, and offers neither copy nor revoke", async () => {
    listed = [invite({ token: "tok-spent", uses: 3, max_uses: 3 })];
    renderTeamPage();
    expect(await screen.findByText("Fully redeemed")).toBeTruthy();
    expect(screen.queryByText("Copy")).toBeNull();
    expect(screen.queryByText("Revoke")).toBeNull();
  });

  it("never calls an open link spent, however many have redeemed it", async () => {
    // REQ-1594: `used_at` is set on an open link from its first redemption onward, and it is not
    // what makes an invitation spent -- a link with no ceiling always has another redemption in it.
    listed = [
      invite({
        token: "tok-open",
        uses: 99,
        max_uses: null,
        used_at: new Date("2026-08-20T00:00:00Z").toISOString(),
      }),
    ];
    renderTeamPage();
    expect(await screen.findByText("Revoke")).toBeTruthy();
    expect(screen.queryByText("Fully redeemed")).toBeNull();
  });
});
