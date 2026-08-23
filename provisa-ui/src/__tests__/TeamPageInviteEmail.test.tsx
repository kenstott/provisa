// Copyright (c) 2026 Kenneth Stott
// Canary: 8a4f61c2-71d5-4b3e-9c07-5e2d84b1f6aa
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1287/REQ-1310/REQ-1569: an invitation is a message, not only a link. The org_admin types the
// address, Provisa sends the branded invitation, and when the send fails they are told so in the
// same breath as the link they now have to pass on themselves. The join rule sits on the same page
// because it decides the other way in: who arrives without an invitation at all.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../test-utils/render";
import { MemoryRouter } from "react-router-dom";
import { TeamPage } from "../pages/TeamPage";

const createInviteSpy = vi.fn();
const updateSettingsSpy = vi.fn(async () => ({
  id: "acme",
  email_rule: null,
  auto_join: false,
  auto_join_role: null,
}));
let mockSettings = {
  id: "acme",
  email_rule: "@acme\\.com$",
  auto_join: true,
  auto_join_role: "analyst",
};

vi.mock("../api/admin", () => ({
  fetchInvites: () => Promise.resolve([]),
  createInvite: (...a: unknown[]) => createInviteSpy(...(a as [])),
  revokeInvite: vi.fn(),
  fetchOrgMembers: () => Promise.resolve([]),
  removeOrgMember: vi.fn(),
  grantOrgAdmin: vi.fn(),
  revokeOrgAdmin: vi.fn(),
  deleteOrg: vi.fn(),
  exportOrgConfig: vi.fn(),
  fetchOrgSettings: () => Promise.resolve(mockSettings),
  updateOrgSettings: (...a: unknown[]) => updateSettingsSpy(...(a as [])),
  OrgError: class OrgError extends Error {
    constructor(
      public status: number,
      public code: string | null,
      message: string,
    ) {
      super(message);
    }
  },
}));

// The branding section shares the page; its own reads are not what these tests are about.
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
const notify = vi.fn();
vi.mock("@mantine/notifications", () => ({
  notifications: { show: (...a: unknown[]) => notify(...(a as [])) },
}));

// The rejection has to be an instance of the class the component imports, which is the mocked one.
const { OrgError } = await import("../api/admin");

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
  mockSettings = {
    id: "acme",
    email_rule: "@acme\\.com$",
    auto_join: true,
    auto_join_role: "analyst",
  };
  Object.defineProperty(navigator, "clipboard", {
    configurable: true,
    value: { writeText: vi.fn(async () => undefined) },
  });
});

describe("inviting someone by email", () => {
  it("sends the invitation to the address the org_admin typed", async () => {
    const user = userEvent.setup();
    createInviteSpy.mockResolvedValue({
      token: "tok-1",
      org_id: "acme",
      org_name: "Acme",
      role_id: "analyst",
      email: "dana@acme.com",
      created_by: "uid-me",
      expires_at: new Date().toISOString(),
      used_at: null,
      used_by: null,
      delivery: "sent",
    });
    renderTeamPage();

    await user.type(await screen.findByTestId("team-invite-email"), "dana@acme.com");
    await user.click(screen.getByTestId("team-invite-role"));
    await user.click(await screen.findByText("analyst"));
    await user.click(screen.getByTestId("team-invite-create"));

    await waitFor(() => expect(createInviteSpy).toHaveBeenCalled());
    expect(createInviteSpy.mock.calls[0]).toEqual(["acme", "analyst", 7, "dana@acme.com"]);
    await waitFor(() => expect(notify.mock.calls[0][0].message).toMatch(/dana@acme\.com/));
    expect(screen.queryByTestId("team-error")).toBeNull();
  });

  it("says the link must be passed on by hand when the send failed", async () => {
    const user = userEvent.setup();
    createInviteSpy.mockResolvedValue({
      token: "tok-2",
      org_id: "acme",
      org_name: "Acme",
      role_id: "analyst",
      email: "dana@acme.com",
      created_by: "uid-me",
      expires_at: new Date().toISOString(),
      used_at: null,
      used_by: null,
      delivery: "failed: no SMTP host is configured",
    });
    renderTeamPage();

    await user.type(await screen.findByTestId("team-invite-email"), "dana@acme.com");
    await user.click(screen.getByTestId("team-invite-role"));
    await user.click(await screen.findByText("analyst"));
    await user.click(screen.getByTestId("team-invite-create"));

    const err = await screen.findByTestId("team-error");
    expect(err.textContent).toMatch(/no SMTP host is configured/);
    expect(err.textContent).toMatch(/tok-2/);
  });

  it("still makes a shareable link when no address is given", async () => {
    const user = userEvent.setup();
    createInviteSpy.mockResolvedValue({
      token: "tok-3",
      org_id: "acme",
      org_name: "Acme",
      role_id: "analyst",
      email: null,
      created_by: "uid-me",
      expires_at: new Date().toISOString(),
      used_at: null,
      used_by: null,
      delivery: "not_addressed",
    });
    renderTeamPage();

    await user.click(await screen.findByTestId("team-invite-role"));
    await user.click(await screen.findByText("analyst"));
    await user.click(screen.getByTestId("team-invite-create"));

    await waitFor(() => expect(createInviteSpy).toHaveBeenCalled());
    expect(createInviteSpy.mock.calls[0][3]).toBeUndefined();
    expect(screen.queryByTestId("team-error")).toBeNull();
  });
});

describe("the auto-join rule after the org exists", () => {
  it("shows the rule the org is running", async () => {
    renderTeamPage();
    const rule = (await screen.findByTestId("org-join-email-rule")) as HTMLInputElement;
    expect(rule.value).toBe("@acme\\.com$");
    expect((screen.getByTestId("org-join-auto-join") as HTMLInputElement).checked).toBe(true);
    expect((screen.getByTestId("org-join-auto-join-role") as HTMLInputElement).value).toBe(
      "analyst",
    );
  });

  it("saves an edited rule", async () => {
    const user = userEvent.setup();
    renderTeamPage();
    const rule = await screen.findByTestId("org-join-email-rule");
    await user.clear(rule);
    await user.type(rule, "@newco\\.com$");
    await user.click(screen.getByTestId("org-join-save"));

    await waitFor(() => expect(updateSettingsSpy).toHaveBeenCalled());
    expect(updateSettingsSpy.mock.calls[0]).toEqual([
      "acme",
      {
        emailRule: "@newco\\.com$",
        autoJoin: true,
        autoJoinRole: "analyst",
        riskAcknowledged: false,
      },
    ]);
  });

  it("asks the author to accept a rule that reaches further than one domain", async () => {
    const user = userEvent.setup();
    updateSettingsSpy.mockRejectedValueOnce(
      new OrgError(
        400,
        "orgs.auto_join_breadth_unacknowledged",
        "besides acme.com, it also accepts addresses such as someone@notacme.com",
      ),
    );
    renderTeamPage();
    await screen.findByTestId("org-join-email-rule");
    await user.click(screen.getByTestId("org-join-save"));

    const warning = await screen.findByTestId("org-join-breadth-warning");
    expect(warning.textContent).toMatch(/someone@notacme\.com/);
    expect(screen.queryByTestId("team-error")).toBeNull();

    await user.click(screen.getByTestId("org-join-accept-risk"));
    await user.click(screen.getByTestId("org-join-save"));
    await waitFor(() => expect(updateSettingsSpy).toHaveBeenCalledTimes(2));
    expect(updateSettingsSpy.mock.calls[1][1]).toMatchObject({ riskAcknowledged: true });
  });
});
