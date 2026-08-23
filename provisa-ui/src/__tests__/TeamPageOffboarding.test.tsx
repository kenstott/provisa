// Copyright (c) 2026 Kenneth Stott
// Canary: 2f8b41d6-0a37-4c9e-b512-7d3e6a9c04f1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1302/1303/1304/1300: the team page's offboarding half. The server enforces every rule here
// (last-org_admin, typed confirmation); these tests pin the page's side of it — that it shows the
// roster with who holds org_admin, refuses to offer the removal or demotion that would strand the
// org, and gates deletion behind the download-then-retype ceremony.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import i18n from "../i18n";
import { MemoryRouter } from "react-router-dom";
import { TeamPage } from "../pages/TeamPage";
import type { OrgMember } from "../api/admin";

const t = i18n.getFixedT("en");

const removeSpy = vi.fn(async () => undefined);
const grantSpy = vi.fn(async () => undefined);
const revokeAdminSpy = vi.fn(async () => undefined);
const deleteOrgSpy = vi.fn(async () => undefined);
const exportSpy = vi.fn(async () => "sources: []\n");
let mockMembers: OrgMember[] = [];
let mockMultitenancy = true;

vi.mock("../api/admin", () => ({
  fetchInvites: () => Promise.resolve([]),
  createInvite: vi.fn(),
  revokeInvite: vi.fn(),
  fetchOrgMembers: () => Promise.resolve(mockMembers),
  removeOrgMember: (...a: unknown[]) => removeSpy(...(a as [])),
  grantOrgAdmin: (...a: unknown[]) => grantSpy(...(a as [])),
  revokeOrgAdmin: (...a: unknown[]) => revokeAdminSpy(...(a as [])),
  deleteOrg: (...a: unknown[]) => deleteOrgSpy(...(a as [])),
  exportOrgConfig: (...a: unknown[]) => exportSpy(...(a as [])),
  fetchOrgSettings: () =>
    Promise.resolve({ id: "acme", email_rule: null, auto_join: false, auto_join_role: null }),
  updateOrgSettings: vi.fn(),
  OrgError: class OrgError extends Error {},
}));

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ activeOrgId: "acme", userId: "uid-me", multitenancy: mockMultitenancy }),
}));

vi.mock("../hooks/useAdminQueries", () => ({
  useRoles: () => ({ roles: [{ id: "analyst" }] }),
}));

vi.mock("@mantine/notifications", () => ({ notifications: { show: vi.fn() } }));

function member(over: Partial<OrgMember>): OrgMember {
  return {
    user_id: "uid-x",
    email: "x@example.test",
    display_name: null,
    provider: "google",
    is_org_admin: false,
    ...over,
  };
}

/** The page reads the ?section= deep link, so it renders under a router as it does in the app. */
function renderTeamPage() {
  return render(
    <MemoryRouter>
      <TeamPage />
    </MemoryRouter>,
  );
}

describe("TeamPage offboarding", () => {
  beforeEach(() => {
    removeSpy.mockClear();
    grantSpy.mockClear();
    revokeAdminSpy.mockClear();
    deleteOrgSpy.mockClear();
    exportSpy.mockClear();
    mockMembers = [];
    mockMultitenancy = true;
  });

  it("lists each member and says who holds org_admin", async () => {
    mockMembers = [
      member({ user_id: "uid-me", display_name: "Me", is_org_admin: true }),
      member({ user_id: "uid-bob", display_name: "Bob" }),
    ];
    renderTeamPage();

    const bob = await screen.findByTestId("team-member-uid-bob");
    expect(bob).toHaveTextContent("Bob");
    expect(bob).toHaveTextContent(t("teamPage.no"));
    expect(screen.getByTestId("team-member-uid-me")).toHaveTextContent(t("teamPage.yes"));
  });

  it("promotes a member to org_admin", async () => {
    mockMembers = [
      member({ user_id: "uid-me", is_org_admin: true }),
      member({ user_id: "uid-bob" }),
    ];
    renderTeamPage();

    fireEvent.click(await screen.findByTestId("team-toggle-admin-uid-bob"));
    await waitFor(() => expect(grantSpy).toHaveBeenCalledWith("acme", "uid-bob"));
  });

  it("demotes a second org_admin, since the org keeps one", async () => {
    mockMembers = [
      member({ user_id: "uid-me", is_org_admin: true }),
      member({ user_id: "uid-bob", is_org_admin: true }),
    ];
    renderTeamPage();

    fireEvent.click(await screen.findByTestId("team-toggle-admin-uid-bob"));
    await waitFor(() => expect(revokeAdminSpy).toHaveBeenCalledWith("acme", "uid-bob"));
  });

  it("offers neither demotion nor removal for the only org_admin", async () => {
    mockMembers = [
      member({ user_id: "uid-me", is_org_admin: true }),
      member({ user_id: "uid-bob" }),
    ];
    renderTeamPage();

    expect(await screen.findByTestId("team-toggle-admin-uid-me")).toBeDisabled();
    expect(screen.getByTestId("team-remove-uid-me")).toBeDisabled();
  });

  it("removes an ordinary member", async () => {
    mockMembers = [
      member({ user_id: "uid-me", is_org_admin: true }),
      member({ user_id: "uid-bob" }),
    ];
    renderTeamPage();

    fireEvent.click(await screen.findByTestId("team-remove-uid-bob"));
    await waitFor(() => expect(removeSpy).toHaveBeenCalledWith("acme", "uid-bob"));
  });

  it("surfaces the server's refusal rather than pretending the removal happened", async () => {
    mockMembers = [
      member({ user_id: "uid-me", is_org_admin: true }),
      member({ user_id: "uid-bob" }),
    ];
    removeSpy.mockRejectedValueOnce(new Error("Cannot remove the last org_admin"));
    renderTeamPage();

    fireEvent.click(await screen.findByTestId("team-remove-uid-bob"));
    expect(await screen.findByTestId("team-error")).toHaveTextContent(
      "Cannot remove the last org_admin",
    );
  });

  it("keeps the delete button inert until the org id is retyped, then sends the confirmation", async () => {
    mockMembers = [member({ user_id: "uid-me", is_org_admin: true })];
    renderTeamPage();

    fireEvent.click(await screen.findByTestId("team-delete-org"));
    const submit = await screen.findByTestId("team-delete-submit");
    expect(submit).toBeDisabled();

    fireEvent.change(screen.getByTestId("team-delete-confirm"), {
      target: { value: "acmee" },
    });
    expect(submit).toBeDisabled();

    fireEvent.change(screen.getByTestId("team-delete-confirm"), { target: { value: "acme" } });
    await waitFor(() => expect(submit).not.toBeDisabled());
    fireEvent.click(submit);
    await waitFor(() => expect(deleteOrgSpy).toHaveBeenCalledWith("acme", "acme"));
  });

  it("offers the configuration download inside the deletion flow", async () => {
    mockMembers = [member({ user_id: "uid-me", is_org_admin: true })];
    // jsdom implements neither object URLs nor navigation-by-anchor.
    const createObjectURL = vi.fn(() => "blob:config");
    const revokeObjectURL = vi.fn();
    vi.stubGlobal("URL", { ...URL, createObjectURL, revokeObjectURL });
    renderTeamPage();

    fireEvent.click(await screen.findByTestId("team-delete-org"));
    fireEvent.click(await screen.findByTestId("team-export-config"));

    await waitFor(() => expect(exportSpy).toHaveBeenCalledWith("acme"));
    await waitFor(() => expect(createObjectURL).toHaveBeenCalled());
    expect(await screen.findByText(t("teamPage.downloadedButton"))).toBeInTheDocument();
    vi.unstubAllGlobals();
  });

  it("hides the delete-organization section on a single-tenant deploy", async () => {
    mockMultitenancy = false;
    mockMembers = [member({ user_id: "uid-me", is_org_admin: true })];
    renderTeamPage();

    await screen.findByTestId("team-page");
    expect(screen.queryByTestId("team-delete-org")).not.toBeInTheDocument();
  });
});
