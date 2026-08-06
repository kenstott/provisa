// Copyright (c) 2026 Kenneth Stott
// Canary: 8d5c1e70-63af-4b29-9c04-1e7a2f8b6d33
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1306/REQ-1307: leaving an org and closing the account are the person's own acts, so they sit
// on their profile. These tests pin that both paths exist there, that account deletion carries the
// same retype ceremony org deletion does, and that a server refusal (last org_admin) is shown
// rather than swallowed.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import i18n from "../i18n";
import { UserProfileModal } from "../components/UserProfileModal";

const t = i18n.getFixedT("en");

const leaveSpy = vi.fn(async () => undefined);
const deleteAccountSpy = vi.fn(async () => undefined);

vi.mock("../api/admin", () => ({
  updateProfile: vi.fn(async () => undefined),
  leaveOrg: (...a: unknown[]) => leaveSpy(...(a as [])),
  deleteAccount: (...a: unknown[]) => deleteAccountSpy(...(a as [])),
  // REQ-1263: the profile also hosts the person's access tokens; this suite is about offboarding.
  listPersonalAccessTokens: vi.fn(async () => []),
  issuePersonalAccessToken: vi.fn(),
  revokePersonalAccessToken: vi.fn(),
}));

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    displayName: "Me",
    email: "me@example.test",
    userId: "uid-me",
    givenName: "Me",
    familyName: null,
    devMode: false,
    availableRoles: [],
    assignments: [],
    capabilities: [],
    orgMemberships: [
      { org_id: "acme", org_name: "Acme" },
      { org_id: "beta", org_name: "Beta" },
    ],
    activeOrgId: "acme",
    refresh: vi.fn(),
  }),
}));

describe("UserProfileModal offboarding", () => {
  beforeEach(() => {
    leaveSpy.mockClear();
    deleteAccountSpy.mockClear();
  });

  it("offers a leave control for each org the person belongs to", async () => {
    render(<UserProfileModal onClose={() => {}} />);
    expect(await screen.findByTestId("profile-leave-acme")).toBeInTheDocument();
    expect(screen.getByTestId("profile-leave-beta")).toBeInTheDocument();
  });

  it("leaves the org that was clicked, not the active one", async () => {
    render(<UserProfileModal onClose={() => {}} />);
    fireEvent.click(await screen.findByTestId("profile-leave-beta"));
    await waitFor(() => expect(leaveSpy).toHaveBeenCalledWith("beta"));
  });

  it("shows the server's refusal when leaving would strand the org", async () => {
    leaveSpy.mockRejectedValueOnce(new Error("You are the last org_admin of acme"));
    render(<UserProfileModal onClose={() => {}} />);
    fireEvent.click(await screen.findByTestId("profile-leave-acme"));
    expect(await screen.findByTestId("profile-membership-error")).toHaveTextContent(
      "You are the last org_admin of acme",
    );
  });

  it("keeps account deletion inert until the user id is retyped", async () => {
    render(<UserProfileModal onClose={() => {}} />);
    const submit = await screen.findByTestId("profile-delete-submit");
    expect(submit).toBeDisabled();

    fireEvent.change(screen.getByTestId("profile-delete-confirm"), {
      target: { value: "uid-you" },
    });
    expect(submit).toBeDisabled();

    fireEvent.change(screen.getByTestId("profile-delete-confirm"), {
      target: { value: "uid-me" },
    });
    await waitFor(() => expect(submit).not.toBeDisabled());
    fireEvent.click(submit);
    await waitFor(() => expect(deleteAccountSpy).toHaveBeenCalledWith("uid-me"));
  });

  it("says what deletion keeps, so nobody expects their orgs to vanish with them", async () => {
    render(<UserProfileModal onClose={() => {}} />);
    expect(
      await screen.findByText(t("userProfileModal.deleteAccountHelp")),
    ).toBeInTheDocument();
  });
});
