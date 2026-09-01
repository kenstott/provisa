// Copyright (c) 2026 Kenneth Stott
// Canary: 9b41c7de-3a05-4f62-8d19-71ce4a20f5b3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1616: an invitation has to survive the provider round trip to be redeemable at all — there is
// no bearer to redeem it with until sign-in returns, and between the click on the link and that
// return the token lives in nothing but the address bar. These tests hold the two ends: the login
// page writes the token down the moment it sees it and redeems from that memory rather than from
// this tab's query, and the onboarding page — where a member of nothing is routed — spends an
// invitation that was never redeemed instead of asking someone who followed a link to type it.
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import type { ReactElement } from "react";
import { MemoryRouter } from "react-router-dom";
import { render, screen, fireEvent, waitFor } from "../test-utils/render";
import { LoginPage } from "../pages/LoginPage";
import { OnboardOrgPage } from "../pages/OnboardOrgPage";
import { forgetInvite, pendingInvite, rememberInvite } from "../lib/pendingInvite";

const renderRouted = (ui: ReactElement) => render(<MemoryRouter>{ui}</MemoryRouter>);

vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchProviderType: vi.fn().mockResolvedValue("firebase"),
  fetchBootstrapStatus: vi.fn().mockResolvedValue(false),
  claimBootstrap: vi.fn().mockResolvedValue(true),
  fetchInviteInfo: vi.fn(),
  redeemInvite: vi.fn(),
  createOrg: vi.fn(),
  fetchOrgStatus: vi.fn(),
  fetchMyInvites: vi.fn().mockResolvedValue([]),
  fetchAutoJoinOffers: vi.fn().mockResolvedValue([]),
  acceptAutoJoin: vi.fn(),
  declineAutoJoin: vi.fn(),
}));
vi.mock("../api/billing", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/billing")>()),
  fetchMyReservation: vi.fn().mockResolvedValue(null),
  fetchCatalog: vi.fn(),
}));
vi.mock("../lib/firebase", () => ({
  signInWithGoogle: vi.fn().mockResolvedValue("firebase-id-token"),
  signInWithGithub: vi.fn(),
  signInWithMicrosoft: vi.fn(),
  signInWithEmailPassword: vi.fn(),
  registerWithEmailPassword: vi.fn(),
}));

const navigate = vi.fn();
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => navigate,
}));

const mockAuth = { billing: false, email: "visitor@example.com", selectOrg: vi.fn(), refresh: vi.fn() };
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => mockAuth,
}));

import { fetchInviteInfo, fetchProviderType, redeemInvite } from "../api/admin";

const mockInviteInfo = vi.mocked(fetchInviteInfo);
const mockProviderType = vi.mocked(fetchProviderType);
const mockRedeem = vi.mocked(redeemInvite);

const TOKEN = "d3011ee8-01db-408c-ac79-10226898b649";

const setQuery = (search: string) => window.history.replaceState({}, "", `/login${search}`);

beforeEach(() => {
  vi.clearAllMocks();
  forgetInvite();
  setQuery("");
  mockProviderType.mockResolvedValue("firebase");
  mockInviteInfo.mockResolvedValue({ token: TOKEN, org_id: "sandbox", org_name: "Sandbox", role_id: "sandbox_user", valid: true });
});

afterEach(() => {
  forgetInvite();
  localStorage.removeItem("provisa_token");
  setQuery("");
});

describe("REQ-1616 an invitation kept until it is redeemed", () => {
  it("writes down a token the login page sees in the address bar", async () => {
    setQuery(`?invite=${TOKEN}`);
    renderRouted(<LoginPage onLoginSuccess={vi.fn()} authDisabled={false} />);

    await waitFor(() => expect(pendingInvite()).toBe(TOKEN));
  });

  it("redeems the remembered token when the query string no longer carries it", async () => {
    // The state an org-subdomain bounce or a second tab leaves behind: the account is about to
    // exist, the invitation is in hand, and this tab's URL says nothing about it.
    rememberInvite(TOKEN);
    mockRedeem.mockResolvedValue({ user_id: "visitor", org_id: "sandbox", role_id: "sandbox_user" });
    renderRouted(<LoginPage onLoginSuccess={vi.fn()} authDisabled={false} />);

    fireEvent.click(await screen.findByTestId("firebase-signin-button"));

    await waitFor(() => expect(mockRedeem).toHaveBeenCalledWith(TOKEN));
    await waitFor(() => expect(pendingInvite()).toBeNull());
  });

  it("keeps the token when redemption fails, so the visitor is not left holding nothing", async () => {
    rememberInvite(TOKEN);
    mockRedeem.mockRejectedValue(new Error("Internal Server Error"));
    renderRouted(<LoginPage onLoginSuccess={vi.fn()} authDisabled={false} />);

    fireEvent.click(await screen.findByTestId("firebase-signin-button"));

    await waitFor(() => expect(mockRedeem).toHaveBeenCalledWith(TOKEN));
    expect(pendingInvite()).toBe(TOKEN);
  });

  it("signs in with no redemption at all when no invitation is held", async () => {
    const onLoginSuccess = vi.fn();
    renderRouted(<LoginPage onLoginSuccess={onLoginSuccess} authDisabled={false} />);

    fireEvent.click(await screen.findByTestId("firebase-signin-button"));

    await waitFor(() => expect(onLoginSuccess).toHaveBeenCalledWith("firebase-id-token"));
    expect(mockRedeem).not.toHaveBeenCalled();
  });
});

describe("REQ-1616 onboarding spends an invitation that went unredeemed", () => {
  it("redeems the held invitation on arrival and enters the org", async () => {
    rememberInvite(TOKEN);
    mockRedeem.mockResolvedValue({ user_id: "visitor", org_id: "sandbox", role_id: "sandbox_user" });

    render(<OnboardOrgPage />);

    await waitFor(() => expect(mockRedeem).toHaveBeenCalledWith(TOKEN));
    expect(mockRedeem).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(mockAuth.selectOrg).toHaveBeenCalledWith("sandbox"));
  });

  it("attempts a held invitation once, so a refusal is not replayed on every render", async () => {
    rememberInvite(TOKEN);
    mockRedeem.mockRejectedValue(new Error("Invite already used"));

    render(<OnboardOrgPage />);

    await waitFor(() => expect(screen.getByTestId("onboard-org-error")).toHaveTextContent(
      "Invite already used",
    ));
    expect(mockRedeem).toHaveBeenCalledTimes(1);
    expect(pendingInvite()).toBeNull();
  });

  it("leaves the refused token in the field, so retrying is a click and not a lost link", async () => {
    rememberInvite(TOKEN);
    mockRedeem.mockRejectedValue(new Error("Invite already used"));

    render(<OnboardOrgPage />);

    await waitFor(() => expect(screen.getByTestId("onboard-org-invite")).toHaveValue(TOKEN));
  });

  it("opens on org creation and asks for a token as before when none is held", async () => {
    render(<OnboardOrgPage />);

    expect(await screen.findByLabelText("Join with an invite")).not.toBeChecked();
    expect(screen.queryByTestId("onboard-org-invite")).not.toBeInTheDocument();
    expect(mockRedeem).not.toHaveBeenCalled();
  });
});
