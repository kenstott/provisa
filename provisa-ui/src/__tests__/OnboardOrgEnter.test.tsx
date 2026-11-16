// Copyright (c) 2026 Kenneth Stott
// Canary: 2f7a9c41-8d3e-4b56-a1f0-6e9b2c5d7a18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1276: an org create lands on its welcome; a join enters the app at /query. Where the host
// names no org (localhost, a desktop install) that entry is an in-app route. The identity refresh
// that follows a create or a join is what lifts the membership gate, and the shell opens on the URL
// shown at that moment. The route was taken after the refresh, so the shell had already opened at
// `/`, whose landing (the role's first page, REQ-1327) replaced the welcome: onboard-org.spec.ts
// ended on /admin/my-secrets (run 43725827456). The route is taken before the refresh.
import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { useLocation } from "react-router-dom";
import { render, screen, waitFor } from "../test-utils/render";
import { OnboardOrgPage } from "../pages/OnboardOrgPage";

vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  createOrg: vi.fn(),
  fetchOrgStatus: vi.fn(),
  fetchMyInvites: vi.fn().mockResolvedValue([]),
  redeemInvite: vi.fn(),
}));
vi.mock("../api/billing", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/billing")>()),
  fetchMyReservation: vi.fn().mockResolvedValue(null),
  fetchCatalog: vi.fn(),
}));

// The refresh is held open, as /auth/me in flight holds it: what the page has done by then is
// what the shell opens on.
let settleRefresh: () => void = () => {};
const mockAuth = {
  billing: false,
  email: "carol@example.com",
  selectOrg: vi.fn(),
  refresh: vi.fn(
    () =>
      new Promise<void>((resolve) => {
        settleRefresh = resolve;
      }),
  ),
  rebind: vi.fn(),
};
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => mockAuth,
}));

import { createOrg, redeemInvite } from "../api/admin";

function Where() {
  const location = useLocation();
  return <div data-testid="where">{location.pathname}</div>;
}

beforeEach(() => {
  vi.clearAllMocks();
  settleRefresh = () => {};
});

describe("leaving onboarding for an org on a host that names none", () => {
  it("is on the welcome before the refresh lifts the gate, with the new org bound", async () => {
    vi.mocked(createOrg).mockResolvedValue({
      id: "carolco",
      name: "Carol Co",
      provisioning_state: "ready",
    });
    render(
      <>
        <OnboardOrgPage />
        <Where />
      </>,
      { initialEntries: ["/"] },
    );
    const user = userEvent.setup();
    await user.type(screen.getByTestId("onboard-org-id"), "carolco");
    await user.type(screen.getByTestId("onboard-org-name"), "Carol Co");
    await user.click(screen.getByTestId("onboard-org-submit"));

    await waitFor(() => expect(mockAuth.refresh).toHaveBeenCalledTimes(1));
    expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco");
    expect(screen.getByTestId("where").textContent).toBe("/welcome");
    settleRefresh();
    await waitFor(() => expect(screen.getByTestId("where").textContent).toBe("/welcome"));
  });

  it("is on /query before the refresh lifts the gate after a join", async () => {
    vi.mocked(redeemInvite).mockResolvedValue({
      user_id: "carol",
      org_id: "carolco",
      role_id: "analyst",
    });
    render(
      <>
        <OnboardOrgPage />
        <Where />
      </>,
      { initialEntries: ["/"] },
    );
    const user = userEvent.setup();
    await user.click(screen.getByText("Join with an invite"));
    await user.type(screen.getByTestId("onboard-org-invite"), "tok-1");
    await user.click(screen.getByTestId("onboard-org-join-submit"));

    await waitFor(() => expect(mockAuth.refresh).toHaveBeenCalledTimes(1));
    expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco");
    expect(screen.getByTestId("where").textContent).toBe("/query");
  });
});
