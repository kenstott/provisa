// Copyright (c) 2026 Kenneth Stott
// Canary: 7ed407ac-c87f-4563-8689-f06a4682905e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1568: when several orgs claim the signed-in address, sign-in joins none of them and the page
// puts the choice to the person. Picking one joins exactly that org; turning them all down leaves
// the page as it is for anyone no rule matched, so org creation is still there.
import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../test-utils/render";
import { OnboardOrgPage } from "../pages/OnboardOrgPage";

vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  createOrg: vi.fn(),
  fetchOrgStatus: vi.fn(),
  fetchMyInvites: vi.fn().mockResolvedValue([]),
  fetchAutoJoinOffers: vi.fn(),
  acceptAutoJoin: vi.fn(),
  declineAutoJoin: vi.fn(),
  redeemInvite: vi.fn(),
}));
vi.mock("../api/billing", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/billing")>()),
  fetchMyReservation: vi.fn().mockResolvedValue(null),
  fetchCatalog: vi.fn(),
}));

const navigate = vi.fn();
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => navigate,
}));

const mockAuth = { billing: false, selectOrg: vi.fn(), refresh: vi.fn() };
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => mockAuth,
}));

import { acceptAutoJoin, declineAutoJoin, fetchAutoJoinOffers } from "../api/admin";

const mockOffers = vi.mocked(fetchAutoJoinOffers);
const mockAccept = vi.mocked(acceptAutoJoin);
const mockDecline = vi.mocked(declineAutoJoin);

const OFFERS = [
  { org_id: "acme", org_name: "Acme", role_id: "analyst" },
  { org_id: "globex", org_name: "Globex", role_id: "analyst" },
];

beforeEach(() => {
  vi.clearAllMocks();
  mockAuth.billing = false;
});

describe("REQ-1568 the choice between orgs claiming one address", () => {
  it("lists every org that matched rather than joining any of them", async () => {
    mockOffers.mockResolvedValue(OFFERS);
    render(<OnboardOrgPage />);

    expect(await screen.findByTestId("onboard-auto-join-offers")).toBeInTheDocument();
    expect(screen.getByText("Acme")).toBeInTheDocument();
    expect(screen.getByText("Globex")).toBeInTheDocument();
    expect(mockAccept).not.toHaveBeenCalled();
  });

  it("joins only the org that was picked", async () => {
    const user = userEvent.setup();
    mockOffers.mockResolvedValue(OFFERS);
    mockAccept.mockResolvedValue({ org_id: "globex", role_id: "analyst" });
    render(<OnboardOrgPage />);

    await user.click(await screen.findByTestId("onboard-join-offered-globex"));

    await waitFor(() => expect(mockAccept).toHaveBeenCalledWith("globex"));
    expect(mockAccept).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(navigate).toHaveBeenCalledWith("/query"));
  });

  it("clears the question once every claim is turned down", async () => {
    const user = userEvent.setup();
    mockOffers.mockResolvedValue(OFFERS);
    mockDecline.mockResolvedValue(["acme", "globex"]);
    render(<OnboardOrgPage />);

    await user.click(await screen.findByTestId("onboard-decline-auto-join"));

    await waitFor(() =>
      expect(screen.queryByTestId("onboard-auto-join-offers")).not.toBeInTheDocument(),
    );
    // Creating an org is still the way forward for someone who belongs to none of them.
    expect(screen.getByTestId("onboard-org-submit")).toBeInTheDocument();
    expect(mockAccept).not.toHaveBeenCalled();
  });

  it("asks nothing when one org or none claimed the address", async () => {
    mockOffers.mockResolvedValue([]);
    render(<OnboardOrgPage />);

    expect(await screen.findByTestId("onboard-org-submit")).toBeInTheDocument();
    expect(screen.queryByTestId("onboard-auto-join-offers")).not.toBeInTheDocument();
  });
});
