// Copyright (c) 2026 Kenneth Stott
// Canary: 88b62bf8-5014-46d3-926a-70a79349925c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1476: where the org is sold, creating one is a sign-up. The create call reserves the id and
// builds nothing, so the checkout overlay is the rest of the create — and a reservation left behind
// by an abandoned checkout is offered back rather than colliding with a second create. The same
// page on a self-hosted deployment must be unchanged: create, poll, done, with no billing call.
import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
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
  fetchMyReservation: vi.fn(),
  startTrial: vi.fn(),
  openCheckout: vi.fn(),
  reconcileCheckout: vi.fn(),
  startEgressSubscription: vi.fn(),
}));
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => vi.fn(),
}));

const mockAuth = { billing: false, selectOrg: vi.fn(), refresh: vi.fn() };
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => mockAuth,
}));

import { createOrg, fetchOrgStatus } from "../api/admin";
import {
  fetchMyReservation,
  openCheckout,
  reconcileCheckout,
  startEgressSubscription,
  startTrial,
} from "../api/billing";

const mockCreateOrg = vi.mocked(createOrg);
const mockFetchOrgStatus = vi.mocked(fetchOrgStatus);
const mockFetchMyReservation = vi.mocked(fetchMyReservation);
const mockStartTrial = vi.mocked(startTrial);
const mockOpenCheckout = vi.mocked(openCheckout);
const mockReconcile = vi.mocked(reconcileCheckout);
const mockStartEgress = vi.mocked(startEgressSubscription);

beforeEach(() => {
  vi.clearAllMocks();
  mockAuth.billing = false;
  mockFetchMyReservation.mockResolvedValue(null);
  mockStartTrial.mockResolvedValue("https://store.lemonsqueezy.com/checkout/x");
  mockStartEgress.mockResolvedValue("https://store.lemonsqueezy.com/checkout/egress");
});

async function fillAndSubmit() {
  const user = userEvent.setup();
  await user.type(screen.getByTestId("onboard-org-id"), "carolco");
  await user.type(screen.getByTestId("onboard-org-name"), "Carolco");
  await user.click(screen.getByTestId("onboard-org-submit"));
}

describe("org onboarding where the org is sold", () => {
  it("labels the create as a sign-up only on a commercial deployment", async () => {
    render(<OnboardOrgPage />);
    expect(screen.queryByTestId("onboard-org-signup-desc")).toBeNull();

    mockAuth.billing = true;
    render(<OnboardOrgPage />);
    await waitFor(() =>
      expect(screen.getAllByTestId("onboard-org-signup-desc").length).toBeGreaterThan(0),
    );
  });

  it("opens the checkout when the create comes back as a reservation", async () => {
    mockAuth.billing = true;
    mockCreateOrg.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "awaiting_checkout",
    });
    render(<OnboardOrgPage />);

    await fillAndSubmit();

    await waitFor(() => expect(mockOpenCheckout).toHaveBeenCalled());
    expect(mockStartTrial).toHaveBeenCalledWith("carolco", expect.any(String));
    expect(screen.getByTestId("onboard-org-checkout")).toBeTruthy();
  });

  it("reconciles a completed checkout whose webhook has not landed", async () => {
    mockAuth.billing = true;
    mockCreateOrg.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "awaiting_checkout",
    });
    // The overlay's success callback is what the customer's payment triggers.
    mockOpenCheckout.mockImplementation(async (_url, onSuccess) => {
      onSuccess();
    });
    mockFetchOrgStatus
      .mockResolvedValueOnce({
        id: "carolco",
        name: "Carolco",
        provisioning_state: "awaiting_checkout",
      })
      .mockResolvedValue({ id: "carolco", name: "Carolco", provisioning_state: "ready" });
    mockReconcile.mockResolvedValue({ reconciled: true, state: "provisioning" });
    render(<OnboardOrgPage />);

    await fillAndSubmit();

    await waitFor(() => expect(mockReconcile).toHaveBeenCalledWith("carolco"));
    await waitFor(() => expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco"), {
      timeout: 5000,
    });
  });

  it("does not reconcile when the webhook already built the org", async () => {
    mockAuth.billing = true;
    mockCreateOrg.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "awaiting_checkout",
    });
    mockOpenCheckout.mockImplementation(async (_url, onSuccess) => {
      onSuccess();
    });
    mockFetchOrgStatus.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "ready",
    });
    render(<OnboardOrgPage />);

    await fillAndSubmit();

    await waitFor(() => expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco"));
    expect(mockReconcile).not.toHaveBeenCalled();
  });

  // REQ-1482: transfer is a second subscription, so signup is a second checkout — ordered once the
  // org exists, which is when there is a billing row to bind it to.
  it("orders the transfer subscription after the org is built", async () => {
    mockAuth.billing = true;
    mockCreateOrg.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "awaiting_checkout",
    });
    mockOpenCheckout.mockImplementation(async (_url, onSuccess) => {
      onSuccess();
    });
    mockFetchOrgStatus.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "ready",
    });
    render(<OnboardOrgPage />);

    await fillAndSubmit();

    await waitFor(() =>
      expect(mockStartEgress).toHaveBeenCalledWith("carolco", expect.any(String)),
    );
    expect(mockOpenCheckout.mock.calls.at(-1)![0]).toBe(
      "https://store.lemonsqueezy.com/checkout/egress",
    );
    // The org is built and selected before the second checkout is offered.
    expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco");
  });

  it("offers to resume a checkout that was abandoned", async () => {
    mockAuth.billing = true;
    mockFetchMyReservation.mockResolvedValue({
      org_id: "carolco",
      name: "Carolco",
      expires_at: "2026-03-01T12:30:00+00:00",
    });
    render(<OnboardOrgPage />);

    const resume = await screen.findByTestId("onboard-resume-checkout");
    await userEvent.setup().click(resume);

    await waitFor(() => expect(mockStartTrial).toHaveBeenCalledWith("carolco", expect.any(String)));
    expect(mockCreateOrg).not.toHaveBeenCalled();
  });

  it("provisions immediately with no billing call on a self-hosted deployment", async () => {
    mockCreateOrg.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "ready",
    });
    render(<OnboardOrgPage />);

    await fillAndSubmit();

    await waitFor(() => expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco"));
    expect(mockStartTrial).not.toHaveBeenCalled();
    expect(mockFetchMyReservation).not.toHaveBeenCalled();
  });
});
