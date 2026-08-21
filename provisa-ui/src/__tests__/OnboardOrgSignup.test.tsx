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
  fetchCatalog: vi.fn(),
  startPlanCheckout: vi.fn(),
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
import { BillingError } from "../api/billing";
import type { PlanOffer } from "../api/billing";
import {
  fetchCatalog,
  fetchMyReservation,
  openCheckout,
  reconcileCheckout,
  startEgressSubscription,
  startPlanCheckout,
} from "../api/billing";

const mockCreateOrg = vi.mocked(createOrg);
const mockFetchOrgStatus = vi.mocked(fetchOrgStatus);
const mockFetchMyReservation = vi.mocked(fetchMyReservation);
const mockFetchCatalog = vi.mocked(fetchCatalog);
const mockStartPlanCheckout = vi.mocked(startPlanCheckout);
const mockOpenCheckout = vi.mocked(openCheckout);
const mockReconcile = vi.mocked(reconcileCheckout);
const mockStartEgress = vi.mocked(startEgressSubscription);

// REQ-1514: the shape the store's price objects arrive in — a monthly minimum covering some hours,
// an hourly rate past them, and the transfer terms off the plan's second variant.
const CATALOG: PlanOffer[] = [
  {
    plan: "starter",
    fixed_cents: 2500,
    fixed_kind: "minimum",
    fixed_interval: "month",
    hourly_cents: 130,
    included_hours: 19,
    egress: { included_gb: 25, per_gb_cents: 48 },
    trial_days: 14,
    source_limit: 10,
    lane: "shared",
    engine: null,
  },
  {
    plan: "pro_m",
    fixed_cents: 19900,
    fixed_kind: "minimum",
    fixed_interval: "month",
    hourly_cents: 275,
    included_hours: 72,
    egress: { included_gb: 100, per_gb_cents: 48 },
    trial_days: null,
    source_limit: 100,
    lane: "isolated",
    engine: {
      label: "Pro M",
      machine_type: "n2-highmem-8",
      vcpu: 8,
      memory_gib: 64,
      query_max_memory_gb: 18,
    },
  },
];

beforeEach(() => {
  vi.clearAllMocks();
  mockAuth.billing = false;
  mockFetchMyReservation.mockResolvedValue(null);
  mockFetchCatalog.mockResolvedValue(CATALOG);
  mockStartPlanCheckout.mockResolvedValue("https://store.lemonsqueezy.com/checkout/x");
  mockStartEgress.mockResolvedValue("https://store.lemonsqueezy.com/checkout/egress");
});

async function fillAndSubmit() {
  const user = userEvent.setup();
  await user.type(screen.getByTestId("onboard-org-id"), "carolco");
  await user.type(screen.getByTestId("onboard-org-name"), "Carolco");
  // Where plans are sold the submit waits on the catalog: the plan decides the rate and the
  // machine, so there is nothing to order until one has arrived and been picked.
  await waitFor(() =>
    expect((screen.getByTestId("onboard-org-submit") as HTMLButtonElement).disabled).toBe(false),
  );
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
    expect(mockStartPlanCheckout).toHaveBeenCalledWith("carolco", "starter", expect.any(String));
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

    // The first reconcile is a poll tick behind the status read that found the reservation.
    await waitFor(() => expect(mockReconcile).toHaveBeenCalledWith("carolco"), { timeout: 5000 });
    await waitFor(() => expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco"), {
      timeout: 5000,
    });
  });

  it("keeps waiting when the purchase has not reached the store yet", async () => {
    // The store publishes the subscription seconds after the overlay closes, so the first reconcile
    // finds nothing. That is the race, not the answer: the webhook lands on a later tick.
    mockAuth.billing = true;
    mockCreateOrg.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "awaiting_checkout",
    });
    mockOpenCheckout.mockImplementation(async (_url, onSuccess) => {
      onSuccess();
    });
    mockFetchOrgStatus
      .mockResolvedValueOnce({
        id: "carolco",
        name: "Carolco",
        provisioning_state: "awaiting_checkout",
      })
      .mockResolvedValueOnce({
        id: "carolco",
        name: "Carolco",
        provisioning_state: "awaiting_checkout",
      })
      .mockResolvedValue({ id: "carolco", name: "Carolco", provisioning_state: "ready" });
    mockReconcile.mockRejectedValueOnce(
      new BillingError(404, "billing.no_subscription_found", "not yet"),
    );
    mockReconcile.mockResolvedValue({ reconciled: false, state: "ready" });
    render(<OnboardOrgPage />);

    await fillAndSubmit();

    await waitFor(() => expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco"), {
      timeout: 10000,
    });
    expect(screen.queryByTestId("onboard-org-submit")).toBeNull();
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

  // The create runs before any checkout exists, so the copy on screen while it runs cannot assert a
  // payment — the card has not even been asked for yet.
  it("does not claim payment while the org is still being created", async () => {
    mockAuth.billing = true;
    let releaseCreate: (() => void) | null = null;
    mockCreateOrg.mockImplementation(
      () =>
        new Promise((resolve) => {
          releaseCreate = () =>
            resolve({ id: "carolco", name: "Carolco", provisioning_state: "awaiting_checkout" });
        }),
    );
    render(<OnboardOrgPage />);

    await fillAndSubmit();

    const pending = await screen.findByTestId("onboard-org-provisioning");
    expect(pending.textContent).not.toContain("Payment received");
    releaseCreate!();
    await waitFor(() => expect(mockStartPlanCheckout).toHaveBeenCalled());
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

    await waitFor(() =>
      expect(mockStartPlanCheckout).toHaveBeenCalledWith("carolco", "starter", expect.any(String)),
    );
    expect(mockCreateOrg).not.toHaveBeenCalled();
  });

  // REQ-1514: the picked plan is what is ordered, and REQ-1510 makes the lane the plan's — a Pro
  // size is a dedicated engine whether or not anyone ticked a box.
  it("orders the plan the buyer picked, on that plan's lane", async () => {
    mockAuth.billing = true;
    mockCreateOrg.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "awaiting_checkout",
    });
    render(<OnboardOrgPage />);
    const user = userEvent.setup();

    await user.click(await screen.findByLabelText(/Pro M/));
    await fillAndSubmit();

    await waitFor(() =>
      expect(mockStartPlanCheckout).toHaveBeenCalledWith("carolco", "pro_m", expect.any(String)),
    );
    expect(mockCreateOrg.mock.calls[0][4]).toBe(true);
    // The lane is not asked twice.
    expect(screen.queryByTestId("onboard-org-isolated-engine")).toBeNull();
  });

  // REQ-1514: only Starter's variant grants a trial, so a Pro card must not promise one.
  it("states the trial on the plan that carries it and not on the one that does not", async () => {
    mockAuth.billing = true;
    render(<OnboardOrgPage />);

    const starter = await screen.findByTestId("onboard-org-plan-starter");
    expect(starter.textContent).toContain("14-day free trial");
    expect(screen.getByTestId("onboard-org-plan-pro_m").textContent).toContain("No trial");
    // The terms the checkout overlay hides.
    expect(starter.textContent).toContain("$1.30 per engine hour");
    expect(starter.textContent).toContain("$25.00/month minimum, covering 19 engine hours");
    expect(starter.textContent).toContain("25 GB");
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
    expect(mockStartPlanCheckout).not.toHaveBeenCalled();
    expect(mockFetchCatalog).not.toHaveBeenCalled();
    expect(mockFetchMyReservation).not.toHaveBeenCalled();
  });
});
