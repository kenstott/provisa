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
// builds nothing, so the embedded checkout is the rest of the create — and a reservation left behind
// by an abandoned checkout is offered back rather than colliding with a second create. The same
// page on a self-hosted deployment must be unchanged: create, poll, done, with no billing call.
import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, waitFor } from "../test-utils/render";
import { OnboardOrgPage } from "../pages/OnboardOrgPage";
import { signOut } from "../lib/session";

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
  mountCheckout: vi.fn(),
  reconcileCheckout: vi.fn(),
}));
vi.mock("../lib/session", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../lib/session")>()),
  signOut: vi.fn(),
}));
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => vi.fn(),
}));

const mockAuth = {
  billing: false,
  email: "ks@example.com",
  selectOrg: vi.fn(),
  refresh: vi.fn(),
  rebind: vi.fn(),
};
vi.mock("../context/AuthContext", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../context/AuthContext")>()),
  useAuth: () => mockAuth,
}));

import { createOrg, fetchOrgStatus } from "../api/admin";
import { BillingError } from "../api/billing";
import type { CheckoutSession, PlanOffer } from "../api/billing";
import {
  fetchCatalog,
  fetchMyReservation,
  mountCheckout,
  reconcileCheckout,
  startPlanCheckout,
} from "../api/billing";

const mockCreateOrg = vi.mocked(createOrg);
const mockFetchOrgStatus = vi.mocked(fetchOrgStatus);
const mockFetchMyReservation = vi.mocked(fetchMyReservation);
const mockFetchCatalog = vi.mocked(fetchCatalog);
const mockStartPlanCheckout = vi.mocked(startPlanCheckout);
const mockMount = vi.mocked(mountCheckout);
const mockReconcile = vi.mocked(reconcileCheckout);

const SESSION: CheckoutSession = {
  client_secret: "cs_test_secret",
  session_id: "cs_test_1",
  publishable_key: "pk_test_1",
  trial: true,
};

/** The page load Stripe makes after payment: the return URL with the session id it added. */
function returnFromCheckout(query: string) {
  window.history.replaceState(null, "", `/onboard?${query}`);
}

// REQ-1514: the shape the store's price objects arrive in — a monthly minimum covering some hours,
// an hourly rate past them, and the transfer terms off the plan's second price.
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
  mockFetchCatalog.mockResolvedValue({ plans: CATALOG, trial_available: true });
  mockStartPlanCheckout.mockResolvedValue(SESSION);
  mockMount.mockResolvedValue({ mount: vi.fn(), destroy: vi.fn() });
  window.history.replaceState(null, "", "/onboard");
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

  // REQ-1566: a buyer whose evaluation is spent can still order Starter, so the plan stays on the
  // page — but the fourteen free days do not, because the checkout is going to bill on day one.
  it("stops advertising the trial once this account has used it", async () => {
    mockAuth.billing = true;
    mockFetchCatalog.mockResolvedValue({ plans: CATALOG, trial_available: false });
    render(<OnboardOrgPage />);

    await waitFor(() => expect(screen.getByTestId("onboard-org-plan-starter")).toBeTruthy());
    expect(screen.queryByText(/14-day free trial/)).toBeNull();
    expect(screen.getByText(/free trial has already been used/)).toBeTruthy();
    expect(screen.getByTestId("onboard-org-signup-desc").textContent).toMatch(
      /Billing starts immediately/,
    );
  });

  it("still offers the trial to an account that has not used one", async () => {
    mockAuth.billing = true;
    render(<OnboardOrgPage />);

    await waitFor(() => expect(screen.getByText(/14-day free trial/)).toBeTruthy());
  });

  // The membership gate holds an org-less account here and the page carries no navbar, so signing
  // in with the wrong account would otherwise be a dead end.
  it("offers a way off the page for the account signed in", async () => {
    render(<OnboardOrgPage />);

    expect(screen.getByText(/signed in as ks@example.com/)).toBeTruthy();
    await userEvent.setup().click(screen.getByTestId("onboard-sign-out"));
    expect(vi.mocked(signOut)).toHaveBeenCalled();
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

    await waitFor(() =>
      expect(mockMount).toHaveBeenCalledWith(SESSION, screen.getByTestId("checkout-container")),
    );
    // The checkout is embedded in this page, so it is minted with the scheme the page is in —
    // the test app renders dark, and a checkout minted light would be a white card over it. It
    // returns here naming the org, which nothing else on the reloaded page would.
    expect(mockStartPlanCheckout).toHaveBeenCalledWith(
      "carolco",
      "starter",
      `${window.location.origin}/onboard?checkout_org=carolco`,
      { scheme: "dark", locale: expect.any(String) },
    );
    expect(screen.getByTestId("onboard-org-checkout")).toBeTruthy();
  });

  // Closing the modal without paying abandons the checkout, not the org: Stripe's iframe is torn
  // down and the page goes back to the form, offering the reservation the create left behind.
  it("returns to the form offering the reservation when the checkout is closed", async () => {
    mockAuth.billing = true;
    mockCreateOrg.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "awaiting_checkout",
    });
    const destroy = vi.fn();
    mockMount.mockResolvedValue({ mount: vi.fn(), destroy });
    mockFetchMyReservation.mockResolvedValueOnce(null).mockResolvedValue({
      org_id: "carolco",
      name: "Carolco",
      expires_at: "2026-09-01T00:00:00Z",
    });
    render(<OnboardOrgPage />);

    await fillAndSubmit();
    await waitFor(() => expect(mockMount).toHaveBeenCalled());
    await waitFor(() => expect(screen.queryByTestId("checkout-loading")).toBeNull());
    await userEvent.setup().keyboard("{Escape}");

    const resume = await screen.findByTestId("onboard-resume-checkout");
    expect(resume).toBeTruthy();
    expect(destroy).toHaveBeenCalledTimes(1);
    expect(screen.queryByTestId("onboard-org-checkout")).toBeNull();
  });

  // REQ-1476: Stripe returns to the page with the session id. Reconcile reads that session, binds
  // it when the webhook has not, and the page polls the org until it is built.
  it("reconciles the returned checkout whose webhook has not landed", async () => {
    mockAuth.billing = true;
    returnFromCheckout("checkout_org=carolco&session_id=cs_test_1");
    mockReconcile.mockResolvedValue({ reconciled: true, state: "provisioning" });
    mockFetchOrgStatus.mockResolvedValue({
      id: "carolco",
      name: "Carolco",
      provisioning_state: "ready",
    });
    render(<OnboardOrgPage />);

    expect(screen.getByTestId("onboard-org-provisioning")).toBeTruthy();
    await waitFor(() => expect(mockReconcile).toHaveBeenCalledWith("carolco", "cs_test_1"));
    await waitFor(() => expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco"), {
      timeout: 5000,
    });
    // Handled once: a reload does not carry the session id into a second reconcile.
    expect(window.location.search).toBe("");
    expect(mockReconcile).toHaveBeenCalledTimes(1);
  });

  // The session is named, so a checkout that has not completed is the answer, not a race to wait
  // out: it is reported and the form is back, holding the reservation to resume.
  it("reports a returned checkout that has not completed", async () => {
    mockAuth.billing = true;
    returnFromCheckout("checkout_org=carolco&session_id=cs_test_1");
    mockReconcile.mockRejectedValue(
      new BillingError(
        409,
        "billing.checkout_not_complete",
        "This checkout has not completed, so there is no subscription to attach yet.",
      ),
    );
    mockFetchMyReservation.mockResolvedValue({
      org_id: "carolco",
      name: "Carolco",
      expires_at: "2026-09-01T00:00:00Z",
    });
    render(<OnboardOrgPage />);

    expect(await screen.findByText(/This checkout has not completed/)).toBeTruthy();
    expect(await screen.findByTestId("onboard-resume-checkout")).toBeTruthy();
    expect(mockAuth.selectOrg).not.toHaveBeenCalled();
  });

  it("enters the org at once when the webhook already built it", async () => {
    mockAuth.billing = true;
    returnFromCheckout("checkout_org=carolco&session_id=cs_test_1");
    mockReconcile.mockResolvedValue({ reconciled: false, state: "ready" });
    render(<OnboardOrgPage />);

    await waitFor(() => expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco"));
    expect(mockFetchOrgStatus).not.toHaveBeenCalled();
  });

  // REQ-1482: data transfer is the second price on the one subscription, so the return from the
  // plan's checkout finishes the sign-up: no second checkout is opened.
  it("opens no second checkout after the org is built", async () => {
    mockAuth.billing = true;
    returnFromCheckout("checkout_org=carolco&session_id=cs_test_1");
    mockReconcile.mockResolvedValue({ reconciled: false, state: "ready" });
    render(<OnboardOrgPage />);

    await waitFor(() => expect(mockAuth.selectOrg).toHaveBeenCalledWith("carolco"));
    expect(mockStartPlanCheckout).not.toHaveBeenCalled();
    expect(mockMount).not.toHaveBeenCalled();
    expect(screen.queryByTestId("checkout-container")).toBeNull();
  });

  // The return address always names the org; one that does not cannot be reconciled, and says so.
  it("reports a return from checkout that names no organization", async () => {
    mockAuth.billing = true;
    returnFromCheckout("session_id=cs_test_1");
    render(<OnboardOrgPage />);

    expect(await screen.findByText(/did not name its organization/)).toBeTruthy();
    expect(mockReconcile).not.toHaveBeenCalled();
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
      expect(mockStartPlanCheckout).toHaveBeenCalledWith("carolco", "starter", expect.any(String), {
        scheme: "dark",
        locale: expect.any(String),
      }),
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
      expect(mockStartPlanCheckout).toHaveBeenCalledWith("carolco", "pro_m", expect.any(String), {
        scheme: "dark",
        locale: expect.any(String),
      }),
    );
    expect(mockCreateOrg.mock.calls[0][4]).toBe(true);
    // The lane is not asked twice.
    expect(screen.queryByTestId("onboard-org-isolated-engine")).toBeNull();
  });

  // REQ-1514: only Starter's checkout grants a trial, so a Pro card must not promise one.
  it("states the trial on the plan that carries it and not on the one that does not", async () => {
    mockAuth.billing = true;
    render(<OnboardOrgPage />);

    const starter = await screen.findByTestId("onboard-org-plan-starter");
    expect(starter.textContent).toContain("14-day free trial");
    expect(screen.getByTestId("onboard-org-plan-pro_m").textContent).toContain("No trial");
    // The terms stated before the checkout opens.
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
