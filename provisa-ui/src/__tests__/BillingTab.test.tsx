// Copyright (c) 2026 Kenneth Stott
// Canary: 3a7e2c19-5b40-4d86-9f13-8c2d604ba7e5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "../test-utils/render";
import { BillingTab } from "../components/admin/BillingTab";
import type { BillingSummary } from "../api/billing";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ activeOrgId: "acme", billing: true }),
}));

vi.mock("../api/billing", async () => {
  const actual = await vi.importActual<typeof import("../api/billing")>("../api/billing");
  return {
    BillingError: actual.BillingError,
    fetchBillingSummary: vi.fn(),
    startTrial: vi.fn(),
    cancelTrial: vi.fn(),
    fetchPortalUrl: vi.fn(),
    mountCheckout: vi.fn(),
    checkoutReturnUrl: actual.checkoutReturnUrl,
    readCheckoutReturn: actual.readCheckoutReturn,
    clearCheckoutReturn: actual.clearCheckoutReturn,
    fetchPlans: vi.fn(),
    changePlan: vi.fn(),
  };
});

import {
  BillingError,
  cancelTrial,
  changePlan,
  fetchBillingSummary,
  fetchPlans,
  fetchPortalUrl,
  mountCheckout,
  startTrial,
} from "../api/billing";
import type { CheckoutSession, PlanOffers } from "../api/billing";
const mockSummary = vi.mocked(fetchBillingSummary);
const mockStart = vi.mocked(startTrial);
const mockCancel = vi.mocked(cancelTrial);
const mockMount = vi.mocked(mountCheckout);
const mockPortal = vi.mocked(fetchPortalUrl);

/** What /billing/trial/start answers: the session the page mounts in place. */
const SESSION: CheckoutSession = {
  client_secret: "cs_test_secret",
  session_id: "cs_test_1",
  publishable_key: "pk_test_1",
  trial: true,
};
const mockPlans = vi.mocked(fetchPlans);
const mockChange = vi.mocked(changePlan);

/** The four plan offers the server prices and orders (REQ-1509/REQ-1511). */
function offers(overrides: Partial<PlanOffers> = {}): PlanOffers {
  return {
    org_id: "acme",
    plan: "starter",
    on_trial: false,
    plans: [
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
        plan: "pro_s",
        fixed_cents: 50000,
        fixed_kind: "minimum",
        fixed_interval: "month",
        hourly_cents: 150,
        included_hours: 66,
        egress: { included_gb: 50, per_gb_cents: 48 },
        trial_days: null,
        source_limit: 100,
        lane: "isolated",
        engine: {
          label: "Pro S",
          machine_type: "n2-highmem-4",
          vcpu: 4,
          memory_gib: 32,
          query_max_memory_gb: 20,
        },
      },
      {
        plan: "pro_m",
        fixed_cents: 90000,
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
          query_max_memory_gb: 40,
        },
      },
      {
        plan: "pro_l",
        fixed_cents: 170000,
        fixed_kind: "minimum",
        fixed_interval: "month",
        hourly_cents: 550,
        included_hours: 72,
        egress: { included_gb: 200, per_gb_cents: 48 },
        trial_days: null,
        source_limit: 100,
        lane: "isolated",
        engine: {
          label: "Pro L",
          machine_type: "n2-highmem-16",
          vcpu: 16,
          memory_gib: 128,
          query_max_memory_gb: 80,
        },
      },
    ],
    ...overrides,
  };
}

function summary(overrides: Partial<BillingSummary> = {}): BillingSummary {
  return {
    org_id: "acme",
    plan: "free",
    subscription_status: null,
    entitled: false,
    trial: null,
    period_start: "2026-08-01T00:00:00+00:00",
    period_end: "2026-08-17T00:00:00+00:00",
    active_hours: 0,
    egress_bytes: 0,
    subscription: null,
    bill: null,
    bill_unavailable: null,
    next_charge: null,
    has_portal: false,
    ...overrides,
  };
}

/** An org on a live paid Starter subscription. */
function subscribed(): Partial<BillingSummary> {
  return {
    subscription_status: "active",
    entitled: true,
    subscription: {
      status: "active",
      renews_at: "2026-09-01T00:00:00+00:00",
      cancel_at_period_end: false,
      ends_at: null,
      trial_ends_at: null,
      card_brand: "visa",
      card_last_four: "4242",
      product_name: "Provisa Starter",
      fixed_cents: 2500,
      fixed_kind: "minimum" as const,
      fixed_interval: "month",
    },
  };
}

const realLocation = window.location;

describe("BillingTab", () => {
  beforeEach(() => {
    mockSummary.mockReset();
    mockStart.mockReset();
    mockCancel.mockReset();
    mockMount.mockReset();
    mockPortal.mockReset();
    window.history.replaceState(null, "", "/");
    mockPlans.mockReset();
    mockChange.mockReset();
    mockPlans.mockResolvedValue(offers());
  });

  // REQ-1455: the org with no subscription and no trial is the one the trial exists for. Its
  // absence here is what left a new account with no way to start an evaluation at all.
  it("offers the trial to an org that has neither a trial nor a subscription", async () => {
    mockSummary.mockResolvedValue(summary());
    render(<BillingTab />);
    expect(await screen.findByTestId("billing-start-trial")).toBeInTheDocument();
  });

  it("does not offer the trial to an org that already has a subscription", async () => {
    mockSummary.mockResolvedValue(summary(subscribed()));
    render(<BillingTab />);
    await screen.findByTestId("billing-next-charge");
    expect(screen.queryByTestId("billing-start-trial")).not.toBeInTheDocument();
  });

  // REQ-1482: data transfer is the second price on the one subscription, so a subscribed org is
  // never offered a second checkout for it.
  it("offers no second checkout for data transfer", async () => {
    mockSummary.mockResolvedValue(summary(subscribed()));
    render(<BillingTab />);
    await screen.findByTestId("billing-next-charge");
    expect(screen.queryByTestId("billing-add-transfer")).not.toBeInTheDocument();
    expect(screen.queryByText(/Add data transfer/)).not.toBeInTheDocument();
  });

  // REQ-1469: the portal is told where its back link goes, which is this page.
  it("opens the portal with this page as its return address", async () => {
    mockSummary.mockResolvedValue(summary({ ...subscribed(), has_portal: true }));
    mockPortal.mockResolvedValue("https://billing.stripe.com/p/session/test");
    const href = vi.fn();
    const here = "http://localhost:3000/admin/billing";
    Object.defineProperty(window, "location", {
      configurable: true,
      value: {
        ...window.location,
        set href(v: string) {
          href(v);
        },
        get href() {
          return here;
        },
      },
    });
    try {
      render(<BillingTab />);
      fireEvent.click(await screen.findByTestId("billing-portal"));
      await waitFor(() =>
        expect(href).toHaveBeenCalledWith("https://billing.stripe.com/p/session/test"),
      );
      expect(mockPortal).toHaveBeenCalledWith("acme", here);
    } finally {
      Object.defineProperty(window, "location", { configurable: true, value: realLocation });
    }
  });

  it("states the three bounds the trial ends on", async () => {
    mockSummary.mockResolvedValue(summary());
    render(<BillingTab />);
    await screen.findByTestId("billing-start-trial");
    const bounds = screen.getAllByRole("listitem").map((li) => li.textContent);
    expect(bounds).toEqual([
      "14 days",
      "40 active hours (any hour in which you run at least one query)",
      "25 GB of egress",
    ]);
  });

  it("mounts the checkout the server minted in the modal", async () => {
    mockSummary.mockResolvedValue(summary());
    mockStart.mockResolvedValue(SESSION);
    mockMount.mockResolvedValue({ mount: vi.fn(), destroy: vi.fn() });
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-start-trial"));
    await waitFor(() =>
      expect(mockMount).toHaveBeenCalledWith(SESSION, screen.getByTestId("checkout-container")),
    );
    // The checkout is minted in the scheme this page is being read in; the test app renders dark.
    // It returns to this page, which is where the purchase is picked up.
    expect(mockStart).toHaveBeenCalledWith("acme", window.location.href, {
      scheme: "dark",
      locale: expect.any(String),
    });
  });

  // Closing the modal without paying is an abandoned checkout: Stripe's iframe is destroyed and the
  // page is left as it was, with the trial still on offer.
  it("destroys the checkout when the modal is closed without paying", async () => {
    mockSummary.mockResolvedValue(summary());
    mockStart.mockResolvedValue(SESSION);
    const destroy = vi.fn();
    mockMount.mockResolvedValue({ mount: vi.fn(), destroy });
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-start-trial"));
    await waitFor(() => expect(mockMount).toHaveBeenCalled());
    await waitFor(() => expect(screen.queryByTestId("checkout-loading")).not.toBeInTheDocument());
    fireEvent.keyDown(screen.getByTestId("checkout-container"), { key: "Escape" });
    await waitFor(() => expect(destroy).toHaveBeenCalledTimes(1));
    await waitFor(() =>
      expect(screen.queryByTestId("checkout-container")).not.toBeInTheDocument(),
    );
    expect(screen.getByTestId("billing-start-trial")).toBeEnabled();
  });

  it("shows why the checkout could not be shown", async () => {
    mockSummary.mockResolvedValue(summary());
    mockStart.mockResolvedValue(SESSION);
    mockMount.mockRejectedValue(new Error("Could not load the checkout."));
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-start-trial"));
    expect(await screen.findByTestId("checkout-error")).toHaveTextContent(
      "Could not load the checkout.",
    );
  });

  // REQ-1476: after payment Stripe navigates back to this page with the session id. The summary
  // is read afresh and the id is taken off the address so a reload does not carry it.
  it("refetches the summary on the return from checkout and clears the session id", async () => {
    window.history.replaceState(null, "", "/admin/billing?tab=billing&session_id=cs_test_1");
    mockSummary.mockResolvedValue(summary({ plan: "starter", entitled: true }));
    render(<BillingTab />);
    await waitFor(() => expect(mockSummary).toHaveBeenCalledWith("acme"));
    expect(window.location.search).toBe("?tab=billing");
    expect(window.location.pathname).toBe("/admin/billing");
  });

  // REQ-1474: the trial is once per person. The server's message names both readings of the
  // refusal and the address that resets it, so the page shows that message rather than a generic
  // failure that leaves the customer with nothing to do.
  it("shows the server's refusal when the trial was already used", async () => {
    mockSummary.mockResolvedValue(summary());
    mockStart.mockRejectedValue(
      new BillingError(
        409,
        "billing.trial_already_used",
        "This identity has already used its free trial. Contact billing@provisa.dev.",
      ),
    );
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-start-trial"));
    expect(await screen.findByTestId("billing-error")).toHaveTextContent(
      "already used its free trial",
    );
  });

  it("shows trial usage against both metered bounds", async () => {
    mockSummary.mockResolvedValue(
      summary({
        trial: {
          started_at: "2026-08-10T00:00:00+00:00",
          ends_at: "2026-08-24T00:00:00+00:00",
          trial_days: 14,
          active_hours_used: 12,
          active_hours_included: 40,
          egress_bytes_used: 5 * 1024 ** 3,
          egress_bytes_included: 25 * 1024 ** 3,
          converted_at: null,
        },
      }),
    );
    render(<BillingTab />);
    const card = await screen.findByTestId("billing-trial");
    expect(card).toHaveTextContent("Active hours: 12 of 40");
    expect(card).toHaveTextContent("5.00 GB of 25.00 GB");
  });

  it("reloads after the trial is cancelled", async () => {
    mockSummary.mockResolvedValue(
      summary({
        trial: {
          started_at: "2026-08-10T00:00:00+00:00",
          ends_at: "2026-08-24T00:00:00+00:00",
          trial_days: 14,
          active_hours_used: 1,
          active_hours_included: 40,
          egress_bytes_used: 0,
          egress_bytes_included: 25 * 1024 ** 3,
          converted_at: null,
        },
      }),
    );
    mockCancel.mockResolvedValue(undefined);
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-cancel-trial"));
    await waitFor(() => expect(mockCancel).toHaveBeenCalledWith("acme"));
    await waitFor(() => expect(mockSummary).toHaveBeenCalledTimes(2));
  });

  // REQ-1469: a cancelled subscription says so. Printing a renewal date for a charge that will
  // never be taken is the failure the requirement names.
  it("states no further charge for a cancelled subscription", async () => {
    mockSummary.mockResolvedValue(
      summary({
        subscription_status: "canceled",
        subscription: {
          status: "canceled",
          renews_at: null,
          cancel_at_period_end: false,
          ends_at: "2026-09-01T00:00:00+00:00",
          trial_ends_at: null,
          card_brand: null,
          card_last_four: null,
          product_name: "Provisa Starter",
          fixed_cents: 2500,
          fixed_kind: "minimum" as const,
          fixed_interval: "month",
        },
      }),
    );
    render(<BillingTab />);
    const card = await screen.findByTestId("billing-next-charge");
    expect(card).toHaveTextContent("There is no further charge");
    expect(card).not.toHaveTextContent("plus metered usage");
  });

  // A subscription set to end at the period's close says when, and a trial already set to end
  // offers no second cancel.
  it("states the end of a subscription set to cancel at the period's end", async () => {
    const live = subscribed();
    mockSummary.mockResolvedValue(
      summary({
        ...live,
        subscription_status: "trialing",
        trial: {
          started_at: "2026-08-10T00:00:00+00:00",
          ends_at: "2026-08-24T00:00:00+00:00",
          trial_days: 14,
          active_hours_used: 1,
          active_hours_included: 40,
          egress_bytes_used: 0,
          egress_bytes_included: 25 * 1024 ** 3,
          converted_at: null,
        },
        subscription: {
          ...live.subscription!,
          status: "trialing",
          renews_at: null,
          cancel_at_period_end: true,
          ends_at: "2026-08-24T00:00:00+00:00",
          trial_ends_at: "2026-08-24T00:00:00+00:00",
        },
      }),
    );
    render(<BillingTab />);
    expect(await screen.findByTestId("billing-ends-at-period-end")).toHaveTextContent(
      "This subscription ends Aug 24, 2026 and does not renew.",
    );
    expect(screen.getByTestId("billing-trial")).toBeInTheDocument();
    expect(screen.queryByTestId("billing-cancel-trial")).not.toBeInTheDocument();
  });

  it("asks for a new payment method when Stripe could not collect", async () => {
    const live = subscribed();
    mockSummary.mockResolvedValue(
      summary({
        ...live,
        subscription_status: "unpaid",
        subscription: { ...live.subscription!, status: "unpaid" },
      }),
    );
    render(<BillingTab />);
    expect(await screen.findByTestId("billing-next-charge")).toHaveTextContent(
      "The last payment failed",
    );
  });

  // REQ-1469: the metered half of the bill and the next charge are money, and every amount is
  // the server's, priced by the merchant of record from the usage already posted to it.
  it("shows the current bill and the next charge as money", async () => {
    mockSummary.mockResolvedValue(
      summary({
        ...subscribed(),
        bill: {
          period_start: "2026-08-15T00:00:00+00:00",
          period_end: "2026-09-15T00:00:00+00:00",
          fixed_cents: 2500,
          fixed_kind: "minimum",
          lines: [
            { kind: "active_hours", posted: 30, included: 19, rate_cents: 130, cents: 1430 },
            { kind: "egress_gb", posted: 40, included: 25, rate_cents: 48, cents: 720 },
          ],
          total_cents: 4650,
        },
        next_charge: { date: "2026-09-15T00:00:00+00:00", cents: 4650 },
      }),
    );
    render(<BillingTab />);
    const bill = await screen.findByTestId("billing-current-bill");
    expect(bill).toHaveTextContent("Plan minimum: $25.00");
    expect(screen.getByTestId("billing-line-active_hours")).toHaveTextContent(
      "Active hours: 30 posted, 19 included, $1.30 each beyond that: $14.30",
    );
    expect(screen.getByTestId("billing-line-egress_gb")).toHaveTextContent(
      "Data transfer: 40 GB posted, 25 included, $0.48 each beyond that: $7.20",
    );
    expect(screen.getByTestId("billing-bill-total")).toHaveTextContent("$46.50");
    expect(screen.getByTestId("billing-next-charge-amount")).toHaveTextContent("$46.50");
  });

  it("says why when the amount cannot be stated, and shows no amount", async () => {
    mockSummary.mockResolvedValue(
      summary({
        ...subscribed(),
        bill_unavailable: "the subscription carries no data transfer price",
      }),
    );
    render(<BillingTab />);
    const notice = await screen.findByTestId("billing-bill-unavailable");
    expect(notice).toHaveTextContent("the subscription carries no data transfer price");
    expect(screen.queryByTestId("billing-current-bill")).not.toBeInTheDocument();
    expect(screen.queryByTestId("billing-next-charge-amount")).not.toBeInTheDocument();
    expect(screen.getByTestId("billing-next-charge")).toHaveTextContent(
      "The amount cannot be stated",
    );
  });

  // REQ-1469, REQ-1511: the trial card quotes the server's Starter prices, never rates of its own.
  it("quotes the trial's conversion terms from the server's Starter prices", async () => {
    const priced = offers();
    priced.plans[0] = {
      ...priced.plans[0],
      fixed_cents: 3100,
      hourly_cents: 145,
      egress: { included_gb: 30, per_gb_cents: 52.5 },
    };
    mockPlans.mockResolvedValue(priced);
    mockSummary.mockResolvedValue(summary());
    render(<BillingTab />);
    const terms = await screen.findByTestId("billing-trial-terms");
    await waitFor(() =>
      expect(terms).toHaveTextContent(
        "Starter is $1.45 per active hour against a minimum of $31.00 per month, with 30 GB of egress included and $0.525 per GB beyond it.",
      ),
    );
  });

  it("quotes no rate when the prices cannot be read", async () => {
    mockPlans.mockRejectedValue(new Error("price price_starter carries no unit amount"));
    mockSummary.mockResolvedValue(summary());
    render(<BillingTab />);
    const notice = await screen.findByTestId("billing-prices-unavailable");
    expect(notice).toHaveTextContent("carries no unit amount");
    expect(screen.getByTestId("billing-trial-terms")).not.toHaveTextContent("per active hour");
  });

  // A billing date is a calendar day sent as midnight UTC: west of UTC its local date is the day
  // before, so the page reads the day in UTC.
  it("shows a billing date as its calendar day in any time zone", async () => {
    const zone = process.env.TZ;
    process.env.TZ = "America/Los_Angeles";
    try {
      mockSummary.mockResolvedValue(
        summary({
          period_start: "2026-10-01T00:00:00+00:00",
          period_end: "2026-11-01T00:00:00+00:00",
        }),
      );
      render(<BillingTab />);
      expect(await screen.findByText("Oct 1, 2026 to Nov 1, 2026")).toBeInTheDocument();
    } finally {
      process.env.TZ = zone;
    }
  });

  it("shows month-to-date usage as quantities, not as a metered total", async () => {
    mockSummary.mockResolvedValue(
      summary({ active_hours: 7, egress_bytes: 2 * 1024 ** 3, plan: "starter" }),
    );
    render(<BillingTab />);
    expect(await screen.findByText("Active hours metered: 7")).toBeInTheDocument();
    expect(screen.getByText("Egress metered: 2.00 GB")).toBeInTheDocument();
  });

  // REQ-1511: the plans are the server's — priced, ordered and marked by it — so the card grid
  // shows what it sent, with the org's own plan marked and not offered as a change to itself.
  it("shows the server's plans and marks the current one", async () => {
    mockSummary.mockResolvedValue(summary(subscribed()));
    render(<BillingTab />);
    expect(await screen.findByTestId("billing-plan-starter")).toBeInTheDocument();
    expect(screen.getByTestId("billing-plan-pro_l")).toHaveTextContent("n2-highmem-16");
    expect(screen.getByTestId("billing-plan-current-starter")).toBeInTheDocument();
    expect(screen.queryByTestId("billing-choose-starter")).not.toBeInTheDocument();
    expect(screen.getByTestId("billing-choose-pro_m")).toBeInTheDocument();
  });

  // REQ-1509: no proration either way. Moving up and moving down both bill each plan's minimum in
  // full: the old one with its usage to date now, the new one at the period's end.
  it("says both plans' minimums are charged in full, up or down", async () => {
    mockSummary.mockResolvedValue(summary({ ...subscribed(), plan: "pro_m" }));
    mockPlans.mockResolvedValue(offers({ plan: "pro_m" }));
    const rule =
      "There is no proration. Your current plan's usage to date is invoiced now with its full minimum, and the new plan's full minimum is invoiced at the end of this period.";
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-choose-pro_l"));
    expect(await screen.findByTestId("billing-plan-confirm-minimums")).toHaveTextContent(rule);
    fireEvent.click(screen.getByText("Cancel"));
    fireEvent.click(await screen.findByTestId("billing-choose-starter"));
    expect(await screen.findByTestId("billing-plan-confirm-minimums")).toHaveTextContent(rule);
    expect(screen.queryByText(/credit/i)).not.toBeInTheDocument();
  });

  it("reports the dedicated engine the change moved the org onto", async () => {
    mockSummary.mockResolvedValue(summary(subscribed()));
    mockChange.mockResolvedValue({
      org_id: "acme",
      plan: "pro_m",
      changed: true,
      minimums: "each_plan_in_full",
      subscription_status: "active",
      engine: {
        lane: "isolated",
        moved: true,
        shard: "org_acme",
        size: {
          label: "Pro M",
          machine_type: "n2-highmem-8",
          vcpu: 8,
          memory_gib: 64,
          query_max_memory_gb: 40,
        },
      },
      engine_error: null,
    });
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-choose-pro_m"));
    fireEvent.click(await screen.findByTestId("billing-plan-confirm"));
    const changed = await screen.findByTestId("billing-plan-changed");
    expect(changed).toHaveTextContent("n2-highmem-8");
    expect(screen.getByTestId("billing-plan-minimums")).toHaveTextContent(
      "The previous plan's usage to date has been invoiced with its full minimum",
    );
    await waitFor(() => expect(mockChange).toHaveBeenCalledWith("acme", "pro_m"));
  });

  // REQ-1509: the plan changed even though the engine move did not. Reporting success alone would
  // leave the administrator believing they are running on hardware they are not.
  it("reports a plan change whose engine move failed", async () => {
    mockSummary.mockResolvedValue(summary(subscribed()));
    mockChange.mockResolvedValue({
      org_id: "acme",
      plan: "pro_s",
      changed: true,
      minimums: "each_plan_in_full",
      subscription_status: "active",
      engine: null,
      engine_error: "the cluster refused the shard",
    });
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-choose-pro_s"));
    fireEvent.click(await screen.findByTestId("billing-plan-confirm"));
    expect(await screen.findByTestId("billing-plan-changed")).toHaveTextContent(
      "the cluster refused the shard",
    );
  });

  // REQ-1509: the source count that refuses a Starter downgrade is the server's at the moment of
  // the change, so its sentence is shown verbatim rather than restated from a stale count here.
  it("shows the server's refusal verbatim", async () => {
    mockSummary.mockResolvedValue(summary({ ...subscribed(), plan: "pro_m" }));
    mockPlans.mockResolvedValue(offers({ plan: "pro_m" }));
    mockChange.mockRejectedValue(
      new BillingError(
        409,
        "billing.plan_sources_exceed_limit",
        "Starter allows 10 data sources; this organization has 24.",
      ),
    );
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-choose-starter"));
    fireEvent.click(await screen.findByTestId("billing-plan-confirm"));
    expect(await screen.findByTestId("billing-plan-refused")).toHaveTextContent(
      "this organization has 24",
    );
  });

  it("warns that changing plan ends a running trial", async () => {
    const trialing = subscribed();
    mockSummary.mockResolvedValue(
      summary({
        ...trialing,
        subscription: {
          ...trialing.subscription!,
          status: "trialing",
          trial_ends_at: "2026-09-01T00:00:00+00:00",
        },
      }),
    );
    mockPlans.mockResolvedValue(offers({ plan: "starter", on_trial: true }));
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-choose-pro_s"));
    expect(await screen.findByText(/ends your trial now/)).toBeInTheDocument();
  });

  // REQ-1509: a subscription that does not carry both of this deployment's prices cannot be moved.
  // The refusal is shown and nothing is reported as changed.
  it("reports a subscription it cannot move and claims no change", async () => {
    mockSummary.mockResolvedValue(summary(subscribed()));
    mockChange.mockRejectedValue(
      new BillingError(
        502,
        "billing.subscription_items_unrecognized",
        "Subscription sub_1 does not carry both a plan price and a data transfer price this deployment sells, so it cannot be moved.",
      ),
    );
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-choose-pro_s"));
    fireEvent.click(await screen.findByTestId("billing-plan-confirm"));
    expect(await screen.findByTestId("billing-plan-refused")).toHaveTextContent(
      "cannot be moved",
    );
    expect(screen.queryByTestId("billing-plan-changed")).not.toBeInTheDocument();
  });
});
