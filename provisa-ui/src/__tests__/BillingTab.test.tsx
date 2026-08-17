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
    openCheckout: vi.fn(),
    startEgressSubscription: vi.fn(),
  };
});

import {
  BillingError,
  cancelTrial,
  fetchBillingSummary,
  openCheckout,
  startEgressSubscription,
  startTrial,
} from "../api/billing";
const mockSummary = vi.mocked(fetchBillingSummary);
const mockStart = vi.mocked(startTrial);
const mockCancel = vi.mocked(cancelTrial);
const mockOpen = vi.mocked(openCheckout);
const mockEgress = vi.mocked(startEgressSubscription);

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
    has_portal: false,
    has_egress_subscription: true,
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
      ends_at: null,
      trial_ends_at: null,
      card_brand: "visa",
      card_last_four: "4242",
      product_name: "Provisa",
      variant_name: "Starter",
      fixed_cents: 2500,
      fixed_kind: "minimum" as const,
      fixed_interval: "month",
    },
  };
}

describe("BillingTab", () => {
  beforeEach(() => {
    mockSummary.mockReset();
    mockStart.mockReset();
    mockCancel.mockReset();
    mockOpen.mockReset();
    mockEgress.mockReset();
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

  // REQ-1482: transfer is a second Lemon Squeezy subscription, so an org can hold a plan and be
  // metering transfer that nothing bills. This card is the only place that is offered.
  it("offers the transfer subscription to a subscribed org that has none", async () => {
    mockSummary.mockResolvedValue(summary({ ...subscribed(), has_egress_subscription: false }));
    mockEgress.mockResolvedValue("https://provisa.lemonsqueezy.com/checkout/egress");
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-add-transfer-button"));
    await waitFor(() =>
      expect(mockOpen).toHaveBeenCalledWith(
        "https://provisa.lemonsqueezy.com/checkout/egress",
        expect.any(Function),
      ),
    );
    expect(mockEgress).toHaveBeenCalledWith("acme", window.location.href);
  });

  it("does not offer the transfer subscription to an org that has one", async () => {
    mockSummary.mockResolvedValue(summary(subscribed()));
    render(<BillingTab />);
    await screen.findByTestId("billing-next-charge");
    expect(screen.queryByTestId("billing-add-transfer")).not.toBeInTheDocument();
  });

  it("states the three bounds the trial ends on", async () => {
    mockSummary.mockResolvedValue(summary());
    render(<BillingTab />);
    await screen.findByTestId("billing-start-trial");
    const bounds = screen.getAllByRole("listitem").map((li) => li.textContent);
    expect(bounds).toEqual([
      "14 days",
      "40 active hours — any hour in which you run at least one query",
      "25 GB of egress",
    ]);
  });

  it("opens the checkout the server minted as an overlay", async () => {
    mockSummary.mockResolvedValue(summary());
    mockStart.mockResolvedValue("https://provisa.lemonsqueezy.com/checkout/x");
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-start-trial"));
    await waitFor(() =>
      expect(mockOpen).toHaveBeenCalledWith(
        "https://provisa.lemonsqueezy.com/checkout/x",
        expect.any(Function),
      ),
    );
    expect(mockStart).toHaveBeenCalledWith("acme", window.location.href);
  });

  // The checkout is an overlay, so the page stays mounted through payment; the summary it is
  // showing is stale the moment the subscription lands.
  it("refetches the summary when the checkout reports success", async () => {
    mockSummary.mockResolvedValue(summary());
    mockStart.mockResolvedValue("https://provisa.lemonsqueezy.com/checkout/x");
    render(<BillingTab />);
    fireEvent.click(await screen.findByTestId("billing-start-trial"));
    await waitFor(() => expect(mockOpen).toHaveBeenCalled());
    mockSummary.mockResolvedValue(summary({ plan: "starter", entitled: true }));
    mockOpen.mock.calls[0][1]();
    await waitFor(() => expect(mockSummary).toHaveBeenCalledTimes(2));
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
        subscription_status: "cancelled",
        subscription: {
          status: "cancelled",
          renews_at: "2026-09-01T00:00:00+00:00",
          ends_at: "2026-09-01T00:00:00+00:00",
          trial_ends_at: null,
          card_brand: null,
          card_last_four: null,
          product_name: "Provisa",
          variant_name: "Starter",
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

  it("shows month-to-date usage as quantities, not as a metered total", async () => {
    mockSummary.mockResolvedValue(
      summary({ active_hours: 7, egress_bytes: 2 * 1024 ** 3, plan: "starter" }),
    );
    render(<BillingTab />);
    expect(await screen.findByText("Active hours metered: 7")).toBeInTheDocument();
    expect(screen.getByText("Egress metered: 2.00 GB")).toBeInTheDocument();
  });
});
