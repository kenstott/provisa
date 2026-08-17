// Copyright (c) 2026 Kenneth Stott
// Canary: 6c1f0a54-9d3b-4f27-9e6a-1b7c2d8e5a90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The /billing API (REQ-1455, REQ-1469). Mounted only where the commercial plugin is installed —
// `billing` on /auth/me says whether these routes exist, and nothing here should be called when
// it is false.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/** A /billing error carrying the server's stable code, which the trial copy branches on. */
export class BillingError extends Error {
  readonly status: number;
  readonly code: string | null;
  constructor(status: number, code: string | null, message: string) {
    super(message);
    this.name = "BillingError";
    this.status = status;
    this.code = code;
  }
}

async function billingFetch(path: string, init?: RequestInit): Promise<unknown> {
  const res = await fetch(`${API_BASE}/billing${path}`, init);
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    const body = data as { code?: string | null };
    throw new BillingError(
      res.status,
      body.code ?? null,
      serverMessage(data, requestFailed(`billing${path}`, res.status)),
    );
  }
  return res.json();
}

export interface TrialSummary {
  started_at: string;
  ends_at: string;
  trial_days: number;
  active_hours_used: number;
  active_hours_included: number;
  egress_bytes_used: number;
  egress_bytes_included: number;
  converted_at: string | null;
}

export interface SubscriptionSummary {
  status: string;
  renews_at: string | null;
  ends_at: string | null;
  trial_ends_at: string | null;
  card_brand: string | null;
  card_last_four: string | null;
  product_name: string | null;
  variant_name: string | null;
  fixed_cents: number | null;
  /** "minimum" — metered usage is drawn against it; "fee" — metered usage adds to it. */
  fixed_kind: "minimum" | "fee" | null;
  fixed_interval: string | null;
}

export interface BillingSummary {
  org_id: string;
  plan: string;
  subscription_status: string | null;
  entitled: boolean;
  trial: TrialSummary | null;
  period_start: string;
  period_end: string;
  active_hours: number;
  egress_bytes: number;
  subscription: SubscriptionSummary | null;
  has_portal: boolean;
  /** REQ-1482: whether the separate data transfer subscription has been ordered. */
  has_egress_subscription: boolean;
}

export async function fetchBillingSummary(orgId: string): Promise<BillingSummary> {
  return (await billingFetch(`/summary?org_id=${encodeURIComponent(orgId)}`)) as BillingSummary;
}

// REQ-1476: an org id held for a checkout that was started and not finished. Reservations exist
// only where the org is sold, so this lives on /billing with the rest of the commercial surface.
export interface OrgReservation {
  org_id: string;
  name: string;
  expires_at: string;
}

export async function fetchMyReservation(): Promise<OrgReservation | null> {
  const data = (await billingFetch("/reservation")) as { reservation: OrgReservation | null };
  return data.reservation;
}

/** Mint the Starter trial checkout. The card is captured at the merchant of record, not here. */
export async function startTrial(orgId: string, redirectUrl: string): Promise<string> {
  const data = (await billingFetch("/trial/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId, redirect_url: redirectUrl }),
  })) as { checkout_url: string };
  return data.checkout_url;
}

/**
 * Mint the checkout for the data transfer subscription (REQ-1482).
 *
 * A second subscription, because a Lemon Squeezy variant carries one usage-based price and the plan
 * variant's is the active hour. Lemon Squeezy can only start a subscription from a checkout, so the
 * customer completes a second one.
 */
export async function startEgressSubscription(orgId: string, redirectUrl: string): Promise<string> {
  const data = (await billingFetch("/egress/checkout", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId, redirect_url: redirectUrl }),
  })) as { checkout_url: string };
  return data.checkout_url;
}

declare global {
  interface Window {
    createLemonSqueezy?: () => void;
    LemonSqueezy?: {
      Setup: (opts: { eventHandler: (e: { event: string }) => void }) => void;
      Url: { Open: (url: string) => void };
    };
  }
}

const LEMON_JS = "https://assets.lemonsqueezy.com/lemon.js";

function loadLemonJs(): Promise<void> {
  if (window.LemonSqueezy) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const existing = document.querySelector<HTMLScriptElement>(`script[src="${LEMON_JS}"]`);
    const script = existing ?? document.createElement("script");
    script.addEventListener("load", () => resolve());
    script.addEventListener("error", () => reject(new Error("Could not load the checkout.")));
    if (existing) return;
    script.src = LEMON_JS;
    script.defer = true;
    document.head.appendChild(script);
  });
}

/**
 * Open a Lemon Squeezy checkout as an overlay on the current page.
 *
 * The checkout is created with `embed`, so its URL renders as an overlay driven by lemon.js rather
 * than a full page. `onSuccess` fires when the customer completes payment, which is when the
 * summary is worth refetching — the subscription itself is linked by the webhook.
 */
export async function openCheckout(url: string, onSuccess: () => void): Promise<void> {
  await loadLemonJs();
  window.createLemonSqueezy!();
  window.LemonSqueezy!.Setup({
    eventHandler: (e) => {
      if (e.event === "Checkout.Success") onSuccess();
    },
  });
  window.LemonSqueezy!.Url.Open(url);
}

/**
 * Bind a completed checkout whose webhook has not landed, and start building the org (REQ-1476).
 *
 * Called on the return from checkout when the org is still a reservation. `reconciled` is false when
 * the webhook won the race, which is the normal outcome; the caller polls the org either way.
 */
export async function reconcileCheckout(
  orgId: string,
): Promise<{ reconciled: boolean; state: string }> {
  return (await billingFetch("/checkout/reconcile", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId }),
  })) as { reconciled: boolean; state: string };
}

export async function cancelTrial(orgId: string): Promise<void> {
  await billingFetch(`/trial/cancel?org_id=${encodeURIComponent(orgId)}`, { method: "POST" });
}

/** The Lemon Squeezy customer portal — payment method, invoices and cancellation live there. */
export async function fetchPortalUrl(orgId: string): Promise<string> {
  const data = (await billingFetch(`/portal?org_id=${encodeURIComponent(orgId)}`)) as {
    portal_url: string;
  };
  return data.portal_url;
}
