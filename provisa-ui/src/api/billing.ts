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
import type { OrgProvisioning } from "./admin";

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

/** Stripe's subscription statuses. Only `trialing` and `active` entitle the org. */
export type SubscriptionStatus =
  | "trialing"
  | "active"
  | "past_due"
  | "unpaid"
  | "canceled"
  | "paused"
  | "incomplete"
  | "incomplete_expired";

export interface SubscriptionSummary {
  status: SubscriptionStatus;
  /** Null when the subscription is set to end at the close of this period. */
  renews_at: string | null;
  /** Set when the subscription ends at the close of this period instead of renewing. */
  cancel_at_period_end: boolean;
  ends_at: string | null;
  trial_ends_at: string | null;
  card_brand: string | null;
  card_last_four: string | null;
  product_name: string | null;
  fixed_cents: number | null;
  /** "minimum" — metered usage is drawn against it; "fee" — metered usage adds to it. */
  fixed_kind: "minimum" | "fee" | null;
  fixed_interval: string | null;
}

/** One metered line of the current bill (REQ-1469): what was posted, and what it adds. */
export interface BillLine {
  kind: "active_hours" | "egress_gb";
  /** Units already posted to the merchant of record in this billing period. */
  posted: number;
  /** Units the fixed component already covers. */
  included: number;
  /** Cents per unit past the included ones; may carry decimal places. */
  rate_cents: number;
  /** What this line adds to the fixed component, in whole cents. */
  cents: number;
}

/** The charge accrued in the subscription's current billing period, in integer cents (REQ-1469). */
export interface CurrentBill {
  period_start: string;
  period_end: string;
  fixed_cents: number;
  fixed_kind: "minimum" | "fee";
  lines: BillLine[];
  total_cents: number;
}

export interface BillingSummary {
  org_id: string;
  plan: string;
  subscription_status: SubscriptionStatus | null;
  entitled: boolean;
  trial: TrialSummary | null;
  period_start: string;
  period_end: string;
  active_hours: number;
  egress_bytes: number;
  subscription: SubscriptionSummary | null;
  /** Null with no subscription, and when a price could not be read (see `bill_unavailable`). */
  bill: CurrentBill | null;
  /** Why the bill cannot be stated. No amount is shown from a rate kept anywhere else. */
  bill_unavailable: string | null;
  /** Billed in arrears at the period's end; null unless the subscription is active or trialing. */
  next_charge: { date: string; cents: number } | null;
  /** Whether the org has a billing customer, and so a portal to open. */
  has_portal: boolean;
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

/**
 * How the buyer is reading the app, sent with every checkout so it opens dressed to match.
 *
 * The checkout is embedded in this page, so the two are on screen together: a light checkout in
 * the dark app is two designs at once. The scheme and the language travel per checkout because
 * only the browser knows them. Build it with `useCheckoutAppearance`, which resolves both from the
 * running app rather than restating them.
 */
export interface CheckoutAppearance {
  scheme: "light" | "dark";
  locale: string;
}

/** What every checkout route answers: the session the page mounts in place (REQ-1476). */
export interface CheckoutSession {
  client_secret: string;
  session_id: string;
  /** The key Stripe.js is initialised with. Public by design. */
  publishable_key: string;
  /** Whether this checkout carries the free evaluation (REQ-1566). */
  trial: boolean;
}

/** Mint the Starter trial checkout. The card is captured at the merchant of record, not here. */
export async function startTrial(
  orgId: string,
  returnUrl: string,
  appearance: CheckoutAppearance,
): Promise<CheckoutSession> {
  return (await billingFetch("/trial/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId, return_url: returnUrl, ...appearance }),
  })) as CheckoutSession;
}

/** An embedded checkout as Stripe.js hands it back: attached to one element, then destroyed. */
export interface EmbeddedCheckout {
  mount: (el: HTMLElement) => void;
  destroy: () => void;
}

interface StripeJs {
  createEmbeddedCheckoutPage: (opts: {
    fetchClientSecret: () => Promise<string>;
  }) => Promise<EmbeddedCheckout>;
}

declare global {
  interface Window {
    Stripe?: (publishableKey: string) => StripeJs;
  }
}

// Stripe requires Stripe.js to be loaded from js.stripe.com, never bundled or self-hosted. The
// version is the one the server's API calls are pinned to (dahlia), whose embedded checkout is
// created with `createEmbeddedCheckoutPage`.
export const STRIPE_JS = "https://js.stripe.com/dahlia/stripe.js";

function loadStripeJs(): Promise<(publishableKey: string) => StripeJs> {
  if (window.Stripe) return Promise.resolve(window.Stripe);
  return new Promise((resolve, reject) => {
    const existing = document.querySelector<HTMLScriptElement>(`script[src="${STRIPE_JS}"]`);
    const script = existing ?? document.createElement("script");
    script.addEventListener("load", () => {
      if (window.Stripe) resolve(window.Stripe);
      else reject(new Error("Stripe.js loaded without defining Stripe."));
    });
    script.addEventListener("error", () => reject(new Error("Could not load the checkout.")));
    if (existing) return;
    script.src = STRIPE_JS;
    document.head.appendChild(script);
  });
}

/**
 * Mount the checkout `session` names into `container` and return it for the caller to destroy.
 *
 * Payment happens entirely inside Stripe's iframe. On completion Stripe navigates this page to the
 * checkout's return URL with `session_id` added, which is where the page picks the purchase up
 * (`readCheckoutReturn`). Closing the host without paying is the caller's `destroy()`.
 */
export async function mountCheckout(
  session: CheckoutSession,
  container: HTMLElement,
): Promise<EmbeddedCheckout> {
  const stripe = (await loadStripeJs())(session.publishable_key);
  const checkout = await stripe.createEmbeddedCheckoutPage({
    fetchClientSecret: () => Promise.resolve(session.client_secret),
  });
  checkout.mount(container);
  return checkout;
}

// The return trip. The server appends `session_id` to the return URL; the onboarding page also
// needs the org the checkout was for, which no longer exists anywhere on the page after the
// navigation, so it rides the return URL as `checkout_org`.
const SESSION_PARAM = "session_id";
const ORG_PARAM = "checkout_org";

/** The current address with any previous checkout's return parameters taken off. */
function currentUrlWithoutReturn(): URL {
  const url = new URL(window.location.href);
  url.searchParams.delete(SESSION_PARAM);
  url.searchParams.delete(ORG_PARAM);
  return url;
}

/** Where a checkout opened from this page returns to; `orgId` is carried when given. */
export function checkoutReturnUrl(orgId?: string): string {
  const url = currentUrlWithoutReturn();
  if (orgId !== undefined) url.searchParams.set(ORG_PARAM, orgId);
  return url.toString();
}

export interface CheckoutReturn {
  sessionId: string;
  /** Null when the checkout was opened without one (the Billing page knows its org). */
  orgId: string | null;
}

/** The checkout this page load is the return from, or null when it is not one. */
export function readCheckoutReturn(): CheckoutReturn | null {
  const params = new URLSearchParams(window.location.search);
  const sessionId = params.get(SESSION_PARAM);
  if (!sessionId) return null;
  return { sessionId, orgId: params.get(ORG_PARAM) };
}

/** Take the return parameters off the address bar, so a reload does not handle them twice. */
export function clearCheckoutReturn(): void {
  window.history.replaceState(window.history.state, "", currentUrlWithoutReturn().toString());
}

/**
 * Bind a completed checkout whose webhook has not landed, and start building the org (REQ-1476).
 *
 * Called on the return from checkout with the `session_id` Stripe put on the return URL.
 * `reconciled` is false when the webhook won the race, which is the normal outcome; the caller
 * polls the org either way.
 */
export async function reconcileCheckout(
  orgId: string,
  sessionId: string,
): Promise<{ reconciled: boolean; state: OrgProvisioning["provisioning_state"] }> {
  return (await billingFetch("/checkout/reconcile", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId, session_id: sessionId }),
  })) as { reconciled: boolean; state: OrgProvisioning["provisioning_state"] };
}

export async function cancelTrial(orgId: string): Promise<void> {
  await billingFetch(`/trial/cancel?org_id=${encodeURIComponent(orgId)}`, { method: "POST" });
}

/**
 * The billing portal: payment method, invoices and cancellation live there. `returnUrl` is where
 * its back link goes.
 */
export async function fetchPortalUrl(orgId: string, returnUrl: string): Promise<string> {
  const data = (await billingFetch(
    `/portal?org_id=${encodeURIComponent(orgId)}&return_url=${encodeURIComponent(returnUrl)}`,
  )) as { portal_url: string };
  return data.portal_url;
}

// REQ-1509, REQ-1511: the plans an org can move to, and the move itself. Every figure here is the
// server's — the price from the merchant of record's own price object, the machine from the size
// table that provisions the engine — so nothing on the card is a constant typed into the page.

export interface PlanEngine {
  label: string;
  machine_type: string;
  vcpu: number;
  memory_gib: number;
  query_max_memory_gb: number;
}

/** What a plan bills for data transfer: the monthly allowance, then the rate past it (REQ-1482). */
export interface PlanEgress {
  included_gb: number;
  per_gb_cents: number;
}

export interface PlanOffer {
  plan: string;
  fixed_cents: number | null;
  fixed_kind: "minimum" | "fee" | null;
  fixed_interval: string | null;
  /** Cents per engine hour past `included_hours`. */
  hourly_cents: number;
  /** Engine hours the fixed minimum has already bought. */
  included_hours: number;
  egress: PlanEgress;
  /** Null on every plan the checkout gives no trial — today, all three Pro sizes. */
  trial_days: number | null;
  source_limit: number;
  lane: "shared" | "isolated";
  engine: PlanEngine | null;
}

export interface PlanOffers {
  org_id: string;
  plan: string;
  /** Whether a plan change would end a running trial and begin billing (REQ-1455). */
  on_trial: boolean;
  plans: PlanOffer[];
}

export async function fetchPlans(orgId: string): Promise<PlanOffers> {
  return (await billingFetch(`/plans?org_id=${encodeURIComponent(orgId)}`)) as PlanOffers;
}

// REQ-1514: the same offers with no org, for the signup page — the buyer picking a size has no org
// to ask about yet, and the terms are stated on the page before the checkout opens.

export interface Catalog {
  plans: PlanOffer[];
  /**
   * Whether the signed-in account is still owed a free evaluation (REQ-1566). A plan's
   * `trial_days` is what the STORE sells; this is whether this buyer may have it. False means
   * Starter is still orderable — same plan, same limits — but billed from the first invoice, so
   * the page must not advertise free days the checkout is not going to give.
   */
  trial_available: boolean;
}

export async function fetchCatalog(): Promise<Catalog> {
  return (await billingFetch("/catalog")) as Catalog;
}

/** Open the signup checkout for `plan`. The server resolves the prices; the client never names them. */
export async function startPlanCheckout(
  orgId: string,
  plan: string,
  returnUrl: string,
  appearance: CheckoutAppearance,
): Promise<CheckoutSession> {
  return (await billingFetch("/plan/checkout", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId, plan, return_url: returnUrl, ...appearance }),
  })) as CheckoutSession;
}

export interface PlanChanged {
  org_id: string;
  plan: string;
  changed: true;
  /**
   * REQ-1509: no proration. The old plan's usage to date is invoiced now with its minimum in full,
   * and the new plan's minimum is invoiced in full at the period's end.
   */
  minimums: "each_plan_in_full";
  subscription_status: string;
  /** What the org's engine now runs on, or null when the move failed (REQ-1510). */
  engine: { lane: string; moved: boolean; shard?: string | null; size?: PlanEngine | null } | null;
  /** The engine move's failure. The plan change stands regardless — the org is invoiced for it. */
  engine_error: string | null;
}

export async function changePlan(orgId: string, plan: string): Promise<PlanChanged> {
  return (await billingFetch("/plan", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ org_id: orgId, plan }),
  })) as PlanChanged;
}
