// Copyright (c) 2026 Kenneth Stott
// Canary: 3b6d0e71-52a4-4c8f-9e1d-7a2f5c9b8e40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 OFFERS: a data product's offers, the contracts each gives on each dataset, the bids for
// them, and the product's sanitized profiles -- the client of provisa/api/admin/
// data_product_offers_router.py. Shapes are the server's (snake_case), never re-derived here.

import { serverMessage, requestFailed } from "../i18n/serverMessage";
import type { ProfileRun, ProfileRunResults } from "./profiler";
import type { ContractVersion } from "../types/admin";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export type DataLevel = "full" | "sample" | "synthetic";

/** A dataset's column as an offer's role sees it; ``masked`` is the mask's kind, null unmasked. */
export interface OfferField {
  name: string;
  type: string;
  description: string | null;
  masked: string | null;
}

/** A data quality promise as a contract states it. */
export interface OfferCheck {
  name: string;
  column: string | null;
  severity: "fail" | "warn";
  definition: string;
}

/** What one offer gives on one dataset. */
export interface OfferTerms {
  fields: OfferField[];
  row_rules: string[];
  data_level: DataLevel;
  sample_views: string[];
  checks: OfferCheck[];
  dq_results: boolean;
  profiles: string[];
  duration_days: number | null;
}

export interface OfferContract {
  version: string;
  terms: OfferTerms;
  created_at: string;
  /** Every saved version, oldest first, the last this one. */
  versions?: ContractVersion[];
}

/** One dataset's contract under an offer. */
export interface OfferDatasetContract {
  table_id: number;
  table: string;
  contract: OfferContract;
}

/** What a product owner sends to add or change an offer. */
export interface OfferInput {
  name: string;
  role_id: string;
  data_level: DataLevel;
  sample_views: number[];
  dq_results: boolean;
  profiles: boolean;
  open: boolean;
  duration_days: number | null;
}

/** An offer as listed: its terms, its contracts, what accepting it would extend for the caller,
 *  and whether it is open public data to query now. */
export interface Offer extends OfferInput {
  id: number;
  product_id: string;
  version: number;
  published_by: string | null;
  /** Why it is listed for review; null when it is not. */
  review: string | null;
  created_at: string;
  updated_at: string;
  contracts: OfferDatasetContract[];
  summary: { extension: string | null };
  query_now: boolean;
}

export type BidStatus = "pending" | "accepted" | "declined" | "withdrawn" | "cancelled";

/** A bid for an offer, or the cancel of an accepted one, and its outcome. */
export interface Bid {
  id: number;
  request_id: string;
  kind: "bid" | "cancel";
  product_id: string;
  offer_id: number | null;
  offer_version: number;
  terms: Partial<OfferInput>;
  requester: string;
  reason: string;
  status: BidStatus;
  decided_by: string | null;
  decided_at: string | null;
  decline_reason: string | null;
  created_at: string;
}

/** A dataset of the product with its latest profile, sanitized on the server for the viewer. */
export interface ProductProfile {
  table_id: number;
  table: string;
  run: ProfileRun | null;
  results: ProfileRunResults | null;
}

async function call<T>(
  op: string,
  path: string,
  method: "GET" | "POST" | "PUT" | "DELETE" = "GET",
  body?: unknown,
  headers: Record<string, string> = {},
): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    method,
    headers: body === undefined ? headers : { ...headers, "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, resp.status)));
  }
  return resp.json() as Promise<T>;
}

const base = (productId: string) => `/admin/data-products/${encodeURIComponent(productId)}`;

export const fetchOffers = (productId: string) =>
  call<{ offers: Offer[] }>("fetchOffers", `${base(productId)}/offers`).then((r) => r.offers);

/** The roles the caller may put in an offer: those they may grant. */
export const fetchGrantableRoles = (productId: string) =>
  call<string[]>("fetchGrantableRoles", `${base(productId)}/grantable-roles`);

export const addOffer = (productId: string, offer: OfferInput) =>
  call<Offer>("addOffer", `${base(productId)}/offers`, "POST", offer);

export const removeOffer = (productId: string, offerId: number) =>
  call<{ deleted: number }>("removeOffer", `${base(productId)}/offers/${offerId}`, "DELETE");

export const bidForOffer = (productId: string, offerId: number, reason: string) =>
  call<Bid>("bidForOffer", `${base(productId)}/offers/${offerId}/bids`, "POST", { reason });

/** Every bid for its owner role; else the caller's own. */
export const fetchBids = (productId: string) =>
  call<Bid[]>("fetchBids", `${base(productId)}/bids`);

export const acceptBid = (productId: string, bidId: number) =>
  call<Bid>("acceptBid", `${base(productId)}/bids/${bidId}/accept`, "POST");

export const declineBid = (productId: string, bidId: number, reason: string) =>
  call<Bid>("declineBid", `${base(productId)}/bids/${bidId}/decline`, "POST", { reason });

/** Each dataset's latest profile as ``role`` sees the data. */
export const fetchProductProfiles = (productId: string, role: string) =>
  call<ProductProfile[]>("fetchProductProfiles", `${base(productId)}/profiles`, "GET", undefined, {
    "X-Provisa-Role": role,
  });
