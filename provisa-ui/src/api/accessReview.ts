// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2d9c41-5e86-4b13-9f07-c3e8b1d6a254
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2266: access reviews (provisa/api/admin/access_review_router.py). The server decides who
// reads which rows and who may decide each (can_decide); the UI only shows it.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";
const BASE = `${API_BASE}/admin/access-reviews`;

export interface AccessReviewSummary {
  id: number;
  /** The person who asked for it, or "schedule". */
  started_by: string;
  started_at: string;
  rows: number;
  decided: number;
  /** A scheduled review's post to the webhook: null when none was due. */
  webhook_status: "delivered" | "failed" | null;
  webhook_error: string | null;
}

export interface ReviewEntitlement {
  kind: string;
  capability: string | null;
  identifiers: string[];
}

export type ReviewDecision = "confirmed" | "revoked";

export interface AccessReviewRow {
  id: number;
  user_id: string;
  display_name: string | null;
  email: string | null;
  role_id: string;
  domain_id: string;
  entitlements: ReviewEntitlement[];
  expires_at: string | null;
  last_used_at: string | null;
  decision: ReviewDecision | null;
  decided_by: string | null;
  decided_at: string | null;
  reason: string | null;
  can_decide: boolean;
  /** "identity_provider": held through claims or a role mapping, listed read-only. */
  source: "provisa" | "identity_provider";
}

export const IDENTITY_PROVIDER = "identity_provider";

export interface AccessReviewSchedule {
  cron: string | null;
  webhook_url: string | null;
  /** The secret is write-only: the server says only whether one is held. */
  webhook_secret_set: boolean;
}

export const SCHEDULE_STARTER = "schedule";

async function call<T>(op: string, url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, init);
  if (!res.ok) {
    const body = await res.json().catch(() => null);
    throw new Error(serverMessage(body, requestFailed(op, res.status)));
  }
  return res.json();
}

const json = (method: string, body: unknown): RequestInit => ({
  method,
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

export function fetchAccessReviews(): Promise<AccessReviewSummary[]> {
  return call("fetchAccessReviews", BASE);
}

export function startAccessReview(): Promise<{ id: number; rows: number }> {
  return call("startAccessReview", BASE, { method: "POST" });
}

export function fetchAccessReview(id: number): Promise<{ id: number; rows: AccessReviewRow[] }> {
  return call("fetchAccessReview", `${BASE}/${id}`);
}

export function decideAccessReviewRow(
  rowId: number,
  decision: ReviewDecision,
  reason: string,
): Promise<AccessReviewRow> {
  return call("decideAccessReviewRow", `${BASE}/rows/${rowId}`, json("POST", { decision, reason }));
}

export function accessReviewExportUrl(id: number): string {
  return `${BASE}/${id}/export`;
}

export function fetchAccessReviewSchedule(): Promise<AccessReviewSchedule> {
  return call("fetchAccessReviewSchedule", `${BASE}/schedule`);
}

/** `webhookSecret` null keeps the held secret. */
export function saveAccessReviewSchedule(
  cron: string | null,
  webhookUrl: string | null,
  webhookSecret: string | null,
): Promise<AccessReviewSchedule> {
  return call(
    "saveAccessReviewSchedule",
    `${BASE}/schedule`,
    json("PUT", { cron, webhook_url: webhookUrl, webhook_secret: webhookSecret }),
  );
}
