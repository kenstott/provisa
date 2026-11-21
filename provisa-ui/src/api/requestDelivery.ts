// Copyright (c) 2026 Kenneth Stott
// Canary: 4e8b2d17-9c3a-4f60-b5e1-0d7a6c93f258
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2216: the org's request delivery setting -- email (the default), events only, or both.
import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";
const URL = `${API_BASE}/admin/settings/request-delivery`;

export type RequestDelivery = "email" | "events" | "both";

async function delivery(res: Response, op: string): Promise<RequestDelivery> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  const body = (await res.json()) as { delivery: RequestDelivery };
  return body.delivery;
}

export async function fetchRequestDelivery(): Promise<RequestDelivery> {
  return delivery(await fetch(URL), "Request delivery");
}

export async function saveRequestDelivery(next: RequestDelivery): Promise<RequestDelivery> {
  return delivery(
    await fetch(URL, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ delivery: next }),
    }),
    "Request delivery",
  );
}
