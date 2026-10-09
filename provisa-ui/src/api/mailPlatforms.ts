// Copyright (c) 2026 Kenneth Stott
// Canary: e5387127-ab2f-4bd3-bd0d-d334103a8d36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: the organisation's mail platforms -- the client its sources sign in to Google
// Workspace (and Microsoft 365) with, entered once by an organisation administrator. No call
// here can be handed the client's secret back: an answer says whether a platform is connected,
// its client id and settings, and the address to register with the platform.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface MailPlatform {
  platform: string;
  /** The settings the platform takes beside its client (Microsoft's tenant). */
  settings_fields: string[];
  configured: boolean;
  client_id: string | null;
  settings: Record<string, string>;
}

export interface MailPlatforms {
  /** The address to register with a platform; null when the deployment cannot state it. */
  redirect_address: string | null;
  redirect_problem?: string;
  platforms: MailPlatform[];
}

export interface MailPlatformBody {
  client_id: string;
  /** Left out to keep the secret already entered. */
  client_secret?: string;
  settings: Record<string, string>;
}

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

const base = (orgId: string) =>
  `${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/mail-platforms`;

export async function fetchMailPlatforms(orgId: string): Promise<MailPlatforms> {
  return ok(await fetch(base(orgId)), "fetchMailPlatforms");
}

export async function putMailPlatform(
  orgId: string,
  platform: string,
  body: MailPlatformBody,
): Promise<MailPlatform> {
  return ok(
    await fetch(`${base(orgId)}/${encodeURIComponent(platform)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "putMailPlatform",
  );
}

export async function deleteMailPlatform(orgId: string, platform: string): Promise<void> {
  await ok(
    await fetch(`${base(orgId)}/${encodeURIComponent(platform)}`, { method: "DELETE" }),
    "deleteMailPlatform",
  );
}
