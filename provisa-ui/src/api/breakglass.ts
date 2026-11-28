// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1264: the break-glass account's forced first-sign-in rotation. The endpoint is on the auth
// middleware's skip list (REQ-594): before the rotation the account has no token, so the current
// password travels in the body and the server checks it under the login throttle.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/** The code /auth/superuser-login answers with (403) until the generated password is replaced. */
export const PASSWORD_CHANGE_REQUIRED = "auth.password_change_required";

/** The server's floor for a new break-glass password (provisa/auth/superuser.py
 *  MIN_PASSWORD_LENGTH); checked here too so the form says so before it sends. */
export const BREAKGLASS_MIN_PASSWORD_LENGTH = 12;

export interface BreakglassPasswordChange {
  username: string;
  current_password: string;
  new_password: string;
}

/**
 * Replace the break-glass password. Resolves to the session the server issued with the new
 * password, or null when the deployment has no session signing key (the server then answers
 * `{changed: true}` alone, by design in provisa/api/auth_router.py). A refusal throws an Error
 * whose message is the server's.
 */
export async function changeBreakglassPassword(
  change: BreakglassPasswordChange,
): Promise<string | null> {
  const resp = await fetch(`${API_BASE}/auth/superuser-password`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(change),
  });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(body, requestFailed("Change password", resp.status)));
  }
  const data = (await resp.json()) as { changed: true; access_token?: string };
  return data.access_token ?? null;
}
