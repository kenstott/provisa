// Copyright (c) 2026 Kenneth Stott
// Canary: 1c8b47f2-90ad-4e63-8a15-7d2c6be4f019
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { clearPersistedAdminCache } from "../apolloClient";
import { CHECKED_DOMAINS_KEY, KNOWN_DOMAINS_KEY } from "./domainFilterKeys";

/** localStorage keys that scope client state to ONE signed-in session. */
export const SESSION_KEYS = [
  "provisa_token",
  "provisa_org",
  "provisa_role",
  // REQ-1297: the domain filter names domains of one org. Carried into a new session or a new org,
  // a previously-unchecked domain stays unchecked (mergeCheckedDomains keeps `known`-but-not-
  // `checked` off), which presented a correctly-provisioned org as one missing its meta/ops domains.
  CHECKED_DOMAINS_KEY,
  KNOWN_DOMAINS_KEY,
] as const;

/**
 * REQ-1326: drop every trace of the previous session's client state.
 *
 * `provisa_org`, `provisa_role` and the persisted Apollo snapshot outlive a sign-out and a token
 * swap, so the next user inherited them: `provisa_org` rides on every request as `X-Org-Provisa`
 * (lib/authFetch, apolloClient) and is read straight into AuthContext's `selectedOrg` — naming an
 * org the new identity does not belong to, and in the reported failure one that had been deleted
 * outright. The signed-in app then rendered against an org that was not theirs until a logout
 * cleared it. Session-scoped state is cleared when the session starts, not patched around later.
 */
export function clearSessionState(): void {
  for (const key of SESSION_KEYS) localStorage.removeItem(key);
  clearPersistedAdminCache();
}

/** Begin a session for `token`: clear the prior identity's state first, then store the credential. */
export function startSession(token: string): void {
  clearSessionState();
  localStorage.setItem("provisa_token", token);
}
