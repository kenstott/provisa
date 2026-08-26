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
import { SUPERUSER_TOKEN_KEY } from "./sessionToken";

/**
 * Remembers the last submenu item visited within each nav group. Lives here rather than in NavBar
 * so it can be listed in SESSION_KEYS without a cycle.
 */
export const LAST_SUBNAV_KEY = "provisa_nav_last_item";

/** localStorage keys that scope client state to ONE signed-in session. */
export const SESSION_KEYS = [
  "provisa_token",
  SUPERUSER_TOKEN_KEY,
  "provisa_org",
  "provisa_role",
  // REQ-1349: the remembered subnav item is one identity's preference. Two people sharing a browser
  // profile sign in one at a time, so the key needs no per-login namespace — but it must not survive
  // the handover: the previous identity's remembered tab can name a route the next one has no right
  // to, and entering the group on it lands on the denial page.
  LAST_SUBNAV_KEY,
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

/**
 * End the session and return to the public entry point.
 *
 * Lives here rather than in the navbar because the onboarding page has no navbar: an account that
 * belongs to no org is held on that page by the membership gate, so without its own sign-out there
 * is no way to reach a different account from it.
 */
export async function signOut(): Promise<void> {
  // Clear the Firebase session too, or signInWithPopup silently reuses the persisted
  // Google account on the next login and never offers the account chooser.
  const { signOutFirebase } = await import("./firebase");
  await signOutFirebase();
  // REQ-1326: sign-out clears exactly what sign-in clears — token, org, role and the persisted
  // Apollo snapshot. Clearing a subset left provisa_role and the cached org-scoped admin data
  // behind for the next identity.
  clearSessionState();
  // Full document load, not navigate(): App reads the token only on an authVersion bump (login
  // path), so an in-app navigate would keep the shell mounted and render /login inside the
  // navbar. A hard load re-reads the token-less localStorage into the public LandingPage branch
  // and drops the Apollo/auth state built for the signed-in session.
  window.location.assign("/");
}

/** Begin a session for `token`: clear the prior identity's state first, then store the credential. */
export function startSession(token: string): void {
  clearSessionState();
  localStorage.setItem("provisa_token", token);
}

/** REQ-1472: begin a break-glass session — same clearing, stored under the key Firebase does not own. */
export function startSuperuserSession(token: string): void {
  clearSessionState();
  localStorage.setItem(SUPERUSER_TOKEN_KEY, token);
}
