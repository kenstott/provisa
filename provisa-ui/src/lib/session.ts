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
import { controlPlaneOrigin, isOrgSubdomainHost } from "./authHost";
import { CHECKED_DOMAINS_KEY, KNOWN_DOMAINS_KEY } from "./domainFilterKeys";
import { SUPERUSER_TOKEN_KEY } from "./sessionToken";
import { clearOrgSelectionRequired } from "./orgSelection";
import {
  endVisit,
  removeStored,
  removeStoredEverywhere,
  setStored,
  visitorActive,
} from "./browserStorage";

/**
 * Remembers the last submenu item visited within each nav group. Lives here rather than in NavBar
 * so it can be listed in SESSION_KEYS without a cycle.
 */
export const LAST_SUBNAV_KEY = "provisa_nav_last_item";

/** localStorage keys that scope client state to ONE signed-in session: the session's identity,
 * saved for the browser as a whole (lib/browserStorage). */
export const SESSION_KEYS = [
  "provisa_token",
  SUPERUSER_TOKEN_KEY,
  "provisa_org",
  "provisa_role",
] as const;

/** UI state saved per control plane and org (REQ-2008) that must not outlive the session that
 * saved it either: on sign-out it goes from every control plane and org it was saved under. */
export const SESSION_SCOPED_KEYS = [
  // REQ-1349: the remembered subnav item is one identity's preference. Two people sharing a browser
  // profile sign in one at a time, so the key needs no per-login namespace — but it must not survive
  // the handover: the previous identity's remembered tab can name a route the next one has no right
  // to, and entering the group on it lands on the denial page.
  LAST_SUBNAV_KEY,
  // REQ-1297: the domain filter names domains of one org (now saved per org, REQ-2008). Carried
  // into a new session, a previously-unchecked domain stays unchecked (mergeCheckedDomains keeps
  // `known`-but-not-`checked` off), which presented a correctly-provisioned org as one missing its
  // meta/ops domains.
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
  // REQ-2006: in a live demo's tab the session is the visitor's: it ends, and the browser's own
  // signed-in session -- which the visit never touched -- is the one the tab has again.
  if (visitorActive()) {
    endVisit();
    clearPersistedAdminCache();
    return;
  }
  for (const key of SESSION_KEYS) removeStored(key, { global: true });
  for (const key of SESSION_SCOPED_KEYS) removeStoredEverywhere(key);
  clearPersistedAdminCache();
  // REQ-1935: a refusal for naming no org belongs to the session that drew it. Left set, its
  // non-dismissible prompt stood over the sign-in page the next identity has to use.
  clearOrgSelectionRequired();
}

/**
 * End the session in place, for a gate that sends the user back to sign-in (REQ-1289, REQ-1292).
 *
 * Unlike {@link signOut} it does not navigate: the caller re-renders sign-in. The Firebase SDK is
 * signed out first, because its session outlives localStorage and `currentBearer` (lib/authFetch)
 * asks it for a fresh bearer on every request. Clearing `provisa_token` alone left the sign-in page
 * sending that bearer with no org named, and the refusal raised the select-an-org prompt over the
 * page (cloud-dev, 2026-11-17). A demo visitor's tab ends only the visit (REQ-2006): the browser's
 * own login is one the visit never touched.
 */
export async function endSession(): Promise<void> {
  if (!visitorActive()) {
    const { signOutFirebase } = await import("./firebase");
    await signOutFirebase();
  }
  clearSessionState();
}

/**
 * The control plane's sign-out path. An org subdomain sends the browser here because the session
 * being ended is not its own; the control plane serves it out of the SPA fallback like any route,
 * and the boot answers it before React mounts (main.tsx).
 */
export const LOGOUT_PATH = "/logout";

/**
 * End the session and return to the public entry point.
 *
 * Lives here rather than in the navbar because the onboarding page has no navbar: an account that
 * belongs to no org is held on that page by the membership gate, so without its own sign-out there
 * is no way to reach a different account from it.
 *
 * REQ-1603: on an org subdomain this hands off to the control plane instead of signing out locally.
 * REQ-1348 gives an org host no session of its own — it borrows the bearer through the relay iframe
 * — and the Firebase session that mints that bearer lives in the control-plane origin's storage,
 * which nothing on this origin can reach. Clearing here and reloading had the boot borrow the very
 * same session straight back, which is the "logging out does nothing" that was reported.
 */
export async function signOut(): Promise<void> {
  if (isOrgSubdomainHost()) {
    // Clear this origin anyway: the hand-off is a navigation, and a token left behind here would
    // be read by anything that loads before the control plane finishes the sign-out.
    clearSessionState();
    window.location.assign(`${controlPlaneOrigin()}${LOGOUT_PATH}`);
    return;
  }
  // Clear the Firebase session too, or signInWithPopup silently reuses the persisted
  // Google account on the next login and never offers the account chooser.
  const { installFirebaseTokenSync, signOutFirebase } = await import("./firebase");
  // Settle the restored user first. Firebase signs out whoever is current, and on the /logout
  // entry point (a cold document load) no user has been read back out of IndexedDB yet, so a
  // sign-out issued before that lands on nobody and leaves the session intact.
  await installFirebaseTokenSync();
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
  if (visitorActive()) clearSessionState(); // a sign-in from a demo's tab ends the visit first
  clearSessionState();
  setStored("provisa_token", token, { global: true });
}

/** REQ-1472: begin a break-glass session — same clearing, stored under the key Firebase does not own. */
export function startSuperuserSession(token: string): void {
  if (visitorActive()) clearSessionState(); // a sign-in from a demo's tab ends the visit first
  clearSessionState();
  setStored(SUPERUSER_TOKEN_KEY, token, { global: true });
}
