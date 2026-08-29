// Copyright (c) 2026 Kenneth Stott
// Canary: f2791ce3-a6bd-4257-8a2f-7ca0527e3ca1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1348: the org-subdomain side of cross-subdomain sign-in.
//
// An org subdomain cannot run a sign-in of its own (lib/authHost.ts explains why), so it acquires
// the bearer from the control plane in one of two ways:
//
//   1. Silently, by embedding the control plane's auth relay in a hidden iframe and asking it for
//      the token over postMessage. This is the common path — the user signed in on cloud, or on a
//      sibling org subdomain that already handed off, and the control-plane session is still live.
//   2. By redirect, when the control plane has no session either. The subdomain sends the user to
//      the control-plane login with `?next=` pointing back here; after sign-in the login page
//      returns them, and path 1 then succeeds because a session now exists.
//
// Firebase ID tokens expire after ~1h and the SDK that rotates them runs on the control-plane
// origin, not here. Nothing on this origin can refresh the copy we are handed, so the acquired
// token goes stale exactly the way REQ-1266 describes — hence startTokenRefresh below.

import { controlPlaneOrigin, isSiblingOrigin, orgFromHost } from "./authHost";
import { startSession } from "./session";

const REQUEST = "provisa-auth-request";
const TOKEN = "provisa-auth-token";
const READY = "provisa-auth-ready";

const RELAY_PATH = "/auth-relay.html";

/**
 * How long to wait for the relay to load and answer. A relay that never answers means the control
 * plane is unreachable or is not serving this build; that is a broken deployment, and the timeout
 * rejects so it surfaces rather than presenting as a mysterious signed-out state.
 */
const RELAY_TIMEOUT_MS = 8000;

/**
 * Re-acquire cadence. Comfortably inside the ~1h Firebase ID token lifetime, so a page left open
 * keeps a valid bearer; the relay hands back whatever the control plane's own rotation has most
 * recently written, so this only has to be more frequent than expiry.
 */
const REFRESH_INTERVAL_MS = 30 * 60 * 1000;

/**
 * Ask the control plane for the current bearer through a hidden relay iframe.
 *
 * Resolves with the token, or `null` when the control plane has no session either (a real answer —
 * it means "sign in", not "something failed"). Rejects if the relay never answers.
 */
export function acquireTokenFromControlPlane(
  timeoutMs: number = RELAY_TIMEOUT_MS,
): Promise<string | null> {
  const origin = controlPlaneOrigin();
  return new Promise<string | null>((resolve, reject) => {
    const frame = document.createElement("iframe");
    frame.setAttribute("aria-hidden", "true");
    frame.style.display = "none";
    frame.src = `${origin}${RELAY_PATH}`;

    let done = false;
    const finish = (fn: () => void) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      window.removeEventListener("message", onMessage);
      frame.remove();
      fn();
    };

    const onMessage = (event: MessageEvent) => {
      // Only the relay we embedded may speak for the control plane: check the origin before the
      // payload, so a message from any other frame cannot install a bearer on this page.
      if (event.origin !== origin) return;
      if (event.data?.type === READY) {
        frame.contentWindow?.postMessage({ type: REQUEST }, origin);
        return;
      }
      if (event.data?.type !== TOKEN) return;
      const token: unknown = event.data.token;
      if (token !== null && typeof token !== "string") {
        finish(() => reject(new Error("auth relay returned a non-string token")));
        return;
      }
      finish(() => resolve(token));
    };

    const timer = setTimeout(
      () =>
        finish(() =>
          reject(new Error(`auth relay at ${origin} did not respond in ${timeoutMs}ms`)),
        ),
      timeoutMs,
    );

    window.addEventListener("message", onMessage);
    document.body.appendChild(frame);
  });
}

/**
 * Keep the borrowed bearer fresh. The control plane's Firebase SDK rotates the token; this origin
 * only ever holds a copy, so it has to re-ask. A failed re-acquire is left to the next tick rather
 * than logging the user out — the current copy is still valid until it expires.
 */
export function startTokenRefresh(
  onToken: (token: string) => void,
  intervalMs: number = REFRESH_INTERVAL_MS,
): void {
  setInterval(() => {
    acquireTokenFromControlPlane()
      .then((token) => {
        if (token) onToken(token);
      })
      .catch((err: unknown) => console.error("cross-subdomain token refresh failed:", err));
  }, intervalMs);
}

/**
 * Send the user to the control-plane login, with `?next=` set to return them to this exact page.
 *
 * An `invite` token in this page's query goes with them rather than staying inside `next`: an
 * invitation link addresses the org (REQ-1276), but redemption happens on the sign-in page, which
 * only ever runs on the control plane (REQ-1348). Left in `next` it would arrive back here after
 * sign-in with nothing on this origin able to redeem it. It is dropped from `next` for the same
 * reason — the return trip is into the org, not back onto a register page.
 */
export function redirectToControlPlaneLogin(here: Location = window.location): void {
  const url = new URL(here.href);
  const invite = url.searchParams.get("invite");
  url.searchParams.delete("invite");
  const login = new URL(`${controlPlaneOrigin(here)}/login`);
  login.searchParams.set("next", url.href);
  if (invite) login.searchParams.set("invite", invite);
  here.replace(login.href);
}

/**
 * Boot an org subdomain: borrow the control plane's bearer, or send the user there to sign in.
 *
 * Returns true when the page may render, false when a redirect to the control-plane login is
 * already under way and rendering would only flash the app before navigation. Rejects when the
 * relay is unreachable — a broken control plane is not a signed-out state and must not be
 * silently downgraded into one.
 *
 * REQ-1602: sandbox invites should render on the sandbox subdomain for full UX, so don't redirect
 * if there's an invite parameter in the URL.
 */
export async function establishOrgSubdomainSession(): Promise<boolean> {
  const token = await acquireTokenFromControlPlane();
  if (!token) {
    // Don't redirect if there's an invite — let the login page handle it
    if (!new URLSearchParams(window.location.search).get("invite")) {
      redirectToControlPlaneLogin();
    }
    return false;
  }
  // A different bearer than the one this origin last held means a different identity may be
  // signing in, so clear the previous session's org/role/cache the same way a local sign-in does
  // (REQ-1326) instead of letting it ride along under the new token.
  if (token !== localStorage.getItem("provisa_token")) startSession(token);
  // REQ-1276: on an org host the Host header is the authoritative org, so it is set from the URL
  // rather than from anything carried over — and after startSession, which clears this key.
  localStorage.setItem("provisa_org", orgFromHost());
  startTokenRefresh((refreshed) => localStorage.setItem("provisa_token", refreshed));
  return true;
}

/**
 * The validated return-to URL from `?next=`, or null.
 *
 * `next` is attacker-controllable — it arrives in a URL anyone can send a user — so an unvalidated
 * value turns the login page into an open redirect that forwards a fresh bearer off-site. Only a
 * host of this same deployment is accepted.
 */
export function nextParam(search: string = window.location.search): string | null {
  const raw = new URLSearchParams(search).get("next");
  if (!raw) return null;
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return null;
  }
  if (!isSiblingOrigin(url.origin)) return null;
  return url.href;
}
