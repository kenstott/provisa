// Copyright (c) 2026 Kenneth Stott
// Canary: fbf25cc2-452f-460b-8d08-d1c2822067ba
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2006: a demo sandbox's visitor holds no account. Opening a demo answers the sandbox's org and
// a personal access token; the page that opened it hands both to the sandbox's own address in the
// URL fragment (never sent to a server), and that address keeps them as the tab's own visitor
// identity (browserStorage's visit), never in place of the browser's Provisa login.

import {
  endVisit,
  getStored,
  SessionStorageError,
  setStored,
  startVisit,
  visitorActive,
} from "./browserStorage";

const TOKEN_PARAM = "provisa_sandbox";
const ORG_PARAM = "provisa_sandbox_org";

/** The demo sandbox token this tab's visitor holds, if any. */
export function sandboxToken(): string | null {
  return visitorActive() ? getStored("provisa_token", { global: true }) : null;
}

/**
 * Where to go once a demo sandbox is opened: the sandbox's own address (its org as the leftmost
 * label, REQ-1276) with the token in the fragment; on a host that addresses no org by name
 * (`localhost`), this app's root, the org named in the fragment too.
 */
export function sandboxDestination(orgId: string, token: string, orgOrigin: string | null): string {
  const fragment = new URLSearchParams({ [TOKEN_PARAM]: token });
  if (orgOrigin === null) {
    fragment.set(ORG_PARAM, orgId);
    return `/#${fragment.toString()}`;
  }
  return `${orgOrigin}/#${fragment.toString()}`;
}

/**
 * At boot: keep a demo sandbox token handed over in the fragment, and take the fragment off the
 * address so it is neither bookmarked nor shown. Returns whether one was adopted.
 */
export function adoptSandboxTokenFromLocation(location: Location = window.location): boolean {
  const fragment = new URLSearchParams(location.hash.replace(/^#/, ""));
  const token = fragment.get(TOKEN_PARAM);
  if (!token) return false;
  // The token leaves the address however its keeping goes.
  window.history.replaceState(null, "", `${location.pathname}${location.search}`);
  // The tab becomes the visitor's: its bearer and org are the visitor's own, beside -- never in
  // place of -- a login the browser holds.
  try {
    startVisit();
    setStored("provisa_token", token, { global: true });
    const org = fragment.get(ORG_PARAM);
    if (org) setStored("provisa_org", org, { global: true });
    return true;
  } catch (e) {
    if (!(e instanceof SessionStorageError)) throw e;
    // Logged and noticed where it failed: the visit did not start, so the tab is as it was.
    if (visitorActive()) endVisit();
    return false;
  }
}
