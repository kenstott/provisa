// Copyright (c) 2026 Kenneth Stott
// Canary: 6f1e2a90-4c7b-4d21-9a83-2e5b7c0d1f44
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { currentFirebaseToken } from "./firebase";

// REQ-1267: on an auth-enforced deploy (firebase/basic) the bearer token lives in
// localStorage and must ride on EVERY same-origin request. The Apollo link already adds it
// for GraphQL, but the ~100 REST fetch() call sites (api/*.ts, pages, hooks) did not — so
// /auth/me and every REST-backed feature 401'd, leaving the client in a broken half-authed
// state. Installing one same-origin fetch interceptor at startup covers all of them (and any
// future call site) in one place instead of threading a header through every caller.

// REQ-1317: the active org rides along by the same argument. REQ-1276 makes the org the Host
// subdomain everywhere EXCEPT the control-plane host (cloud.provisa.*), where the server reads it
// only from `x-org-provisa`. Three REST call sites were sending `X-Org-Id`, a name no server code
// reads, so org selection silently did nothing on cloud.*. Attaching the canonical name here — next
// to the bearer, for the same reason — is what keeps the sent name and the read name from drifting.
// On a subdomain host the middleware ignores the header (the Host wins), so this is safe to send
// unconditionally.
export const ORG_HEADER = "X-Org-Provisa";

/**
 * REQ-1434: the bearer to put on the next request.
 *
 * A live Firebase session is asked for its current token, which the SDK re-mints when the one it
 * holds has expired. Deployments without one (basic auth; an org subdomain holding a copy borrowed
 * from the control plane, refreshed on its own interval by crossSubdomainAuth) keep the stored
 * token as their only bearer — this is the whole set of token sources, not a guess at one.
 */
export async function currentBearer(): Promise<string | null> {
  const live = await currentFirebaseToken();
  if (live !== null) return live;
  return localStorage.getItem("provisa_token");
}

/** Wrap window.fetch to attach `Authorization: Bearer <provisa_token>` to same-origin requests. */
export function installAuthFetch(): void {
  const originalFetch = window.fetch.bind(window);

  const isSameOrigin = (url: string): boolean => {
    // Relative URLs ("/auth/me", "auth/me") are same-origin by definition. Absolute URLs
    // must match window.location.origin — never leak the token to an external host (Google,
    // Firebase, etc.).
    if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(url)) return true;
    return url.startsWith(window.location.origin);
  };

  window.fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const token = await currentBearer();
    const orgId = localStorage.getItem("provisa_org");
    if (!token) return originalFetch(input, init);

    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    if (!isSameOrigin(url)) return originalFetch(input, init);

    // Merge onto whichever headers source applies: init overrides a Request's own headers.
    const headers = new Headers(
      init?.headers ?? (input instanceof Request ? input.headers : undefined),
    );
    // Do not clobber an explicit header (e.g. Apollo's authLink already set one).
    if (!headers.has("authorization")) headers.set("Authorization", `Bearer ${token}`);
    if (orgId && !headers.has(ORG_HEADER)) headers.set(ORG_HEADER, orgId);
    return originalFetch(input, { ...init, headers });
  };
}
