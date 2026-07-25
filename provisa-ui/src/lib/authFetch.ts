// Copyright (c) 2026 Kenneth Stott
// Canary: 6f1e2a90-4c7b-4d21-9a83-2e5b7c0d1f44
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1267: on an auth-enforced deploy (firebase/basic) the bearer token lives in
// localStorage and must ride on EVERY same-origin request. The Apollo link already adds it
// for GraphQL, but the ~100 REST fetch() call sites (api/*.ts, pages, hooks) did not — so
// /auth/me and every REST-backed feature 401'd, leaving the client in a broken half-authed
// state. Installing one same-origin fetch interceptor at startup covers all of them (and any
// future call site) in one place instead of threading a header through every caller.

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

  window.fetch = (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const token = localStorage.getItem("provisa_token");
    if (!token) return originalFetch(input, init);

    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.href
          : input.url;
    if (!isSameOrigin(url)) return originalFetch(input, init);

    // Merge onto whichever headers source applies: init overrides a Request's own headers.
    const headers = new Headers(
      init?.headers ?? (input instanceof Request ? input.headers : undefined),
    );
    // Do not clobber an explicit header (e.g. Apollo's authLink already set one).
    if (!headers.has("authorization")) headers.set("Authorization", `Bearer ${token}`);
    return originalFetch(input, { ...init, headers });
  };
}
