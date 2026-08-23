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
import { storedToken } from "./sessionToken";
import {
  markServerReachable,
  markServerUnreachable,
  setReachabilityProbeFetch,
} from "./serverReachability";
import { isEngineProbePath, noteRequestEnd, noteRequestStart } from "./engineWake";

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

// REQ-1487: the ENVIRONMENT rides along by the same argument as the org — one interceptor rather
// than a header threaded through every call site. A request that names no environment is served
// `prod` (provisa/api/env_routing.py), so the header is attached only while a branch is selected,
// and clearing the selection is deleting the key rather than sending "prod".
//
// It is attached whether or not there is a bearer, unlike the org: a deployment with auth disabled
// branches its model exactly as an authenticated one does, and gating this on a token would pin
// every such deployment to prod with no way to say otherwise.
export const ENV_HEADER = "X-Provisa-Env";

/** Where the selected environment is kept. Per-browser, like the active org. */
export const ENV_STORAGE_KEY = "provisa_env";

/** The environment the next request will name, or null for prod. */
export function selectedEnv(): string | null {
  return localStorage.getItem(ENV_STORAGE_KEY);
}

/**
 * REQ-1434: the bearer to put on the next request.
 *
 * A live Firebase session is asked for its current token, which the SDK re-mints when the one it
 * holds has expired. Deployments without one (basic auth; an org subdomain holding a copy borrowed
 * from the control plane, refreshed on its own interval by crossSubdomainAuth) keep the stored
 * token as their only bearer — this is the whole set of token sources, not a guess at one.
 * REQ-1472 adds one more: a break-glass operator session, which `storedToken` prefers over
 * `provisa_token` because it is an explicit override of whatever else the browser is holding.
 */
export async function currentBearer(): Promise<string | null> {
  const live = await currentFirebaseToken();
  if (live !== null) return live;
  return storedToken();
}

/** The error code the server answers a request naming an environment it does not have (REQ-1487). */
const UNKNOWN_ENV_CODE = "env.unknown";

/**
 * REQ-1487: drop a stored environment the server no longer has, from wherever the 404 landed.
 *
 * EnvSwitcher repairs the same stale name, but only once it has rendered — and it cannot render
 * when the stale header is on `/setup/status`, whose 404 stops the app before any of it mounts.
 * The browser is then wedged on a deleted branch with no affordance to leave it. Repairing at the
 * one place every request already passes through covers that case and every other one.
 *
 * Only THIS error clears the key. A 404 is otherwise an ordinary answer (a missing table, a
 * deleted org), and a body that is not this JSON object is not evidence about the environment at
 * all — hence the shape check rather than the status alone.
 */
async function repairStaleEnv(res: Response): Promise<void> {
  if (res.status !== 404) return;
  let body: unknown;
  try {
    body = await res.clone().json();
  } catch {
    return; // not a JSON body, so not this error — every other 404 is left exactly as it is
  }
  const code = (body as { error?: { code?: unknown } })?.error?.code;
  if (code !== UNKNOWN_ENV_CODE) return;
  localStorage.removeItem(ENV_STORAGE_KEY);
  window.location.reload();
}

/** Wrap window.fetch to attach `Authorization: Bearer <provisa_token>` to same-origin requests. */
export function installAuthFetch(): void {
  const originalFetch = window.fetch.bind(window);
  setReachabilityProbeFetch(originalFetch);

  const isSameOrigin = (url: string): boolean => {
    // Relative URLs ("/auth/me", "auth/me") are same-origin by definition. Absolute URLs
    // must match window.location.origin — never leak the token to an external host (Google,
    // Firebase, etc.).
    if (!/^[a-z][a-z0-9+.-]*:\/\//i.test(url)) return true;
    return url.startsWith(window.location.origin);
  };

  // REQ-1514: every request to our own deployment is also a reachability sample. A response of any
  // status proves the server is answering; a rejection that is not a caller's abort is the
  // transport failing, which is the outage the app-wide notice exists to name. The error is
  // re-thrown untouched — the caller still sees exactly what it saw before.
  //
  // REQ-1516: it is also a duration sample. A request outstanding long enough that a cold engine
  // start would explain it is what starts the engine-state poll — no call site opts in, and no path
  // allowlist has to be kept current, because the server's answer is what decides whether anything
  // is shown. The engine endpoints themselves are excluded: a probe counted as an outstanding
  // request would hold the count above zero and keep polling on its own.
  const sampled = async (
    input: RequestInfo | URL,
    init: RequestInit | undefined,
    sameOrigin: boolean,
    url: string,
  ): Promise<Response> => {
    if (!sameOrigin) return originalFetch(input, init);
    const timed = !isEngineProbePath(url);
    if (timed) noteRequestStart();
    try {
      const res = await originalFetch(input, init);
      markServerReachable();
      return res;
    } catch (err) {
      if (!(err instanceof DOMException && err.name === "AbortError")) markServerUnreachable();
      throw err;
    } finally {
      if (timed) noteRequestEnd();
    }
  };

  window.fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.href : input.url;
    const sameOrigin = isSameOrigin(url);

    const token = await currentBearer();
    const orgId = localStorage.getItem("provisa_org");
    const env = selectedEnv();
    if (!sameOrigin || (!token && env === null)) return sampled(input, init, sameOrigin, url);

    // Merge onto whichever headers source applies: init overrides a Request's own headers.
    const headers = new Headers(
      init?.headers ?? (input instanceof Request ? input.headers : undefined),
    );
    // Do not clobber an explicit header (e.g. Apollo's authLink already set one).
    if (token && !headers.has("authorization")) headers.set("Authorization", `Bearer ${token}`);
    if (token && orgId && !headers.has(ORG_HEADER)) headers.set(ORG_HEADER, orgId);
    if (env !== null && !headers.has(ENV_HEADER)) headers.set(ENV_HEADER, env);
    const res = await sampled(input, { ...init, headers }, sameOrigin, url);
    if (env !== null) await repairStaleEnv(res);
    return res;
  };
}
