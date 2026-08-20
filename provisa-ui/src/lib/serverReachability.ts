// Copyright (c) 2026 Kenneth Stott
// Canary: cc037b58-ef42-4575-8cf9-7386ab12d00a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1514: whether the deployment is answering at all, as one piece of app-wide state.
 *
 * A same-origin fetch that never gets a response — the server is restarting, the node is coming
 * back from an idle stop, the network dropped — rejects with a TypeError, and every call site then
 * renders that transport error in its own vocabulary: GraphiQL prints "TypeError: Failed to fetch"
 * over the query result, a page shows an empty list. None of them can say the one thing that is
 * true and useful, which is that the server is down and the user should wait rather than retry.
 *
 * The signal is derived, never asserted by a caller: a same-origin request that fails at the
 * transport marks the deployment unreachable, and any same-origin response — the health probe below
 * or any other request that happens to succeed — marks it reachable again. An HTTP error status is
 * a reachable server, not an outage.
 *
 * While it is down, the health probe polls until the server answers, so the app clears itself
 * without a reload.
 */

const PROBE_PATH = "/health";
const PROBE_INTERVAL_MS = 3_000;

type Listener = (unreachable: boolean) => void;

let unreachable = false;
let probeTimer: number | null = null;
/**
 * The unwrapped fetch, injected by installAuthFetch. The probe must not go through the wrapper:
 * the wrapper awaits a bearer, and a token refresh against an unreachable deployment is exactly the
 * thing that cannot complete while the server is down.
 */
let probeFetch: typeof globalThis.fetch | null = null;
const listeners = new Set<Listener>();

function emit(): void {
  for (const listener of listeners) listener(unreachable);
}

function probe(): void {
  if (!probeFetch) throw new Error("server-reachability probe used before installAuthFetch()");
  probeFetch(PROBE_PATH, { cache: "no-store" })
    .then(() => {
      markServerReachable();
    })
    .catch(() => {
      /* still down; the interval below is the retry. Any other outcome is reported by the
         response path of the request that produced it. */
    });
}

function startProbing(): void {
  if (probeTimer !== null) return;
  probeTimer = window.setInterval(probe, PROBE_INTERVAL_MS);
  probe();
}

function stopProbing(): void {
  if (probeTimer === null) return;
  window.clearInterval(probeTimer);
  probeTimer = null;
}

/** Give the module the unwrapped fetch to probe with. Called once, by installAuthFetch. */
export function setReachabilityProbeFetch(fetchImpl: typeof globalThis.fetch): void {
  probeFetch = fetchImpl;
}

export function serverUnreachable(): boolean {
  return unreachable;
}

export function markServerUnreachable(): void {
  if (unreachable) return;
  unreachable = true;
  startProbing();
  emit();
}

export function markServerReachable(): void {
  if (!unreachable) return;
  unreachable = false;
  stopProbing();
  emit();
}

export function subscribeServerReachability(listener: Listener): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** Test seam: drop every listener and the down state between cases. */
export function resetServerReachability(): void {
  stopProbing();
  unreachable = false;
  listeners.clear();
  probeFetch = null;
}
