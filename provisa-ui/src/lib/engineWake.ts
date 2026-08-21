// Copyright (c) 2026 Kenneth Stott
// Canary: 4594cb1d-9593-44e0-84a4-53990219e516
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1516: whether the query engine is starting, as one piece of app-wide state.
 *
 * Engine shards idle to zero (REQ-1448), so the first query after a quiet period waits ~2-4min for
 * Autopilot to provision a node and Trino to start. The request does come back — it is not an
 * outage, which is what {@link ./serverReachability} covers — but until it does, every surface
 * renders an unexplained spinner, and the operator has no way to tell a cold start from a hang.
 *
 * The signal is derived, never asserted by a caller. A same-origin request outstanding longer than
 * {@link SLOW_MS} starts polling `/data/engine/state`, and only the server's own answer raises the
 * notice: `starting`/`stopped` means a wake is what the wait is for, `ready` means it is not and
 * nothing is shown. Any slow request can therefore start a poll without a path allowlist to
 * maintain — a wrong guess costs one cheap request and shows nothing.
 *
 * The state reported is the engine THIS org queries: the shared shard on the pooled lane, the org's
 * own coordinator on the dedicated one (REQ-1510), `always-on` for a BYO engine or a desktop
 * install. The org rides on the request, so no caller has to know which lane it is on.
 */

const STATE_PATH = "/data/engine/state";
const PREWARM_PATH = "/data/engine/prewarm";

/** How long a request must be outstanding before a cold start is worth asking about. Below this a
 *  normal query is still in its ordinary range, and polling would be noise on every page load. */
const SLOW_MS = 5_000;
const POLL_INTERVAL_MS = 3_000;

type Listener = (waking: boolean) => void;

let waking = false;
let inFlight = 0;
let busySince: number | null = null;
let pollTimer: number | null = null;
let tickTimer: number | null = null;
const listeners = new Set<Listener>();

/** When the current wake started, for the elapsed time the notice shows. */
let wakingSince: number | null = null;

function emit(): void {
  for (const listener of listeners) listener(waking);
}

function setWaking(next: boolean): void {
  if (waking === next) return;
  waking = next;
  wakingSince = next ? Date.now() : null;
  emit();
}

/**
 * The engine endpoints are exempt from the in-flight count that triggers them. A probe that counted
 * itself would keep `inFlight` above zero on its own and poll forever once it started.
 */
export function isEngineProbePath(url: string): boolean {
  const path = url.startsWith("http") ? new URL(url).pathname : url.split("?")[0];
  return path.endsWith(STATE_PATH) || path.endsWith(PREWARM_PATH);
}

function poll(): void {
  fetch(STATE_PATH, { cache: "no-store" })
    .then((res) => (res.ok ? res.json() : null))
    .then((body) => {
      if (body === null) return;
      setWaking(body.state === "starting" || body.state === "stopped");
    })
    .catch(() => {
      /* An unreachable server is serverReachability's notice, not this one. */
    });
}

function startPolling(): void {
  if (pollTimer !== null) return;
  pollTimer = window.setInterval(poll, POLL_INTERVAL_MS);
  poll();
}

function stopPolling(): void {
  if (pollTimer === null) return;
  window.clearInterval(pollTimer);
  pollTimer = null;
  setWaking(false);
}

function tick(): void {
  if (busySince === null) {
    stopPolling();
    return;
  }
  if (Date.now() - busySince >= SLOW_MS) startPolling();
}

/** Count a same-origin request as outstanding. Called by the fetch wrapper, not by call sites. */
export function noteRequestStart(): void {
  inFlight += 1;
  if (busySince === null) busySince = Date.now();
  if (tickTimer === null) tickTimer = window.setInterval(tick, 1_000);
}

export function noteRequestEnd(): void {
  inFlight = Math.max(0, inFlight - 1);
  if (inFlight > 0) return;
  busySince = null;
  stopPolling();
  if (tickTimer !== null) {
    window.clearInterval(tickTimer);
    tickTimer = null;
  }
}

/**
 * Ask the server to start this org's engine now, and do not wait for it.
 *
 * Sign-in already prewarms (REQ-1471), which covers a session that queries soon after it starts. It
 * does not cover the tab left open past the idle window — the shard is reaped underneath it and the
 * next query pays the full cold start. Calling this on arrival at a query surface spends the
 * operator's read-and-compose time on the node provision instead.
 */
export function prewarmEngine(): void {
  fetch(PREWARM_PATH, { method: "POST" }).catch(() => {
    /* A prewarm is a head start, not a step. The query path runs the same wake and reports
       whatever this hit, so there is nothing here for the user to act on. */
  });
}

export function engineWaking(): boolean {
  return waking;
}

/** How long the current wake has been running, in seconds. Null when nothing is waking. */
export function engineWakingSeconds(): number | null {
  if (wakingSince === null) return null;
  return Math.floor((Date.now() - wakingSince) / 1000);
}

export function subscribeEngineWake(listener: Listener): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** Test seam: drop every listener, timer and count between cases. */
export function resetEngineWake(): void {
  stopPolling();
  if (tickTimer !== null) {
    window.clearInterval(tickTimer);
    tickTimer = null;
  }
  waking = false;
  wakingSince = null;
  inFlight = 0;
  busySince = null;
  listeners.clear();
}
