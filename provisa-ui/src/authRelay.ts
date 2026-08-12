// Copyright (c) 2026 Kenneth Stott
// Canary: 3d5a1c88-7e64-4a19-b0f2-9c41d7e6a205
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1348: the control-plane side of cross-subdomain sign-in.
//
// This module is the entry point of `auth-relay.html`, which is only ever loaded inside a hidden
// iframe on the control-plane origin (`cloud.<base>`) by an org subdomain that has no credential of
// its own. Sign-in runs on the control plane and nowhere else (see lib/authHost.ts for why), so the
// bearer lives in the control plane's origin-scoped storage; this page is the only way an org
// subdomain can read it.
//
// The two origins share a registrable domain, so the iframe is same-site and its storage is not
// partitioned — the relay sees the same localStorage and the same Firebase IndexedDB session the
// user signed into on cloud.
//
// Handing a bearer to the parent is a credential disclosure, so the only thing standing between a
// hostile embedder and a live token is `isSiblingOrigin`: the requester must be a single-label host
// under the same base domain, on the same scheme and port. Every reply is addressed to that exact
// origin — never `"*"` — so even a passing request cannot be read by a frame that navigated away.

import { installFirebaseTokenSync } from "./lib/firebase";
import { isSiblingOrigin } from "./lib/authHost";
import { currentBearer } from "./lib/authFetch";

/** Parent → relay: "send me the current bearer". */
const REQUEST = "provisa-auth-request";
/** Relay → parent: the bearer, or null when the control plane has no session either. */
const TOKEN = "provisa-auth-token";
/** Relay → parent: "I am listening", so the parent can request without racing frame load. */
const READY = "provisa-auth-ready";

// REQ-1434: ask Firebase for the current token rather than reading the mirrored copy. The copy is
// only as fresh as the last rotation, and this frame is loaded on demand — often in a tab that has
// been idle long enough for the mirror to have expired, which would hand the org subdomain a dead
// bearer it has no way to refresh. currentBearer falls back to the stored key for the auth modes
// that never pass through Firebase (basic auth).
function onMessage(event: MessageEvent): void {
  if (event.data?.type !== REQUEST) return;
  if (!isSiblingOrigin(event.origin)) return;
  const source = event.source as WindowProxy | null;
  const origin = event.origin;
  void currentBearer().then((token) => {
    source?.postMessage({ type: TOKEN, token }, origin);
  });
}

async function main(): Promise<void> {
  window.addEventListener("message", onMessage);
  // Settle the token before announcing readiness: a parent that requests the instant the frame
  // loads would otherwise read whatever stale bearer the last session left behind, or nothing at
  // all on a cold profile where Firebase has not yet restored the user from IndexedDB.
  await installFirebaseTokenSync();
  // The parent's origin is not knowable before it speaks, and this frame is only ever embedded by
  // one. `"*"` here carries no secret — READY has no payload — and the token reply that follows is
  // addressed to the verified requester.
  window.parent.postMessage({ type: READY }, "*");
}

void main();
