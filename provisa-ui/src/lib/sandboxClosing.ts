// Copyright (c) 2026 Kenneth Stott
// Canary: 4f734cc9-b5d0-4221-bf11-e9bb6c2f0ffb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2065: a visitor's demo sandbox is released soon after its tab closes. As the page unloads it
// tells the server it is closing; the server deletes the sandbox unless a request reaches it within
// a couple of minutes. A reload is such a request; another tab of the sandbox is told over a
// BroadcastChannel and sends one. Only a tab holding a visitor token says it: a signed-in person's
// tab never does.
//
// fetch with keepalive, not navigator.sendBeacon: a beacon cannot carry the Authorization header,
// and the visitor's token is its only credential. The native fetch is taken before authFetch wraps
// it, so the request leaves while the page unloads, with the headers given here.

import { getStored } from "./browserStorage";
import { sandboxToken } from "./sandboxSession";
import { ORG_HEADER } from "./authFetch";

const API_BASE = import.meta.env.VITE_API_BASE || "";
const CHANNEL = "provisa-sandbox";
const CLOSING = "closing";

function headers(token: string): Record<string, string> {
  const out: Record<string, string> = { Authorization: `Bearer ${token}` };
  // On a host that names no org (localhost), the org rides in the header, as authFetch sends it.
  const org = getStored("provisa_org", { global: true });
  if (org) out[ORG_HEADER] = org;
  return out;
}

/** Install the closing signal for this tab; returns its removal (for tests). Does nothing in a
 *  tab with no visitor token. */
export function installSandboxClosing(
  nativeFetch: typeof fetch = window.fetch.bind(window),
): () => void {
  const token = sandboxToken();
  if (token === null) return () => {};
  const channel = new BroadcastChannel(CHANNEL);

  const stillHere = () => {
    // Any request reaching the sandbox clears its closing mark.
    void nativeFetch(`${API_BASE}/auth/me`, { headers: headers(token) });
  };
  const onHide = () => {
    void nativeFetch(`${API_BASE}/sandboxes/closing`, {
      method: "POST",
      keepalive: true,
      headers: headers(token),
    });
    channel.postMessage(CLOSING);
  };
  const onShow = (e: PageTransitionEvent) => {
    // Back from the back/forward cache: the tab is open again.
    if (e.persisted) stillHere();
  };
  const onMessage = (e: MessageEvent) => {
    if (e.data === CLOSING) stillHere();
  };

  window.addEventListener("pagehide", onHide);
  window.addEventListener("pageshow", onShow);
  channel.addEventListener("message", onMessage);
  return () => {
    window.removeEventListener("pagehide", onHide);
    window.removeEventListener("pageshow", onShow);
    channel.removeEventListener("message", onMessage);
    channel.close();
  };
}
