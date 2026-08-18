// Copyright (c) 2026 Kenneth Stott
// Canary: a9f889bb-8065-40b7-96a5-bd05c1f05a63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1486: apply an org's branding to the running page.
//
// Branding is light by design: it moves the accent colors and names the org, it does not re-skin
// the product. Applying it means overriding a handful of theme tokens on :root — the same
// variables theme/tokens.css defines — so every component that already reads them follows without
// knowing an org set anything. Light and dark are both driven by these tokens, so the override
// holds across a theme switch.

import type { OrgBranding } from "../api/branding";
import { isOrgSubdomainHost, orgFromHost } from "./authHost";

/**
 * The tokens an org may move. primary_color drives the three existing --primary tokens, so every
 * button and active-nav state follows it. accent_color has no existing token to take over — it is
 * the org's own second color and lands on --org-accent, which the branded header reads.
 */
const PRIMARY_TOKENS = ["--primary", "--primary-hover", "--primary-strong"];
const ACCENT_TOKEN = "--org-accent";

export function applyOrgBranding(branding: OrgBranding): void {
  const root = document.documentElement;
  if (branding.primary_color) {
    for (const token of PRIMARY_TOKENS) root.style.setProperty(token, branding.primary_color);
  }
  if (branding.accent_color) root.style.setProperty(ACCENT_TOKEN, branding.accent_color);
}

/** Undo an applied override, so signing out of one org does not leave its colors behind. */
export function clearOrgBranding(): void {
  const root = document.documentElement;
  for (const token of [...PRIMARY_TOKENS, ACCENT_TOKEN]) root.style.removeProperty(token);
}

/**
 * The org whose branding this page should show, or null when the page belongs to no org.
 *
 * On an org subdomain that is the Host's own org. On the control plane it is the org named by
 * `?next=` — the address the user was sent here from and will be returned to (REQ-1348) — which is
 * the only org this sign-in is about. A control-plane sign-in reached directly names none.
 */
export function brandingOrg(search: string = window.location.search): string | null {
  if (isOrgSubdomainHost()) return orgFromHost();
  const next = new URLSearchParams(search).get("next");
  if (!next) return null;
  try {
    const host = new URL(next).hostname;
    return isOrgSubdomainHost(host) ? orgFromHost(host) : null;
  } catch {
    return null;
  }
}
