// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1486: per-org light branding — the read the sign-in page and the app shell make, and the
// writes the org_admin's Team page makes.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface OrgBranding {
  display_name?: string;
  primary_color?: string;
  accent_color?: string;
  welcome_message?: string;
  invite_message?: string;
}

export interface PublicBranding {
  org_id: string | null;
  name: string | null;
  branding: OrgBranding;
  logo: boolean;
}

/**
 * The branding of the org this page is for, read without a token.
 *
 * The server derives the org from the Host subdomain (REQ-1276). On the control-plane host the
 * Host names no org, so the sign-in page — which an org subdomain redirected the user to — names
 * it with `x-org-provisa`, the same header the control plane already uses to address an org.
 */
export async function fetchPublicBranding(orgId?: string | null): Promise<PublicBranding> {
  const res = await fetch(`${API_BASE}/orgs/branding`, {
    headers: orgId ? { "x-org-provisa": orgId } : undefined,
  });
  if (!res.ok) throw new Error(requestFailed("fetchPublicBranding", res.status));
  return res.json();
}

/** The URL an <img> loads the org's logo from. Same org resolution as above. */
export function publicLogoUrl(orgId?: string | null): string {
  return orgId
    ? `${API_BASE}/orgs/branding/logo?org=${encodeURIComponent(orgId)}`
    : `${API_BASE}/orgs/branding/logo`;
}

export async function fetchOrgBranding(
  orgId: string,
): Promise<{ org_id: string; branding: OrgBranding; logo_media_type: string | null }> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/branding`);
  if (!res.ok) throw new Error(requestFailed("fetchOrgBranding", res.status));
  return res.json();
}

export async function saveOrgBranding(orgId: string, branding: OrgBranding): Promise<OrgBranding> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/branding`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(branding),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("saveOrgBranding", res.status)));
  }
  return (await res.json()).branding;
}

/** Upload the logo as its own bytes — the file is the body, typed by Content-Type. */
export async function uploadOrgLogo(orgId: string, file: File): Promise<string> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/branding/logo`, {
    method: "PUT",
    headers: { "Content-Type": file.type },
    body: file,
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("uploadOrgLogo", res.status)));
  }
  return (await res.json()).logo_media_type;
}

export async function deleteOrgLogo(orgId: string): Promise<void> {
  const res = await fetch(`${API_BASE}/admin/orgs/${orgId}/branding/logo`, { method: "DELETE" });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("deleteOrgLogo", res.status)));
  }
}
