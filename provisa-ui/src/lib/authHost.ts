// Copyright (c) 2026 Kenneth Stott
// Canary: 04d286d9-182d-4656-8d46-937d5fe2b832
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1348: which host may run a sign-in.
//
// REQ-1276 makes the leftmost Host label the org, with `cloud` as the sole org-agnostic
// exception. That addressing model collides with Firebase Authentication: its authorized-domain
// list takes exact hostnames only — the API rejects `*.provisa.dev` with
// `INVALID_AUTHORIZED_DOMAIN` — so an org subdomain that has never been registered fails
// `signInWithPopup` with `auth/unauthorized-domain`. Registering each org would make org creation
// write to Google's auth config, which REQ-1054 forbids (org creation is a schema and a row, and
// nothing else).
//
// The resolution: exactly one host — the control plane — ever runs a sign-in, and org subdomains
// obtain the bearer from it (see lib/crossSubdomainAuth.ts). This module is the single place that
// decides which host we are on, so the parent, the relay, and the login page cannot disagree.
//
// All of it applies only where the deployment declares its base domain (cloud.provisa.dev declares
// provisa.dev). Any other host — a GitHub Codespace's forwarded port, localhost, a custom single
// host — is the deployment's one address, whatever its shape: `<name>-3000.app.github.dev` has three
// labels but names no org, and there is no `cloud.app.github.dev` to borrow a session from.

declare global {
  interface Window {
    /** Set by /deployment-config.js (ui_server) from the UI container's VITE_ORG_BASE_DOMAIN. */
    __PROVISA_ORG_BASE_DOMAIN__?: string | null;
  }
}

/** The org-agnostic control-plane label. Must match `_requested_org_from_host` (auth/middleware.py). */
export const CONTROL_PLANE_LABEL = "cloud";

function labelsOf(hostname: string): string[] {
  return hostname.split(".").filter(Boolean);
}

/**
 * The base domain this deployment declares for org-subdomain addressing, or null when it declares
 * none. Runtime-injected first (ui_server's /deployment-config.js, so one built image serves any
 * deployment), build-time VITE_ORG_BASE_DOMAIN second — the same order as the Firebase config.
 */
export function declaredBaseDomain(): string | null {
  const env = (import.meta as unknown as Record<string, Record<string, string | undefined>>).env;
  const raw = window.__PROVISA_ORG_BASE_DOMAIN__ || env.VITE_ORG_BASE_DOMAIN || "";
  return labelsOf(raw.trim().toLowerCase()).join(".") || null;
}

/** The single label in front of the declared base domain, or null when the host is not one. */
function subdomainLabel(hostname: string): string | null {
  const base = declaredBaseDomain();
  if (!base) return null;
  const labels = labelsOf(hostname.toLowerCase());
  const baseLabels = labelsOf(base);
  if (labels.length !== baseLabels.length + 1) return null;
  return labels.slice(1).join(".") === base ? labels[0] : null;
}

/**
 * The deployment's base domain when `hostname` is it or a single label in front of it:
 * `kstott.provisa.dev` → `provisa.dev`. Empty on every other host — and on every host when the
 * deployment declares no base domain — because such a host addresses nothing but itself.
 */
export function baseDomain(hostname: string = window.location.hostname): string {
  const base = declaredBaseDomain();
  if (!base) return "";
  return hostname.toLowerCase() === base || subdomainLabel(hostname) !== null ? base : "";
}

/** True on the control-plane host itself (`cloud.<base>`), where sign-in is allowed to run. */
export function isControlPlaneHost(hostname: string = window.location.hostname): boolean {
  return subdomainLabel(hostname) === CONTROL_PLANE_LABEL;
}

/**
 * True on a host whose leftmost label names an org — the case that cannot sign in locally.
 *
 * A single label in front of the declared base domain, other than `cloud` — the server's rule
 * (auth/middleware.py `_requested_org_from_host`) restricted to the declared zone. The apex, any
 * other host, and every host of a deployment that declares no base domain are not org hosts.
 */
export function isOrgSubdomainHost(hostname: string = window.location.hostname): boolean {
  const label = subdomainLabel(hostname);
  return label !== null && label !== CONTROL_PLANE_LABEL;
}

/** The org named by the current Host, i.e. the leftmost label. Only valid on an org host. */
export function orgFromHost(hostname: string = window.location.hostname): string {
  if (!isOrgSubdomainHost(hostname)) {
    throw new Error(`orgFromHost called on a non-org host: ${hostname}`);
  }
  return subdomainLabel(hostname)!;
}

/**
 * The origin that owns sign-in for this deployment: the current scheme and port, with the
 * leftmost label replaced by `cloud`. Throws on a host with no base domain rather than
 * guessing an origin — a wrong auth origin would send the bearer to a host we do not own.
 */
export function controlPlaneOrigin(location: Location = window.location): string {
  const base = baseDomain(location.hostname);
  if (!base) throw new Error(`no control-plane origin for host: ${location.hostname}`);
  return `${location.protocol}//${CONTROL_PLANE_LABEL}.${base}${location.port ? `:${location.port}` : ""}`;
}

/**
 * The address an org is reached at on this deployment: its id as the leftmost label in front of
 * the same base domain (REQ-1276). `null` on a host with no base domain — a desktop install or
 * `localhost` addresses no org by name, so there is no such URL to show rather than a guessed one.
 */
export function orgOrigin(orgId: string, location: Location = window.location): string | null {
  const base = baseDomain(location.hostname);
  if (!base) return null;
  return `${location.protocol}//${orgId}.${base}${location.port ? `:${location.port}` : ""}`;
}

/**
 * The link an invitation is sent as: the invited org's own address (REQ-1276), so the invitee
 * arrives at the org they were invited to rather than at the control plane's front door.
 *
 * The org host cannot run the sign-in itself (REQ-1348), so a signed-out invitee is bounced to the
 * control-plane login — `redirectToControlPlaneLogin` carries the `invite` token across with them,
 * because that page is where redemption happens, and returns them here afterwards.
 *
 * On a host that addresses no org by name — a desktop install, `localhost` — `orgOrigin` is null
 * and this deployment has exactly one address, which is the one we are already on.
 */
export function inviteUrl(orgId: string, token: string, location: Location = window.location) {
  const origin = orgOrigin(orgId, location) ?? location.origin;
  return `${origin}/register?invite=${token}`;
}

/**
 * True when `origin` is a host of THIS deployment — same scheme, same port, and a single
 * label in front of its declared base domain. The relay answers token requests only from these,
 * so a sibling tenant of some other zone can never ask the control plane for a bearer.
 */
export function isSiblingOrigin(origin: string, self: Location = window.location): boolean {
  let url: URL;
  try {
    url = new URL(origin);
  } catch {
    return false;
  }
  if (url.protocol !== self.protocol || url.port !== self.port) return false;
  return baseDomain(self.hostname) !== "" && subdomainLabel(url.hostname) !== null;
}
