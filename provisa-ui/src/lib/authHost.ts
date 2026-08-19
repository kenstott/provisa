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

/** The org-agnostic control-plane label. Must match `_requested_org_from_host` (auth/middleware.py). */
export const CONTROL_PLANE_LABEL = "cloud";

function labelsOf(hostname: string): string[] {
  return hostname.split(".").filter(Boolean);
}

/** True for a literal IPv4 host, which has no subdomain structure to read an org from. */
function isIpLiteral(hostname: string): boolean {
  return /^\d+(\.\d+)*$/.test(hostname);
}

/**
 * The deployment's base domain — everything after the leftmost label. `kstott.provisa.dev`
 * → `provisa.dev`. Empty when the host has no leftmost label to strip (`localhost`).
 */
export function baseDomain(hostname: string = window.location.hostname): string {
  return labelsOf(hostname).slice(1).join(".");
}

/** True on the control-plane host itself (`cloud.<base>`), where sign-in is allowed to run. */
export function isControlPlaneHost(hostname: string = window.location.hostname): boolean {
  return labelsOf(hostname)[0] === CONTROL_PLANE_LABEL;
}

/**
 * True on a host whose leftmost label names an org — the case that cannot sign in locally.
 *
 * Mirrors the server's rule (auth/middleware.py `_requested_org_from_host`): three or more
 * labels, leftmost not `cloud`. An apex or bare host resolves to no org and is not an org host.
 */
export function isOrgSubdomainHost(hostname: string = window.location.hostname): boolean {
  if (isIpLiteral(hostname)) return false;
  const labels = labelsOf(hostname);
  return labels.length >= 3 && labels[0] !== CONTROL_PLANE_LABEL;
}

/** The org named by the current Host, i.e. the leftmost label. Only valid on an org host. */
export function orgFromHost(hostname: string = window.location.hostname): string {
  if (!isOrgSubdomainHost(hostname)) {
    throw new Error(`orgFromHost called on a non-org host: ${hostname}`);
  }
  return labelsOf(hostname)[0];
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
 * True when `origin` is a host of THIS deployment — same scheme, same port, and a single
 * label in front of the same base domain. The relay answers token requests only from these,
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
  if (isIpLiteral(url.hostname)) return false;
  const selfBase = baseDomain(self.hostname);
  if (!selfBase) return false;
  return labelsOf(url.hostname).length >= 3 && baseDomain(url.hostname) === selfBase;
}
