// Copyright (c) 2026 Kenneth Stott
// Canary: 1e5b7a83-4c20-4d96-8f3a-b60d2e9c7145
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A role as the server returns it. The server gives a role's definition to a caller holding
// user_management and to the roles the caller itself holds; any other role arrives as its id alone
// (capabilities, domain access and demonstrated rights absent or null).

import type { Capability, Role } from "../types/auth";

export interface RawRole {
  id: string;
  description: string; // REQ-2080
  capabilities?: string[] | null;
  effectiveCapabilities?: string[] | null; // REQ-2155: sent with the definition, as capabilities is
  demonstrated?: string[] | null;
  domain_access?: string[] | null;
  domainAccess?: string[] | null;
  rateLimit?: Role["rateLimit"];
  parents?: string[] | null; // REQ-2155
  standard: boolean; // REQ-2155: the server names it for every role
  residencyValues?: string[] | null; // REQ-1921
  sessionVars?: Role["sessionVars"] | null; // REQ-1682
  inactivityDays?: number | null; // REQ-2077
}

/**
 * The client's `Role`. A role whose definition the server withheld is marked `detailsHidden` and
 * carries empty lists: the marker, not the empty lists, says "unknown". Anything that shows or edits
 * a role's definition must check it; anything that computes a caller's own rights only ever sees
 * roles the caller holds, which are never hidden.
 */
export function normalizeRole(raw: RawRole): Role {
  const domainAccess = raw.domainAccess ?? raw.domain_access;
  if (raw.capabilities == null || raw.effectiveCapabilities == null || domainAccess == null) {
    return {
      id: raw.id,
      description: raw.description,
      capabilities: [],
      effectiveCapabilities: [],
      demonstrated: [],
      domain_access: [],
      standard: raw.standard,
      detailsHidden: true,
    };
  }
  return {
    id: raw.id,
    description: raw.description,
    capabilities: raw.capabilities as Capability[],
    // REQ-2155: the role's own capabilities and every ancestor's; the server sends it with the
    // definition.
    effectiveCapabilities: raw.effectiveCapabilities as Capability[],
    // REQ-1602: rights the role is shown but does not hold.
    demonstrated: (raw.demonstrated ?? []) as Capability[],
    domain_access: domainAccess,
    rateLimit: raw.rateLimit,
    parents: raw.parents ?? [],
    standard: raw.standard,
    residencyValues: raw.residencyValues ?? [],
    sessionVars: raw.sessionVars ?? [],
    inactivityDays: raw.inactivityDays ?? null,
  };
}

/** REQ-2155: the roles a caller holds -- its acting roles and every role they inherit from, the
 *  way the server's role chains resolve them. A parent the caller cannot see is still held. */
export function heldRoles(
  acting: readonly { id: string; parents?: string[] }[],
  known: readonly { id: string; parents?: string[] }[],
): Set<string> {
  const byId = new Map(known.map((r) => [r.id, r]));
  const held = new Set<string>();
  const visit = (id: string, parents: string[] | undefined) => {
    if (held.has(id)) return;
    held.add(id);
    for (const p of parents ?? []) visit(p, byId.get(p)?.parents);
  };
  for (const r of acting) visit(r.id, r.parents ?? byId.get(r.id)?.parents);
  return held;
}
