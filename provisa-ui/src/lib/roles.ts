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
import type { Origin } from "../types/admin";

export interface RawRole {
  id: string;
  origin: Origin;
  capabilities?: string[] | null;
  demonstrated?: string[] | null;
  domain_access?: string[] | null;
  domainAccess?: string[] | null;
  rateLimit?: Role["rateLimit"];
  parentRoleId?: string | null;
}

/**
 * The client's `Role`. A role whose definition the server withheld is marked `detailsHidden` and
 * carries empty lists: the marker, not the empty lists, says "unknown". Anything that shows or edits
 * a role's definition must check it; anything that computes a caller's own rights only ever sees
 * roles the caller holds, which are never hidden.
 */
export function normalizeRole(raw: RawRole): Role {
  const domainAccess = raw.domainAccess ?? raw.domain_access;
  if (raw.capabilities == null || domainAccess == null) {
    return {
      id: raw.id,
      origin: raw.origin,
      capabilities: [],
      demonstrated: [],
      domain_access: [],
      detailsHidden: true,
    };
  }
  return {
    id: raw.id,
    origin: raw.origin,
    capabilities: raw.capabilities as Capability[],
    // REQ-1602: rights the role is shown but does not hold.
    demonstrated: (raw.demonstrated ?? []) as Capability[],
    domain_access: domainAccess,
    rateLimit: raw.rateLimit,
    parentRoleId: raw.parentRoleId,
  };
}
