// Copyright (c) 2026 Kenneth Stott
// Canary: 6f0b3d18-72c4-4e9a-b5d1-90ae7c3f214b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Capability } from "../types/auth";

/**
 * Does this capability set confer `cap`? Only by holding it: no capability stands in for another
 * (REQ-1327), exactly as on the server, where every gate names the right it reads.
 *
 * A plain function rather than only the `useCapability` hook because the same question has to be
 * answered outside a render — choosing which route to ENTER a nav group on cannot call a hook per
 * candidate item.
 */
export function hasCapability(capabilities: string[], cap: Capability): boolean {
  return capabilities.includes(cap);
}

/**
 * REQ-1361: a gate that a second right also opens.
 *
 * A surface can carry two things gated differently — the org's vault under `org_settings`, the
 * deployment's choice of secrets service under `platform_settings` — and the entry belongs in the
 * menu when EITHER is reachable. Platform authority is not org authority: a platform administrator
 * reaches the second half and never the first, because the client says exactly what the server
 * says and neither lets one right answer for another.
 */
export interface CapabilityRequirement {
  capability: Capability;
  orCapability?: Capability;
  /** REQ-1944: further rights, any one of which also opens the surface. */
  anyOf?: Capability[];
}

/**
 * REQ-1944: the governance rights that change how a table's columns are hidden -- grants, masks,
 * sensitive columns' fakes and tags. Each opens the Tables surface without `table_registration`;
 * the holder edits only those fields, in the domains its roles reach.
 */
export const HIDING_RIGHTS: Capability[] = [
  "masking_config",
  "column_grant",
  "access_config",
  "sensitive_data",
];

/** REQ-1944: who reaches the Tables surface -- the table editor, or a holder of a hiding right. */
export const TABLES_SURFACE: CapabilityRequirement = {
  capability: "table_registration",
  anyOf: HIDING_RIGHTS,
};

/**
 * REQ-1944: the domains a holder of the hiding rights governs -- the union of `domain_access`
 * over the roles that CARRY one of them (never every role held, as on the server), or `null`
 * when one of those roles reaches every domain.
 */
export function hidingDomains(
  roles: { capabilities: string[]; domain_access: string[] }[],
): Set<string> | null {
  const out = new Set<string>();
  for (const role of roles) {
    if (!role.capabilities.some((c) => (HIDING_RIGHTS as string[]).includes(c))) continue;
    if (role.domain_access.includes("*")) return null;
    for (const d of role.domain_access) out.add(d);
  }
  return out;
}

/**
 * REQ-1602: is `req` a surface this role is being SHOWN rather than given? Read only after
 * `meetsRequirement` says no -- a right that is held needs no explanation of itself. Either half of
 * an `orCapability` pair being demonstrated is enough: the surface is the same surface.
 */
export function isDemonstrated(demonstrated: string[], req: CapabilityRequirement): boolean {
  if (demonstrated.includes(req.capability)) return true;
  if ((req.anyOf ?? []).some((c) => demonstrated.includes(c))) return true;
  return req.orCapability !== undefined && demonstrated.includes(req.orCapability);
}

/**
 * REQ-1602: what a SET of roles is shown but not given -- the union of their `demonstrated` lists,
 * minus everything the same set actually holds. A right one selected role withholds and another
 * grants is simply held: selecting both is holding both, and a held right is used rather than
 * explained.
 */
export function unionDemonstrated(
  roles: { demonstrated: Capability[] }[],
  held: Capability[],
): Capability[] {
  const set = new Set<Capability>();
  for (const r of roles) {
    for (const c of r.demonstrated) set.add(c);
  }
  for (const c of held) set.delete(c);
  return [...set];
}

/** Does this capability set open a surface described by `req`? */
export function meetsRequirement(capabilities: string[], req: CapabilityRequirement): boolean {
  if (hasCapability(capabilities, req.capability)) return true;
  if ((req.anyOf ?? []).some((c) => hasCapability(capabilities, c))) return true;
  return req.orCapability !== undefined && hasCapability(capabilities, req.orCapability);
}
