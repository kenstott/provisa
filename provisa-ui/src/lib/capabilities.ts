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
 * Does this capability set confer `cap`? `admin` is the platform wildcard (rights.py
 * PLATFORM_BYPASS_CAPABILITIES), which only platform_admin carries.
 *
 * A plain function rather than only the `useCapability` hook because the same question has to be
 * answered outside a render — choosing which route to ENTER a nav group on cannot call a hook per
 * candidate item.
 */
export function hasCapability(capabilities: string[], cap: Capability): boolean {
  if (capabilities.length === 0) return false;
  return capabilities.includes(cap) || capabilities.includes("admin");
}

/**
 * REQ-1361: a gate that the platform wildcard may not answer, and/or that a second right also
 * opens.
 *
 * `admin` is platform authority, and platform authority is not org authority. Most surfaces are
 * happy to let it stand in — the server lets it stand in too — but a few refuse it by name on the
 * server (an org's secrets answer to the administrator of that org and to nobody above it), and a
 * client gate that keeps saying yes there only buys a page that 403s, or worse, a menu entry that
 * announces the surface exists for this caller when it does not. `strict` makes the client say
 * exactly what the server says.
 *
 * `orCapability` is the other half of the same honesty: a surface can carry two things gated
 * differently — the org's vault under `org_settings`, the deployment's choice of secrets service
 * under `platform_settings` — and the entry belongs in the menu when EITHER is reachable.
 */
export interface CapabilityRequirement {
  capability: Capability;
  strict?: boolean;
  orCapability?: Capability;
}

/**
 * REQ-1602: is `req` a surface this role is being SHOWN rather than given? Read only after
 * `meetsRequirement` says no -- a right that is held needs no explanation of itself. Either half of
 * an `orCapability` pair being demonstrated is enough: the surface is the same surface.
 */
export function isDemonstrated(demonstrated: string[], req: CapabilityRequirement): boolean {
  if (demonstrated.includes(req.capability)) return true;
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
  if (capabilities.length === 0) return false;
  const primary = req.strict
    ? capabilities.includes(req.capability)
    : hasCapability(capabilities, req.capability);
  if (primary) return true;
  return req.orCapability !== undefined && hasCapability(capabilities, req.orCapability);
}
