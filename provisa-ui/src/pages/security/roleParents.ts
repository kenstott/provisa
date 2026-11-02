// Copyright (c) 2026 Kenneth Stott
// Canary: 7a2e5c41-9d06-4f38-b1c7-3e8f60a2d915
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2155: a role narrows its parents. The form offers what they hold, all selected, for a person
// to unselect, and shows the rest disabled. What each role holds comes from the server, which
// worked it out by the one rule (security/inheritance.py); nothing is worked out here.

import type { Capability, Role } from "../../types/auth";

const ALL_DOMAINS = "*";
const RESIDENCY: Capability = "data_residency";

/** What a role's parents hold, none selected meaning no parent. */
export interface ParentRights {
  capabilities: ReadonlySet<string>;
  domains: ReadonlySet<string>;
  regions: ReadonlySet<string>;
}

export function parentRights(parents: string[], roles: Role[]): ParentRights | null {
  if (parents.length === 0) return null;
  const held = roles.filter((r) => parents.includes(r.id));
  return {
    capabilities: new Set(held.flatMap((r) => r.effectiveCapabilities ?? [])),
    domains: new Set(held.flatMap((r) => r.effectiveDomains ?? [])),
    regions: new Set(held.flatMap((r) => r.effectiveResidencyValues ?? [])),
  };
}

/** Whether a role with these parents may be given this domain. */
export function reachable(domain: string, held: ParentRights | null): boolean {
  return held === null || held.domains.has(ALL_DOMAINS) || held.domains.has(domain);
}

function merged<T extends string>(
  current: T[],
  held: ReadonlySet<string>,
  before: ReadonlySet<string> | undefined,
): T[] {
  const kept = current.filter((value) => held.has(value));
  const gained = [...held].filter((value) => !before?.has(value)) as T[];
  return [...new Set([...kept, ...gained])];
}

/** The form with ``parents`` chosen: what no parent holds goes, and what a parent newly brings is
 *  selected for a person to unselect. With no parent left the form keeps what it has. */
export function withParents<
  F extends {
    parents: string[];
    capabilities: Capability[];
    domainAccess: string[];
    residencyValues: string[];
  },
>(form: F, parents: string[], roles: Role[]): F {
  const before = parentRights(form.parents, roles);
  const held = parentRights(parents, roles);
  if (held === null) return { ...form, parents };
  const capabilities = merged(form.capabilities, held.capabilities, before?.capabilities);
  const domainAccess = held.domains.has(ALL_DOMAINS)
    ? before?.domains.has(ALL_DOMAINS)
      ? form.domainAccess
      : [ALL_DOMAINS]
    : merged(
        form.domainAccess.includes(ALL_DOMAINS) ? [...held.domains] : form.domainAccess,
        held.domains,
        before?.domains,
      );
  return {
    ...form,
    parents,
    capabilities,
    domainAccess,
    residencyValues: capabilities.includes(RESIDENCY)
      ? merged(form.residencyValues, held.regions, before?.regions)
      : [],
  };
}
