// Copyright (c) 2026 Kenneth Stott
// Canary: 5f2a8c19-3d6e-4b71-9e04-a7c6d1b38e52
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1591 / REQ-1723: the navbar's checked domains, as the explorers read them. The selection is
// a VIEW preference: it narrows what is shown and nothing else. Every domain checked, or none, is
// no narrowing.

import { useDomainFilter } from "../context/DomainFilterContext";

/** The domains a view is narrowed to: ``checked`` when domains are on and some but not all of
 * ``all`` are checked, else null (no narrowing). ``checked`` itself is returned, so its identity
 * is stable enough to be a refresh dependency. */
export function viewDomainsOf(
  all: readonly string[],
  checked: ReadonlySet<string>,
  enabled: boolean,
): ReadonlySet<string> | null {
  return enabled && checked.size > 0 && checked.size < all.length ? checked : null;
}

/** The navbar's narrowing (viewDomainsOf), or null. */
export function useViewDomains(): ReadonlySet<string> | null {
  const { domains, checkedDomains, domainsEnabled } = useDomainFilter();
  return viewDomainsOf(domains, checkedDomains, domainsEnabled);
}
