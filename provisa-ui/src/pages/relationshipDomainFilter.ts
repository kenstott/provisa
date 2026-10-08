// Copyright (c) 2026 Kenneth Stott
// Canary: 7ceaadf4-d0d8-4f58-9364-8334348aeab2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The navbar's domain multi-select narrows the Relationships list: a relationship stays when its
// source, target or owner domain is checked. No selection, or every domain selected, is no
// narrowing (as on the Tables page). `narrowTo` holds normalized domain ids.
export function relationshipInCheckedDomains(
  narrowTo: ReadonlySet<string> | null,
  domains: ReadonlyArray<string | null | undefined>,
): boolean {
  if (narrowTo === null) return true;
  return domains.some((d) => d != null && narrowTo.has(d));
}
