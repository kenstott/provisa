// Copyright (c) 2026 Kenneth Stott
// Canary: 8da6e434-6cf4-4b3f-9092-18cb2e837c3d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the kinds of entitlement list a role grant carries, mirroring KINDS in
// provisa/entitlements/index.py. The write API refuses any other kind by code
// (entitlements.unknown_kind), so a kind added there and not here is refused, never sent wrong.

/** Every kind, in the order the UI lists them: the semantic hierarchy first, then the rest. */
export const ENTITLEMENT_KINDS = [
  "domains",
  "tables",
  "columns",
  "metrics",
  "relationships",
  "commands",
  "data_products",
  "sources",
  "environments",
  "roles",
  "regions",
] as const;

export type EntitlementKind = (typeof ENTITLEMENT_KINDS)[number];

/**
 * How deep the names tree goes for a kind picked from it: domains are its first level, tables its
 * second, columns its third. A shorter identifier selects what is beneath it, so checking a node
 * includes its children.
 */
export const TREE_DEPTH: Partial<Record<EntitlementKind, 1 | 2 | 3>> = {
  domains: 1,
  tables: 2,
  columns: 3,
};

/** The kinds whose lists the expiry of a list can end (SEMANTIC_LISTS on the server). */
export const SEMANTIC_KINDS: ReadonlySet<string> = new Set([
  "domains",
  "tables",
  "columns",
  "metrics",
  "relationships",
  "commands",
  "data_products",
]);

/** Everything of the kind, including objects added later. */
export const WILDCARD = "*";

export function isKind(kind: string): kind is EntitlementKind {
  return (ENTITLEMENT_KINDS as readonly string[]).includes(kind);
}
