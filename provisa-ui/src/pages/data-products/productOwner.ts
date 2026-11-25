// Copyright (c) 2026 Kenneth Stott
// Canary: 7d2e4b19-3a6c-4f85-9e01-b8c5d3a7f264
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-07, WHAT THE GRAPH DRAWS) and REQ-2219 (OWNERSHIP IS AN ENTITLEMENT):
// who owns a data product, and so sees its Consumers card. No one else sees the card at all.

import type { Role } from "../../types/auth";

/** The roles a session acts as, as far as ownership reads them. */
export type ActingRole = Pick<Role, "id" | "effectiveCapabilities" | "domain_access">;

/**
 * Whether the signed-in person (``userId``), acting as ``acting``, owns ``product``: they are one
 * of its owners (REQ-2219, amended 2026-11-23: a person whose data_product_owner row names it,
 * held through a role they act as that carries data_product_owner), or one of their roles holds
 * data_product_rw reaching the product's domain (a role's domain_access naming it, or "*"). The
 * server decides (``entitlements.owners``); this only says which view to draw.
 */
export function isProductOwner(
  product: { owners: readonly string[]; domainId: string },
  userId: string | null,
  acting: readonly ActingRole[],
): boolean {
  const named = userId !== null && product.owners.includes(userId);
  return acting.some(
    (r) =>
      (named && r.effectiveCapabilities.includes("data_product_owner")) ||
      (r.effectiveCapabilities.includes("data_product_rw") &&
        (r.domain_access.includes("*") || r.domain_access.includes(product.domainId))),
  );
}
