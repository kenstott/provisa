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
 * Whether a session acting as ``acting`` owns ``product``: one of its roles is the product's
 * owner_role, or holds data_product_rw reaching the product's domain (a role's domain_access
 * naming it, or "*").
 *
 * SEAM: REQ-2219 makes ownership an entitlement -- data_product_owner on the product, which its
 * creator holds from creation -- and removes owner_role. When the entitlement store reaches the
 * UI (J916/J942) this reads the session's data_product_owner (and data_product_rw) entitlements on
 * the product instead; its callers stay as they are.
 */
export function isProductOwner(
  product: { ownerRole?: string | null; domainId: string },
  acting: readonly ActingRole[],
): boolean {
  return acting.some(
    (r) =>
      (product.ownerRole != null && r.id === product.ownerRole) ||
      (r.effectiveCapabilities.includes("data_product_rw") &&
        (r.domain_access.includes("*") || r.domain_access.includes(product.domainId))),
  );
}
