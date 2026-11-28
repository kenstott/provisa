// Copyright (c) 2026 Kenneth Stott
// Canary: 28195c3f-a35d-4bab-bbf3-b389ba4cc82f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1659 (amended 2026-11-28, supersedes the marketplace-only domain list): the Data Products
// page's list and its navbar Domains filter follow the active role, as one rule taken as a union.
// Each domain where the role holds data product read or write contributes that domain and ALL its
// products, whatever their status; every product offered in the marketplace (status live)
// contributes its domain and that product. A meta role is the union of its roles.

import { domainsCarrying } from "../../lib/capabilities";
import type { DataProduct } from "../../types/admin";

/** The rights that bring a domain's every product into the list. */
export const PRODUCT_RIGHTS: readonly string[] = ["data_product_read", "data_product_rw"];

/** The status of a product offered in the marketplace. */
const OFFERED = "live";

export interface ProductScope {
  /** The products the list shows, in the order read. */
  products: DataProduct[];
  /** The navbar's domains on this page, sorted. */
  domains: string[];
}

/**
 * The products and domains ``roles`` (the active role set) see on the Data Products page.
 * A role reaching every domain with a product right (`*`) brings in every product read, and their
 * domains: the products read are all the domains it names here.
 */
export function productScope(
  products: readonly DataProduct[],
  roles: readonly { effectiveCapabilities: string[]; domain_access: string[] }[],
): ProductScope {
  const reach = domainsCarrying(roles, PRODUCT_RIGHTS);
  const shown = products.filter(
    (p) => p.status === OFFERED || reach === null || reach.has(p.domainId),
  );
  const domains = new Set(shown.map((p) => p.domainId));
  for (const d of reach ?? []) domains.add(d);
  return { products: shown, domains: [...domains].sort() };
}
