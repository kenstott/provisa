// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** REQ-2180 (amended 2026-11-11, THE PRODUCT PAGE IS SUMMARY ROWS): a data product page's sections,
 *  in their fixed order, each a row that expands in place; Requests is its owner's alone. The
 *  same rows for the read view and the edit view. */
export const PRODUCT_SECTIONS = [
  "offers",
  "quality",
  "profile",
  "lineage",
  "related",
  "requests",
  "access",
] as const;
export type ProductSection = (typeof PRODUCT_SECTIONS)[number];
