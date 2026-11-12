// Copyright (c) 2026 Kenneth Stott
// Canary: e4a7c219-6b3d-4f85-9c02-b1d8f6a3e570
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** A data product panel's tabs, in order, the same for its read view and its edit view; Requests
 *  is its owner's alone (REQ-2180 OFFERS). */
export const PRODUCT_TABS = [
  "offers",
  "quality",
  "profile",
  "lineage",
  "related",
  "requests",
] as const;
export type ProductTab = (typeof PRODUCT_TABS)[number];
