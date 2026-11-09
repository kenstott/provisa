// Copyright (c) 2026 Kenneth Stott
// Canary: 3b9e1c47-58a2-4d06-b7f3-2e6c0a94d1f8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { hasCapability } from "../../lib/capabilities";
import type { Capability } from "../../types/auth";

export type CacheTabKey = "response" | "hot" | "materialized" | "redirect";

// REQ-1327: each tab of the Cache page is one right's: the org's cache settings are org_settings,
// the refresh and enable of its materialized views table_registration, the rights their mutations
// ask. In page order.
const TAB_RIGHT: [CacheTabKey, Capability][] = [
  ["response", "org_settings"],
  ["hot", "org_settings"],
  ["materialized", "table_registration"],
  ["redirect", "org_settings"],
];

/** The tabs a caller is offered: those whose right it holds, or is shown (REQ-1602). */
export function cacheTabsFor(capabilities: string[], demonstrated: string[]): CacheTabKey[] {
  return TAB_RIGHT.filter(
    ([, right]) => hasCapability(capabilities, right) || demonstrated.includes(right),
  ).map(([key]) => key);
}
