// Copyright (c) 2026 Kenneth Stott
// Canary: 6f0b3d18-72c4-4e9a-b5d1-90ae7c3f214b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Capability } from "../types/auth";

/**
 * Does this capability set confer `cap`? `admin` is the platform wildcard (rights.py
 * PLATFORM_BYPASS_CAPABILITIES), which only platform_admin carries.
 *
 * A plain function rather than only the `useCapability` hook because the same question has to be
 * answered outside a render — choosing which route to ENTER a nav group on cannot call a hook per
 * candidate item.
 */
export function hasCapability(capabilities: string[], cap: Capability): boolean {
  if (capabilities.length === 0) return false;
  return capabilities.includes(cap) || capabilities.includes("admin");
}
