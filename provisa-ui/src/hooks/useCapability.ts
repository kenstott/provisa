// Copyright (c) 2025 Kenneth Stott
// Canary: 89a957f9-2815-45ba-8ec3-8b5bf65dcc48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useAuth } from "../context/AuthContext";
import type { Capability } from "../types/auth";

/** Check if unioned capabilities include a capability (admin has all). */
export function useCapability(cap: Capability): boolean {
  const { capabilities } = useAuth();
  if (capabilities.length === 0) return false;
  return capabilities.includes(cap) || capabilities.includes("admin");
}

/** Check multiple capabilities — returns true if unioned capabilities have ALL. */
export function useCapabilities(caps: Capability[]): boolean {
  const { capabilities } = useAuth();
  if (capabilities.length === 0) return false;
  if (capabilities.includes("admin")) return true;
  return caps.every((c) => capabilities.includes(c));
}
