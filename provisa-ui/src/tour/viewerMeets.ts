// Copyright (c) 2026 Kenneth Stott
// Canary: 140237d1-a940-4a64-b785-54b6b50092a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { isDemoSandbox } from "../api/demoSandbox";
import { sandboxReadable } from "../components/navGroups";
import { hasCapability } from "../lib/capabilities";
import type { Capability } from "../types/auth";

/** Whether this viewer may open a step's page: its capability, or (REQ-1922) a page a demo
 * sandbox's visitor may read without it, the Platform page's regions. */
export function viewerMeets(
  capabilities: string[],
  orgId: string | null,
): (capability: Capability, route: string) => boolean {
  const sandbox = isDemoSandbox(orgId);
  return (capability, route) =>
    hasCapability(capabilities, capability) || (sandbox && sandboxReadable(route));
}
