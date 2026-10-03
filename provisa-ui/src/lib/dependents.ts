// Copyright (c) 2026 Kenneth Stott
// Canary: a9f33a43-a6d6-402c-88f6-d49c4be65631
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { MutationResult } from "../types/admin";

/** One object that still refers to the object a delete named (REQ-1918). */
export interface Dependent {
  kind: string;
  id: string | number;
  name: string;
  via: string[];
  /** A column grant's table: what the grant is revoked through (revokeRoleFromTable). */
  table_id?: number;
  /** A role assignment's holder: what the assignment is removed from. */
  user_id?: string;
}

/** The dependents a refused delete reported, or null when the result is not such a refusal.
 *  A delete is refused while anything depends on the object: the server answers with a code
 *  ending in `_has_dependents` (a calendar's is `calendar_in_use`) and lists them in `params`. */
export function dependentsOf(result: MutationResult | null | undefined): Dependent[] | null {
  if (!result || result.success) return null;
  const listed = result.params?.dependents;
  if (!Array.isArray(listed) || listed.length === 0) return null;
  return listed as Dependent[];
}
