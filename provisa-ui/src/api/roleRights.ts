// Copyright (c) 2026 Kenneth Stott
// Canary: 4a72ac81-bd87-4fdd-a6c9-e754ac6dc29c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2161: a role's effective rights, as GET /admin/roles/{id}/effective answers them -- only for
// a role whose definition the caller may read.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface RoleEffective {
  role: string;
  standard: boolean;
  /** The role and every ancestor, depth-first in declared parent order. */
  chain: string[];
  /** Each role in the chain → its parents; a meta-role's are its members. */
  parents: Record<string, string[]>;
  /** What each role in the chain adds: the rights none of its ancestors holds. */
  adds: Record<string, string[]>;
  /** The standard roles in the chain. */
  standards: string[];
  /** Every capability the role holds, with the role it enters at. */
  capabilities: { right: string; from: string }[];
}

export async function fetchRoleEffective(roleId: string): Promise<RoleEffective> {
  const resp = await fetch(`${API_BASE}/admin/roles/${encodeURIComponent(roleId)}/effective`);
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Effective rights", resp.status)));
  }
  return resp.json();
}
