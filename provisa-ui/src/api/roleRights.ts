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
  /** Every capability the role holds, with the roles it enters at: more than one when it enters
   *  in several branches. */
  capabilities: { right: string; from: string[] }[];
  /** Per table it reaches, its access to each column the caller may read. */
  data: RoleTableAccess[];
}

/** REQ-2161/2162: what a role does with one column, and the grant behind it (none named without
 *  the governance right). */
export interface ColumnAccess {
  read: boolean;
  write: ("insert" | "update" | "delete")[];
  mask: { type: string } | null;
  row_rule: string | null;
  /** The role the column's grant names, or "*" for every role; null when not shown. */
  granted_by: string | null;
  /** The path from the role up to granted_by. */
  via: string[];
  reach: "domain" | "public";
}

/** REQ-2161: one table a role reaches, with the columns it has access to. */
export interface RoleTableAccess {
  table_id: number;
  domain: string;
  /** As the catalog names it: domain, then alias or table name. */
  name: string;
  table_name: string;
  reach: "domain" | "across";
  columns: ({ name: string; scope: string } & ColumnAccess)[];
}

/** REQ-2162: who can read or write each column of a table. */
export interface TableAccess {
  table_id: number;
  domain: string;
  name: string;
  table_name: string;
  columns: {
    name: string;
    scope: string;
    access: ({ role: string; standard: boolean } & ColumnAccess)[];
  }[];
}

export async function fetchTableAccess(tableId: number): Promise<TableAccess> {
  const resp = await fetch(`${API_BASE}/admin/tables/${tableId}/access`);
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Column access", resp.status)));
  }
  return resp.json();
}

export async function fetchRoleEffective(roleId: string): Promise<RoleEffective> {
  const resp = await fetch(`${API_BASE}/admin/roles/${encodeURIComponent(roleId)}/effective`);
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(data, requestFailed("Effective rights", resp.status)));
  }
  return resp.json();
}
