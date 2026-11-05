// Copyright (c) 2026 Kenneth Stott
// Canary: 25d0e437-7365-4d0c-aafe-7e14f6b45497
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2161: a role's lineage as the lineage diagram draws it, left to right from the standard
// roles to the selected role. Each role is a box: its first row names what kind of role it is and
// carries the parent-to-child edges; the rows below are what the role adds, and the selected
// role's are the accumulated set.

import type { LineageGraphData, LineageNode } from "../../api/lineage";
import type { RoleEffective } from "../../api/roleRights";

// A meta-role acts as a set of held roles (security/meta_role.py META_PREFIX).
const META_PREFIX = "meta:";

export type RoleKind = "standard" | "custom" | "meta";

const kindRow = (role: string) => `${role}::kind`;

export function roleLineageGraph(
  view: RoleEffective,
  kindLabel: (kind: RoleKind) => string,
): LineageGraphData {
  const standards = new Set(view.standards);
  const kindOf = (role: string): RoleKind =>
    role.startsWith(META_PREFIX) ? "meta" : standards.has(role) ? "standard" : "custom";
  const node = (role: string, id: string, column: string): LineageNode => ({
    id,
    column,
    relation: role,
    kind: standards.has(role) ? "source" : "derived",
    materialized: false,
  });
  const nodes: LineageNode[] = [];
  for (const role of view.chain) {
    nodes.push(node(role, kindRow(role), kindLabel(kindOf(role))));
    const rows =
      role === view.role ? view.capabilities.map((c) => c.right).sort() : (view.adds[role] ?? []);
    for (const right of rows) nodes.push(node(role, `${role}::${right}`, right));
  }
  const edges = view.chain.flatMap((child) =>
    (view.parents[child] ?? []).map((parent) => ({
      source: kindRow(parent),
      target: kindRow(child),
      transform: "",
      ops: [],
    })),
  );
  return {
    nodes,
    edges,
    outputs: nodes.filter((n) => n.relation === view.role).map((n) => n.id),
  };
}
