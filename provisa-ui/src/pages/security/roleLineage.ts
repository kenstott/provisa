// Copyright (c) 2026 Kenneth Stott
// Canary: 25d0e437-7365-4d0c-aafe-7e14f6b45497
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2161: a role's lineage, left to right from the standard roles to the selected role, drawn by
// the one graph component (REQ-2239). Each role is one box: its first line names what kind of role
// it is, the lines below what the role adds, and the selected role's the accumulated set. A line
// joins each parent to its child.

import type { RoleEffective } from "../../api/roleRights";
import type { GraphSpec } from "../../components/provisa-graph/model";

// A meta-role acts as a set of held roles (security/meta_role.py META_PREFIX).
const META_PREFIX = "meta:";

export type RoleKind = "standard" | "custom" | "meta";

export function roleLineageGraph(
  view: RoleEffective,
  kindLabel: (kind: RoleKind) => string,
): GraphSpec {
  const standards = new Set(view.standards);
  const kindOf = (role: string): RoleKind =>
    role.startsWith(META_PREFIX) ? "meta" : standards.has(role) ? "standard" : "custom";
  const nodes = view.chain.map((role) => {
    const rows =
      role === view.role ? view.capabilities.map((c) => c.right).sort() : (view.adds[role] ?? []);
    return {
      id: role,
      type: "role" as const,
      name: role,
      lines: [{ text: kindLabel(kindOf(role)) }, ...rows.map((text) => ({ text }))],
    };
  });
  const edges = view.chain.flatMap((child) =>
    (view.parents[child] ?? []).map((parent) => ({
      id: `${parent}->${child}`,
      source: parent,
      target: child,
      arrow: "target" as const,
    })),
  );
  return { nodes, edges };
}
