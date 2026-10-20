// Copyright (c) 2026 Kenneth Stott
// Canary: 9b4f1e27-3c58-4a06-8d92-6e0a7c5b1f38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2116: every element of the lineage graph links to where it is defined -- a native box to
// its source, a table or its column to the table, a view or MV to its definition, a command to the
// command -- as far as the user's own rights open that page. A built-in SQL function has no page.

import type { LineageGraphData } from "../../api/lineage";
import { HIDING_RIGHTS, type CapabilityRequirement } from "../../lib/capabilities";
import { relationName } from "../../naming";
import { DERIVED_SOURCE_ID } from "../../types/admin";

export interface LineageLinks {
  /** Where a box's relation is defined. */
  relation(relation: string): string | null;
  /** Where a field is defined: its command's, else its relation's. */
  node(nodeId: string): string | null;
  /** Where a command is defined. */
  command(name: string): string | null;
}

interface LinkedTable {
  sourceId: string;
  domainId: string;
  tableName: string;
}

/** The capability each page's route asks (App.tsx), so a link is offered only where it opens. */
const SOURCES: CapabilityRequirement = { capability: "source_registration" };
const TABLES: CapabilityRequirement = { capability: "table_registration", anyOf: HIDING_RIGHTS };
const VIEWS: CapabilityRequirement = { capability: "table_registration" };
const COMMANDS: CapabilityRequirement = { capability: "table_registration" };

export function lineageLinks(
  graph: LineageGraphData,
  tables: readonly LinkedTable[],
  can: (req: CapabilityRequirement) => boolean,
): LineageLinks {
  const byRelation = new Map(tables.map((t) => [relationName(t), t]));
  const nodes = new Map(graph.nodes.map((n) => [n.id, n]));
  // A command call's output field is named by the call's alias; the command is the one its
  // incoming edges name.
  const commandOf = new Map<string, string>();
  for (const e of graph.edges) {
    const op = e.ops.find((o) => o.kind === "command");
    if (op) commandOf.set(e.target, op.name);
  }
  const command = (name: string) =>
    can(COMMANDS) ? `/commands?command=${encodeURIComponent(name)}` : null;
  const relation = (rel: string) => {
    const native = graph.natives?.[rel];
    if (native) return can(SOURCES) ? `/sources?expanded=${encodeURIComponent(native.source)}` : null;
    const table = byRelation.get(rel);
    if (!table) return null;
    const open = encodeURIComponent(`${table.sourceId}.${table.tableName}`);
    if (table.sourceId === DERIVED_SOURCE_ID) return can(VIEWS) ? `/views?open=${open}` : null;
    return can(TABLES) ? `/tables?open=${open}` : null;
  };
  return {
    relation,
    command,
    node(nodeId) {
      const node = nodes.get(nodeId);
      if (!node) return null;
      if (node.kind === "command") {
        const name = commandOf.get(nodeId);
        return name ? command(name) : null;
      }
      return node.relation ? relation(node.relation) : null;
    },
  };
}
