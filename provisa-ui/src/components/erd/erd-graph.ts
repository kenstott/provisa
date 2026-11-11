// Copyright (c) 2026 Kenneth Stott
// Canary: 1c7a3e9d-4b82-4f65-a0c3-9d6e2b8f5a17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: the ERD as the one graph draws it -- its domains the groups, its tables the nodes (a
// table or a view; a junction, REQ-1588), its relationships the lines. What each is, not how it
// looks: that is ProvisaGraph's.

import type { ListedTable } from "../../types/admin";
import type { GraphSpec } from "../provisa-graph/model";
import { columnLine, type ErdElements, type ErdNodeDomain, type ErdNodeTable } from "./erd-model";

/** The arrowheads of a relationship's line: at the "many" end, and at both ends of one read from
 * both sides. */
function arrowOf(cardinality: string, bidirectional: boolean): "target" | "both" {
  return bidirectional || cardinality === "many-to-many" || cardinality === "many-to-one"
    ? "both"
    : "target";
}

export function erdGraph(elements: ErdElements, tables: ListedTable[]): GraphSpec {
  const views = new Set(tables.filter((t) => Boolean(t.viewSql)).map((t) => t.id));
  const data = elements.nodes.map((n) => n.data);
  return {
    groups: data
      .filter((d): d is ErdNodeDomain => d.type === "domain")
      .map((d) => ({
        id: d.id,
        label: d.label,
        description: d.description,
        hue: d.domainId,
        data: { domainId: d.domainId },
      })),
    nodes: data
      .filter((d): d is ErdNodeTable => d.type === "table")
      .map((d) => ({
        id: d.id,
        type: d.junction ? "junction" : views.has(d.tableId) ? "view" : "table",
        name: d.tableName,
        parent: d.parent,
        description: d.description,
        lines: d.junction
          ? []
          : d.columns.map((c) => ({ text: columnLine(c), key: c.isPrimaryKey || c.isForeignKey })),
        data: { tableId: d.tableId, tableName: d.tableName, domainId: d.domainId },
      })),
    edges: elements.edges.map(({ data: e }) => ({
      id: e.id,
      source: e.source,
      target: e.target,
      label: e.label,
      arrow: arrowOf(e.cardinality, e.bidirectional),
      dashed: e.proxy,
      ...(e.via ? { tone: "via" as const } : {}),
      data: { relationshipId: e.relationshipId },
    })),
  };
}
