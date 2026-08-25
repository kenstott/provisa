// Copyright (c) 2026 Kenneth Stott
// Canary: f2a8c1d7-3e9b-4f5a-8c2d-1b6e7a4f9c3d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { RegisteredTable, Relationship, Domain, TableColumn } from "../../types/admin";
import { cypherRelType } from "../../naming";

export type ColumnDetail = "all" | "key" | "none";

export interface ErdNodeDomain {
  type: "domain";
  id: string;
  domainId: string;
  label: string;
  description: string;
}

export interface ErdNodeTable {
  type: "table";
  id: string;
  parent: string;
  displayLabel: string;
  lineCount: number;
  domainId: string;
  tableId: number;
  tableName: string;
  description: string;
  columns: TableColumn[];
  // REQ-1588: this table is declared as the junction of at least one relationship, so it is drawn
  // as an edge waypoint (diamond) rather than an entity.
  junction: boolean;
}

export interface ErdEdge {
  type: "rel";
  id: string;
  source: string;
  target: string;
  cardinality: string;
  label: string;
  // REQ-1588: a junction-backed relationship draws two legs through the junction table's node
  // instead of one direct line. The legs carry cardinality in `label`; the relationship type is
  // written once per path, at the junction end of the inbound leg, as `pathLabel`. `pathType`
  // identifies the path so the several relationships sharing one junction stay distinct edges
  // rather than deduplicating into a single pair of legs.
  pathLabel: string;
  pathType: string;
  via: boolean;
  proxy: boolean;
}

export interface ErdElements {
  nodes: Array<{ data: ErdNodeDomain | ErdNodeTable; classes: string }>;
  edges: Array<{ data: ErdEdge; classes: string }>;
}

const SEPARATOR = "─".repeat(18);

function colPrefix(col: TableColumn): string {
  if (col.isPrimaryKey) return "🔑 ";
  if (col.isForeignKey) return "⇝ ";
  return "  ";
}

export function buildTableLabel(
  name: string,
  columns: TableColumn[],
  columnDetail: ColumnDetail,
): { label: string; lineCount: number } {
  if (columnDetail === "none") return { label: name, lineCount: 1 };

  const cols =
    columnDetail === "key" ? columns.filter((c) => c.isPrimaryKey || c.isForeignKey) : columns;

  if (cols.length === 0) return { label: name, lineCount: 1 };

  const colLines = cols.map((c) => `${colPrefix(c)}${c.computedSqlAlias || c.columnName}`);
  const lines = [name, SEPARATOR, ...colLines];
  return { label: lines.join("\n"), lineCount: lines.length };
}

function cardinalityLabel(cardinality: string): string {
  switch (cardinality) {
    case "one_to_many":
      return "1:N";
    case "many_to_one":
      return "N:1";
    case "many_to_many":
      return "N:M";
    case "one_to_one":
      return "1:1";
    default:
      return cardinality;
  }
}

// Resolve which Cytoscape node id an endpoint maps to, given the collapsed/hidden state.
// Returns null if the table's domain is hidden (not rendered at all).
function resolveEndpoint(
  tableId: number,
  tableMap: Map<number, RegisteredTable>,
  visibleTableIds: Set<number>,
  collapsedDomains: Set<string>,
  hiddenDomains: Set<string>,
): string | null {
  if (visibleTableIds.has(tableId)) return `t:${tableId}`;
  const table = tableMap.get(tableId);
  if (!table) return null;
  if (hiddenDomains.has(table.domainId)) return null;
  if (collapsedDomains.has(table.domainId)) return `d:${table.domainId}`;
  return null;
}

export function buildErdElements(
  tables: RegisteredTable[],
  relationships: Relationship[],
  domains: Domain[],
  collapsedDomains: Set<string>,
  hiddenDomains: Set<string>,
  columnDetail: ColumnDetail,
): ErdElements {
  const domainMap = new Map(domains.map((d) => [d.id, d]));
  const tableMap = new Map(tables.map((t) => [t.id, t]));

  const filteredTables = tables.filter((t) => !hiddenDomains.has(t.domainId));

  const usedDomainIds = new Set(filteredTables.map((t) => t.domainId));

  const domainNodes: ErdElements["nodes"] = [...usedDomainIds].map((domainId) => {
    const domain = domainMap.get(domainId);
    return {
      data: {
        type: "domain",
        id: `d:${domainId}`,
        domainId,
        label: domainId,
        description: domain?.description ?? "",
      } as ErdNodeDomain,
      classes: "erd-domain",
    };
  });

  // REQ-1588: tables declared as a junction are waypoints on an edge, not entities. They carry the
  // relationship's attributes, but the ERD shows only the table's name — the path types that run
  // through it are written on the legs, and one node cannot hold several of them.
  const junctionTableIds = new Set(
    relationships.map((r) => r.viaTableId).filter((id): id is number => id != null),
  );

  const tableNodes: ErdElements["nodes"] = filteredTables
    .filter((t) => !collapsedDomains.has(t.domainId))
    .map((table) => {
      const name = table.alias || table.tableName;
      const junction = junctionTableIds.has(table.id);
      const { label, lineCount } = junction
        ? { label: name, lineCount: 1 }
        : buildTableLabel(name, table.columns, columnDetail);
      return {
        data: {
          type: "table",
          id: `t:${table.id}`,
          parent: `d:${table.domainId}`,
          displayLabel: label,
          lineCount,
          domainId: table.domainId,
          tableId: table.id,
          tableName: name,
          description: table.description ?? "",
          columns: table.columns,
          junction,
        } as ErdNodeTable,
        classes: junction ? "erd-table erd-junction" : "erd-table",
      };
    });

  const visibleTableIds = new Set(tableNodes.map((n) => (n.data as ErdNodeTable).tableId));

  // Build edges, routing through domain proxy nodes when a table is collapsed.
  const seenEdges = new Set<string>();
  const edges: ErdElements["edges"] = [];

  const pushEdge = (
    r: Relationship,
    src: string,
    tgt: string,
    opts: { label: string; pathLabel: string; pathType: string; leg: "" | "in" | "out" },
  ) => {
    if (src === tgt) return;
    const isProxy = src.startsWith("d:") || tgt.startsWith("d:");
    // Deduplicate by pair + label — allows multiple distinct relationships between the same
    // collapsed-domain proxy pair to show as separate dashed edges. The path identity is part of
    // the key so the legs of two relationships through one junction do not merge.
    const key = `${src}→${tgt}:${opts.label}:${opts.pathType}:${opts.leg}`;
    if (seenEdges.has(key)) return;
    seenEdges.add(key);
    const classes = ["erd-rel"];
    if (isProxy) classes.push("erd-rel--proxy");
    if (opts.leg) classes.push("erd-rel--via");
    edges.push({
      data: {
        type: "rel",
        id: isProxy ? `rp:${key}` : `r:${r.id}${opts.leg ? `:${opts.leg}` : ""}`,
        source: src,
        target: tgt,
        cardinality: r.cardinality,
        label: opts.label,
        pathLabel: opts.pathLabel,
        pathType: opts.pathType,
        via: opts.leg !== "",
        proxy: isProxy,
      } as ErdEdge,
      classes: classes.join(" "),
    });
  };

  for (const r of relationships) {
    if (r.targetTableId == null) continue;

    const src = resolveEndpoint(
      r.sourceTableId,
      tableMap,
      visibleTableIds,
      collapsedDomains,
      hiddenDomains,
    );
    const tgt = resolveEndpoint(
      r.targetTableId,
      tableMap,
      visibleTableIds,
      collapsedDomains,
      hiddenDomains,
    );

    if (!src || !tgt) continue;

    // REQ-1588: a declared junction is drawn, so the two entities connect through its node rather
    // than by a line that hides it. Only its own node will do: routing the path through a domain
    // proxy would claim a hop that the collapsed domain does not describe, so a junction that is
    // not rendered as a table collapses back to the direct A→B edge instead.
    const junctionNode =
      r.viaTableId == null
        ? null
        : visibleTableIds.has(r.viaTableId)
          ? `t:${r.viaTableId}`
          : null;

    if (junctionNode) {
      // The type is written once per path, beside the junction. A row whose nomination names no
      // type is a defective row (REQ-1586), and it gets no label rather than a substituted one.
      const pathType = cypherRelType(r) ?? "";
      const leg = cardinalityLabel(r.cardinality);
      pushEdge(r, src, junctionNode, { label: leg, pathLabel: pathType, pathType, leg: "in" });
      pushEdge(r, junctionNode, tgt, { label: leg, pathLabel: "", pathType, leg: "out" });
      continue;
    }

    if (src === tgt) continue;

    // A junction that is not on the canvas still names the edge it backs, so the collapsed form
    // carries the same relationship type the two legs would have shown.
    const junctionLabel = r.viaTableId == null ? null : cypherRelType(r);
    const label =
      junctionLabel ?? (r.alias || r.computedCypherAlias || cardinalityLabel(r.cardinality));
    pushEdge(r, src, tgt, { label, pathLabel: "", pathType: "", leg: "" });
  }

  return { nodes: [...domainNodes, ...tableNodes], edges };
}
