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
}

export interface ErdEdge {
  type: "rel";
  id: string;
  source: string;
  target: string;
  cardinality: string;
  label: string;
}

export interface ErdElements {
  nodes: Array<{ data: ErdNodeDomain | ErdNodeTable; classes: string }>;
  edges: Array<{ data: ErdEdge; classes: string }>;
}

const SEPARATOR = "─".repeat(18);

function colPrefix(col: TableColumn): string {
  if (col.isPrimaryKey) return "🔑 "; // 🔑
  if (col.isForeignKey) return "⇝ "; // ↝
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

export function buildErdElements(
  tables: RegisteredTable[],
  relationships: Relationship[],
  domains: Domain[],
  collapsedDomains: Set<string>,
  columnDetail: ColumnDetail,
  activeDomain: string | null,
): ErdElements {
  const domainMap = new Map(domains.map((d) => [d.id, d]));

  const filteredTables = activeDomain ? tables.filter((t) => t.domainId === activeDomain) : tables;

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

  const tableNodes: ErdElements["nodes"] = filteredTables
    .filter((t) => !collapsedDomains.has(t.domainId))
    .map((table) => {
      const name = table.alias || table.tableName;
      const { label, lineCount } = buildTableLabel(name, table.columns, columnDetail);
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
        } as ErdNodeTable,
        classes: "erd-table",
      };
    });

  const visibleTableIds = new Set(
    tableNodes.map((n) => (n.data as ErdNodeTable).tableId),
  );

  const edges: ErdElements["edges"] = relationships
    .filter(
      (r) =>
        r.targetTableId != null &&
        visibleTableIds.has(r.sourceTableId) &&
        visibleTableIds.has(r.targetTableId!),
    )
    .map((r) => ({
      data: {
        type: "rel",
        id: `r:${r.id}`,
        source: `t:${r.sourceTableId}`,
        target: `t:${r.targetTableId}`,
        cardinality: r.cardinality,
        label: cardinalityLabel(r.cardinality),
      } as ErdEdge,
      classes: "erd-rel",
    }));

  return { nodes: [...domainNodes, ...tableNodes], edges };
}
