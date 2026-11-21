// Copyright (c) 2026 Kenneth Stott
// Canary: 4e9b1c27-8d3a-4f60-a5e2-7c1d9b0f3a86
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174, REQ-2244: a table's columns read on asking. A term's graph reads only the columns its
// terms reach, with a count of each table's columns (ontologyData's COLUMN_COUNT); a table with
// more is offered to expand, and expanding it reads that table's columns alone, drawn as the
// graph's own columns are while it stays expanded.

import { operation, rootField, rowsOf, type MetaReader } from "../../api/metaGraph";
import type { GraphData } from "./graphModel";
import { COLUMN_FACTS, columnFacts, facts, type ColumnFacts } from "./columnFacts";

/** A table's columns as the focused read answers them (tableColumnsQuery). */
export interface TableColumnsRow {
  id: number;
  tableName: string;
  domainId: string;
  tableColumns?:
    | (ColumnFacts & {
        columnName: string;
        columnKey: string;
        sensitivity?: { tags: string }[] | null;
      })[]
    | null;
}

/** The read of one table's columns, asked for when a reader expands the table in a term's graph,
 * which reads only the columns its terms reach (REQ-2174, REQ-2244): that table alone, each column
 * with the facts the graph's columns carry; null when the role cannot read its columns. */
export function tableColumnsQuery(m: MetaReader, table: string): string | null {
  const field = tableColumnsField(m, [table]);
  return field === null ? null : operation("TableColumns", [field]);
}

/** The root field reading ``tables``' columns (table node ids); null when the role cannot. */
export function tableColumnsField(m: MetaReader, tables: string[]): string | null {
  const ids = tables.map((t) => {
    if (!t.startsWith("t:")) throw new Error(`a table's node id is t:<id>, not ${t}`);
    return Number(t.slice(2));
  });
  return rootField(
    m,
    "registered_tables",
    {
      fields: ["id", "table_name", "domain_id"],
      rels: {
        tableColumns: {
          to: "table_columns",
          sel: {
            fields: ["column_name", "column_key", ...COLUMN_FACTS],
            rels: { sensitivity: { sel: { fields: ["tags"] } } },
          },
        },
      },
    },
    ids.length === 1 ? { id: { eq: ids[0] } } : { id: { in: ids } },
  );
}

/** ``g`` with the columns ``rows`` read of their tables, each joined to its table as the graph's
 * own columns are: a column already drawn is kept as it is. A column whose table a data product
 * holds is coloured apart, as the rest of the table's are. */
export function withTableColumns(g: GraphData, rows: TableColumnsRow[]): GraphData {
  const have = new Set(g.nodes.map((n) => n.id));
  const nodes = [...g.nodes];
  const edges = [...g.edges];
  for (const t of rows) {
    const tid = `t:${t.id}`;
    const held = g.edges.some((e) => e.kind === "member" && e.b === tid);
    for (const c of t.tableColumns ?? []) {
      const cid = `c:${c.columnKey}`;
      if (have.has(cid)) continue;
      have.add(cid);
      const sensitivity = rowsOf(c.sensitivity);
      nodes.push({
        id: cid,
        type: "column",
        label: `${t.tableName}.${c.columnName}`,
        parent: tid,
        domains: [t.domainId],
        inProduct: held,
        ...facts({
          ...columnFacts(c),
          sensitive: sensitivity?.flatMap((x) => x.tags.split(", ")),
        }),
      });
      edges.push({ id: `has_column:${cid}>${tid}`, a: cid, b: tid, kind: "has_column" });
    }
  }
  return { nodes, edges };
}
