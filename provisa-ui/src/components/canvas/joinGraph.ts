// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The joins of a join canvas -- the SQL Explorer's and the SQL modeling modal's -- and the one
// statement they make: the joins the server approves between the tables on the canvas, and the
// SELECT those tables and joins build.

import { tableSqlRef, type SqlNamedTable } from "../../naming";

/** A join drawn between two columns of two tables on the canvas, foreign key to key. */
export interface CanvasEdge {
  id: string;
  fromTable: string;
  fromCol: string;
  toTable: string;
  toCol: string;
  cardinality: "many-to-one" | "one-to-many";
}

/** A join the server approves for the acting role (GET /data/sql/approved-joins). */
export interface ApprovedJoin {
  fromTableId: number;
  fromColumn: string;
  toTableId: number;
  toColumn: string;
}

/**
 * The lines the canvas draws: each approved join whose two tables are both on the canvas
 * (``onCanvas``, by registered table id to name). Exactly the joins its statements may write --
 * a junction relationship's are its two foreign-key joins, as the server approves them.
 */
export function approvedEdges(
  joins: ApprovedJoin[],
  tableNames: Map<number, string>,
  onCanvas: Set<string>,
): CanvasEdge[] {
  const out: CanvasEdge[] = [];
  for (const j of joins) {
    const from = tableNames.get(j.fromTableId);
    const to = tableNames.get(j.toTableId);
    if (from === undefined || to === undefined || !onCanvas.has(from) || !onCanvas.has(to))
      continue;
    out.push({
      id: `${from}__${j.fromColumn}__${to}__${j.toColumn}`,
      fromTable: from,
      fromCol: j.fromColumn,
      toTable: to,
      toCol: j.toColumn,
      cardinality: "many-to-one",
    });
  }
  return out;
}

/** The alias a table is read under first. */
export function aliasOf(tableName: string): string {
  return tableName.replace(/\W/g, "_").toLowerCase();
}

export type JoinSql = { sql: string } | { unjoined: string[] };

/**
 * The statement the canvas's ``tables`` (in the order they were dropped) and ``edges`` build: the
 * first table, then every table the edges reach from it, breadth first, each joined on the edge
 * that reached it. An edge between two tables already joined joins the table on its key side
 * (``keys``: each table's key columns) again, under an alias named for the foreign key
 * (``companion_pet`` for ``companion_pet_id``), so two joins between one pair of tables are two
 * joins, not one ANDed ON. A table no edge reaches is
 * named (``unjoined``) rather than given a statement that cannot run. ``selected`` columns, by
 * table, are the SELECT list (a name two tables share is qualified with its table); none is *.
 */
export function buildJoinSql(
  tables: SqlNamedTable[],
  edges: CanvasEdge[],
  keys: Map<string, Set<string>>,
  selected: Map<string, Set<string>> = new Map(),
): JoinSql {
  if (tables.length === 0) return { sql: "" };
  const byName = new Map(tables.map((t) => [t.tableName, t]));
  const first = tables[0].tableName;
  const joined = new Set([first]);
  const used = new Set<string>();
  const aliases = new Set([aliasOf(first)]);
  const clauses: string[] = [];
  const queue = [first];
  while (queue.length > 0) {
    const here = queue.shift() as string;
    for (const e of edges) {
      if (used.has(e.id) || (e.fromTable !== here && e.toTable !== here)) continue;
      used.add(e.id);
      const [hereCol, other, otherCol] =
        e.fromTable === here ? [e.fromCol, e.toTable, e.toCol] : [e.toCol, e.fromTable, e.fromCol];
      if (!joined.has(other)) {
        joined.add(other);
        queue.push(other);
        aliases.add(aliasOf(other));
        clauses.push(
          `JOIN ${tableSqlRef(byName.get(other) as SqlNamedTable)} ${aliasOf(other)} ON ` +
            `${aliasOf(here)}."${hereCol}" = ${aliasOf(other)}."${otherCol}"`,
        );
        continue;
      }
      // Both tables are joined: a further foreign key to a key is a further row of the key's
      // table, joined again under an alias named for the foreign key (companion_pet_id →
      // companion_pet). The key's side is the side whose column is its table's key.
      const hereIsKey = !keys.get(other)?.has(otherCol) && Boolean(keys.get(here)?.has(hereCol));
      const [keyTable, keyCol, fkTable, fkCol] = hereIsKey
        ? [here, hereCol, other, otherCol]
        : [other, otherCol, here, hereCol];
      const alias = uniqueAlias(aliasOf(fkCol.replace(/_id$/i, "")) || aliasOf(keyTable), aliases);
      aliases.add(alias);
      clauses.push(
        `JOIN ${tableSqlRef(byName.get(keyTable) as SqlNamedTable)} ${alias} ON ` +
          `${aliasOf(fkTable)}."${fkCol}" = ${alias}."${keyCol}"`,
      );
    }
  }
  const unjoined = tables.map((t) => t.tableName).filter((name) => !joined.has(name));
  if (unjoined.length > 0) return { unjoined };
  return {
    sql: [
      `SELECT ${selectList(tables, selected)}`,
      `FROM ${tableSqlRef(tables[0])} ${aliasOf(first)}`,
      ...clauses,
    ].join("\n"),
  };
}

function uniqueAlias(wanted: string, taken: Set<string>): string {
  if (!taken.has(wanted)) return wanted;
  let n = 2;
  while (taken.has(`${wanted}_${n}`)) n += 1;
  return `${wanted}_${n}`;
}

function selectList(tables: SqlNamedTable[], selected: Map<string, Set<string>>): string {
  const entries = tables.flatMap((t) =>
    [...(selected.get(t.tableName) ?? [])].map((col) => ({ table: t.tableName, col })),
  );
  if (entries.length === 0) return "*";
  const seen = new Map<string, number>();
  entries.forEach(({ col }) => seen.set(col, (seen.get(col) ?? 0) + 1));
  return entries
    .map(({ table, col }) =>
      (seen.get(col) as number) > 1
        ? `${aliasOf(table)}."${col}" ${aliasOf(table)}_${col}`
        : `${aliasOf(table)}."${col}"`,
    )
    .join(", ");
}
