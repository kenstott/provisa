// Copyright (c) 2026 Kenneth Stott
// Canary: 54aa4032-3f65-4159-9eb5-4e639403b22a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1586: the query a pill dropped onto a graph frame expands into.
 *
 * Dropping a table asks for its instances as they connect to what is already on the surface, so the
 * expansion has to name every relationship type that reaches it — not one of them. A junction table
 * backs several types at once (its discriminator column names each edge), which is exactly the case
 * where answering with the first type found would draw one kind of edge and hide the rest.
 *
 * Kept out of the page component so the pattern it writes can be read back in a test.
 */
import type { DropNotice, GNode } from "../components/graph/graph-model";
import type { SchemaRel } from "../components/graph/graph-schema-types";
import {
  MAX_SCHEMA_HOPS,
  MAX_SCHEMA_PATHS,
  shortestSchemaPaths,
} from "../components/graph/schemaPath";

export interface DropExpansion {
  /** The rewritten query, before any native-filter WHERE clause is spliced in. */
  query: string;
  /** Every variable bound to the dropped label, one per path. */
  targetVars: string[];
  /** What the drop says on its frame: the tied paths it left out. */
  notices: DropNotice[];
}

/**
 * ``query`` with ``matchBlock`` (its OPTIONAL MATCH lines) before its last RETURN and
 * ``extraReturn`` (", var, ...") added to that RETURN's projection. The result's own ORDER BY,
 * SKIP and LIMIT stay where Cypher puts them, after the projection: the expansion widens the
 * pattern, never the paging the query asked for. A query with no RETURN returns ``projection``.
 */
export function withExpansion(
  query: string,
  matchBlock: string,
  extraReturn: string,
  projection: string,
): string {
  const q = query.trim();
  const last = [...q.matchAll(/\bRETURN\b/gi)].pop();
  if (!last || last.index === undefined) return `${q}\n${matchBlock}\nRETURN ${projection}`;
  const before = q.slice(0, last.index).trimEnd();
  const clause = q.slice(last.index + "RETURN".length).trim();
  // The projection ends where the paging begins: ORDER BY, SKIP or LIMIT as a clause, preceded by
  // whitespace (a property spelled n.limit is not one).
  const paging = clause.search(/\s(ORDER\s+BY|SKIP|LIMIT)\s/i);
  const own = paging < 0 ? clause : clause.slice(0, paging).trimEnd();
  const tail = paging < 0 ? "" : `\n${clause.slice(paging).trim()}`;
  return `${before}\n${matchBlock}\nRETURN ${own}${extraReturn}${tail}`;
}

export function buildDropExpansion(
  query: string,
  compoundLabel: string,
  schemaRels: SchemaRel[],
): DropExpansion {
  // Map each label the query declares, as written (a compound label whole), to every variable bound
  // to it, by parsing the MATCH clauses. Use the QUERY (not result nodes): an OPTIONAL MATCH branch
  // that returned no rows still declares its label and must remain matchable. A label bound twice
  // anchors from both variables.
  const varsByLabel: Record<string, string[]> = {};
  for (const m of query.matchAll(/\(\s*(\w+)\s*:([\w:]+)\s*\)/g)) {
    const [, varName, labels] = m;
    const vars = (varsByLabel[labels] ??= []);
    if (!vars.includes(varName)) vars.push(varName);
  }

  // The relationship types are the server's, from the acting role's label map
  // (/data/graph-schema, the sidebar's Relationship Types): only a type it registers on the way from
  // a label the query declares to the dropped one is written, so the translator never meets a type
  // it does not know (REQ-1586). EVERY type that reaches the dropped label, from EVERY variable the
  // query binds to the other end's label -- not the first type, nor the first variable. One
  // junction table backs several types at once, and a query can reach the dropped label from
  // several of its nodes (a domain and its products). Each (anchor, types) is its own path, with its
  // own relationship and target variables: a target shared across anchors would require one node
  // to be reached from all of them and hide every row that is not. The graph merges the nodes
  // several paths reach by identity. A label no relationship touches is reached by the shortest
  // paths through others (REQ-2135), the ties among them capped; a hop's ties stay whole.
  const { paths, hops } = shortestSchemaPaths(schemaRels, Object.keys(varsByLabel), [compoundLabel]);
  const seen = new Set<string>();
  const routes = paths.flatMap((steps) =>
    varsByLabel[steps[0].from].flatMap((anchor) => {
      const key = [anchor, ...steps.map((s) => `${s.edge.type} ${s.to}`)].join(" ");
      if (seen.has(key)) return [];
      seen.add(key);
      return [{ anchor, steps }];
    }),
  );
  const kept = hops === 1 ? routes : routes.slice(0, MAX_SCHEMA_PATHS);
  const notices: DropNotice[] = [];
  if (kept.length < routes.length)
    notices.push({
      key: "graphFrame.sparqlDropLeftOut",
      values: { shown: kept.length, total: routes.length },
    });
  if (hops !== null && hops > MAX_SCHEMA_HOPS)
    notices.push({
      key: "graphFrame.cypherDropTooFar",
      values: { name: compoundLabel, hops, most: MAX_SCHEMA_HOPS },
    });
  // No known relationship: the dropped label alone, beside the query's first MATCH variable.
  const firstVar = query.match(/\bMATCH\s*\(\s*(\w+)/i)?.[1] ?? "n";

  const trimmed = query.trim();
  const taken = (v: string) =>
    trimmed.includes(`[${v}`) || trimmed.includes(` ${v}`) || trimmed.includes(`(${v}`);
  const nameOf = (label: string) =>
    (label.split(":").pop() ?? label).replace(/[^a-zA-Z0-9]/g, "").slice(0, 12);
  // One free (rel, target) variable pair per step. Each path gets its own OPTIONAL MATCH: a
  // single-hop pattern resolves one relationship mapping, so alternation is not a spelling the
  // translator accepts.
  const pairs: { relVar: string; targetVar: string }[] = [];
  const counters = new Map<string, number>();
  const freshPair = (label: string) => {
    const suffix = nameOf(label);
    let counter = counters.get(suffix) ?? 1;
    let relVar = `r${suffix}`;
    let targetVar = `m${suffix}`;
    while (
      taken(relVar) ||
      taken(targetVar) ||
      pairs.some((p) => p.relVar === relVar || p.targetVar === targetVar)
    ) {
      counter++;
      relVar = `r${suffix}${counter}`;
      targetVar = `m${suffix}${counter}`;
    }
    counters.set(suffix, counter);
    const pair = { relVar, targetVar };
    pairs.push(pair);
    return pair;
  };
  const written = kept.map(({ anchor, steps }) => {
    const walked = steps.map((s) => ({ ...freshPair(s.to), type: s.edge.type, label: s.to }));
    const chain = walked.map((w) => `-[${w.relVar}:${w.type}]-(${w.targetVar}:${w.label})`).join("");
    return { anchor, pattern: `(${anchor})${chain}`, walked };
  });
  const alone = written.length === 0 ? freshPair(compoundLabel) : null;
  const optMatchPatterns = alone
    ? [`(${alone.targetVar}:${compoundLabel})`]
    : written.map((w) => w.pattern);
  const extraReturn = alone
    ? `, ${alone.targetVar}`
    : written
        .flatMap((w) => w.walked.map((x) => `, ${x.relVar}, ${x.targetVar}`))
        .join("");
  const anchors = alone ? [firstVar] : [...new Set(written.map((w) => w.anchor))];
  const optMatchBlock = optMatchPatterns.map((p) => `OPTIONAL MATCH ${p}`).join("\n");
  const newQueryBase = withExpansion(
    trimmed,
    optMatchBlock,
    extraReturn,
    `${anchors.join(", ")}${extraReturn}`,
  );
  return {
    query: newQueryBase,
    targetVars: alone
      ? [alone.targetVar]
      : written.map((w) => w.walked[w.walked.length - 1].targetVar),
    notices,
  };
}

/** The most rows the paths between two dropped nodes are read to. */
const NODE_PATH_ROWS = 25;

/**
 * The query a node dragged onto another runs (REQ-2135): every shortest path of the data between the
 * two, within MAX_SCHEMA_HOPS, through any relationship. Null where a node's key is not known, which
 * is what names it: ``key`` is a label's key column.
 */
export function nodePathQuery(
  from: GNode,
  to: GNode,
  key: (label: string) => string | undefined,
): string | null {
  const keyed = ([variable, node]: [string, GNode]) => {
    const column = key(node.label);
    const value = column === undefined ? undefined : node.properties[column];
    if (value === undefined || value === null) return null;
    const literal =
      typeof value === "number" ? value : `'${String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
    return `${variable}.${column} = ${literal}`;
  };
  const a = keyed(["a", from]);
  const b = keyed(["b", to]);
  if (a === null || b === null) return null;
  return [
    `MATCH p = allShortestPaths((a:${from.label})-[*..${MAX_SCHEMA_HOPS}]-(b:${to.label}))`,
    `WHERE ${a} AND ${b}`,
    "RETURN p",
    `LIMIT ${NODE_PATH_ROWS}`,
  ].join("\n");
}

const NUMERIC_TYPES = new Set([
  "integer",
  "bigint",
  "int",
  "int4",
  "int8",
  "smallint",
  "float",
  "double precision",
  "numeric",
  "decimal",
  "real",
  "float4",
  "float8",
]);

/** The WHERE clauses binding a parameterized node's native filter columns, for those given a value. */
export function nativeFilterClauses(
  varName: string,
  filterColumns: { name: string; type: string }[],
  params: Record<string, string>,
): string[] {
  return filterColumns
    .filter((col) => params[col.name] !== "")
    .map(({ name, type }) => {
      const v = params[name];
      const lit = NUMERIC_TYPES.has((type ?? "").toLowerCase())
        ? v
        : `'${v.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
      return `${varName}._nf_${name} = ${lit}`;
    });
}
