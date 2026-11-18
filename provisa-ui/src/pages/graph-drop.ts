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
 * REQ-1586, REQ-2135: the query a pill dropped onto a graph frame expands into.
 *
 * The dropped label joins along the one path the shared path finder (dropPath.ts) gives, within the
 * selected domains, from one of the labels the query holds: at one hop, every relationship type
 * joining the two labels is its own hop, since one junction table backs several types at once (its
 * discriminator column names each edge) and answering with the first type found would draw one kind
 * of edge and hide the rest. A query whose nodes have no label is first given its labels from the
 * schema (cypherEndpoints.ts), one MATCH per label pair, unioned; the path then joins each branch,
 * every branch returning the same columns.
 *
 * A drop that no path within the selected domains joins says so by name and changes nothing, as a
 * SPARQL frame's does: a parameterized table is never what a path adds.
 *
 * Kept out of the page component so the pattern it writes can be read back in a test.
 */
import type { DropNotice, GNode } from "../components/graph/graph-model";
import type { SchemaRel } from "../components/graph/graph-schema-types";
import { MAX_SCHEMA_HOPS, shortestSchemaPaths, type SchemaStep } from "../components/graph/schemaPath";
import {
  dropPath,
  noDropPath,
  pathThrough,
  type PathScope,
} from "../components/graph/dropPath";
import { labelEndpoints } from "../components/graph/cypherEndpoints";

export interface DropExpansion {
  /** The rewritten query, before any native-filter WHERE clause is spliced in. */
  query: string;
  /** Every variable bound to the dropped label, one per hop of the path's last step. */
  targetVars: string[];
  /** What the drop says on its frame: that the result's endpoints were labelled. */
  notices: DropNotice[];
}

/** A drop that changes nothing, and why. */
export interface DropRefused {
  refused: DropNotice;
}

/** The refusal of a drop no path within reach joins, by name. */
function noPath(name: string, held: readonly string[], hops: number | null): DropRefused {
  return { refused: noDropPath(name, held.join(", "), hops) };
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
  if (!last || last.index === undefined)
    return `${q}${matchBlock ? `\n${matchBlock}` : ""}\nRETURN ${projection}`;
  const before = q.slice(0, last.index).trimEnd();
  const clause = q.slice(last.index + "RETURN".length).trim();
  // The projection ends where the paging begins: ORDER BY, SKIP or LIMIT as a clause, preceded by
  // whitespace (a property spelled n.limit is not one).
  const paging = clause.search(/\s(ORDER\s+BY|SKIP|LIMIT)\s/i);
  const own = paging < 0 ? clause : clause.slice(0, paging).trimEnd();
  const tail = paging < 0 ? "" : `\n${clause.slice(paging).trim()}`;
  return `${before}${matchBlock ? `\n${matchBlock}` : ""}\nRETURN ${own}${extraReturn}${tail}`;
}

/** Each label ``query`` declares, as written (a compound label whole), to every variable bound to
 * it, in the order written. Read from the query, not the result: an OPTIONAL MATCH branch that
 * returned no rows still declares its label and must remain matchable. */
function variablesByLabel(query: string): Map<string, string[]> {
  const held = new Map<string, string[]>();
  for (const m of query.matchAll(/\(\s*(\w+)\s*:([\w:]+)\s*(?:\{[^}]*\}\s*)?\)/g)) {
    const [, variable, label] = m;
    const vars = held.get(label) ?? [];
    if (!vars.includes(variable)) vars.push(variable);
    held.set(label, vars);
  }
  return held;
}

/** ``query``'s UNION branches and the words joining them; one branch for a query with none. */
function branchesOf(query: string): { branches: string[]; joins: string[] } {
  const parts = query.trim().split(/\s*\b(UNION(?:\s+ALL)?)\b\s*/i);
  return {
    branches: parts.filter((_, i) => i % 2 === 0),
    joins: parts.filter((_, i) => i % 2 === 1),
  };
}

export function buildDropExpansion(
  query: string,
  compoundLabel: string,
  schemaRels: SchemaRel[],
  parameterized: ReadonlySet<string>,
  scope: PathScope,
): DropExpansion | DropRefused {
  // The relationship types are the server's, from the acting role's label map
  // (/data/graph-schema, the sidebar's Relationship Types): only a type it registers is written, so
  // the translator never meets a type it does not know (REQ-1586).
  if (parameterized.has(compoundLabel))
    return {
      refused: { key: "graphFrame.sparqlDropParameterized", values: { name: compoundLabel } },
    };
  const notices: DropNotice[] = [];
  let { branches, joins } = branchesOf(query);
  if (branches.length === 1) {
    const labelled = labelEndpoints(branches[0], schemaRels, parameterized, scope);
    if (labelled !== null) {
      if ("refused" in labelled) return labelled;
      branches = labelled.branches;
      joins = branches.slice(1).map(() => "UNION ALL");
      notices.push(labelled.notice);
    }
  }

  // Each branch's path from the labels it holds, anchored on the first variable of its start.
  const routes = branches.map((branch) => {
    const vars = variablesByLabel(branch);
    const found = dropPath(schemaRels, [...vars.keys()], [compoundLabel], parameterized, scope);
    return { vars, found };
  });
  const held = [...new Set(routes.flatMap((r) => [...r.vars.keys()]))];
  if (held.length === 0) return { refused: { key: "graphFrame.cypherDropNoLabel" } };
  const joined = routes.flatMap(({ vars, found }) =>
    "paths" in found ? [{ anchor: (vars.get(found.start) as string[])[0], paths: found.paths }] : [],
  );
  if (joined.length === 0) {
    const hops = routes
      .map(({ found }) => ("hops" in found ? found.hops : null))
      .filter((h): h is number => h !== null);
    return noPath(compoundLabel, held, hops.length > 0 ? Math.min(...hops) : null);
  }

  // One free (rel, target) variable pair per step of each path, the same names in every branch so
  // the branches return the same columns: a slot is a path's step counted from its end (the dropped
  // label is the same column in every branch), named on the first branch that has it. Each path gets its own OPTIONAL MATCH: a single-hop pattern resolves one relationship
  // mapping, so alternation is not a spelling the translator accepts.
  const trimmed = query.trim();
  const taken = (v: string) =>
    branches.some((b) => b.includes(`[${v}`) || b.includes(` ${v}`) || b.includes(`(${v}`)) ||
    trimmed.includes(`(${v}`);
  const nameOf = (label: string) =>
    (label.split(":").pop() ?? label).replace(/[^a-zA-Z0-9]/g, "").slice(0, 12);
  const slots = new Map<string, { relVar: string; targetVar: string }>();
  const counters = new Map<string, number>();
  const slotOf = (path: number, fromEnd: number, label: string) => {
    const key = `${path}.${fromEnd}`;
    const known = slots.get(key);
    if (known) return known;
    const suffix = nameOf(label);
    let counter = counters.get(suffix) ?? 1;
    let relVar = `r${suffix}`;
    let targetVar = `m${suffix}`;
    const named = [...slots.values()];
    while (
      taken(relVar) ||
      taken(targetVar) ||
      named.some((p) => p.relVar === relVar || p.targetVar === targetVar)
    ) {
      counter++;
      relVar = `r${suffix}${counter}`;
      targetVar = `m${suffix}${counter}`;
    }
    counters.set(suffix, counter);
    const slot = { relVar, targetVar };
    slots.set(key, slot);
    return slot;
  };
  const written = routes.map(({ vars, found }) => {
    if (!("paths" in found)) return null;
    const anchor = (vars.get(found.start) as string[])[0];
    const lines = found.paths.map((steps: SchemaStep<SchemaRel>[], p) => {
      const chain = steps
        .map((s, i) => {
          const { relVar, targetVar } = slotOf(p, i - steps.length, s.to);
          return `-[${relVar}:${s.edge.type}]-(${targetVar}:${s.to})`;
        })
        .join("");
      return `OPTIONAL MATCH (${anchor})${chain}`;
    });
    const keys = new Set(
      found.paths.flatMap((steps, p) => steps.map((_, i) => `${p}.${i - steps.length}`)),
    );
    return { anchor, keys, lines };
  });
  const order = [...slots.entries()];
  const rewritten = branches.map((branch, b) => {
    const w = written[b];
    const extra = order
      .map(([key, { relVar, targetVar }]) =>
        w?.keys.has(key)
          ? `, ${relVar}, ${targetVar}`
          : `, null AS ${relVar}, null AS ${targetVar}`,
      )
      .join("");
    const lines = w?.lines.join("\n") ?? "";
    const projection = `${w?.anchor ?? ""}${extra}`.replace(/^, /, "");
    return withExpansion(branch, lines, extra, projection);
  });
  const lastSteps = new Set(joined.flatMap(({ paths }) => paths.map((_, p) => `${p}.-1`)));
  return {
    query: rewritten.reduce((q, branch, i) => `${q}\n${joins[i - 1]}\n${branch}`),
    targetVars: order.filter(([key]) => lastSteps.has(key)).map(([, s]) => s.targetVar),
    notices,
  };
}

/** The most rows the paths between two dropped nodes are read to. */
const NODE_PATH_ROWS = 25;

/**
 * The query a node dragged onto another runs (REQ-2135): every shortest path of the data between the
 * two, within MAX_SCHEMA_HOPS. Its pattern names the relationship types it may take: each one whose
 * every relationship joins labels a path may pass through (``pathThrough``) or the two ends, and
 * none from a constant, so the walk stays in the selected domains and out of a system domain
 * neither node is of. Refused by name
 * where a node's key is not known (``key`` is a label's key column), and where no such path joins
 * the two labels.
 */
export function nodePathQuery(
  from: GNode,
  to: GNode,
  key: (label: string) => string | undefined,
  schemaRels: readonly SchemaRel[],
  parameterized: ReadonlySet<string>,
  scope: PathScope,
): { query: string } | DropRefused {
  const keyed = ([variable, node]: [string, GNode]) => {
    const column = key(node.label);
    const value = column === undefined ? undefined : node.properties[column];
    if (value === undefined || value === null) return null;
    const literal =
      typeof value === "number"
        ? value
        : `'${String(value).replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
    return `${variable}.${column} = ${literal}`;
  };
  const a = keyed(["a", from]);
  const b = keyed(["b", to]);
  if (a === null || b === null)
    return {
      refused: { key: "graphFrame.cypherPathNoKey", values: { from: from.label, to: to.label } },
    };
  const ends = [from.label, to.label];
  const through = pathThrough(scope, ends);
  const joins = (label: string) => through.has(label) || ends.includes(label);
  // A type is taken whole or not at all: a variable-length pattern names types, not the labels
  // between, so a type with one relationship outside the scope is left out. So is a link from a
  // constant (HAS_TABLE): the server leaves it out of a walk that does not name it, and a walk that
  // names it joins on no column of the table, which the engine refuses.
  const outside = new Set(
    schemaRels
      .filter((r) => r.constantSource || !joins(r.source) || !joins(r.target))
      .map((r) => r.type),
  );
  const taken = schemaRels.filter((r) => !outside.has(r.type));
  const { paths, hops } = shortestSchemaPaths(
    taken,
    [from.label],
    [to.label],
    parameterized,
    through,
  );
  if (paths.length === 0) return noPath(to.label, [from.label], hops);
  const types = [...new Set(taken.map((r) => r.type))].sort().join("|");
  return {
    query: [
      `MATCH p = allShortestPaths((a:${from.label})-[:${types}*..${MAX_SCHEMA_HOPS}]-(b:${to.label}))`,
      `WHERE ${a} AND ${b}`,
      "RETURN p",
      `LIMIT ${NODE_PATH_ROWS}`,
    ].join("\n"),
  };
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
