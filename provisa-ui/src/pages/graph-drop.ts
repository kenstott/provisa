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
 * A drop that no path can join says so by name and changes nothing, as a SPARQL frame's does
 * (REQ-2135): a parameterized table is never what a path adds.
 *
 * Kept out of the page component so the pattern it writes can be read back in a test.
 */
import type { DropNotice, GNode } from "../components/graph/graph-model";
import type { SchemaNodeLabel, SchemaRel } from "../components/graph/graph-schema-types";
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

/** A drop that changes nothing, and why. */
export interface DropRefused {
  refused: DropNotice;
}

/** What /data/graph-schema says of a label that a path's way through depends on. */
export type ScopedLabel = Pick<
  SchemaNodeLabel,
  "domainLabel" | "domainId" | "tableLabel" | "systemDomain"
>;

/**
 * Where a drop's path may go (REQ-2135): the role's labels, and the domains the Domains picker
 * selects (none selected reads as every domain, as the sidebar reads it).
 */
export interface PathScope {
  nodeLabels: readonly ScopedLabel[];
  checkedDomains: ReadonlySet<string>;
}

/** A label as a query writes it: ``Domain:Table``, or the table's alone where it has no domain. */
export function compoundLabelOf(n: Pick<ScopedLabel, "domainLabel" | "tableLabel">): string {
  return n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel;
}

/** The labels of the selected domains: the sidebar's list, and the labels a path may pass through. */
export function inSelectedDomains<L extends ScopedLabel>(
  nodeLabels: readonly L[],
  checkedDomains: ReadonlySet<string>,
): L[] {
  return checkedDomains.size === 0
    ? [...nodeLabels]
    : nodeLabels.filter((n) => !n.domainId || checkedDomains.has(n.domainId));
}

/**
 * The labels a path between ``ends`` may pass through: those of the selected domains, less a system
 * domain's (meta, ontology, ops, as the server says) unless one of ``ends`` is of that domain. The
 * model's own metadata joins every table to every other (HAS_TABLE through its registration), and
 * is no relationship of the data the reader asked about.
 */
export function pathThrough(scope: PathScope, ends: readonly string[]): Set<string> {
  const endDomains = new Set(
    scope.nodeLabels.filter((n) => ends.includes(compoundLabelOf(n))).map((n) => n.domainId),
  );
  return new Set(
    inSelectedDomains(scope.nodeLabels, scope.checkedDomains)
      .filter((n) => !n.systemDomain || endDomains.has(n.domainId))
      .map(compoundLabelOf),
  );
}

/** The refusal of a drop no path within reach joins, by name. */
function noPath(name: string, held: readonly string[], hops: number | null): DropRefused {
  const classes = held.join(", ");
  return {
    refused:
      hops === null
        ? { key: "graphFrame.sparqlDropNoPath", values: { name, classes } }
        : {
            key: "graphFrame.sparqlDropTooFar",
            values: { name, classes, hops, most: MAX_SCHEMA_HOPS },
          },
  };
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
  parameterized: ReadonlySet<string>,
  scope: PathScope,
): DropExpansion | DropRefused {
  // Map each label the query declares, as written (a compound label whole), to every variable bound
  // to it, by parsing the MATCH clauses. Use the QUERY (not result nodes): an OPTIONAL MATCH branch
  // that returned no rows still declares its label and must remain matchable. A label bound twice
  // anchors from both variables.
  const varsByLabel: Record<string, string[]> = {};
  for (const m of query.matchAll(/\(\s*(\w+)\s*:([\w:]+)\s*(?:\{[^}]*\}\s*)?\)/g)) {
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
  if (parameterized.has(compoundLabel))
    return {
      refused: { key: "graphFrame.sparqlDropParameterized", values: { name: compoundLabel } },
    };
  const held = Object.keys(varsByLabel);
  if (held.length === 0) return { refused: { key: "graphFrame.sparqlDropNoClass" } };
  // The path stays in the selected domains, and out of a system domain neither end is of.
  const through = pathThrough(scope, [...held, compoundLabel]);
  const { paths, hops } = shortestSchemaPaths(
    schemaRels,
    held,
    [compoundLabel],
    parameterized,
    through,
  );
  if (paths.length === 0) return noPath(compoundLabel, held, hops);
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
    const chain = walked
      .map((w) => `-[${w.relVar}:${w.type}]-(${w.targetVar}:${w.label})`)
      .join("");
    return { anchor, pattern: `(${anchor})${chain}`, walked };
  });
  const extraReturn = written
    .flatMap((w) => w.walked.map((x) => `, ${x.relVar}, ${x.targetVar}`))
    .join("");
  const anchors = [...new Set(written.map((w) => w.anchor))];
  const optMatchBlock = written.map((w) => `OPTIONAL MATCH ${w.pattern}`).join("\n");
  const newQueryBase = withExpansion(
    trimmed,
    optMatchBlock,
    extraReturn,
    `${anchors.join(", ")}${extraReturn}`,
  );
  return {
    query: newQueryBase,
    targetVars: written.map((w) => w.walked[w.walked.length - 1].targetVar),
    notices,
  };
}

/** The most rows the paths between two dropped nodes are read to. */
const NODE_PATH_ROWS = 25;

/**
 * The query a node dragged onto another runs (REQ-2135): every shortest path of the data between the
 * two, within MAX_SCHEMA_HOPS. Its pattern names the relationship types it may take: each one whose
 * every relationship joins labels a path may pass through (``pathThrough``) or the two ends, so the
 * walk stays in the selected domains and out of a system domain neither node is of. Refused by name
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
  // between, so a type with one relationship outside the scope (HAS_TABLE, every table's) is left out.
  const outside = new Set(
    schemaRels.filter((r) => !joins(r.source) || !joins(r.target)).map((r) => r.type),
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
