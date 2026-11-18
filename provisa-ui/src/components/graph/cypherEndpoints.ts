// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8b1d62-a9e4-4c75-8d02-6e1b7a9c4f58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: a Cypher result whose query has nodes with no label is given its labels from the schema
// before anything joins it: one MATCH per label pair its relationships join within the selected
// domains, unioned, each returning its endpoints. A drop then joins the dropped label from those
// labelled nodes, and Show children reads a node's children the same way. The query this reads is
// one MATCH of a chain of nodes and relationships, then an optional WHERE, a RETURN and its paging;
// a node with no label in any other shape names nothing to label it by.

import type { DropNotice } from "./graph-model";
import type { SchemaRel } from "./graph-schema-types";
import { pathThrough, type PathScope } from "./dropPath";

interface WrittenNode {
  variable: string;
  label: string | null;
  properties: string;
}

interface WrittenRel {
  variable: string;
  type: string | null;
  properties: string;
  /** "out" for -[]->, "in" for <-[]-, "both" for -[]-. */
  direction: "out" | "in" | "both";
}

interface Chain {
  nodes: WrittenNode[];
  rels: WrittenRel[];
  /** The query after the pattern: its WHERE, RETURN and paging. */
  rest: string;
}

const NODE = /\(\s*(\w*)\s*((?::\w+)*)\s*(\{[^}]*\})?\s*\)/y;
const REL = /\s*(<)?-\[\s*(\w*)\s*(?::(\w+))?\s*(\{[^}]*\})?\s*\]-(>)?\s*/y;

/** ``query`` as one MATCH of a chain, or null where it is any other shape. */
function chainOf(query: string): Chain | null {
  const q = query.trim();
  const head = /^MATCH\s+/i.exec(q);
  if (!head) return null;
  if (/\b(MATCH|WITH|UNION|UNWIND|CALL|CREATE|MERGE|SET|DELETE)\b/i.test(q.slice(head[0].length)))
    return null;
  const nodes: WrittenNode[] = [];
  const rels: WrittenRel[] = [];
  let at = head[0].length;
  const node = (): boolean => {
    NODE.lastIndex = at;
    const m = NODE.exec(q);
    if (!m) return false;
    nodes.push({ variable: m[1], label: m[2] ? m[2].slice(1) : null, properties: m[3] ?? "" });
    at = NODE.lastIndex;
    return true;
  };
  if (!node()) return null;
  for (;;) {
    REL.lastIndex = at;
    const m = REL.exec(q);
    if (!m) break;
    if (m[1] && m[5]) return null;
    rels.push({
      variable: m[2],
      type: m[3] ?? null,
      properties: m[4] ?? "",
      direction: m[1] ? "in" : m[5] ? "out" : "both",
    });
    at = REL.lastIndex;
    if (!node()) return null;
  }
  const rest = q.slice(at).trim();
  if (!/^(WHERE|RETURN)\b/i.test(rest)) return null;
  return { nodes, rels, rest };
}

/** One labelling of a chain: a label per node, a relationship type per hop. */
interface Labelling {
  labels: string[];
  types: string[];
}

export type LabelledEndpoints =
  { branches: string[]; notice: DropNotice } | { refused: DropNotice };

/**
 * ``query`` with its unlabelled nodes given their labels from ``schemaRels``: one branch per label
 * pair (per labelling of the chain) within ``scope``, each a MATCH of the labelled chain returning
 * its endpoints. A label of ``parameterized`` labels nothing: its table takes arguments to read.
 * Null where ``query`` is no single MATCH of a chain, or labels every node it writes.
 */
export function labelEndpoints(
  query: string,
  schemaRels: readonly SchemaRel[],
  parameterized: ReadonlySet<string>,
  scope: PathScope,
): LabelledEndpoints | null {
  const chain = chainOf(query);
  if (chain === null || chain.nodes.every((n) => n.label !== null)) return null;
  if (chain.rels.length === 0) return { refused: { key: "graphFrame.cypherDropNoLabel" } };
  const written = chain.nodes.flatMap((n) => (n.label === null ? [] : [n.label]));
  const allowed = pathThrough(scope, written);
  const fits = (node: WrittenNode, label: string): boolean =>
    node.label === null ? allowed.has(label) && !parameterized.has(label) : node.label === label;

  /** Every labelling of the chain from hop ``i`` on, its node ``i`` labelled ``label``. */
  const from = (i: number, label: string): Labelling[] => {
    if (i === chain.rels.length) return [{ labels: [label], types: [] }];
    const rel = chain.rels[i];
    const next = chain.nodes[i + 1];
    const seen = new Set<string>();
    const steps = schemaRels.flatMap((r) => {
      if (rel.type !== null && r.type !== rel.type) return [];
      const ways: string[] = [];
      if (rel.direction !== "in" && r.source === label) ways.push(r.target);
      if (rel.direction !== "out" && r.target === label && r.source !== r.target)
        ways.push(r.source);
      return ways
        .filter((other) => fits(next, other))
        .flatMap((other) => {
          // A join declared from both ends is one pair, not two (SUBMITTED and SUBMITTED_BY).
          const key = `${rel.type === null ? r.join : r.type} ${other}`;
          if (seen.has(key)) return [];
          seen.add(key);
          return [{ type: r.type, other }];
        });
    });
    return steps.flatMap(({ type, other }) =>
      from(i + 1, other).map((rest) => ({
        labels: [label, ...rest.labels],
        types: [type, ...rest.types],
      })),
    );
  };
  const firstLabels =
    chain.nodes[0].label !== null
      ? [chain.nodes[0].label]
      : [...allowed].filter((l) => !parameterized.has(l));
  const labellings = firstLabels.flatMap((l) => from(0, l));
  if (labellings.length === 0) return { refused: { key: "graphFrame.cypherDropNoPair" } };

  // A node written with no variable is given one, so its branch can return it.
  const used = new Set([...query.matchAll(/\b[A-Za-z_]\w*\b/g)].map((m) => m[0]));
  const variables = chain.nodes.map((n, i) => {
    if (n.variable) return n.variable;
    let name = `n${i + 1}`;
    for (let k = 2; used.has(name); k++) name = `n${i + 1}_${k}`;
    used.add(name);
    return name;
  });
  const endpoints = variables.filter(
    (_, i) => chain.nodes[i].label === null || !chain.nodes[i].variable,
  );
  const branches = labellings.map(({ labels, types }) => {
    const node = (i: number) => {
      const props = chain.nodes[i].properties ? ` ${chain.nodes[i].properties}` : "";
      return `(${variables[i]}:${labels[i]}${props})`;
    };
    const pattern = chain.rels
      .map((rel, i) => {
        const props = rel.properties ? ` ${rel.properties}` : "";
        const body = `[${rel.variable}:${types[i]}${props}]`;
        const hop =
          rel.direction === "out"
            ? `-${body}->`
            : rel.direction === "in"
              ? `<-${body}-`
              : `-${body}-`;
        return `${hop}${node(i + 1)}`;
      })
      .join("");
    return `MATCH ${node(0)}${pattern}\n${withEndpoints(chain.rest, endpoints)}`;
  });
  return { branches, notice: { key: "graphFrame.cypherDropLabelled" } };
}

/** ``rest`` (WHERE, RETURN, paging) with each of ``endpoints`` its RETURN does not already name. */
function withEndpoints(rest: string, endpoints: readonly string[]): string {
  const last = [...rest.matchAll(/\bRETURN\b/gi)].pop();
  if (!last || last.index === undefined) return rest;
  const before = rest.slice(0, last.index).trim();
  const clause = rest.slice(last.index + "RETURN".length).trim();
  const paging = clause.search(/\s(ORDER\s+BY|SKIP|LIMIT)\s/i);
  const own = paging < 0 ? clause : clause.slice(0, paging).trimEnd();
  const tail = paging < 0 ? "" : ` ${clause.slice(paging).trim()}`;
  const named = new Set(own.split(",").map((item) => item.trim()));
  const added = own.trim() === "*" ? [] : endpoints.filter((v) => !named.has(v));
  const projection = [own, ...added].join(", ");
  return `${before ? `${before}\n` : ""}RETURN ${projection}${tail}`;
}

/**
 * The query Show children (``way`` "children") or Show parents ("parents") of a node of ``label``
 * runs: the node's relationships out of (into) it, the other end labelled from the schema within
 * ``scope``, one MATCH per label pair, unioned. Null where no relationship within ``scope`` has one.
 */
export function neighboursQuery(
  label: string,
  id: number,
  way: "children" | "parents",
  schemaRels: readonly SchemaRel[],
  parameterized: ReadonlySet<string>,
  scope: PathScope,
): string | null {
  const pattern = way === "children" ? `(n:${label})-[r]->(child)` : `(parent)-[r]->(n:${label})`;
  const other = way === "children" ? "child" : "parent";
  const done = labelEndpoints(
    `MATCH ${pattern} WHERE id(n) IN [${id}] RETURN n, r, ${other}`,
    schemaRels,
    parameterized,
    scope,
  );
  return done === null || "refused" in done ? null : done.branches.join("\nUNION ALL\n");
}
