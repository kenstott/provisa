// Copyright (c) 2026 Kenneth Stott
// Canary: 7c3e9b52-4d18-4a6f-a2e0-61b8f5d9c473
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-26, A CONNECTED ONTOLOGY GRAPH IS ONE QUERY): All hops, which no fixed
// GraphQL nesting can express, opens as one Cypher variable-length query over the same model, from
// the terms the view's root read.

import { dataGraphql, metaReader, rootQuery } from "../../api/metaGraph";

/** The ontology's terms as ``role``'s Cypher graph serves them: their label, and the relationship
 * types a term link is served under (each a type of the term edges junction, term to term) -- the
 * server's own (/data/graph-schema, by the terms table's registration), never derived here. */
export interface TermsGraph {
  label: string;
  links: string[];
}

/** The junction a term link is a row of (ontology._term_edges, as ontologyData reads it). */
const TERM_EDGES = "_term_edges";

export async function termsCypherGraph(role: string): Promise<TermsGraph> {
  const m = await metaReader(role);
  const q = rootQuery(
    m,
    "OntologyTermsTable",
    "registered_tables",
    { fields: ["id"] },
    { domain_id: { eq: "ontology" }, table_name: { eq: "_terms" } },
  );
  if (q === null) throw new Error("the role cannot read the ontology's terms table");
  const tables = (await dataGraphql<{ registered_tables: { id: number }[] }>(q, {}, role))
    .registered_tables;
  if (tables.length !== 1)
    throw new Error(`the ontology's terms table is registered ${tables.length} times, not once`);
  const resp = await fetch("/data/graph-schema", { headers: { "X-Provisa-Role": role } });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
  const body = (await resp.json()) as {
    node_labels: { label: string; table_id: number }[];
    relationship_types: {
      type: string;
      source: string;
      target: string;
      junction_table_name: string | null;
    }[];
  };
  const found = body.node_labels.find((n) => n.table_id === tables[0].id);
  if (found === undefined)
    throw new Error("the ontology's terms have no Cypher label for the role");
  const links = body.relationship_types
    .filter(
      (r) =>
        r.source === found.label &&
        r.target === found.label &&
        r.junction_table_name === TERM_EDGES,
    )
    .map((r) => r.type);
  return { label: found.label, links: [...new Set(links)].sort() };
}

/** The view's terms read as one Cypher query: from ``roots`` (the terms the view's root reads -- a
 * search's matches, a focus's terms, or with neither every term its filters keep), each walked
 * ``depth`` steps over the term links (a fixed Hops value, or All's: as far as the view's walk went
 * to reach the end of the connected part), the terms reached each once and the links leaving them.
 * ``edgeTypes``: the link types the view keeps (every link type, when none). Typed, its ends
 * labelled and bounded as the view is, it runs as it is opened, never every path through the whole
 * graph (REQ-2174, amended 2026-11-26). With no root, it answers no term, as the view draws none. */
export function allHopsCypher(
  graph: TermsGraph,
  roots: readonly number[],
  depth: number,
  edgeTypes: readonly string[],
): string {
  if (!Number.isInteger(depth) || depth < 0)
    throw new Error(`a walk is a whole number of hops, not ${depth}`);
  const ids = [...new Set(roots)].sort((a, b) => a - b);
  const { label } = graph;
  const types = keptLinks(graph, edgeTypes);
  const one = ids.length === 1;
  const among = `[${ids.join(", ")}]`;
  // No link the view keeps: the roots alone, as the view drew them.
  if (types === null || ids.length === 0)
    return one
      ? `MATCH (a:${label} {id: ${ids[0]}}) RETURN a`
      : `MATCH (a:${label}) WHERE a.id IN ${among} RETURN a`;
  const links = `OPTIONAL MATCH (a)-[r:${types}]->(b:${label}) RETURN DISTINCT a, r, b`;
  if (depth === 0)
    return one
      ? `MATCH (a:${label} {id: ${ids[0]}}) ${links}`
      : `MATCH (a:${label}) WHERE a.id IN ${among} ${links}`;
  const start = one ? `(t:${label} {id: ${ids[0]}})` : `(t:${label})`;
  const where = one ? "" : ` WHERE t.id IN ${among}`;
  return `MATCH ${start}-[:${types}*0..${depth}]-(a:${label})${where} ${links}`;
}

/** The terms a view draws, ``terms``, as one Cypher query: each term once and the links between
 * them of the types the view keeps. What a focus draws within the view's read -- a neighbourhood
 * no reach filter states -- is still these terms and links (REQ-2136). */
export function drawnCypher(
  graph: TermsGraph,
  terms: readonly number[],
  edgeTypes: readonly string[],
): string {
  const ids = [...new Set(terms)].sort((a, b) => a - b);
  const { label } = graph;
  const among = `[${ids.join(", ")}]`;
  const types = keptLinks(graph, edgeTypes);
  if (types === null || ids.length === 0)
    return `MATCH (a:${label}) WHERE a.id IN ${among} RETURN a`;
  return (
    `MATCH (a:${label}) WHERE a.id IN ${among} ` +
    `OPTIONAL MATCH (a)-[r:${types}]->(b:${label}) WHERE b.id IN ${among} RETURN a, r, b`
  );
}

/** The term link types the view keeps, as a Cypher type alternation; null when it keeps none. */
function keptLinks(graph: TermsGraph, edgeTypes: readonly string[]): string | null {
  const types = graph.links.filter((t) => edgeTypes.length === 0 || edgeTypes.includes(t));
  return types.length === 0 ? null : types.join("|");
}

/** A Cypher read written as SPARQL over ``role``'s vocabulary, by the server's own export (the
 * one a GraphQL read's SPARQL is written by, REQ-2136): the SPARQL, or why the read has none. */
export async function cypherAsSparql(
  role: string,
  cypher: string,
): Promise<{ sparql: string | null; error: string | null }> {
  const resp = await fetch("/data/sparql/of-cypher", {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Provisa-Role": role },
    body: JSON.stringify({ cypher }),
  });
  if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${await resp.text()}`);
  const body = (await resp.json()) as { sparql: string | null; sparql_error: string | null };
  if (body.sparql === null && body.sparql_error === null)
    throw new Error("the server wrote no SPARQL and said no reason");
  return { sparql: body.sparql, error: body.sparql_error };
}
