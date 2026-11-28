// Copyright (c) 2026 Kenneth Stott
// Canary: 4e7a2c91-b8d3-4f06-a5e2-913c6d0f7b48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-11-27, THE ONE QUERY IS A REACH FILTER): the ontology's read. A search's
// or a focus's hops are one root field whose where keeps the terms within the hop count chosen,
// each read once, in one round trip, and the query it opens as is that same text. All hops, which
// no fixed filter can express, is walked a hop at a time from its root (each term read once,
// R509-C2), and opens as one Cypher variable-length query from the terms its root read
// (ontologyCypher.ts).

import type { MetaReader } from "../../api/metaGraph";
import type { QueryScope } from "./graphModel";
import {
  ontologyHopQuery,
  ontologyQuery,
  termLevels,
  type OntologyRows,
  type TermRow,
} from "./ontologyData";

/** The terms a term's edges reach, those the role can read. */
const reached = (t: TermRow): number[] => [
  ...(t.edgesOut ?? []).flatMap((e) => (e.targetTerm ? [e.targetTerm.id] : [])),
  ...(t.edgesIn ?? []).flatMap((e) => (e.sourceTerm ? [e.sourceTerm.id] : [])),
];

/** What a read opens as: the GraphQL it read, or -- All, walked past its root -- the Cypher
 * variable-length query from the terms its root read (allHopsCypher, with the server's label),
 * ``depth`` steps out: the hops the walk read to reach the end of the connected part. */
export type Opened =
  { language: "graphql"; query: string } | { language: "cypher"; roots: number[]; depth: number };

/** What a read of the ontology answered -- its terms each once -- the queries it sent,
 * in order, and what it opens as: null when the role cannot read the terms. ``over``: All's walk
 * stopped, the terms read past the budget, before it was done; it opens as the walk that far. */
export interface OntologyRead {
  rows: OntologyRows;
  queries: string[];
  opened: Opened | null;
  over: boolean;
}

const graphql = (query: string | null): Opened | null =>
  query === null ? null : { language: "graphql", query };

/** REQ-2174: the ontology as ``s`` reads it, through ``read`` (one query's answer). The root --
 * ontologyQuery's, its terms only with ``terms`` -- keeps a search's or a focus's hops by its
 * where: one read, opened as itself. At All (termLevels Infinity) the root is read alone, then each
 * hop's terms -- those the last hop's edges reach that no hop has read, by their ids -- to the end
 * of the connected part, or until more terms are read than ``budget`` (each a node drawn): ``over``.
 */
export async function readOntology(
  m: MetaReader,
  s: QueryScope,
  read: (query: string) => Promise<OntologyRows>,
  terms = true,
  budget = Infinity,
): Promise<OntologyRead> {
  const root = ontologyQuery(m, s, terms);
  // Nothing to read (no term at the root, no product apart): it opens as the terms' query.
  if (root === null)
    return { rows: { _terms: [] }, queries: [], opened: graphql(ontologyQuery(m, s)), over: false };
  const rows = await read(root);
  const got = [...(rows._terms ?? [])];
  if (Number.isFinite(termLevels(s)) || got.length === 0)
    return { rows: { ...rows, _terms: got }, queries: [root], opened: graphql(root), over: false };
  const roots = got.map((t) => t.id);
  const seen = new Set(roots);
  const queries = [root];
  let frontier = got;
  for (let hop = 1; ; hop++) {
    // Past the budget the walk stops: it opens as the walk as far as it went.
    if (got.length > budget)
      return {
        rows: { ...rows, _terms: got },
        queries,
        opened: { language: "cypher", roots, depth: queries.length - 1 },
        over: true,
      };
    const ids = [...new Set(frontier.flatMap(reached))].filter((id) => !seen.has(id));
    if (ids.length === 0) break;
    const query = ontologyHopQuery(m, s, hop, ids);
    // The root read the terms, so the role can: a hop that cannot is a broken schema.
    if (query === null) throw new Error("the ontology's terms read at the root but not at a hop");
    ids.forEach((id) => seen.add(id));
    frontier = (await read(query))._terms ?? [];
    got.push(...frontier);
    queries.push(query);
  }
  return {
    rows: { ...rows, _terms: got },
    queries,
    // Each hop read after the root found terms no hop had: the last is the walk's depth.
    opened: { language: "cypher", roots, depth: queries.length - 1 },
    over: false,
  };
}
