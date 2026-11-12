// Copyright (c) 2026 Kenneth Stott
// Canary: 4e7a2c91-b8d3-4f06-a5e2-913c6d0f7b48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (ONE ONTOLOGY GRAPH; EVERY OTHER GRAPH IS A SNIPPET OF IT): the ontology's read, a hop
// at a time. A search's or a focus's neighbourhood was one query, each level nested both ways
// under the last, so each term was read again on each path to it, with its refs, and the query
// grew some 2.5 times a hop (R509-C2). Read frontier by frontier, each term is read once.

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

/** What a read of the ontology answered, and the queries it sent, in order. */
export interface OntologyRead {
  rows: OntologyRows;
  queries: string[];
}

/** REQ-2174: the ontology as ``s`` reads it, through ``read`` (one query's answer). The root --
 * ontologyQuery's, its terms only with ``terms`` -- then, under a search or a focus, each hop to
 * termLevels: the terms the last hop's edges reach that no hop has read, by their ids. Each term
 * is read once, with its refs once, so the reads grow with what is drawn, not a level nested both
 * ways under each path. It stops at a hop that reaches nothing new. */
export async function readOntology(
  m: MetaReader,
  s: QueryScope,
  read: (query: string) => Promise<OntologyRows>,
  terms = true,
): Promise<OntologyRead> {
  const root = ontologyQuery(m, s, terms);
  if (root === null) return { rows: { _terms: [] }, queries: [] };
  const rows = await read(root);
  const got = [...(rows._terms ?? [])];
  const seen = new Set(got.map((t) => t.id));
  const queries = [root];
  let frontier = got;
  for (let hop = 1; hop <= termLevels(s); hop++) {
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
  return { rows: { ...rows, _terms: got }, queries };
}
