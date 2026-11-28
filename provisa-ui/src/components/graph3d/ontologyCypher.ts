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

/** The Cypher label the ontology's terms are served under, as ``role`` reads them: the server's
 * own (/data/graph-schema, by the terms table's registration), never derived here. */
export async function termsCypherLabel(role: string): Promise<string> {
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
  const body = (await resp.json()) as { node_labels: { label: string; table_id: number }[] };
  const found = body.node_labels.find((n) => n.table_id === tables[0].id);
  if (found === undefined) throw new Error("the ontology's terms have no Cypher label for the role");
  return found.label;
}

/** All hops from ``roots`` (the terms the root read: a focus's, or a search's matches), every
 * relationship any number of steps away, as one Cypher query. */
export function allHopsCypher(label: string, roots: readonly number[]): string {
  const ids = [...roots].sort((a, b) => a - b);
  return ids.length === 1
    ? `MATCH p = (t:${label} {id: ${ids[0]}})-[*]-(n) RETURN p`
    : `MATCH p = (t:${label})-[*]-(n) WHERE t.id IN [${ids.join(", ")}] RETURN p`;
}
