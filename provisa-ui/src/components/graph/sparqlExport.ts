// Copyright (c) 2026 Kenneth Stott
// Canary: 21720d9d-244c-4598-aede-0d440b9e1eb3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: the SPARQL tab's exports, in SPARQL's own formats. A graph answer (a CONSTRUCT's
// triples) is RDF -- Turtle, N-Triples or JSON-LD -- and a SELECT's or an ASK's is SPARQL results.
// An export asks /data/sparql the frame's query again in the chosen format, so the server's one
// writer (provisa.sparql_query.results) writes it, with IRIs on server.public_url as SPARQL
// answers them.

import type { SparqlForm } from "./graph-model";
import { refusalText } from "./sparql";

export interface SparqlExportFormat {
  /** Its label's key, under sparqlExportModal.formats. */
  key: string;
  media: string;
  ext: string;
}

const SPARQL_RESULTS_JSON = { key: "json", media: "application/sparql-results+json", ext: "json" };

/** The formats each form of answer is written in, its default first. */
export const SPARQL_EXPORT_FORMATS: Record<SparqlForm, SparqlExportFormat[]> = {
  graph: [
    { key: "turtle", media: "text/turtle", ext: "ttl" },
    { key: "ntriples", media: "application/n-triples", ext: "nt" },
    { key: "jsonld", media: "application/ld+json", ext: "jsonld" },
  ],
  rows: [
    SPARQL_RESULTS_JSON,
    { key: "csv", media: "text/csv", ext: "csv" },
    { key: "tsv", media: "text/tab-separated-values", ext: "tsv" },
  ],
  // A boolean has no CSV or TSV.
  boolean: [SPARQL_RESULTS_JSON],
};

/** A frame's answer an export can write: its query and the form it was answered in. */
export interface SparqlExportable {
  id: string;
  query: string;
  form: SparqlForm;
}

/** ``query`` answered by /data/sparql in ``media``; a refusal throws, worded as the server words it. */
export async function fetchSparqlExport(
  query: string,
  media: string,
  roleHeader: string | null | undefined,
): Promise<Blob> {
  const headers: Record<string, string> = {
    "Content-Type": "application/sparql-query",
    Accept: media,
  };
  if (roleHeader) headers["X-Provisa-Role"] = roleHeader;
  const res = await fetch("/data/sparql", { method: "POST", headers, body: query });
  if (!res.ok) throw new Error(await refusalText(res));
  return res.blob();
}
