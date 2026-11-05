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
// answers them. A graph no one query answers -- one drawn from a SELECT's rows, or holding what
// expanding its nodes merged in -- has its triples written here instead, as Turtle or N-Triples.

import type { SparqlForm } from "./graph-model";
import { compactIri, refusalText, type RdfCell, type Triple } from "./sparql";

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

/** The formats a graph's own triples are written in here, its default first. */
export const TRIPLE_FORMATS: SparqlExportFormat[] = SPARQL_EXPORT_FORMATS.graph.slice(0, 2);

/** A frame's answer an export can write: its query and the form it was answered in. */
export interface SparqlExportable {
  id: string;
  query: string;
  form: SparqlForm;
  /** The graph drawn, where no one query answers it: written here, by ``prefixes``. */
  triples?: Triple[];
  prefixes?: Record<string, string>;
  /** Its rows draw a graph whose edges no predicate names, so they hold no triples to write. */
  unnamedEdges?: boolean;
}

/** The formats ``frame`` is written in, its default first. */
export function exportFormats(frame: SparqlExportable): SparqlExportFormat[] {
  if (!frame.triples) return SPARQL_EXPORT_FORMATS[frame.form];
  return frame.form === "graph"
    ? TRIPLE_FORMATS
    : [...SPARQL_EXPORT_FORMATS[frame.form], ...TRIPLE_FORMATS];
}

const STRING_ESCAPES: Record<string, string> = {
  "\\": "\\\\",
  '"': '\\"',
  "\n": "\\n",
  "\r": "\\r",
  "\t": "\\t",
};
const quoted = (value: string): string =>
  `"${value.replace(/[\\"\n\r\t]/g, (c) => STRING_ESCAPES[c])}"`;

const XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";
const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/** One RDF term in N-Triples, or in Turtle where ``iri`` names its IRIs by a prefix. */
function termText(c: RdfCell, iri: (value: string) => string): string {
  if (c.rdfTerm === "iri") return iri(c.value);
  if (c.rdfTerm === "bnode") return `_:${c.value}`;
  if (c.lang) return `${quoted(c.value)}@${c.lang}`;
  return c.datatype && c.datatype !== XSD_STRING
    ? `${quoted(c.value)}^^${iri(c.datatype)}`
    : quoted(c.value);
}

const wholeIri = (value: string): string => `<${value}>`;

/** ``triples`` as an N-Triples document. */
export const writeNTriples = (triples: Triple[]): string =>
  triples
    .map((t) => `${termText(t.s, wholeIri)} ${termText(t.p, wholeIri)} ${termText(t.o, wholeIri)} .\n`)
    .join("");

/** ``triples`` as a Turtle document: declaring the prefixes it uses, each subject once, its
 * predicates and their objects under it. */
export function writeTurtle(triples: Triple[], prefixes: Record<string, string>): string {
  const used = new Set<string>();
  const name = (value: string): string => {
    const compact = compactIri(value, prefixes);
    if (!compact.startsWith("<")) used.add(compact.slice(0, compact.indexOf(":")));
    return compact;
  };
  const subjects = new Map<string, Map<string, string[]>>();
  for (const t of triples) {
    const s = termText(t.s, name);
    const p = t.p.value === RDF_TYPE ? "a" : termText(t.p, name);
    const predicates = subjects.get(s) ?? subjects.set(s, new Map()).get(s)!;
    predicates.set(p, [...(predicates.get(p) ?? []), termText(t.o, name)]);
  }
  const blocks = [...subjects].map(([s, predicates]) => {
    const said = [...predicates].map(([p, os]) => `  ${p} ${os.join(" , ")}`).join(" ;\n");
    return `${s}\n${said} .\n`;
  });
  const declared = [...used].sort().map((p) => `@prefix ${p}: <${prefixes[p]}> .\n`);
  return `${declared.join("")}${declared.length > 0 ? "\n" : ""}${blocks.join("\n")}`;
}

/** ``triples`` in ``format``, ready to download. */
export function tripleBlob(
  triples: Triple[],
  format: SparqlExportFormat,
  prefixes: Record<string, string>,
): Blob {
  const text = format.key === "ntriples" ? writeNTriples(triples) : writeTurtle(triples, prefixes);
  return new Blob([text], { type: format.media });
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
