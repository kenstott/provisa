// Copyright (c) 2026 Kenneth Stott
// Canary: 7e2c9a41-6b5d-4f83-a0e7-3d1b8c5f9a26
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135 phase 5: SPARQL in the graph visualizer. The role's vocabulary as /data/sparql/vocabulary
// serves it, IRIs shown compacted by its prefixes, a pill's triple pattern added to the query in the
// bar, and an answer of /data/sparql made a frame: a CONSTRUCT's triples drawn as a graph (a literal
// a node of its own, drawn apart from the resources), a SELECT's and an ASK's rows a table.

import type { FrameStatement, GEdge, GNode } from "./graph-model";

/** The role's vocabulary (GET /data/sparql/vocabulary). */
export interface SparqlVocabulary {
  base: string;
  /** Prefix → namespace: the standard ones, ``rel``, ``term`` and one per domain. */
  prefixes: Record<string, string>;
  /** A table's own class ("table") or a Class term's ("term"), with the labels it types and, for a
   * Class term, the terms it is a KIND_OF. */
  classes: { iri: string; kind: "table" | "term"; labels: string[]; superclasses: string[] }[];
  /** A column's predicate ("column") or a Property term's ("term"). */
  predicates: {
    iri: string;
    kind: "column" | "term";
    label: string;
    property: string;
    datatype: string;
  }[];
  /** An edge type's predicate, from its source label's class to its target's. */
  edges: { iri: string; type: string; source: string; target: string }[];
  /** The live Concept terms (the SKOS vocabulary). */
  concepts: { iri: string; name: string; definition: string | null }[];
}

/** A term's name, read back from its IRI under the ``term`` prefix; null for any other IRI. */
export function termName(iri: string, prefixes: Record<string, string>): string | null {
  const ns = prefixes.term;
  return ns !== undefined && iri.startsWith(ns) ? decodeURIComponent(iri.slice(ns.length)) : null;
}

/** The prefixes every vocabulary has; any other is a domain's. */
export const STANDARD_PREFIXES = ["rdf", "rdfs", "owl", "skos", "xsd", "rel", "term"];

export const domainPrefixes = (v: SparqlVocabulary): [string, string][] =>
  Object.entries(v.prefixes).filter(([p]) => !STANDARD_PREFIXES.includes(p));

/** ``prefix:local`` where a prefix's namespace starts the IRI and the rest is a plain local name;
 * otherwise the IRI in angle brackets. */
export function compactIri(iri: string, prefixes: Record<string, string>): string {
  let best: [string, string] | null = null;
  for (const [p, ns] of Object.entries(prefixes))
    if (iri.startsWith(ns) && (best === null || ns.length > best[1].length)) best = [p, ns];
  if (best) {
    const local = iri.slice(best[1].length);
    if (/^[A-Za-z_][\w.-]*$/.test(local) && !local.endsWith(".")) return `${best[0]}:${local}`;
  }
  return `<${iri}>`;
}

/** The prefix a compacted IRI uses, or null for one in angle brackets. */
const prefixOf = (term: string): string | null =>
  term.startsWith("<") ? null : term.slice(0, term.indexOf(":"));

/** The variables the query names, so a pattern added takes a fresh one. */
const variables = (query: string): Set<string> =>
  new Set([...query.matchAll(/[?$]([A-Za-z_]\w*)/g)].map((m) => m[1]));

/** A variable of the query not yet used: ``base``, then ``base2``, ``base3`` and so on. */
export function freshVariable(query: string, base: string): string {
  const used = variables(query);
  if (!used.has(base)) return `?${base}`;
  for (let i = 2; ; i++) if (!used.has(`${base}${i}`)) return `?${base}${i}`;
}

/** ``query`` with its PREFIX lines for ``prefixes`` (those it has not declared), first. */
export function withPrefixes(
  query: string,
  prefixes: string[],
  namespaces: Record<string, string>,
): string {
  const declared = new Set(
    [...query.matchAll(/^\s*PREFIX\s+([A-Za-z_][\w-]*)?:/gim)].map((m) => m[1] ?? ""),
  );
  const missing = [...new Set(prefixes)].filter((p) => !declared.has(p) && p in namespaces);
  if (missing.length === 0) return query;
  const lines = missing.map((p) => `PREFIX ${p}: <${namespaces[p]}>`).join("\n");
  return `${lines}\n${query}`;
}

/**
 * ``query`` with ``pattern`` added to its graph pattern -- the last ``{ ... }`` group, as the reader
 * builds a query up pill by pill -- and the PREFIX lines its terms use. A query with no group
 * becomes a SELECT of the pattern.
 */
/** The group's text so far, its last triple ended with "." where it is left open. */
const closeTriple = (before: string): string => (/[.{}]$/.test(before) ? before : `${before} .`);

export function addPattern(
  query: string,
  pattern: string,
  terms: string[],
  namespaces: Record<string, string>,
): string {
  const close = query.lastIndexOf("}");
  const body =
    close === -1 || query.indexOf("{") === -1
      ? `SELECT * WHERE {\n  ${pattern}\n}\nLIMIT 25`
      : `${closeTriple(query.slice(0, close).replace(/\s*$/, ""))}\n  ${pattern}\n${query.slice(close)}`;
  const used = terms.map(prefixOf).filter((p): p is string => p !== null);
  return withPrefixes(body, used, namespaces);
}

/** A term of a SPARQL answer, as the table and the drawing show it. */
export interface RdfCell {
  rdfTerm: "iri" | "literal" | "bnode";
  /** The IRI, the literal's lexical form, or the blank node's label. */
  value: string;
  /** What is shown: an IRI compacted, a literal's value. */
  shown: string;
  datatype?: string;
  lang?: string;
}

export const isRdfCell = (v: unknown): v is RdfCell =>
  typeof v === "object" && v !== null && "rdfTerm" in v && "shown" in v;

interface JsonTerm {
  type: "uri" | "literal" | "typed-literal" | "bnode";
  value: string;
  datatype?: string;
  "xml:lang"?: string;
}

function cellOf(term: JsonTerm, prefixes: Record<string, string>): RdfCell {
  if (term.type === "uri")
    return { rdfTerm: "iri", value: term.value, shown: compactIri(term.value, prefixes) };
  if (term.type === "bnode")
    return { rdfTerm: "bnode", value: term.value, shown: `_:${term.value}` };
  return {
    rdfTerm: "literal",
    value: term.value,
    shown: term.value,
    datatype: term.datatype,
    lang: term["xml:lang"],
  };
}

/** One triple of an N-Triples document. */
export interface Triple {
  s: RdfCell;
  p: RdfCell;
  o: RdfCell;
}

const NT_TERM = /<([^>]*)>|_:(\S+)|"((?:[^"\\]|\\.)*)"(?:\^\^<([^>]*)>|@([A-Za-z-]+))?/g;

const unescape = (s: string): string =>
  s.replace(/\\(u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8}|.)/g, (_, e: string) =>
    e.length > 1
      ? String.fromCodePoint(parseInt(e.slice(1), 16))
      : (({ n: "\n", t: "\t", r: "\r", b: "\b", f: "\f" } as Record<string, string>)[e] ?? e),
  );

/** The triples of an N-Triples document, each term compacted by ``prefixes`` for showing. */
export function parseNTriples(text: string, prefixes: Record<string, string>): Triple[] {
  const triples: Triple[] = [];
  for (const line of text.split("\n")) {
    if (!line.trim() || line.trimStart().startsWith("#")) continue;
    const terms: RdfCell[] = [];
    for (const m of line.matchAll(NT_TERM)) {
      if (m[1] !== undefined) terms.push(cellOf({ type: "uri", value: m[1] }, prefixes));
      else if (m[2] !== undefined) terms.push(cellOf({ type: "bnode", value: m[2] }, prefixes));
      else
        terms.push(
          cellOf(
            { type: "literal", value: unescape(m[3]), datatype: m[4], "xml:lang": m[5] },
            prefixes,
          ),
        );
    }
    if (terms.length === 3) triples.push({ s: terms[0], p: terms[1], o: terms[2] });
  }
  return triples;
}

const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
/** A literal's label in the drawing: one of its own, so a literal is drawn apart from resources. */
export const LITERAL_LABEL = "Literal";

/**
 * The triples as the canvas draws them: each resource a node, labelled by its class where a triple
 * types it (else its namespace's prefix), each literal object a node of its own labelled Literal,
 * and each triple an edge named by its predicate. An rdf:type triple is drawn too.
 */
export function triplesGraph(triples: Triple[]): {
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
} {
  const typeOf = new Map<string, string>();
  for (const { s, p, o } of triples)
    if (p.value === RDF_TYPE && o.rdfTerm === "iri" && !typeOf.has(s.value))
      typeOf.set(s.value, o.shown);
  // Keyed as GraphCanvas keys a node (`${label}:${id}`), which draws an edge only between two
  // nodes it finds so; a resource is found again by its term.
  const nodes = new Map<string, GNode>();
  const edges = new Map<string, GEdge>();
  const resources = new Map<string, GNode>();
  let next = 0;
  const add = (n: GNode): GNode => {
    nodes.set(`${n.label}:${n.id}`, n);
    return n;
  };
  const resource = (c: RdfCell): GNode => {
    const key = `${c.rdfTerm}:${c.value}`;
    let n = resources.get(key);
    if (!n) {
      const label =
        typeOf.get(c.value) ??
        (c.rdfTerm === "iri" && !c.shown.startsWith("<") ? c.shown.split(":")[0] : "Resource");
      n = {
        id: next++,
        label,
        tableLabel: label,
        properties: { name: c.shown, iri: c.value },
      };
      resources.set(key, add(n));
    }
    return n;
  };
  triples.forEach(({ s, p, o }, i) => {
    const a = resource(s);
    // A literal is the triple's own: the same value under two subjects is two nodes.
    const b =
      o.rdfTerm === "literal"
        ? add({
            id: next++,
            label: LITERAL_LABEL,
            tableLabel: LITERAL_LABEL,
            properties: {
              name: o.shown,
              ...(o.datatype ? { datatype: o.datatype } : {}),
              ...(o.lang ? { lang: o.lang } : {}),
            },
          })
        : resource(o);
    const identity = `t${i}`;
    edges.set(identity, {
      identity,
      start: a.id,
      end: b.id,
      type: p.shown,
      properties: { iri: p.value },
      startNode: a,
      endNode: b,
    });
  });
  return { nodes, edges };
}

/** A refused /data/sparql request's message: the error its JSON body names, else its body. */
export async function refusalText(res: Response): Promise<string> {
  const text = await res.text();
  try {
    return (JSON.parse(text) as { error?: string }).error ?? text;
  } catch {
    // A body that is not JSON is said as it came.
    return text;
  }
}

/** A /data/sparql answer as a frame's statement: a refusal its error; a SELECT's or an ASK's rows a
 * table of RDF cells; a CONSTRUCT's triples drawn. */
export async function sparqlStatement(
  query: string,
  res: Response,
  prefixes: Record<string, string>,
): Promise<FrameStatement> {
  const empty = { nodes: new Map(), edges: new Map(), rows: [], columns: [] };
  if (!res.ok) return { ...empty, query, status: "error", error: await refusalText(res) };
  const type = (res.headers.get("content-type") ?? "").split(";")[0].trim();
  if (type === "application/n-triples" || type === "text/plain") {
    const triples = parseNTriples(await res.text(), prefixes);
    const { nodes, edges } = triplesGraph(triples);
    return {
      query,
      status: "done",
      nodes,
      edges,
      rows: triples.map((t) => ({ subject: t.s, predicate: t.p, object: t.o })),
      columns: ["subject", "predicate", "object"],
      sparqlForm: "graph",
    };
  }
  const body = (await res.json()) as {
    head?: { vars?: string[] };
    boolean?: boolean;
    results?: { bindings: Record<string, JsonTerm>[] };
  };
  if (body.boolean !== undefined)
    return {
      ...empty,
      query,
      status: "done",
      rows: [{ ask: body.boolean }],
      columns: ["ask"],
      sparqlForm: "boolean",
    };
  const columns = body.head?.vars ?? [];
  const rows = (body.results?.bindings ?? []).map((b) =>
    Object.fromEntries(columns.map((v) => [v, b[v] ? cellOf(b[v], prefixes) : null])),
  );
  return { ...empty, query, status: "done", rows, columns, sparqlForm: "rows" };
}

/** What /data/sparql is asked to answer in: a SELECT's or an ASK's rows as JSON, a CONSTRUCT's
 * triples as N-Triples. */
export const SPARQL_ACCEPT = "application/sparql-results+json, application/n-triples;q=0.9";
