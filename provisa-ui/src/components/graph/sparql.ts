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
// bar, and an answer of /data/sparql made a frame: a CONSTRUCT's or DESCRIBE's triples drawn as a
// graph (a literal a property of its subject, or a node of its own on request; an answer about the
// ontology its classes as nodes, with subClassOf, domains and ranges as edges), a SELECT's and an
// ASK's rows a table, a SELECT whose rows hold triples also drawable as a graph.

import type { FrameData, FrameStatement, GEdge, GNode } from "./graph-model";

/** The role's vocabulary (GET /data/sparql/vocabulary). */
export interface SparqlVocabulary {
  base: string;
  /** Prefix → namespace: the standard ones, ``rel``, ``term`` and one per domain. */
  prefixes: Record<string, string>;
  /** A table's own class ("table") or a Class term's ("term"), with the labels it types and, for a
   * Class term, the terms it is a KIND_OF. */
  classes: {
    iri: string;
    kind: "table" | "term";
    labels: string[];
    superclasses: string[];
    /** A Class term's domains (REQ-1723); none for a table's own class. */
    domains?: string[];
  }[];
  /** A column's own predicate ("column"), once per column, or a Property term's ("term"), once,
   * with the classes whose columns bind it (REQ-2163). */
  predicates: {
    iri: string;
    kind: "column" | "term";
    /** A column's own predicate: its node label, property and datatype. */
    label?: string;
    property?: string;
    datatype?: string;
    /** A Property term: its rdfs:domain per class whose column binds it. */
    classes?: { class: string; label: string; property: string; datatype: string }[];
    /** A Property term's domains (REQ-1723); none for a column's own predicate. */
    domains?: string[];
  }[];
  /** An edge type's predicate, from its source label's class to its target's. */
  edges: { iri: string; type: string; source: string; target: string }[];
  /** The live Concept terms (the SKOS vocabulary). */
  concepts: { iri: string; name: string; definition: string | null; domains: string[] }[];
}

/** A term's name, read back from its IRI under the ``term`` prefix; null for any other IRI. */
export function termName(iri: string, prefixes: Record<string, string>): string | null {
  const ns = prefixes.term;
  return ns !== undefined && iri.startsWith(ns) ? decodeURIComponent(iri.slice(ns.length)) : null;
}

/** The datatypes of a numeric column (provisa.sparql_query.vocabulary.xsd_of). */
const NUMERIC_DATATYPES = new Set(
  ["integer", "decimal", "double", "float"].map(
    (d) => `http://www.w3.org/2001/XMLSchema#${d}`,
  ),
);

/** The predicates of a class's columns and the Property terms they bind, by name. */
function classPredicates(
  cls: SparqlVocabulary["classes"][number],
  vocabulary: SparqlVocabulary,
): { iri: string; name: string; datatype: string | undefined }[] {
  const held = new Map<string, { iri: string; name: string; datatype: string | undefined }>();
  for (const x of vocabulary.predicates) {
    const column = x.kind === "column" && cls.labels.includes(x.label ?? "");
    const bound = x.classes?.find((k) => k.class === cls.iri || cls.labels.includes(k.label));
    if (!column && !bound) continue;
    const name = column ? x.property : termName(x.iri, vocabulary.prefixes);
    held.set(x.iri, { iri: x.iri, name: name ?? x.iri, datatype: (bound ?? x).datatype });
  }
  return [...held.values()].sort((a, b) => a.name.localeCompare(b.name));
}

/** What a class's menu offers: its predicates, each by the key a drawn node holds it under, named
 * as the panel names it; and those that are numeric. */
export function classMenuProperties(
  cls: SparqlVocabulary["classes"][number],
  vocabulary: SparqlVocabulary,
  prefixes: Record<string, string>,
): { properties: string[]; propertyNames: Record<string, string>; numericProperties: string[] } {
  const predicates = classPredicates(cls, { ...vocabulary, prefixes }).map((x) => ({
    ...x,
    key: shownIri(x.iri, prefixes),
  }));
  return {
    properties: predicates.map((x) => x.key),
    propertyNames: Object.fromEntries(predicates.map((x) => [x.key, x.name])),
    numericProperties: predicates
      .filter((x) => NUMERIC_DATATYPES.has(x.datatype ?? ""))
      .map((x) => x.key),
  };
}

/** The prefixes every vocabulary has; any other is a domain's. */
export const STANDARD_PREFIXES = ["rdf", "rdfs", "owl", "skos", "xsd", "rel", "term", "id"];

export const domainPrefixes = (v: SparqlVocabulary): [string, string][] =>
  Object.entries(v.prefixes).filter(([p]) => !STANDARD_PREFIXES.includes(p));

/** ``prefix:local`` where a prefix's namespace starts the IRI and the rest is a plain local name;
 * otherwise the IRI in angle brackets. */
export function compactIri(iri: string, prefixes: Record<string, string>): string {
  const best = longestPrefix(iri, prefixes);
  if (best) {
    const local = iri.slice(best[1].length);
    if (/^[A-Za-z_][\w.-]*$/.test(local) && !local.endsWith(".")) return `${best[0]}:${local}`;
  }
  return `<${iri}>`;
}

function longestPrefix(iri: string, prefixes: Record<string, string>): [string, string] | null {
  let best: [string, string] | null = null;
  for (const [p, ns] of Object.entries(prefixes))
    if (ns !== "" && iri.startsWith(ns) && (best === null || ns.length > best[1].length))
      best = [p, ns];
  return best;
}

/** An IRI as a result cell or a graph label shows it: compacted by the longest prefix whose
 * namespace starts it, even where the rest could not stand in a query (``sales:orders#total``,
 * ``id:sales/orders/1``); the whole IRI, in angle brackets, where no prefix does. compactIri is what
 * a query or a Turtle document is written with. */
export function shownIri(iri: string, prefixes: Record<string, string>): string {
  const best = longestPrefix(iri, prefixes);
  if (!best) return `<${iri}>`;
  const local = iri.slice(best[1].length);
  return local === "" || /\s/.test(local) ? `<${iri}>` : `${best[0]}:${local}`;
}

const XSD = "http://www.w3.org/2001/XMLSchema#";
/** The datatypes a literal shows no datatype for, its value saying what it is: text, the numeric
 * types, booleans, and dates and times. Its hover names the datatype. */
const PLAIN_DATATYPES = new Set(
  [
    "string",
    "integer",
    "int",
    "long",
    "short",
    "byte",
    "nonNegativeInteger",
    "positiveInteger",
    "nonPositiveInteger",
    "negativeInteger",
    "unsignedLong",
    "unsignedInt",
    "unsignedShort",
    "unsignedByte",
    "decimal",
    "double",
    "float",
    "boolean",
    "date",
    "dateTime",
    "dateTimeStamp",
    "time",
    "duration",
    "dayTimeDuration",
    "yearMonthDuration",
    "gYear",
    "gYearMonth",
    "gMonth",
    "gMonthDay",
    "gDay",
  ].map((d) => `${XSD}${d}`),
);

/** A literal as a cell or a graph label shows it: plainly where its value says what it is, one with
 * a language tag or another datatype as Turtle writes it, its datatype compacted. */
export function shownLiteral(
  value: string,
  datatype: string | undefined,
  lang: string | undefined,
  prefixes: Record<string, string>,
): string {
  if (lang) return `"${value}"@${lang}`;
  if (datatype === undefined || PLAIN_DATATYPES.has(datatype)) return value;
  return `"${value}"^^${shownIri(datatype, prefixes)}`;
}

/** The prefixes a query declares for itself, each by its namespace. */
export const queryPrefixes = (query: string): Record<string, string> =>
  Object.fromEntries(
    [...query.matchAll(/\bPREFIX\s+([A-Za-z_][\w-]*)?:\s*<([^>]*)>/gi)].map((m) => [m[1] ?? "", m[2]]),
  );

/** What the viewer has changed of the prefixes: a prefix's namespace, or null where it is removed. */
export type PrefixEdits = Record<string, string | null>;

/** The prefixes in use: the vocabulary's and the ``id`` namespace its instances are minted under,
 * with the viewer's edits. */
export function effectivePrefixes(
  vocabulary: Pick<SparqlVocabulary, "base" | "prefixes">,
  edits: PrefixEdits,
): Record<string, string> {
  const held: Record<string, string | null> = {
    ...vocabulary.prefixes,
    id: `${vocabulary.base}/id/`,
    ...edits,
  };
  return Object.fromEntries(
    Object.entries(held).filter((e): e is [string, string] => e[1] !== null),
  );
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

/** What a vocabulary pill asks of the query. */
export type VocabularyPick =
  | { kind: "class" | "predicate" | "edge" | "concept"; iri: string }
  | { kind: "prefix"; prefix: string };

/** ``query`` with what ``pick`` adds (REQ-2135). A class types ?s, a property hangs a fresh object
 * off it, a relationship adds both its ends typed by its domain and range (relationshipPattern), a
 * concept is read for what is said of it, and a prefix is declared. */
export function pickedQuery(
  query: string,
  pick: VocabularyPick,
  vocabulary: SparqlVocabulary,
): string {
  const p = vocabulary.prefixes;
  if (pick.kind === "prefix") return withPrefixes(query, [pick.prefix], p);
  if (pick.kind === "edge") {
    const rel = relationshipPattern(query, pick.iri, vocabulary);
    return addPattern(query, rel.pattern, rel.terms, p);
  }
  const term = compactIri(pick.iri, p);
  const pattern =
    pick.kind === "class"
      ? `?s a ${term} .`
      : pick.kind === "concept"
        ? `${term} ${freshVariable(query, "p")} ${freshVariable(query, "o")} .`
        : `?s ${term} ${freshVariable(query, "o")} .`;
  return addPattern(query, pattern, [term], p);
}

/** The query a class or property pill runs as its own result card, as a label pill's
 * ``MATCH (n:Label) RETURN n LIMIT 25`` does in Cypher: a class's instances, a property's triples. */
export function pillQuery(
  pick: Extract<VocabularyPick, { iri: string }>,
  vocabulary: SparqlVocabulary,
): string {
  if (pick.kind !== "class") return pickedQuery("", pick, vocabulary);
  const cls = vocabulary.classes.find((x) => x.iri === pick.iri);
  if (!cls) throw new Error(`the vocabulary has no class ${pick.iri}`);
  const wide = classQuery(cls, vocabulary);
  return wide ?? pickedQuery("", pick, vocabulary);
}

/** ``name`` as a variable none of ``used`` is. */
function ownVariable(name: string, used: Set<string>): string {
  const safe = name.replace(/[^A-Za-z0-9_]/g, "_");
  let variable = safe;
  for (let i = 2; used.has(variable); i++) variable = `${safe}${i}`;
  used.add(variable);
  return variable;
}

/**
 * A class as a table, as a Cypher label's nodes read: one row per instance, one column per property
 * the role reads of the class, each by its plain name and OPTIONAL so a row with no value stays.
 * A column's predicate holds a ``#``, which starts a comment in a query, so its class's namespace
 * gets a prefix of its own; a predicate no prefix can name is written whole. Null for a class with
 * no property.
 */
function classQuery(
  cls: SparqlVocabulary["classes"][number],
  vocabulary: SparqlVocabulary,
): string | null {
  const predicates = classPredicates(cls, vocabulary);
  if (predicates.length === 0) return null;
  let own = "p";
  for (let i = 2; own in vocabulary.prefixes; i++) own = `p${i}`;
  const namespaces = { ...vocabulary.prefixes, [own]: `${cls.iri}#` };
  const term = compactIri(cls.iri, namespaces);
  const used = new Set(["s"]);
  const columns = predicates.map((x) => ({
    term: compactIri(x.iri, namespaces),
    variable: ownVariable(x.name, used),
  }));
  const body = [
    `SELECT ?s ${columns.map((c) => `?${c.variable}`).join(" ")} WHERE {`,
    `  ?s a ${term} .`,
    ...columns.map((c) => `  OPTIONAL { ?s ${c.term} ?${c.variable} }`),
    "}",
    "LIMIT 25",
  ].join("\n");
  const named = [term, ...columns.map((c) => c.term)].map(prefixOf);
  return withPrefixes(
    body,
    named.filter((p): p is string => p !== null),
    namespaces,
  );
}

/** The variables ``query`` types, each with the classes it is typed as (``?s a pet-store:pets``). */
function typedVariables(query: string): Map<string, Set<string>> {
  const held = new Map<string, Set<string>>();
  for (const m of query.matchAll(/[?$](\w+)\s+(?:a|rdf:type)\s+(<[^>]*>|[^\s.;,{}]+)/g))
    held.set(m[1], new Set([...(held.get(m[1]) ?? []), m[2]]));
  return held;
}

/**
 * The pattern a relationship pill adds (REQ-2135): both ends of the relationship, each typed by the
 * class the vocabulary gives the edge's source (domain) and target (range) label. Where a variable
 * of the query already holds the source class it is the subject (``?s rel ?o . ?o a range``); where
 * one holds the target class it becomes the object (``?x rel ?s . ?x a domain``); else a fresh pair
 * is added (``?a a domain . ?a rel ?b . ?b a range``). ``terms`` are what the pattern names, for
 * its PREFIX lines.
 */
export function relationshipPattern(
  query: string,
  iri: string,
  vocabulary: SparqlVocabulary,
): { pattern: string; terms: string[] } {
  const p = vocabulary.prefixes;
  const edges = vocabulary.edges.filter((e) => e.iri === iri);
  if (edges.length === 0) throw new Error(`the vocabulary has no relationship ${iri}`);
  const classOf = (label: string): string => {
    const c = vocabulary.classes.find((x) => x.kind === "table" && x.labels.includes(label));
    if (!c) throw new Error(`the vocabulary has no class for the label ${label}`);
    return c.iri;
  };
  const ends = edges.map((e) => ({ source: classOf(e.source), target: classOf(e.target) }));
  const rel = compactIri(iri, p);
  const held = typedVariables(query);
  const holder = (cls: string): string | undefined =>
    [...held].find(([, types]) => types.has(compactIri(cls, p)) || types.has(`<${cls}>`))?.[0];
  for (const e of ends) {
    const subject = holder(e.source);
    if (subject !== undefined) {
      const o = freshVariable(query, "o");
      const range = compactIri(e.target, p);
      return { pattern: `?${subject} ${rel} ${o} . ${o} a ${range} .`, terms: [rel, range] };
    }
  }
  for (const e of ends) {
    const object = holder(e.target);
    if (object !== undefined) {
      const x = freshVariable(query, "x");
      const domain = compactIri(e.source, p);
      return { pattern: `${x} ${rel} ?${object} . ${x} a ${domain} .`, terms: [rel, domain] };
    }
  }
  const [e] = ends;
  const a = freshVariable(query, "s");
  const b = freshVariable(`${query} ${a}`, "o");
  const domain = compactIri(e.source, p);
  const range = compactIri(e.target, p);
  return {
    pattern: `${a} a ${domain} . ${a} ${rel} ${b} . ${b} a ${range} .`,
    terms: [domain, rel, range],
  };
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
    return { rdfTerm: "iri", value: term.value, shown: shownIri(term.value, prefixes) };
  if (term.type === "bnode")
    return { rdfTerm: "bnode", value: term.value, shown: `_:${term.value}` };
  return {
    rdfTerm: "literal",
    value: term.value,
    shown: shownLiteral(term.value, term.datatype, term["xml:lang"], prefixes),
    datatype: term.datatype,
    lang: term["xml:lang"],
  };
}

/** ``cell`` as ``prefixes`` show it. */
function shownBy(cell: RdfCell, prefixes: Record<string, string>): RdfCell {
  if (cell.rdfTerm === "bnode") return cell;
  // A bound predicate nothing names has no term to show.
  if (cell.rdfTerm === "iri" && cell.value === "") return cell;
  return {
    ...cell,
    shown:
      cell.rdfTerm === "iri"
        ? shownIri(cell.value, prefixes)
        : shownLiteral(cell.value, cell.datatype, cell.lang, prefixes),
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

const RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS = "http://www.w3.org/2000/01/rdf-schema#";
const OWL = "http://www.w3.org/2002/07/owl#";
const RDF_TYPE = `${RDF}type`;
const RDFS_DOMAIN = `${RDFS}domain`;
const RDFS_RANGE = `${RDFS}range`;
const RDFS_SUBCLASS = `${RDFS}subClassOf`;
/** What types a class or a property in an answer about the ontology. */
const CLASS_TYPES = new Set([`${OWL}Class`, `${RDFS}Class`]);
const PROPERTY_TYPES = new Set([
  `${RDF}Property`,
  `${OWL}ObjectProperty`,
  `${OWL}DatatypeProperty`,
]);
/** A literal's label in the drawing: one of its own, so a literal is drawn apart from resources. */
export const LITERAL_LABEL = "Literal";

/** The last two path segments of an IRI (``pets/3`` of ``https://x.example/id/pet-store/pets/3``),
 * the tail that identifies it without its base; the whole IRI where it has no path. */
export function iriTail(iri: string): string {
  const path = iri.replace(/^[A-Za-z][A-Za-z0-9+.-]*:\/\/[^/]*/, "");
  const segments = path.split("/").filter((x) => x !== "");
  return segments.length === 0 ? iri : segments.slice(-2).join("/");
}

/** An IRI's local name: what follows its last ``/``, ``#`` or ``:``. */
const iriLocal = (iri: string): string =>
  iri
    .split(/[/#:]/)
    .filter((x) => x !== "")
    .pop() ?? iri;

/** A resource or class named for the drawing: its compacted name where the vocabulary has a prefix
 * for it, else the tail of its IRI; a blank node by its label. */
const resourceName = (c: RdfCell): string =>
  c.rdfTerm === "iri" && !isPrefixedName(c) ? iriTail(c.value) : c.shown;

/** The label a class's nodes are drawn with, which what is kept per class goes by. */
export const classLabel = (iri: string, prefixes: Record<string, string>): string =>
  resourceName({ rdfTerm: "iri", value: iri, shown: shownIri(iri, prefixes) });

/** Whether an IRI's shown form is a prefix and a plain local name: not the whole IRI, and not a
 * shortened path such as ``id:sales/orders/1``, which says nothing a label could. */
const isPrefixedName = (c: RdfCell): boolean => !c.shown.startsWith("<") && !/[/#%]/.test(c.shown);

/** Predicates that name what they are said of, in the order they are preferred: a predicate's
 * local name, lower-cased (``rdfs:label`` and a column's ``name`` both read so). */
const NAMING = ["name", "label", "preflabel"];

const predicateLocal = (iri: string): string => iri.split(/[/#:]/).pop()?.toLowerCase() ?? "";

const isString = (c: RdfCell): boolean =>
  c.datatype === undefined || c.datatype === "http://www.w3.org/2001/XMLSchema#string";

/** What a resource is captioned by: its name-like literal (name, label, rdfs:label,
 * skos:prefLabel), else its first string literal, else its name (resourceName). */
function captionOf(subject: RdfCell, literals: Triple[]): string {
  const mine = literals.filter(
    (t) => t.s.value === subject.value && t.s.rdfTerm === subject.rdfTerm,
  );
  for (const key of NAMING) {
    const named = mine.find((t) => predicateLocal(t.p.value) === key && isString(t.o));
    if (named) return named.o.value;
  }
  const first = mine.find((t) => isString(t.o));
  return first ? first.o.value : resourceName(subject);
}

/** A node's id, the same for one resource in every drawing of a frame (cyrb53). */
function stableId(key: string): number {
  let h1 = 0xdeadbeef;
  let h2 = 0x41c6ce57;
  for (let i = 0; i < key.length; i++) {
    const ch = key.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return 4294967296 * (2097151 & h2) + (h1 >>> 0);
}

const tripleKey = ({ s, p, o }: Triple): string =>
  [s, p, o].map((c) => `${c.rdfTerm}|${c.value}|${c.datatype ?? ""}|${c.lang ?? ""}`).join("\t");

/** Each triple once, the first it was said. */
export const distinctTriples = (triples: Triple[]): Triple[] => [
  ...new Map(triples.map((t) => [tripleKey(t), t])).values(),
];

/** A range that is a datatype: its property holds literals, not resources. */
const isDatatype = (iri: string): boolean => iri.startsWith(XSD) || iri === `${RDFS}Literal`;

/**
 * An answer about the ontology as it is drawn: its classes, with each property that has a domain
 * and a range an edge from the one to the other named by the property, and one whose range is a
 * datatype a property of its class; a property with only one of them stays a node of its own, joined
 * to it by rdfs:domain or rdfs:range. ``classes`` are the classes' IRIs, each with the class that
 * types it. An answer with no such triples comes back as it was.
 */
function ontologyTriples(triples: Triple[]): { triples: Triple[]; classes: Map<string, RdfCell> } {
  const classes = new Map<string, RdfCell>();
  const domains = new Map<string, Triple[]>();
  const ranges = new Map<string, Triple[]>();
  const add = (into: Map<string, Triple[]>, t: Triple) =>
    into.set(t.s.value, [...(into.get(t.s.value) ?? []), t]);
  const meta = { rdfTerm: "iri" as const, value: `${RDFS}Class`, shown: "rdfs:Class" };
  for (const t of triples) {
    if (t.p.value === RDF_TYPE && CLASS_TYPES.has(t.o.value)) classes.set(t.s.value, t.o);
    else if (t.p.value === RDFS_DOMAIN) add(domains, t);
    else if (t.p.value === RDFS_RANGE) add(ranges, t);
  }
  const axiomatic = (t: Triple) => t.p.value === RDFS_SUBCLASS || t.p.value === RDFS_DOMAIN;
  for (const t of triples) {
    if (axiomatic(t) && !classes.has(t.o.value)) classes.set(t.o.value, meta);
    if (t.p.value === RDFS_SUBCLASS && !classes.has(t.s.value)) classes.set(t.s.value, meta);
    if (t.p.value === RDFS_RANGE && !isDatatype(t.o.value) && !classes.has(t.o.value))
      classes.set(t.o.value, meta);
  }
  if (classes.size === 0) return { triples, classes };
  const joined = new Set([...domains.keys()].filter((p) => ranges.has(p)));
  const out: Triple[] = [];
  for (const p of joined)
    for (const d of domains.get(p) ?? [])
      for (const r of ranges.get(p) ?? [])
        out.push(
          isDatatype(r.o.value)
            ? { s: d.o, p: d.s, o: { rdfTerm: "literal", value: r.o.shown, shown: r.o.shown } }
            : { s: d.o, p: d.s, o: r.o },
        );
  for (const t of triples) {
    const metaType =
      t.p.value === RDF_TYPE &&
      (CLASS_TYPES.has(t.o.value) || (PROPERTY_TYPES.has(t.o.value) && joined.has(t.s.value)));
    const held = (t.p.value === RDFS_DOMAIN || t.p.value === RDFS_RANGE) && joined.has(t.s.value);
    if (!metaType && !held) out.push(t);
  }
  return { triples: out, classes };
}

/** The prefixes a frame's terms show by: the ones in use, and those its query declares for what they
 * do not already name. A query's ``p:`` for a class's columns renames none of them: what is kept per
 * class and predicate goes by the names in use. */
function shownPrefixes(
  prefixes: Record<string, string>,
  query: string,
): Record<string, string> {
  const namespaces = Object.values(prefixes).filter((ns) => ns !== "");
  const own = Object.entries(queryPrefixes(query)).filter(
    ([p, ns]) => p in prefixes || !namespaces.some((n) => ns.startsWith(n)),
  );
  return { ...prefixes, ...Object.fromEntries(own) };
}

/** ``value`` beside what a predicate holds: one value alone, several a list. */
const listed = (held: unknown, value: string): unknown =>
  held === undefined ? value : Array.isArray(held) ? [...held, value] : [held, value];

/**
 * The triples as the canvas draws them: each resource a node, labelled by its class where a triple
 * types it (else its namespace's prefix), and each triple whose object is a resource an edge named
 * by its predicate; an rdf:type triple is drawn too. A literal object is a property of its subject,
 * as the Cypher view shows a node's properties (a predicate with several literals holds a list);
 * with ``literalNodes`` it is a node of its own labelled Literal, drawn apart from the resources.
 * An answer about the ontology is drawn as ontologyTriples says.
 */
export function triplesGraph(
  answered: Triple[],
  literalNodes = false,
  /** The class a query types a resource as, where no triple does (a SELECT's rows). */
  typed: ReadonlyMap<string, string> = new Map(),
): {
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
} {
  const { triples, classes } = ontologyTriples(answered);
  const typeOf = new Map<string, string>();
  for (const [iri, cls] of classes) typeOf.set(iri, resourceName(cls));
  for (const { s, p, o } of triples)
    if (p.value === RDF_TYPE && o.rdfTerm === "iri" && !typeOf.has(s.value))
      typeOf.set(s.value, resourceName(o));
  for (const [iri, label] of typed) if (!typeOf.has(iri)) typeOf.set(iri, label);
  const literals = triples.filter((t) => t.o.rdfTerm === "literal");
  // A class (an rdf:type object) is captioned in its circle by its local name (pets), its compacted
  // name (pet-store:pets) kept as a property for the hover and the inspector.
  const classValues = new Set([
    ...classes.keys(),
    ...triples.filter((t) => t.p.value === RDF_TYPE && t.o.rdfTerm === "iri").map((t) => t.o.value),
  ]);
  // Keyed as GraphCanvas keys a node (`${label}:${id}`), which draws an edge only between two
  // nodes it finds so; a resource is found again by its term.
  const nodes = new Map<string, GNode>();
  const edges = new Map<string, GEdge>();
  const resources = new Map<string, GNode>();
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
        (c.rdfTerm === "iri" && isPrefixedName(c) ? c.shown.split(":")[0] : "Resource");
      const identity = c.rdfTerm === "iri" ? { iri: c.value } : {};
      n = {
        id: stableId(key),
        label,
        tableLabel: label,
        propertyIris: {},
        propertyValues: {},
        propertyDatatypes: {},
        properties: classValues.has(c.value)
          ? { name: iriLocal(c.value), class: resourceName(c), ...identity }
          : { name: captionOf(c, literals), ...identity },
      };
      resources.set(key, add(n));
    }
    return n;
  };
  triples.forEach((t, i) => {
    const { s, p, o } = t;
    const a = resource(s);
    if (o.rdfTerm === "literal" && !literalNodes) {
      a.properties[p.shown] = listed(a.properties[p.shown], o.shown);
      if (a.propertyIris) a.propertyIris[p.shown] = p.value;
      if (a.propertyValues) a.propertyValues[p.shown] = listed(a.propertyValues[p.shown], o.value);
      if (a.propertyDatatypes && o.datatype) a.propertyDatatypes[p.shown] = o.datatype;
      return;
    }
    // A literal node is the triple's own: the same value under two subjects is two nodes.
    const b =
      o.rdfTerm === "literal"
        ? add({
            id: stableId(`literal:${i}:${tripleKey(t)}`),
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
      properties: p.value === "" ? {} : { iri: p.value },
      startNode: a,
      endNode: b,
    });
  });
  return { nodes, edges };
}

/** The class each variable of ``query`` is typed as (``?s a pet-store:pets``): by the label its
 * nodes are drawn with where ``prefixes`` read it, else as the query names it, the tail of its IRI
 * where the query writes it in full. */
function queryClasses(query: string, prefixes?: Record<string, string>): Map<string, string> {
  return new Map(
    [...typedVariables(query)].map(([variable, classes]) => {
      const [first] = classes;
      const iri = prefixes
        ? writtenPredicate(first, { ...queryPrefixes(query), ...prefixes })
        : null;
      if (prefixes && iri) return [variable, classLabel(iri, prefixes)];
      return [variable, first.startsWith("<") ? iriTail(first.slice(1, -1)) : first];
    }),
  );
}

/** How a SELECT's variables pair into an edge: its subject and object columns, and the column a
 * bound predicate is read from (null where the query binds none). */
export interface SelectPairing {
  subject: string;
  predicate: string | null;
  object: string;
  /** The predicate the query's own pattern names between them, where it names one. */
  iri?: string;
}

const isResource = (v: unknown): boolean =>
  isRdfCell(v) && (v.rdfTerm === "iri" || v.rdfTerm === "bnode");

/** A triple pattern of a query's WHERE, each term as it is written. */
interface WrittenPattern {
  s: string;
  p: string;
  o: string;
}

const PATTERN_TOKEN = /<[^>]*>|"(?:[^"\\]|\\.)*"|[?$]\w+|[\w-]*:[^\s;,{}()]*|[;,{}()]|\.(?=\s|$|})|[^\s;,{}()]+/g;

/** The triple patterns written in ``query``'s WHERE, ``;`` and ``,`` read as they abbreviate. Text
 * that is no triple (a FILTER's expression, a keyword) resets the reading, so it gives none. */
function writtenPatterns(query: string): WrittenPattern[] {
  const out: WrittenPattern[] = [];
  let run: string[] = [];
  const close = () => {
    if (run.length === 3) out.push({ s: run[0], p: run[1], o: run[2] });
  };
  for (const [token] of query.slice(query.indexOf("{")).matchAll(PATTERN_TOKEN)) {
    if (token === ";") {
      close();
      run = run.slice(0, 1);
    } else if (token === ",") {
      close();
      run = run.slice(0, 2);
    } else if (token === "." || "{}()".includes(token)) {
      close();
      run = [];
    } else {
      run.push(token);
    }
  }
  return out;
}

/** A predicate as ``query`` writes it, read as its IRI; null where it names none. */
function writtenPredicate(token: string, namespaces: Record<string, string>): string | null {
  if (token === "a") return RDF_TYPE;
  if (token.startsWith("<")) return token.slice(1, -1);
  const colon = token.indexOf(":");
  if (colon === -1 || token.startsWith("?") || token.startsWith("$")) return null;
  const ns = namespaces[token.slice(0, colon)];
  return ns === undefined ? null : `${ns}${token.slice(colon + 1)}`;
}

/**
 * Whether a SELECT's rows hold triples, and so can be drawn as a graph (REQ-2135): every row binds a
 * resource to its subject and a term to its object. Three variables hold one where the query's own
 * pattern puts them in subject, predicate and object position; two where they are the ends of one
 * of its patterns, whose predicate names the edge and whose object may be a literal, or are both
 * resources. Null for any other. ``namespaces`` reads a predicate the query writes as a prefixed
 * name, with the prefixes it declares itself.
 */
export function selectPairing(
  query: string,
  columns: string[],
  rows: Record<string, unknown>[],
  namespaces: Record<string, string> = {},
): SelectPairing | null {
  if (rows.length === 0 || columns.length < 2 || columns.length > 3) return null;
  const patterns = writtenPatterns(query);
  const named = (name: string, token: string) => token === `?${name}` || token === `$${name}`;
  const bound = (column: string, holds: (v: unknown) => boolean) =>
    rows.every((r) => holds(r[column]));
  if (columns.length === 3) {
    const [s, p, o] = columns;
    return patterns.some((t) => named(s, t.s) && named(p, t.p) && named(o, t.o)) &&
      bound(s, isResource) &&
      bound(p, isResource) &&
      bound(o, isRdfCell)
      ? { subject: s, predicate: p, object: o }
      : null;
  }
  const [a, b] = columns;
  if (!bound(a, isResource)) return null;
  const ns = { ...namespaces, ...queryPrefixes(query) };
  const written = patterns.find((t) => named(a, t.s) && named(b, t.o));
  const iri = written && writtenPredicate(written.p, ns);
  if (iri && bound(b, isRdfCell)) return { subject: a, predicate: null, object: b, iri };
  return bound(b, isResource) ? { subject: a, predicate: null, object: b } : null;
}

/** A SELECT's rows as triples, one per row: the predicate bound or the one its pattern names, else
 * an unnamed edge. */
export function pairedTriples(pairing: SelectPairing, rows: Record<string, unknown>[]): Triple[] {
  const written: RdfCell = pairing.iri
    ? { rdfTerm: "iri", value: pairing.iri, shown: `<${pairing.iri}>` }
    : { rdfTerm: "iri", value: "", shown: "" };
  return rows.map((r) => ({
    s: r[pairing.subject] as RdfCell,
    p: pairing.predicate === null ? written : (r[pairing.predicate] as RdfCell),
    o: r[pairing.object] as RdfCell,
  }));
}

/** The triples a frame's drawing holds that are triples at all: an edge of a SELECT no predicate
 * names is none. Empty for a frame that holds none. */
export function frameTriples(frame: FrameData): Triple[] {
  const { sparqlPairing: pairing } = frame;
  const answered =
    frame.sparqlTriples ??
    (pairing && (pairing.predicate !== null || pairing.iri)
      ? pairedTriples(pairing, frame.rows)
      : []);
  return distinctTriples([...answered, ...Object.values(frame.sparqlExpansions ?? {}).flat()]);
}

/** What a DESCRIBE's literal triples say of their resource as node properties, keyed and listed as
 * the drawing lists a literal (a predicate with several values holds a list). Without a
 * drawing's own identity rows (name, iri, class): the node already has those. */
export function describedProperties(literals: Triple[]): {
  properties: Record<string, unknown>;
  propertyIris: Record<string, string>;
  propertyValues: Record<string, unknown>;
  propertyDatatypes: Record<string, string>;
} {
  const own = new Set(["name", "iri", "class"]);
  const properties: Record<string, unknown> = {};
  const propertyIris: Record<string, string> = {};
  const propertyValues: Record<string, unknown> = {};
  const propertyDatatypes: Record<string, string> = {};
  for (const node of triplesGraph(literals).nodes.values()) {
    for (const [k, v] of Object.entries(node.properties)) if (!own.has(k)) properties[k] = v;
    Object.assign(propertyIris, node.propertyIris);
    Object.assign(propertyValues, node.propertyValues);
    Object.assign(propertyDatatypes, node.propertyDatatypes);
  }
  return { properties, propertyIris, propertyValues, propertyDatatypes };
}

const shownTriple = (t: Triple, prefixes: Record<string, string>): Triple => ({
  s: shownBy(t.s, prefixes),
  p: shownBy(t.p, prefixes),
  o: shownBy(t.o, prefixes),
});

/** ``drawn`` with what a DESCRIBE said of each of its nodes on the node, by the prefixes in use. */
function withDescribed(drawn: FrameData, prefixes?: Record<string, string>): FrameData {
  const { sparqlDescribed: described } = drawn;
  if (!described || drawn.nodes.size === 0) return drawn;
  const nodes = new Map(drawn.nodes);
  for (const [key, n] of nodes) {
    const entry = typeof n.properties.iri === "string" ? described[n.properties.iri] : undefined;
    if (!entry || !("literals" in entry)) continue;
    const said = describedProperties(
      prefixes ? entry.literals.map((t) => shownTriple(t, prefixes)) : entry.literals,
    );
    nodes.set(key, {
      ...n,
      properties: { ...said.properties, ...n.properties },
      propertyIris: { ...said.propertyIris, ...n.propertyIris },
      propertyValues: { ...said.propertyValues, ...n.propertyValues },
      propertyDatatypes: { ...said.propertyDatatypes, ...n.propertyDatatypes },
    });
  }
  return { ...drawn, nodes };
}

/** ``frame`` as it is drawn: a CONSTRUCT's triples with their literals as properties or, with
 * ``literalNodes``, as nodes; a pairable SELECT's rows, once ``drawSelect`` asks, as nodes and
 * edges; and whatever expanding its nodes merged in. Any other frame is as answered. With
 * ``prefixes``, every term shows by them, and by those the frame's query declares. */
export function sparqlDrawn(
  frame: FrameData,
  literalNodes: boolean,
  drawSelect: boolean,
  prefixes?: Record<string, string>,
): FrameData {
  const shown = prefixes && shownPrefixes(prefixes, frame.query);
  return withDescribed(drawnAnswer(frame, literalNodes, drawSelect, shown), shown);
}

function drawnAnswer(
  frame: FrameData,
  literalNodes: boolean,
  drawSelect: boolean,
  shown: Record<string, string> | undefined,
): FrameData {
  const cells = (ts: Triple[]): Triple[] => (shown ? ts.map((t) => shownTriple(t, shown)) : ts);
  const rows = shown
    ? frame.rows.map((r) =>
        Object.fromEntries(
          Object.entries(r).map(([k, v]) => [k, isRdfCell(v) ? shownBy(v, shown) : v]),
        ),
      )
    : frame.rows;
  const merged = Object.values(frame.sparqlExpansions ?? {}).flat();
  if (frame.sparqlTriples) {
    const triples = cells(distinctTriples([...frame.sparqlTriples, ...merged]));
    return {
      ...frame,
      ...triplesGraph(triples, literalNodes),
      rows: triples.map((t) => ({ subject: t.s, predicate: t.p, object: t.o })),
    };
  }
  if (drawSelect && frame.sparqlPairing) {
    const pairing = frame.sparqlPairing;
    // The class the query types each end as (``?s a pet-store:pets``), for the nodes' labels.
    const classes = queryClasses(frame.query, shown);
    const typed = new Map<string, string>();
    for (const [column, variable] of [
      [pairing.subject, pairing.subject],
      [pairing.object, pairing.object],
    ]) {
      const cls = classes.get(variable);
      if (cls === undefined) continue;
      for (const r of rows)
        if ((r[column] as RdfCell).rdfTerm !== "literal") typed.set((r[column] as RdfCell).value, cls);
    }
    const triples = cells(distinctTriples([...pairedTriples(pairing, rows), ...merged]));
    return { ...frame, ...triplesGraph(triples, false, typed), rows };
  }
  if (merged.length > 0) return { ...frame, ...triplesGraph(cells(merged), literalNodes), rows };
  return shown ? { ...frame, rows } : frame;
}

/** A refused request's error: its body's ``detail`` (the app's error body) or ``error`` when it is
 * JSON that has one, else the body as sent. */
export function refusalOf(body: string): string {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return body;
  }
  if (parsed !== null && typeof parsed === "object") {
    const said =
      (parsed as { detail?: unknown; error?: unknown }).detail ??
      (parsed as { error?: unknown }).error;
    if (typeof said === "string") return said;
    if (said !== undefined) return JSON.stringify(said);
  }
  return body;
}

/** A refused /data/sparql request's message. */
export const refusalText = async (res: Response): Promise<string> => refusalOf(await res.text());

/** A /data/sparql answer as a frame's statement: a refusal its error; a SELECT's or an ASK's rows a
 * table of RDF cells; a CONSTRUCT's or a DESCRIBE's triples drawn. Terms show by ``prefixes`` and
 * those the query declares. */
export async function sparqlStatement(
  query: string,
  res: Response,
  namespaces: Record<string, string>,
): Promise<FrameStatement> {
  const prefixes = shownPrefixes(namespaces, query);
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
      sparqlTriples: triples,
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
  const pairing = selectPairing(query, columns, rows, prefixes);
  return {
    ...empty,
    query,
    status: "done",
    rows,
    columns,
    sparqlForm: "rows",
    ...(pairing ? { sparqlPairing: pairing } : {}),
  };
}

/** What /data/sparql is asked to answer in: a SELECT's or an ASK's rows as JSON, a CONSTRUCT's or a
 * DESCRIBE's triples as N-Triples. */
export const SPARQL_ACCEPT = "application/sparql-results+json, application/n-triples;q=0.9";

/** The DESCRIBE a node's expansion runs: what the model says of the resource. */
export const describeQuery = (iri: string): string => `DESCRIBE <${iri}>`;

/** The CONSTRUCT a class pill dropped on a frame runs: the class's instances and what is said of
 * them, merged into the frame. */
export const classConstruct = (iri: string): string =>
  `CONSTRUCT { ?s ?p ?o } WHERE { ?s a <${iri}> ; ?p ?o } LIMIT 25`;
