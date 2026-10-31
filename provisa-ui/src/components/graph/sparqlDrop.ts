// Copyright (c) 2026 Kenneth Stott
// Canary: e41c8f07-96ab-4d23-b7e0-5a2d1f8c3b96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: a pill dropped on a SPARQL frame edits the frame's query, which is then run, so the query
// always says what is on screen. A class or a domain is reached by the shortest paths through the
// role's relationships from the classes the query holds, a relationship or a property from the
// variables whose class has it. What is added sits in OPTIONAL groups, one per path, so what the
// frame held stays. The subject of a variable predicate (?s ?p ?o) is of every class, so a drop on
// one fans out over them. A drop that cannot join the result says so and changes nothing, and a
// parameterized table, which only arguments read, is never what a path adds.

import type { DropNotice } from "./graph-model";
import { MAX_SCHEMA_HOPS, MAX_SCHEMA_PATHS, shortestSchemaPaths } from "./schemaPath";
import {
  QUERY_TOKEN,
  RDF_TYPE,
  addPattern,
  allPrefixes,
  classLabel,
  closeTriple,
  compactIri,
  freshVariable,
  iriLocal,
  queryPrefixes,
  shownIri,
  termName,
  writtenPatterns,
  writtenPredicate,
  type SparqlVocabulary,
} from "./sparql";

/** What a LIMIT is raised by: a path can answer a row several times, and a LIMIT left as it was would
 * cut rows the frame held. A sub-SELECT could hold the frame's rows and read paths from them, but
 * the endpoint refuses one, and dropping the LIMIT would take the allowance a limited query has for
 * its parts (provisa.gql.terms.union_allowance). */
export const DROP_LIMIT_FACTOR = 10;

/** What a relationship pill and a property pill carry to a frame. */
export const EDGE_DRAG = "text/x-provisa-edge";
export const PROPERTY_DRAG = "text/x-provisa-property";

/** What is dropped on a frame. */
export type SparqlDrop =
  | { kind: "class"; iri: string }
  | { kind: "edge"; iri: string }
  | { kind: "predicate"; iri: string }
  | { kind: "domain"; prefix: string };

/** The node of a drawing a pill was dropped on. */
export interface DropNode {
  iri: string;
  label: string;
}

export type SparqlDropped =
  | {
      query: string;
      notices: DropNotice[];
      /** The LIMIT the query now has, where it has one. */
      limit?: number;
    }
  | { refused: DropNotice };

type Edge = SparqlVocabulary["edges"][number];

/** One OPTIONAL a drop adds: its triples, the terms they name, and the variables they bind. */
interface Attachment {
  triples: string[];
  terms: string[];
  variables: string[];
  filter?: string;
}

interface Token {
  text: string;
  at: number;
}

/** ``query``'s tokens, without its white space and comments. */
const tokensOf = (query: string): Token[] =>
  [...query.matchAll(QUERY_TOKEN)]
    .map((m) => ({ text: m[0], at: m.index }))
    .filter((t) => t.text.trim() !== "" && !t.text.startsWith("#"));

const isWord = (t: Token | undefined, ...words: string[]): boolean =>
  t !== undefined && words.includes(t.text.toUpperCase());

/** The aggregates of SPARQL 1.1. */
const AGGREGATES = ["COUNT", "SUM", "AVG", "MIN", "MAX", "SAMPLE", "GROUP_CONCAT"];

/** Why a pattern can't be added to a query of this shape; null where one can. */
function shapeRefusal(tokens: Token[]): DropNotice | null {
  const form = tokens.find((t) => isWord(t, "SELECT", "CONSTRUCT", "ASK", "DESCRIBE"));
  if (!form || isWord(form, "ASK", "DESCRIBE"))
    return { key: "graphFrame.sparqlDropForm", values: { form: form?.text ?? "" } };
  const followedBy = (i: number, text: string) => tokens[i + 1]?.text.toUpperCase() === text;
  if (tokens.some((t) => isWord(t, "UNION"))) return { key: "graphFrame.sparqlDropUnion" };
  if (tokens.some((t, i) => isWord(t, "HAVING") || (isWord(t, "GROUP") && followedBy(i, "BY"))))
    return { key: "graphFrame.sparqlDropGroupBy" };
  if (tokens.some((t, i) => isWord(t, ...AGGREGATES) && followedBy(i, "(")))
    return { key: "graphFrame.sparqlDropAggregate" };
  if (tokens.filter((t) => isWord(t, "SELECT")).length > 1)
    return { key: "graphFrame.sparqlDropSubquery" };
  return null;
}

/** The labels no path includes: their tables are read by arguments. */
const parameterizedLabels = (vocabulary: SparqlVocabulary): Set<string> =>
  new Set(vocabulary.classes.flatMap((c) => c.parameterized_labels));

/** The labels each variable of ``query`` can be: those its triples allow, intersected, as the
 * compiler reads them; and the variables only a variable predicate places, which can be of any
 * class a query can read without arguments. */
function variableLabels(
  query: string,
  vocabulary: SparqlVocabulary,
  namespaces: Record<string, string>,
): { held: Map<string, string[]>; open: Set<string> } {
  const held = new Map<string, string[]>();
  const placed = new Set<string>();
  const parameterized = parameterizedLabels(vocabulary);
  const everyClass = vocabulary.classes
    .filter((c) => c.kind === "table")
    .flatMap((c) => c.labels)
    .filter((l) => !parameterized.has(l));
  const intersect = (name: string, labels: string[]) => {
    const known = held.get(name);
    held.set(name, known ? known.filter((l) => labels.includes(l)) : labels);
  };
  const allow = (token: string, labels: string[]) => {
    if (!/^[?$]\w/.test(token) || labels.length === 0) return;
    placed.add(token.slice(1));
    intersect(token.slice(1), labels);
  };
  for (const { s, p, o } of writtenPatterns(query)) {
    if (/^[?$]\w/.test(p)) {
      if (/^[?$]\w/.test(s)) intersect(s.slice(1), everyClass);
      continue;
    }
    const predicate = writtenPredicate(p, namespaces);
    if (predicate === null) continue;
    if (predicate === RDF_TYPE) {
      const iri = writtenPredicate(o.replace(/\.$/, ""), namespaces);
      allow(s, vocabulary.classes.find((c) => c.iri === iri)?.labels ?? []);
      continue;
    }
    const edges = vocabulary.edges.filter((e) => e.iri === predicate);
    allow(
      s,
      edges.map((e) => e.source),
    );
    allow(
      o,
      edges.map((e) => e.target),
    );
    allow(
      s,
      vocabulary.predicates.flatMap((x) =>
        x.iri !== predicate
          ? []
          : x.classes
            ? x.classes.map((k) => k.label)
            : x.label !== undefined
              ? [x.label]
              : [],
      ),
    );
  }
  return { held, open: new Set([...held.keys()].filter((name) => !placed.has(name))) };
}

/** ``name`` as a variable's base: letters, digits and underscores, not starting with a digit. */
const variableBase = (name: string): string => {
  const safe = name.replace(/[^A-Za-z0-9_]/g, "_");
  return /^[A-Za-z_]/.test(safe) ? safe : `v${safe}`;
};

/** ``query`` with ``triples`` in its CONSTRUCT template. The short form, whose template is its
 * pattern, is written out first: an OPTIONAL can't stand in it. */
function withTemplate(query: string, triples: string[]): string {
  const tokens = tokensOf(query);
  const form = tokens.findIndex((t) => isWord(t, "CONSTRUCT"));
  const open = tokens.findIndex((t, i) => i > form && t.text === "{");
  let depth = 0;
  let close = open;
  for (; close < tokens.length; close++) {
    if (tokens[close].text === "{") depth++;
    if (tokens[close].text === "}" && --depth === 0) break;
  }
  const end = tokens[close].at;
  const before = query.slice(0, end).trimEnd();
  const added = `\n  ${triples.join(" . ")} .\n`;
  if (!isWord(tokens[form + 1], "WHERE"))
    return `${closeTriple(before)}${added}${query.slice(end)}`;
  const template = query.slice(tokens[open].at + 1, end).trim();
  const head = query.slice(0, tokens[form + 1].at);
  return `${head}{\n  ${closeTriple(template)}${added}} WHERE {\n  ${template}\n${query.slice(end)}`;
}

/** ``query`` with ``variables`` in its SELECT's named projection; a ``SELECT *`` projects them. */
function withProjection(query: string, variables: string[]): string {
  const tokens = tokensOf(query);
  const select = tokens.findIndex((t) => isWord(t, "SELECT"));
  const first = isWord(tokens[select + 1], "DISTINCT", "REDUCED") ? select + 2 : select + 1;
  if (tokens[first].text === "*") return query;
  const end = tokens.findIndex((t, i) => i > select && (isWord(t, "WHERE") || t.text === "{"));
  const at = tokens[end].at;
  return `${query.slice(0, at).trimEnd()} ${variables.join(" ")} ${query.slice(at)}`;
}

/** ``query`` with its LIMIT raised by DROP_LIMIT_FACTOR, and that LIMIT; a query with none keeps
 * none. */
function withRaisedLimit(query: string): { query: string; limit?: number } {
  const tokens = tokensOf(query);
  const last = tokens.findLastIndex((t) => isWord(t, "LIMIT"));
  const given = tokens[last + 1];
  if (last === -1 || !/^\d+$/.test(given?.text ?? "")) return { query };
  const limit = Number(given.text) * DROP_LIMIT_FACTOR;
  const rest = query.slice(given.at + given.text.length);
  return { query: `${query.slice(0, given.at)}${limit}${rest}`, limit };
}

/** ``query`` with each of ``found`` in an OPTIONAL of its own, projected or templated, and its LIMIT
 * raised. */
function withAttachments(
  query: string,
  found: Attachment[],
  namespaces: Record<string, string>,
): { query: string; limit?: number } {
  const construct = tokensOf(query).some((t) => isWord(t, "CONSTRUCT"));
  const shaped = construct
    ? withTemplate(
        query,
        found.flatMap((a) => a.triples),
      )
    : withProjection(
        query,
        found.flatMap((a) => a.variables),
      );
  const added = found.reduce((q, a) => {
    const filter = a.filter ? ` ${a.filter}` : "";
    return addPattern(q, `OPTIONAL { ${a.triples.join(" . ")} .${filter} }`, a.terms, namespaces);
  }, shaped);
  return withRaisedLimit(added);
}

/**
 * ``query`` with ``drop`` added (REQ-2135), or why it can't be. ``on`` is the node of a drawing it
 * was dropped on, which what is added then starts from and only there; ``rows`` the frame's rows,
 * which say the variable holding that node. ``vocabulary.prefixes`` are the prefixes in use.
 */
export function droppedQuery(
  query: string,
  drop: SparqlDrop,
  vocabulary: SparqlVocabulary,
  on: DropNode | null,
  rows: Record<string, unknown>[],
): SparqlDropped {
  const refusal = shapeRefusal(tokensOf(query));
  if (refusal) return { refused: refusal };
  const named = allPrefixes(vocabulary);
  const namespaces = { ...named, ...queryPrefixes(query) };
  const term = (iri: string) => compactIri(iri, namespaces);
  const shown = (iri: string) => shownIri(iri, namespaces);
  const { held: all, open } = variableLabels(query, vocabulary, namespaces);
  const parameterized = parameterizedLabels(vocabulary);

  /** The variables the drop may start from: those of the node it was dropped on, else all. */
  const holders = ((): Map<string, string[]> => {
    if (on === null) return all;
    const cls = vocabulary.classes.find((c) => classLabel(c.iri, namespaces) === on.label);
    const of = [...all].filter(([, labels]) => labels.some((l) => cls?.labels.includes(l)));
    const holding = of.filter(([name]) =>
      rows.some((r) => (r[name] as { value?: string } | null | undefined)?.value === on.iri),
    );
    // A CONSTRUCT's rows are its triples, which name no variable: the class says.
    return new Map(
      (holding.length > 0 ? holding : of)
        .slice(0, 1)
        .map(([name, labels]) => [name, labels.filter((l) => cls?.labels.includes(l))]),
    );
  })();
  if (on !== null && holders.size === 0) return { refused: { key: "graphFrame.sparqlDropNode" } };
  if (holders.size === 0) return { refused: { key: "graphFrame.sparqlDropNoClass" } };
  const holding = (label: string): string[] =>
    [...holders].filter(([, labels]) => labels.includes(label)).map(([name]) => name);
  const filterOf = (variable: string): string | undefined =>
    on === null ? undefined : `FILTER(?${variable} = <${on.iri}>)`;

  const classOf = (label: string): string => {
    const c = vocabulary.classes.find((x) => x.kind === "table" && x.labels.includes(label));
    if (!c) throw new Error(`the vocabulary has no class for the label ${label}`);
    return c.iri;
  };
  // Every variable written so far, so no two of those added are one.
  let written = query;
  const fresh = (base: string): string => {
    const variable = freshVariable(written, variableBase(base));
    written = `${written} ${variable}`;
    return variable;
  };
  const className = (iri: string): string => termName(iri, vocabulary.prefixes) ?? iriLocal(iri);

  /** A path of ``steps`` from ``start``, each step typing the class it reaches. */
  const pathFrom = (
    start: string,
    steps: { edge: Edge; from: string; to: string }[],
    ending: string | null,
  ): Attachment => {
    const out: Attachment = { triples: [], terms: [], variables: [], filter: filterOf(start) };
    let at = `?${start}`;
    steps.forEach(({ edge, from, to }, i) => {
      const cls = i === steps.length - 1 && ending !== null ? ending : classOf(to);
      const next = fresh(className(cls));
      const rel = term(edge.iri);
      // Along the relationship, or against it when the path walks it from its target.
      const forward = edge.source === from && edge.target === to;
      out.triples.push(forward ? `${at} ${rel} ${next}` : `${next} ${rel} ${at}`);
      out.triples.push(`${next} a ${term(cls)}`);
      out.terms.push(rel, term(cls));
      out.variables.push(next);
      at = next;
    });
    return out;
  };

  /** The attachments of ``candidates``, up to MAX_SCHEMA_PATHS, with the notice of those left out. */
  const added = (candidates: (() => Attachment)[]): SparqlDropped => {
    const kept = candidates.slice(0, MAX_SCHEMA_PATHS).map((make) => make());
    const notices: DropNotice[] =
      candidates.length > kept.length
        ? [
            {
              key: "graphFrame.sparqlDropLeftOut",
              values: { shown: kept.length, total: candidates.length },
            },
          ]
        : [];
    return { ...withAttachments(query, kept, namespaces), notices };
  };

  if (drop.kind === "edge") {
    const edges = vocabulary.edges.filter((e) => e.iri === drop.iri);
    if (edges.length === 0) throw new Error(`the vocabulary has no relationship ${drop.iri}`);
    const ways = (edge: Edge, from: string, to: string) =>
      holding(from).map((v) => () => pathFrom(v, [{ edge, from, to }], null));
    const reachable = (from: string, to: string) => !parameterized.has(to) && holding(from);
    const candidates = edges.flatMap((edge) => [
      ...(reachable(edge.source, edge.target) ? ways(edge, edge.source, edge.target) : []),
      ...(edge.source !== edge.target && reachable(edge.target, edge.source)
        ? ways(edge, edge.target, edge.source)
        : []),
    ]);
    if (candidates.length === 0) {
      const held = edges.some((e) => holding(e.source).length + holding(e.target).length > 0);
      return {
        refused: {
          key: held ? "graphFrame.sparqlDropEdgeParameterized" : "graphFrame.sparqlDropNoEnd",
          values: { name: shown(drop.iri) },
        },
      };
    }
    return added(candidates);
  }

  if (drop.kind === "predicate") {
    const entries = vocabulary.predicates.filter((x) => x.iri === drop.iri);
    if (entries.length === 0) throw new Error(`the vocabulary has no predicate ${drop.iri}`);
    const labels = entries.flatMap((x) =>
      x.classes ? x.classes.map((k) => k.label) : x.label !== undefined ? [x.label] : [],
    );
    const variables = [...new Set(labels.flatMap(holding))];
    if (variables.length === 0)
      return {
        refused: { key: "graphFrame.sparqlDropNoProperty", values: { name: shown(drop.iri) } },
      };
    const name =
      entries[0].property ?? termName(drop.iri, vocabulary.prefixes) ?? iriLocal(drop.iri);
    return added(
      variables.map((v) => () => {
        const value = fresh(name);
        return {
          triples: [`?${v} ${term(drop.iri)} ${value}`],
          terms: [term(drop.iri)],
          variables: [value],
          filter: filterOf(v),
        };
      }),
    );
  }

  const heldLabels = [...new Set([...holders.values()].flat())];
  /** The labels a variable is said to be: an open subject stands for every class, a domain's too. */
  const namedLabels = [...holders].flatMap(([name, labels]) => (open.has(name) ? [] : labels));
  const target =
    drop.kind === "class"
      ? { name: shown(drop.iri), ending: drop.iri, labels: classLabels(vocabulary, drop.iri) }
      : {
          name: drop.prefix,
          ending: null,
          labels: vocabulary.classes
            .filter((c) => c.kind === "table" && c.iri.startsWith(vocabulary.prefixes[drop.prefix]))
            .flatMap((c) => c.labels)
            .filter((l) => !namedLabels.includes(l)),
        };
  if (target.labels.length > 0 && target.labels.every((l) => parameterized.has(l)))
    return {
      refused: { key: "graphFrame.sparqlDropParameterized", values: { name: target.name } },
    };
  const { paths, hops } = shortestSchemaPaths(
    vocabulary.edges,
    heldLabels,
    target.labels,
    parameterized,
  );
  if (paths.length === 0) {
    const classes = [
      ...new Set(
        [...holders].flatMap(([name, labels]) =>
          open.has(name) ? [`?${name}`] : labels.map((l) => shown(classOf(l))),
        ),
      ),
    ].join(", ");
    return {
      refused:
        hops === null
          ? { key: "graphFrame.sparqlDropNoPath", values: { name: target.name, classes } }
          : {
              key: "graphFrame.sparqlDropTooFar",
              values: { name: target.name, classes, hops, most: MAX_SCHEMA_HOPS },
            },
    };
  }
  return added(
    paths.flatMap((steps) =>
      holding(steps[0].from).map((v) => () => pathFrom(v, steps, target.ending)),
    ),
  );
}

function classLabels(vocabulary: SparqlVocabulary, iri: string): string[] {
  const cls = vocabulary.classes.find((c) => c.iri === iri);
  if (!cls) throw new Error(`the vocabulary has no class ${iri}`);
  return cls.labels;
}
