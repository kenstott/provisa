// Copyright (c) 2026 Kenneth Stott
// Canary: 5a8d3f72-0c19-4e64-9b2a-6f1e7d4c8b03
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135 phase 5: the SPARQL vocabulary, in the sidebar where Cypher's labels are -- Domains,
// Classes (Class terms and tables' classes, each Class term with the classes it is a KIND_OF),
// Properties (Property terms and columns),
// Relationships (the edge types between the tables' labels),
// Concepts (the SKOS vocabulary) and Prefixes. A pill adds to the query in the bar: a class, a
// property, a relationship or a concept its triple pattern; a prefix or a domain its PREFIX line,
// and a domain picked narrows the lists to its vocabulary.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { Tip } from "../Tip";
import { compactIri, domainPrefixes, termName, type SparqlVocabulary } from "./sparql";

/** What a pill asks of the query. */
export type VocabularyPick =
  | { kind: "class" | "predicate" | "edge" | "concept"; iri: string }
  | { kind: "prefix"; prefix: string };

function Pill({
  text,
  tip,
  onClick,
  testId,
  muted,
}: {
  text: string;
  tip: string;
  onClick: () => void;
  testId: string;
  muted?: string;
}) {
  return (
    <div className="graph-label-item">
      <Tip text={tip}>
        <button type="button" className="graph-label-pill" onClick={onClick} data-testid={testId}>
          {text}
          {muted && (
            <span style={{ opacity: 0.7, fontSize: "0.7em", marginInlineStart: "0.25em" }}>
              {muted}
            </span>
          )}
        </button>
      </Tip>
    </div>
  );
}

export function SparqlVocabularyPanel({
  vocabulary,
  error,
  onPick,
}: {
  /** null while it is read. */
  vocabulary: SparqlVocabulary | null;
  error: string | null;
  onPick: (pick: VocabularyPick) => void;
}) {
  const { t } = useTranslation();
  const [domain, setDomain] = useState<string | null>(null);
  const shown = useMemo(() => {
    if (vocabulary === null) return null;
    const p = vocabulary.prefixes;
    const ns = domain === null ? null : p[domain];
    const inDomain = (iri: string) => ns === null || iri.startsWith(ns);
    const c = (iri: string) => compactIri(iri, p);
    // An edge is between two labels; it is in a domain where either label's table class is.
    const labelInDomain = (label: string) =>
      vocabulary.classes.some(
        (x) => x.kind === "table" && x.labels.includes(label) && inDomain(x.iri),
      );
    const byName = <T,>(rows: T[], name: (r: T) => string) =>
      [...rows].sort((a, b) => name(a).localeCompare(name(b)));
    return {
      classes: byName(
        vocabulary.classes.filter((x) => inDomain(x.iri) || x.kind === "term"),
        (x) => c(x.iri),
      ).map((x) => ({
        ...x,
        shown: c(x.iri),
        term: termName(x.iri, p),
        parents: x.superclasses.map(c),
      })),
      predicates: byName(
        vocabulary.predicates.filter((x) => inDomain(x.iri) || x.kind === "term"),
        (x) => c(x.iri),
      ).map((x) => {
        const iri = c(x.iri);
        // A column's IRI has no prefixed name (its local part holds '#'): it is shown by its column.
        return {
          ...x,
          shown: x.kind === "column" && iri.startsWith("<") ? x.label : iri,
          term: termName(x.iri, p),
        };
      }),
      edges: byName(
        [...new Map(vocabulary.edges.map((e) => [e.iri, e])).values()].filter(
          (e) => labelInDomain(e.source) || labelInDomain(e.target),
        ),
        (e) => c(e.iri),
      ).map((e) => ({ ...e, shown: c(e.iri) })),
      concepts: byName(vocabulary.concepts, (x) => x.name).map((x) => ({ ...x, shown: c(x.iri) })),
    };
  }, [vocabulary, domain]);

  if (error) return <div className="graph-schema-empty">{error}</div>;
  if (vocabulary === null || shown === null)
    return <div className="graph-schema-empty">{t("graphSidebar.loading")}</div>;

  const empty = <div className="graph-schema-empty">{t("sparqlVocabulary.none")}</div>;
  const section = (value: string, title: string, children: React.ReactNode) => (
    <CollapsibleItem value={value} title={title} controlTestId={`sparql-vocab-${value}-toggle`}>
      <div className="graph-label-list">{children}</div>
    </CollapsibleItem>
  );

  return (
    <div className="graph-schema-section" data-testid="sparql-vocabulary">
      <Collapsibles id="sparql.vocabulary">
        {section(
          "domains",
          t("sparqlVocabulary.domains"),
          domainPrefixes(vocabulary).map(([prefix, ns]) => (
            <Pill
              key={prefix}
              text={prefix}
              tip={
                domain === prefix
                  ? t("sparqlVocabulary.domainPicked", { namespace: ns })
                  : t("sparqlVocabulary.domainTip", { namespace: ns })
              }
              muted={domain === prefix ? "✓" : undefined}
              onClick={() => {
                setDomain(domain === prefix ? null : prefix);
                onPick({ kind: "prefix", prefix });
              }}
              testId={`sparql-domain-${prefix}`}
            />
          )),
        )}
        {section(
          "classes",
          t("sparqlVocabulary.classes"),
          shown.classes.length === 0
            ? empty
            : shown.classes.map((x) => (
                <Pill
                  key={x.iri}
                  text={x.shown}
                  muted={x.parents.length > 0 ? `⊂ ${x.parents.join(", ")}` : undefined}
                  tip={[
                    x.iri,
                    x.kind === "term"
                      ? t("sparqlVocabulary.classTerm", { term: x.term ?? "" })
                      : t("sparqlVocabulary.tableClass"),
                  ].join(" · ")}
                  onClick={() => onPick({ kind: "class", iri: x.iri })}
                  testId={`sparql-class-${x.shown}`}
                />
              )),
        )}
        {section(
          "properties",
          t("sparqlVocabulary.properties"),
          shown.predicates.length === 0
            ? empty
            : shown.predicates.map((x) => (
                <Pill
                  key={x.iri}
                  text={x.shown}
                  tip={[
                    x.iri,
                    x.kind === "term"
                      ? t("sparqlVocabulary.propertyTerm", { term: x.term ?? "" })
                      : t("sparqlVocabulary.columnPredicate", { column: x.label }),
                  ].join(" · ")}
                  onClick={() => onPick({ kind: "predicate", iri: x.iri })}
                  testId={`sparql-predicate-${x.shown}`}
                />
              )),
        )}
        {section(
          "relationships",
          t("sparqlVocabulary.relationships"),
          shown.edges.length === 0
            ? empty
            : shown.edges.map((e) => (
                <Pill
                  key={e.iri}
                  text={e.shown}
                  tip={`${e.iri} · ${e.source} → ${e.target}`}
                  onClick={() => onPick({ kind: "edge", iri: e.iri })}
                  testId={`sparql-edge-${e.shown}`}
                />
              )),
        )}
        {section(
          "concepts",
          t("sparqlVocabulary.concepts"),
          shown.concepts.length === 0
            ? empty
            : shown.concepts.map((x) => (
                <Pill
                  key={x.iri}
                  text={x.name}
                  tip={x.definition ? `${x.iri} · ${x.definition}` : x.iri}
                  onClick={() => onPick({ kind: "concept", iri: x.iri })}
                  testId={`sparql-concept-${x.name}`}
                />
              )),
        )}
        {section(
          "prefixes",
          t("sparqlVocabulary.prefixes"),
          Object.entries(vocabulary.prefixes).map(([prefix, ns]) => (
            <Pill
              key={prefix}
              text={`${prefix}:`}
              tip={ns}
              onClick={() => onPick({ kind: "prefix", prefix })}
              testId={`sparql-prefix-${prefix}`}
            />
          )),
        )}
      </Collapsibles>
    </div>
  );
}
