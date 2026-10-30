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
// Concepts (the SKOS vocabulary) and Prefixes. A class, property, relationship or concept pill
// starts a new frame, as a Cypher label pill does; a prefix or a domain adds its PREFIX line to the
// query in the bar, and a domain picked narrows the lists to its vocabulary. A class pill dropped on
// a frame merges the class's instances into it. Properties are named by their local names: a
// Property term once, with the classes that have it, and each column's own under its class; the
// search reads those names across every class.

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { TextInput } from "@mantine/core";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { Tip } from "../Tip";
import { labelColor } from "./graph-model";
import { compactIri, domainPrefixes, termName, type SparqlVocabulary } from "./sparql";

import type { VocabularyPick } from "./sparql";

export type { VocabularyPick };

interface PropertyPill {
  iri: string;
  /** Its local name: the pill's text, and what the search reads. */
  name: string;
  testId: string;
  tip: string;
}

/** The columns' own predicates of one class. */
interface PropertyGroup {
  key: string;
  title: string;
  properties: PropertyPill[];
}

function Pill({
  text,
  tip,
  onClick,
  testId,
  muted,
  color,
  badge,
  dragged,
}: {
  text: string;
  tip: string;
  onClick: () => void;
  testId: string;
  muted?: string;
  /** The pill's colour: a label's, as the Cypher sidebar colours it. */
  color?: string;
  /** Drawn as a relationship type is in the Cypher sidebar. */
  badge?: boolean;
  /** What a drop on a frame reads, as a Cypher label pill carries its label. */
  dragged?: string;
}) {
  return (
    <div className="graph-label-item">
      <Tip text={tip}>
        <button
          type="button"
          className={badge ? "graph-rel-badge" : "graph-label-pill"}
          style={color ? { background: color } : undefined}
          onClick={onClick}
          draggable={dragged !== undefined}
          onDragStart={
            dragged === undefined
              ? undefined
              : (e) => e.dataTransfer.setData("text/x-provisa-label", dragged)
          }
          data-testid={testId}
        >
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
  prefixes,
  error,
  onPick,
  colorOverrides,
  checkedDomains,
}: {
  /** null while it is read. */
  vocabulary: SparqlVocabulary | null;
  /** The prefixes in use, the viewer's edits with them; the vocabulary's own by default. */
  prefixes?: Record<string, string>;
  error: string | null;
  onPick: (pick: VocabularyPick) => void;
  /** The page's per-label colours, shared with the Cypher page: one class, one colour. */
  colorOverrides: Record<string, string>;
  /** The navbar's domain selection; none checked is every domain, as the Cypher sidebar reads it. */
  checkedDomains: ReadonlySet<string>;
}) {
  const { t } = useTranslation();
  const [domain, setDomain] = useState<string | null>(null);
  const [propertySearch, setPropertySearch] = useState("");
  const named = prefixes ?? vocabulary?.prefixes;
  const shown = useMemo(() => {
    if (vocabulary === null || named === undefined) return null;
    const p = vocabulary.prefixes;
    const ns = domain === null ? null : p[domain];
    // The selected domains' namespaces: only their vocabulary is listed.
    const allowed =
      checkedDomains.size === 0 ? null : [...checkedDomains].flatMap((d) => (d in p ? [p[d]] : []));
    const inDomain = (iri: string) =>
      (ns === null || iri.startsWith(ns)) &&
      (allowed === null || allowed.some((a) => iri.startsWith(a)));
    const c = (iri: string) => compactIri(iri, named);
    // REQ-1723, REQ-2163: a term or concept is shown when one of its domains is checked (or it is
    // enterprise-wide, "*"), and one with no domain always; none checked is every domain.
    const termVisible = (domains: readonly string[] | undefined) =>
      checkedDomains.size === 0 ||
      domains === undefined ||
      domains.length === 0 ||
      domains.some((d) => d === "*" || checkedDomains.has(d));
    // An edge is between two labels; it is in a domain where either label's table class is.
    const labelInDomain = (label: string) =>
      vocabulary.classes.some(
        (x) => x.kind === "table" && x.labels.includes(label) && inDomain(x.iri),
      );
    const byName = <T,>(rows: T[], name: (r: T) => string) =>
      [...rows].sort((a, b) => name(a).localeCompare(name(b)));
    // An edge type is listed once per pair of labels it joins, and a Property term could be once
    // per column; a pill picks the IRI, so each IRI is one pill.
    const oncePerIri = <T extends { iri: string }>(rows: T[]) => [
      ...new Map(rows.map((r) => [r.iri, r])).values(),
    ];
    return {
      classes: byName(
        vocabulary.classes.filter((x) =>
          x.kind === "term" ? termVisible(x.domains) : inDomain(x.iri),
        ),
        (x) => c(x.iri),
      ).map((x) => ({
        ...x,
        shown: c(x.iri),
        term: termName(x.iri, p),
        parents: x.superclasses.map(c),
      })),
      // A term's name is its IRI's, whatever a column calls it: one pill, with its classes listed.
      terms: byName(
        oncePerIri(vocabulary.predicates.filter((x) => x.kind === "term" && termVisible(x.domains))),
        (x) => c(x.iri),
      ).map((x) => {
        const term = termName(x.iri, named) ?? c(x.iri);
        return {
          iri: x.iri,
          name: term,
          testId: c(x.iri),
          tip: [
            x.iri,
            t("sparqlVocabulary.propertyTerm", { term }),
            ...(x.classes && x.classes.length > 0
              ? [
                  t("sparqlVocabulary.termClasses", {
                    classes: x.classes.map((k) => c(k.class)).join(", "),
                  }),
                ]
              : []),
          ].join(" · "),
        };
      }),
      // A column's own predicate, under the class of its node label.
      propertyClasses: [
        ...vocabulary.predicates
          .filter((x) => x.kind === "column" && inDomain(x.iri))
          .reduce((groups, x) => {
            const label = x.label ?? "";
            const cls = vocabulary.classes.find(
              (k) => k.kind === "table" && k.labels.includes(label),
            );
            const key = cls?.iri ?? label;
            const group = groups.get(key) ?? {
              key,
              title: cls ? c(cls.iri) : label,
              properties: [],
            };
            const column = `${label}.${x.property ?? ""}`;
            group.properties.push({
              iri: x.iri,
              name: x.property ?? "",
              testId: column,
              tip: `${x.iri} · ${t("sparqlVocabulary.columnPredicate", { column })}`,
            });
            return groups.set(key, group);
          }, new Map<string, PropertyGroup>())
          .values(),
      ].sort((a, b) => a.title.localeCompare(b.title)),
      edges: byName(
        oncePerIri(
          vocabulary.edges.filter((e) => labelInDomain(e.source) || labelInDomain(e.target)),
        ),
        (e) => c(e.iri),
      ).map((e) => ({ ...e, shown: c(e.iri) })),
      concepts: byName(
        vocabulary.concepts.filter((x) => termVisible(x.domains)),
        (x) => x.name,
      ).map((x) => ({ ...x, shown: c(x.iri) })),
    };
  }, [vocabulary, named, domain, checkedDomains, t]);

  if (error) return <div className="graph-schema-empty">{error}</div>;
  if (vocabulary === null || shown === null || named === undefined)
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
          domainPrefixes(vocabulary)
            .filter(([prefix]) => checkedDomains.size === 0 || checkedDomains.has(prefix))
            .map(([prefix, ns]) => (
              <Pill
                key={prefix}
                text={prefix}
                color={colorOverrides[prefix] ?? labelColor(prefix)}
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
                  color={colorOverrides[x.shown] ?? labelColor(x.shown)}
                  muted={x.parents.length > 0 ? `⊂ ${x.parents.join(", ")}` : undefined}
                  tip={[
                    x.iri,
                    x.kind === "term"
                      ? t("sparqlVocabulary.classTerm", { term: x.term ?? "" })
                      : t("sparqlVocabulary.tableClass"),
                  ].join(" · ")}
                  onClick={() => onPick({ kind: "class", iri: x.iri })}
                  dragged={x.iri}
                  testId={`sparql-class-${x.shown}`}
                />
              )),
        )}
        {section(
          "properties",
          t("sparqlVocabulary.properties"),
          (() => {
            const asked = propertySearch.trim().toLowerCase();
            const matching = (p: PropertyPill) => p.name.toLowerCase().includes(asked);
            const pills = (list: PropertyPill[]) =>
              list.map((p) => (
                <Pill
                  key={p.iri}
                  text={p.name}
                  tip={p.tip}
                  onClick={() => onPick({ kind: "predicate", iri: p.iri })}
                  testId={`sparql-predicate-${p.testId}`}
                />
              ));
            const terms = shown.terms.filter(matching);
            const groups = shown.propertyClasses
              .map((g) => ({ ...g, properties: g.properties.filter(matching) }))
              .filter((g) => g.properties.length > 0);
            const none = terms.length === 0 && groups.length === 0;
            return (
              <>
                {shown.terms.length + shown.propertyClasses.length > 0 && (
                  <TextInput
                    size="xs"
                    className="graph-property-search"
                    aria-label={t("sparqlVocabulary.propertySearch")}
                    placeholder={t("sparqlVocabulary.propertySearch")}
                    value={propertySearch}
                    onChange={(e) => setPropertySearch(e.currentTarget.value)}
                    data-testid="sparql-property-search"
                  />
                )}
                {none ? (
                  empty
                ) : (
                  <Collapsibles id="sparql.vocabulary.properties">
                    {terms.length > 0 && (
                      <CollapsibleItem
                        value="terms"
                        title={t("sparqlVocabulary.propertyTerms")}
                        forceOpen={asked !== ""}
                        controlTestId="sparql-property-terms-toggle"
                      >
                        <div className="graph-label-list">{pills(terms)}</div>
                      </CollapsibleItem>
                    )}
                    {groups.map((g) => (
                      <CollapsibleItem
                        key={g.key}
                        value={g.key}
                        title={`${g.title} (${g.properties.length})`}
                        defaultOpen={false}
                        forceOpen={asked !== ""}
                        controlTestId={`sparql-property-class-${g.title}`}
                      >
                        <div className="graph-label-list">{pills(g.properties)}</div>
                      </CollapsibleItem>
                    ))}
                  </Collapsibles>
                )}
              </>
            );
          })(),
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
                  badge
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
          Object.entries(named).map(([prefix, ns]) => (
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
