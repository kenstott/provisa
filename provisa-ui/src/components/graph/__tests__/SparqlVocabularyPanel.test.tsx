// Copyright (c) 2026 Kenneth Stott
// Canary: 2f2b5b06-79ed-4995-9099-a9ac6401cdff
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The endpoint lists a Property term once per column it grounds, and an edge type once per pair of
// labels it joins. A pill picks an IRI, so each IRI is one pill.

import { afterEach, describe, expect, it, vi } from "vitest";
import { render } from "../../../test-utils/render";
import { screen } from "@testing-library/react";
import { labelColor } from "../graph-model";
import { SparqlVocabularyPanel } from "../SparqlVocabularyPanel";
import type { SparqlVocabulary } from "../sparql";

const BASE = "https://e2e.provisa.example";
const HIRE_DATE = `${BASE}/vocab/term/hire%20date`;
const vocabulary: SparqlVocabulary = {
  base: BASE,
  prefixes: { hr: `${BASE}/hr/`, term: `${BASE}/vocab/term/`, rel: `${BASE}/vocab/rel/` },
  classes: [
    { iri: `${BASE}/hr/employees`, kind: "table", labels: ["Employees"], superclasses: [] },
    { iri: `${BASE}/hr/contractors`, kind: "table", labels: ["Contractors"], superclasses: [] },
  ],
  predicates: [
    {
      iri: HIRE_DATE,
      kind: "term",
      label: "Employees",
      property: "hired_on",
      datatype: "xsd:date",
    },
    { iri: HIRE_DATE, kind: "term", label: "Contractors", property: "start", datatype: "xsd:date" },
  ],
  edges: [
    { iri: `${BASE}/vocab/rel/WORKS_IN`, type: "WORKS_IN", source: "Employees", target: "Sites" },
    { iri: `${BASE}/vocab/rel/WORKS_IN`, type: "WORKS_IN", source: "Contractors", target: "Sites" },
  ],
  concepts: [],
};

afterEach(() => vi.restoreAllMocks());

describe("the SPARQL vocabulary", () => {
  it("shows a term reached by several columns, and an edge between several label pairs, once", () => {
    const error = vi.spyOn(console, "error").mockImplementation(() => {});
    const { container } = render(
      <SparqlVocabularyPanel
        vocabulary={vocabulary}
        error={null}
        onPick={vi.fn()}
        colorOverrides={{}}
        checkedDomains={new Set()}
      />,
    );
    // One Property term IRI and one edge IRI: one pill each.
    expect(container.querySelectorAll('[data-testid^="sparql-predicate-"]')).toHaveLength(1);
    expect(container.querySelectorAll('[data-testid^="sparql-edge-"]')).toHaveLength(1);
    expect(error.mock.calls.flat().join(" ")).not.toContain("same key");
  });
});

const two: SparqlVocabulary = {
  base: BASE,
  prefixes: { hr: `${BASE}/hr/`, ops: `${BASE}/ops/`, rel: `${BASE}/vocab/rel/` },
  classes: [
    { iri: `${BASE}/hr/employees`, kind: "table", labels: ["Employees"], superclasses: [] },
    { iri: `${BASE}/ops/shifts`, kind: "table", labels: ["Shifts"], superclasses: [] },
  ],
  predicates: [],
  edges: [{ iri: `${BASE}/vocab/rel/WORKS`, type: "WORKS", source: "Employees", target: "Shifts" }],
  concepts: [],
};
const panel = (checked: string[], colors: Record<string, string> = {}) =>
  render(
    <SparqlVocabularyPanel
      vocabulary={two}
      error={null}
      onPick={vi.fn()}
      colorOverrides={colors}
      checkedDomains={new Set(checked)}
    />,
  );

describe("the SPARQL vocabulary as the Cypher sidebar reads it", () => {
  it("colours a class by its label, as Cypher colours the node label it is", () => {
    panel([]);
    expect(screen.getByTestId("sparql-class-hr:employees")).toHaveStyle({
      background: labelColor("hr:employees"),
    });
    // The page's own colour for the label, shared with the Cypher page, wins.
    panel([], { "ops:shifts": "#123456" });
    expect(screen.getAllByTestId("sparql-class-ops:shifts")[1]).toHaveStyle({
      background: "#123456",
    });
  });

  it("draws a relationship as Cypher draws a relationship type", () => {
    panel([]);
    expect(screen.getByTestId("sparql-edge-rel:WORKS")).toHaveClass("graph-rel-badge");
  });

  it("lists every domain's vocabulary while none is selected", () => {
    panel([]);
    expect(screen.getByTestId("sparql-domain-hr")).toBeInTheDocument();
    expect(screen.getByTestId("sparql-domain-ops")).toBeInTheDocument();
    expect(screen.getByTestId("sparql-class-ops:shifts")).toBeInTheDocument();
  });

  it("narrows the domains, classes and relationships to the domains selected", () => {
    panel(["hr"]);
    expect(screen.getByTestId("sparql-domain-hr")).toBeInTheDocument();
    expect(screen.queryByTestId("sparql-domain-ops")).toBeNull();
    expect(screen.getByTestId("sparql-class-hr:employees")).toBeInTheDocument();
    expect(screen.queryByTestId("sparql-class-ops:shifts")).toBeNull();
    // An edge is in a domain where either of its labels is: hr has Employees.
    expect(screen.getByTestId("sparql-edge-rel:WORKS")).toBeInTheDocument();
  });
});
