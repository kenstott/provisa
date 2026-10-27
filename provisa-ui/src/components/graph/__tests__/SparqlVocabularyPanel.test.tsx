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
    { iri: HIRE_DATE, kind: "term", label: "Employees", property: "hired_on", datatype: "xsd:date" },
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
      <SparqlVocabularyPanel vocabulary={vocabulary} error={null} onPick={vi.fn()} />,
    );
    // One Property term IRI and one edge IRI: one pill each.
    expect(container.querySelectorAll('[data-testid^="sparql-predicate-"]')).toHaveLength(1);
    expect(container.querySelectorAll('[data-testid^="sparql-edge-"]')).toHaveLength(1);
    expect(error.mock.calls.flat().join(" ")).not.toContain("same key");
  });
});
