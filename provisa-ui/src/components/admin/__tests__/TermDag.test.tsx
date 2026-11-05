// Copyright (c) 2026 Kenneth Stott
// Canary: ed2126a2-c814-4acc-af9d-48ea3c565386
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2150, REQ-2174 (ONE ONTOLOGY GRAPH; EVERY OTHER GRAPH IS A SNIPPET OF IT): a term's graph is
// the one ontology graph focused on the term, by the Ontology preset's rules (ONE GRAPH, RULES PER
// PRESET): each arrow between terms named by its relationship type, the grounding chain from the
// term to its columns and their tables, and nothing no shown term reaches. The snippet's reading,
// drawing, click-through and expanding are tested in graph3d/__tests__/views.test.tsx.

import { describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import type { GlossaryTermDetail } from "../../../api/glossary";
import type { SnippetPreset } from "../../graph3d/ontologyPresets";
import { glossaryGraph, termEdges, type TermRow } from "../../graph3d/ontologyData";

// The snippet the term page renders, kept for the test to read: jsdom draws no canvas.
const snippets = vi.hoisted(() => [] as { preset: SnippetPreset; name: string; testId: string }[]);
vi.mock("../../graph3d/OntologyGraph", () => ({
  OntologySnippet: (props: { preset: SnippetPreset; name: string; testId: string }) => {
    snippets.push(props);
    return <div data-testid={props.testId} />;
  },
}));
vi.mock("../GlossaryGroundedTables", () => ({ GlossaryGroundedTables: () => null }));
vi.mock("../GlossaryRelationships", () => ({ GlossaryRelationships: () => null }));

import { TermReadView } from "../TermReadView";

const term: GlossaryTermDetail = {
  id: 1,
  name: "Customer",
  definition: null,
  is_abstract: false,
  deprecated: false,
  ref_count: 1,
  table_count: 1,
  identity_gap: false,
  related_to: 0,
  export_excluded: false,
  retired: false,
  grounding: "grounded",
  agent_visible: true,
  domains: ["sales"],
  findings: 0,
  status: "approved",
  status_reason: null,
  status_by: null,
  retire_on: null,
  refs: [
    {
      table_id: 7,
      column_name: "customer_id",
      source_id: "pg",
      schema_name: "public",
      table_name: "orders",
      alias: null,
      domain_id: "sales",
    },
  ],
  edges_in: [{ term_id: 2, rel_type: "KIND_OF", name: "Buyer" }],
  edges_out: [{ term_id: 3, rel_type: "PART_OF", name: "Party" }],
  experts: [],
  tags: [],
  pending_on: [],
};

/** ``term`` as the ontology's query reads it around the term: its neighbours, its column and
 * that column's table, and the class table it grounds. */
const row = (id: number, name: string, over: Partial<TermRow> = {}): TermRow => ({
  id,
  name,
  definition: null,
  refCount: 0,
  deprecated: false,
  retired: false,
  status: "approved",
  grounding: "not_grounded",
  domains: [{ domainId: "sales" }],
  ...over,
});
const customer = row(1, "Customer", {
  refCount: 1,
  grounding: "grounded",
  edgesOut: [{ id: 2, fromTermId: 1, toTermId: 3, relType: "PART_OF" }],
  edgesIn: [{ id: 1, fromTermId: 2, toTermId: 1, relType: "KIND_OF" }],
  tables: [{ id: 9, tableName: "customers", domainId: "sales" }],
  refs: [
    {
      columnKey: "7:customer_id",
      columnName: "customer_id",
      tableId: 7,
      tableName: "orders",
      domainId: "sales",
    },
  ],
});
const buyer = row(2, "Buyer", {
  edgesOut: [{ id: 1, fromTermId: 2, toTermId: 1, relType: "KIND_OF" }],
});
const party = row(3, "Party", {
  edgesIn: [{ id: 2, fromTermId: 1, toTermId: 3, relType: "PART_OF" }],
});
const TERMS = [customer, buyer, party];

describe("a term's page", () => {
  it("draws the ontology focused on the term, by the Ontology preset's rules", () => {
    snippets.length = 0;
    render(
      <TermReadView
        term={term}
        relTypeOptions={[]}
        otherTermOptions={[]}
        relLabel={(_o, relType) => relType}
        navigateToTerm={vi.fn()}
      />,
    );
    expect(screen.getByTestId("glossary-term-dag")).toBeInTheDocument();
    expect(snippets.at(-1)).toMatchObject({
      preset: { focus: "term:1", products: false },
      name: "Customer",
      testId: "glossary-term-dag",
    });
  });
});

describe("a term's graph", () => {
  it("names each arrow between terms by its relationship type", () => {
    const g = glossaryGraph(TERMS, termEdges(TERMS));
    expect(
      g.edges
        .filter((e) => e.kind === "term_edge")
        .map((e) => [e.a, e.b, e.label])
        .sort(),
    ).toEqual([
      ["term:1", "term:3", "PART_OF"],
      ["term:2", "term:1", "KIND_OF"],
    ]);
  });

  it("draws a relationship read from both its ends once", () => {
    expect(termEdges(TERMS)).toHaveLength(2);
  });

  it("runs from the term to its column and the column's table, and to the table it grounds", () => {
    const g = glossaryGraph(TERMS, termEdges(TERMS));
    const chain = g.edges
      .filter((e) => e.kind === "term_binding" || e.kind === "has_column")
      .map((e) => [e.kind, e.a, e.b])
      .sort();
    expect(chain).toEqual([
      ["has_column", "c:7:customer_id", "t:7"],
      ["term_binding", "term:1", "c:7:customer_id"],
      ["term_binding", "term:1", "t:9"],
    ]);
    // Each table by its catalog name.
    expect(g.nodes.find((n) => n.id === "t:9")?.label).toBe("sales.customers");
  });

  it("draws no table no shown term reaches, and no edge between tables", () => {
    const g = glossaryGraph([buyer, party], []);
    expect(g.nodes.some((n) => n.type === "table")).toBe(false);
    expect(g.edges.some((e) => e.kind === "relationship")).toBe(false);
  });
});
