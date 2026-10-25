// Copyright (c) 2026 Kenneth Stott
// Canary: ed2126a2-c814-4acc-af9d-48ea3c565386
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2150: a term's graph -- its layers left to right, each arrow named by its relationship type,
// each node by its catalog name; a click on a term opens it; the expand control shows it full size.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import type { GlossaryTermDetail } from "../../../api/glossary";
import { termGraph } from "../term-graph";

// What the DAG gives cytoscape, and the tap handler it binds, kept for the test to read.
const { made, cytoscape } = vi.hoisted(() => {
  const made: {
    elements: { group: string; data: Record<string, unknown> }[];
    taps: ((evt: unknown) => void)[];
  }[] = [];
  const cytoscape = Object.assign(
    (options: { elements: { group: string; data: Record<string, unknown> }[] }) => {
      const record = { elements: options.elements, taps: [] as ((evt: unknown) => void)[] };
      made.push(record);
      return {
        on: (_event: string, _selector: string, fn: (evt: unknown) => void) => record.taps.push(fn),
        layout: () => ({ run: vi.fn() }),
        resize: vi.fn(),
        fit: vi.fn(),
        destroy: vi.fn(),
      };
    },
    { use: vi.fn() },
  );
  return { made, cytoscape };
});
vi.mock("cytoscape", () => ({ default: cytoscape }));
vi.mock("cytoscape-elk", () => ({ default: vi.fn() }));

import { TermDag } from "../TermDag";

const term: GlossaryTermDetail = {
  id: 1,
  name: "Customer",
  definition: null,
  is_abstract: false,
  deprecated: false,
  ref_count: 1,
  table_count: 1,
  identity_gap: false,
  export_excluded: false,
  retired: false,
  live: true,
  grounded: true,
  domains: ["sales"],
  findings: 0,
  status: "live",
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
};
const tables = [
  {
    table_id: 9,
    source_id: "pg",
    schema_name: "public",
    table_name: "customers",
    domain_id: "sales",
  },
];
const words = (relType: string) => ({ KIND_OF: "is a kind of", PART_OF: "is part of" })[relType]!;

beforeEach(() => {
  made.length = 0;
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});

describe("termGraph", () => {
  it("lays the term's neighbours out in layers, left to right", () => {
    const { nodes } = termGraph(term, tables, words);
    const layers = Object.fromEntries(nodes.map((n) => [n.label, [n.layer, n.kind]]));
    expect(layers).toEqual({
      Buyer: [0, "term"],
      Customer: [1, "self"],
      Party: [2, "term"],
      "orders.customer_id": [3, "column"],
      "sales.customers": [3, "table"],
    });
  });

  it("names each arrow between terms by its relationship type, one hop each way", () => {
    const { edges } = termGraph(term, tables, words);
    expect(edges.map((e) => [e.source, e.target, e.label])).toEqual([
      ["term:2", "term:1", "is a kind of"],
      ["term:1", "term:3", "is part of"],
      ["term:1", "column:7:customer_id", undefined],
      ["term:1", "table:9", undefined],
    ]);
  });

  it("draws a term related both ways once, among the terms before it", () => {
    const both: GlossaryTermDetail = {
      ...term,
      edges_out: [{ term_id: 2, rel_type: "PART_OF", name: "Buyer" }],
    };
    const { nodes, edges } = termGraph(both, null, words);
    expect(nodes.filter((n) => n.id === "term:2").map((n) => n.layer)).toEqual([0]);
    expect(edges).toHaveLength(3);
  });

  it("draws no tables before they are read", () => {
    expect(termGraph(term, null, words).nodes.some((n) => n.kind === "table")).toBe(false);
  });
});

describe("TermDag", () => {
  it("draws the graph compact, and opens a term clicked in it", () => {
    const navigate = vi.fn();
    render(<TermDag term={term} tables={tables} relLabel={words} navigateToTerm={navigate} />);

    expect(screen.getByTestId("glossary-term-dag")).toBeInTheDocument();
    expect(made).toHaveLength(1);
    expect(made[0].elements.filter((e) => e.group === "nodes")).toHaveLength(5);
    made[0].taps[0]({ target: { data: (key: string) => (key === "termId" ? 3 : undefined) } });
    expect(navigate).toHaveBeenCalledWith(3);
  });

  it("shows the graph at full size when expanded", async () => {
    render(<TermDag term={term} tables={tables} relLabel={words} navigateToTerm={vi.fn()} />);

    fireEvent.click(screen.getByTestId("glossary-term-dag-expand"));

    expect(await screen.findByTestId("glossary-term-dag-full")).toBeInTheDocument();
    expect(made).toHaveLength(2);
  });
});
