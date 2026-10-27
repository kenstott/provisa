// Copyright (c) 2026 Kenneth Stott
// Canary: 875e3813-1d66-4ca9-b39f-73c81517f3f9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: a SPARQL frame is a SPARQL query's frame. Its header reads as SPARQL, and the action
// that rewrites a Cypher query (auto-impute) is not offered on it.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen } from "../../../test-utils/render";
import { GraphFrame } from "../GraphFrame";

// The canvas draws into a 2D context jsdom does not have; what is asked of here is the header.
const canvas = vi.hoisted(() => ({ nodes: [] as number[] }));
vi.mock("../GraphCanvas", () => ({
  GraphCanvas: ({ nodes }: { nodes: Map<string, unknown> }) => {
    canvas.nodes.push(nodes.size);
    return null;
  },
  GraphCutNote: () => null,
}));
import type { FrameData, GNode } from "../graph-model";
import { parseNTriples, selectPairing, triplesGraph } from "../sparql";
import type { GraphLanguage } from "../graph-persistence";

const ORDER: GNode = { id: 1, label: "sales:orders", tableLabel: "orders", properties: {} };

function frame(query: string): FrameData {
  return {
    id: "f1",
    query,
    status: "done",
    nodes: new Map([["1", ORDER]]),
    edges: new Map(),
    rows: [],
    columns: [],
  };
}

function show(query: string, language?: GraphLanguage, f: FrameData = frame(query)) {
  render(
    <GraphFrame
      frame={f}
      language={language}
      onClose={() => {}}
      onRerun={() => {}}
      schemaRels={[]}
      colorOverrides={{}}
      sizeOverrides={{}}
      labelProperty={{}}
      sizeByProperty={{}}
      sizeMultiplier={{}}
      relLineOverrides={{}}
      onColorChange={() => {}}
      pkMap={{}}
      labelToTableLabel={{}}
      labelToTableId={{}}
    />,
  );
}

describe("a SPARQL frame", () => {
  it("offers no auto-impute, which rewrites a Cypher query", () => {
    show("CONSTRUCT { ?s a ?c } WHERE { ?s a ?c }", "sparql");
    expect(screen.queryByTestId("graph-frame-auto-impute-btn")).toBeNull();
  });

  it("is a Cypher frame's offer still", () => {
    show("MATCH (n) RETURN n");
    expect(screen.getByTestId("graph-frame-auto-impute-btn")).toBeInTheDocument();
  });
});

const NS = { ex: "http://x/" };
const NT = [
  "<http://x/a> <http://x/knows> <http://x/b> .",
  '<http://x/a> <http://x/name> "Ann" .',
].join("\n");

describe("a SPARQL answer drawn", () => {
  it("shows a CONSTRUCT's literals as properties, and as nodes on the toggle", () => {
    const triples = parseNTriples(NT, NS);
    const f: FrameData = {
      id: "f2",
      query: "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }",
      status: "done",
      ...triplesGraph(triples),
      rows: [],
      columns: [],
      sparqlForm: "graph",
      sparqlTriples: triples,
    };
    canvas.nodes.length = 0;
    show(f.query, "sparql", f);
    expect(canvas.nodes.at(-1)).toBe(2);
    fireEvent.click(screen.getByTestId("graph-frame-literals-as-nodes"));
    expect(canvas.nodes.at(-1)).toBe(3);
    fireEvent.click(screen.getByTestId("graph-frame-literals-as-nodes"));
    expect(canvas.nodes.at(-1)).toBe(2);
  });

  it("offers a pairable SELECT as a graph, and draws it on asking", () => {
    const cell = (v: string) => ({ rdfTerm: "iri", value: `http://x/${v}`, shown: `ex:${v}` });
    const rows = [{ s: cell("a"), o: cell("b") }];
    const query = "SELECT ?s ?o WHERE { ?s <http://x/knows> ?o }";
    const f: FrameData = {
      id: "f3",
      query,
      status: "done",
      nodes: new Map(),
      edges: new Map(),
      rows,
      columns: ["s", "o"],
      sparqlForm: "rows",
      sparqlPairing: selectPairing(query, ["s", "o"], rows) ?? undefined,
    };
    canvas.nodes.length = 0;
    show(query, "sparql", f);
    expect(canvas.nodes).toHaveLength(0);
    fireEvent.click(screen.getByTestId("graph-frame-draw-as-graph"));
    expect(canvas.nodes.at(-1)).toBe(2);
    expect(screen.queryByTestId("graph-frame-draw-as-graph")).toBeNull();
  });

  it("offers nothing on an ASK or a SELECT that cannot pair", () => {
    show("ASK { ?s ?p ?o }", "sparql", {
      id: "f4",
      query: "ASK { ?s ?p ?o }",
      status: "done",
      nodes: new Map(),
      edges: new Map(),
      rows: [{ ask: true }],
      columns: ["ask"],
      sparqlForm: "boolean",
    });
    expect(screen.queryByTestId("graph-frame-draw-as-graph")).toBeNull();
    expect(screen.queryByTestId("graph-frame-literals-as-nodes")).toBeNull();
  });
});
