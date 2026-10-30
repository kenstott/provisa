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
const canvas = vi.hoisted(() => ({
  nodes: [] as number[],
  props: {} as Record<string, unknown>,
}));
vi.mock("../GraphCanvas", () => ({
  GraphCanvas: (props: { nodes: Map<string, unknown> }) => {
    canvas.nodes.push(props.nodes.size);
    canvas.props = props;
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

function show(
  query: string,
  language?: GraphLanguage,
  f: FrameData = frame(query),
  extra: { onExpandResource?: (id: string, iri: string, toggle: boolean) => void } = {},
) {
  return render(
    <GraphFrame
      frame={f}
      language={language}
      {...extra}
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

  it("stars its query as a Cypher frame does", () => {
    const query = "SELECT ?s WHERE { ?s ?p ?o }";
    const starred = vi.fn();
    render(
      <GraphFrame
        frame={frame(query)}
        language="sparql"
        onAddFavorite={starred}
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
    fireEvent.click(screen.getByTestId("graph-frame-favorite-btn"));
    expect(starred).toHaveBeenCalledWith(query);
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

  const cell = (v: string) => ({ rdfTerm: "iri", value: `http://x/${v}`, shown: `ex:${v}` });
  const select = (over: Partial<FrameData> = {}): FrameData => {
    const query = "SELECT ?s ?o WHERE { ?s <http://x/knows> ?o }";
    const rows = [{ s: cell("a"), o: cell("b") }];
    return {
      id: "fs",
      query,
      status: "done",
      nodes: new Map(),
      edges: new Map(),
      rows,
      columns: ["s", "o"],
      sparqlForm: "rows",
      sparqlPairing: selectPairing(query, ["s", "o"], rows) ?? undefined,
      ...over,
    };
  };

  it("shows a SELECT's table first, a column per variable, with a graph view on offer", () => {
    canvas.nodes.length = 0;
    show(select().query, "sparql", select());
    expect(screen.getByTestId("table-view-table")).toBeInTheDocument();
    const headers = [...document.querySelectorAll(".gf-th-label")].map((h) => h.textContent);
    expect(headers).toEqual(["s", "o"]);
    const graph = screen.getByTestId("graph-frame-view-graph-btn");
    expect(canvas.nodes).toHaveLength(0);
    fireEvent.click(graph);
    expect(canvas.nodes.at(-1)).toBe(2);
    expect(graph).toHaveAttribute("aria-current", "true");
  });

  it("offers no graph view on a SELECT whose rows hold no triples", () => {
    show("SELECT ?s WHERE { ?s ?p ?o }", "sparql", {
      ...select(),
      query: "SELECT ?s WHERE { ?s ?p ?o }",
      sparqlPairing: undefined,
    });
    expect(screen.queryByTestId("graph-frame-view-graph-btn")).toBeNull();
  });

  it("shows a new query's table first again, on a frame that drew the last one", () => {
    const { rerender } = show(select().query, "sparql", select());
    fireEvent.click(screen.getByTestId("graph-frame-draw-as-graph"));
    expect(screen.queryByTestId("graph-frame-draw-as-graph")).toBeNull();
    const next = select({ query: "SELECT ?a ?b WHERE { ?a <http://x/knows> ?b }" });
    const common = {
      onClose: () => {},
      onRerun: () => {},
      schemaRels: [],
      colorOverrides: {},
      sizeOverrides: {},
      labelProperty: {},
      sizeByProperty: {},
      sizeMultiplier: {},
      relLineOverrides: {},
      onColorChange: () => {},
      pkMap: {},
      labelToTableLabel: {},
      labelToTableId: {},
    };
    rerender(<GraphFrame frame={next} language="sparql" {...common} />);
    expect(screen.getByTestId("graph-frame-draw-as-graph")).toBeInTheDocument();
  });

  const described = (): FrameData => {
    const triples = parseNTriples(
      ["<http://x/a> <http://x/knows> <http://x/b> .", '<http://x/a> <http://x/name> "Ann" .'].join(
        "\n",
      ),
      NS,
    );
    return {
      id: "fd",
      query: "DESCRIBE <http://x/a>",
      status: "done",
      ...triplesGraph(triples),
      rows: [],
      columns: [],
      sparqlForm: "graph",
      sparqlTriples: triples,
    };
  };
  const keyOf = (f: FrameData, iri: string) =>
    [...f.nodes.entries()].find(([, n]) => n.properties.iri === iri)![0];

  it("shows a DESCRIBE as its graph", () => {
    canvas.nodes.length = 0;
    show("DESCRIBE <http://x/a>", "sparql", described());
    expect(canvas.nodes.at(-1)).toBe(2);
    expect(screen.getByTestId("graph-frame-view-graph-btn")).toHaveAttribute(
      "aria-current",
      "true",
    );
  });

  it("expands a node by its resource, marked as shown once merged", () => {
    const f = described();
    const expanded = vi.fn();
    show(f.query, "sparql", { ...f, sparqlExpansions: { "http://x/b": [] } }, {
      onExpandResource: expanded,
    });
    const props = canvas.props as {
      describable: boolean;
      keepPositions: boolean;
      onToggleChildren: (k: string) => void;
      onToggleChildrenBatch: (k: string[]) => void;
      onNodeDragged: (k: string) => void;
      showingChildrenNatural: Set<string>;
    };
    expect(props.describable).toBe(true);
    expect(props.keepPositions).toBe(true);
    const a = keyOf(f, "http://x/a");
    const b = keyOf(f, "http://x/b");
    expect(props.showingChildrenNatural).toEqual(new Set([b]));
    props.onToggleChildren(a);
    expect(expanded).toHaveBeenLastCalledWith("fd", "http://x/a", true);
    props.onToggleChildrenBatch([b]);
    expect(expanded).toHaveBeenLastCalledWith("fd", "http://x/b", true);
    // A drag adds, and never takes an expansion back.
    props.onNodeDragged(a);
    expect(expanded).toHaveBeenLastCalledWith("fd", "http://x/a", false);
  });

  it("leaves a Cypher frame's expansion and layout as they are", () => {
    show("MATCH (n) RETURN n");
    const props = canvas.props as { describable: boolean; keepPositions: boolean };
    expect(props.describable).toBe(false);
    expect(props.keepPositions).toBe(false);
  });

  it("shows why a merge was refused, and when it found nothing more", () => {
    show("DESCRIBE <http://x/a>", "sparql", { ...described(), sparqlMerge: { error: "No" } });
    expect(screen.getByTestId("graph-frame-merge-error")).toHaveTextContent("No");
    show("DESCRIBE <http://x/a>", "sparql", {
      ...described(),
      sparqlMerge: { nothingMore: "http://x/z" },
    });
    expect(screen.getByTestId("graph-frame-merge-empty")).toHaveTextContent(
      "Nothing more is said of http://x/z.",
    );
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
