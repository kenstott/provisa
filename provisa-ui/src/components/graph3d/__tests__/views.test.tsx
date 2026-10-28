// Copyright (c) 2026 Kenneth Stott
// Canary: 6a1d3f85-0e2b-4c7a-9d64-b8f2e5c1a039
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28, ONE ONTOLOGY GRAPH; EVERY OTHER GRAPH IS A SNIPPET OF IT): the one
// graph's presets -- a product's panel, a term's page, the Ontology page and the Product map --
// each reading the ontology with its focus and depth, and a snippet expanding to the whole
// ontology with its focus selected. jsdom has no WebGL, so the three.js scene is a recorder.

import { act } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import type { GraphData, QueryScope } from "../graphModel";
import { FACTS } from "./fixture";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
// Each scene's calls, and what it reports to the view (a node picked on the canvas).
const made = vi.hoisted(() => ({
  scenes: [] as Calls[],
  callbacks: [] as { onSelect: (h: { kind: string; id: string }) => void }[],
}));
vi.mock("../graphScene", () => ({
  GraphScene: class {
    constructor(
      _canvas: unknown,
      _labels: unknown,
      callbacks: { onSelect: (h: { kind: string; id: string }) => void },
    ) {
      made.callbacks.push(callbacks);
      const calls: Calls = {};
      for (const m of [
        "setTheme",
        "setRotate",
        "setGrid",
        "dispose",
        "resetView",
        "setHover",
        "setData",
        "setSelected",
        "focus",
        "setFlat",
        "setArcs",
      ])
        calls[m] = vi.fn();
      Object.assign(this, calls);
      made.scenes.push(calls);
    }
  },
}));

// Each observed element, so a test can scroll one into view.
const observed = vi.hoisted(() => ({ fire: [] as (() => void)[] }));
class FakeObserver {
  private cb: IntersectionObserverCallback;
  constructor(cb: IntersectionObserverCallback) {
    this.cb = cb;
  }
  observe() {
    observed.fire.push(() =>
      this.cb(
        [{ isIntersecting: true } as IntersectionObserverEntry],
        this as unknown as IntersectionObserver,
      ),
    );
  }
  disconnect() {}
}
vi.stubGlobal("IntersectionObserver", FakeObserver);

vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));

const nav = vi.hoisted(() => ({ to: [] as string[] }));
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => (to: string) => nav.to.push(to),
}));

// The ontology's read: each scope asked for, answered with the fixture (what a scope reads is
// ontologyData's query, held by ontologyQuery.test.ts and the GraphQL corpus).
const reads = vi.hoisted(() => ({ scopes: [] as QueryScope[], data: null as unknown }));
vi.mock("../ontologyData", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../ontologyData")>()),
  // The query stands for its scope; the server below answers it with the graph read.
  ontologyQuery: (_m: unknown, s: { layers: Set<string> }) =>
    JSON.stringify({ ...s, layers: [...s.layers] }),
  ontologyCountQuery: () => "count",
  ontologyGraph: () => reads.data,
}));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => ({ schema: null, roots: new Map() }),
  dataGraphql: async (query: string) => {
    if (query !== "count") reads.scopes.push(JSON.parse(query) as QueryScope);
    return { _terms: [] };
  },
}));

import { OntologySnippet, OntologyView, ProductMapButton } from "../OntologyGraph";
import { productPreset } from "../ontologyPresets";

const last = () => made.scenes[made.scenes.length - 1];
const drawn = () => last().setData.mock.calls.at(-1)?.[0] as GraphData;
const nodeIds = (g: GraphData) => g.nodes.map((n) => n.id).sort();
const focuses = () => reads.scopes.map((s) => [s.focus, s.depth]);

beforeEach(() => {
  made.scenes.length = 0;
  made.callbacks.length = 0;
  observed.fire.length = 0;
  reads.scopes.length = 0;
  reads.data = FACTS;
  nav.to.length = 0;
  window.localStorage.clear();
});

/** A snippet, scrolled into view. */
function snippet(preset: { focus: string; depth: number }, name: string) {
  render(<OntologySnippet preset={preset} name={name} testId="snip" />);
  act(() => observed.fire.forEach((f) => f()));
}

describe("a product's panel", () => {
  it("reads the ontology focused on the product, to the neighbourhood's depth, once in view", async () => {
    render(<OntologySnippet preset={productPreset("c360")} name="customer_360" testId="snip" />);
    expect(reads.scopes).toEqual([]);
    act(() => observed.fire.forEach((f) => f()));
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(new Set(focuses().map(String))).toEqual(new Set(["p:c360,2"]));
    // Its snippet draws the product, its tables, their columns and the terms bound to them.
    await waitFor(() =>
      expect(nodeIds(drawn())).toEqual(
        expect.arrayContaining(["p:c360", "t:customers", "c:customers.email", "term:customer"]),
      ),
    );
  });

  it("is a glance: its stage at the card's height, its depth, 2D/3D and expand alone", async () => {
    snippet(productPreset("c360"), "customer_360");
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(screen.getByTestId("snip-stage").closest(".g3d-panel-view")).not.toBeNull();
    expect(screen.getByTestId("snip-depth")).toBeInTheDocument();
    expect(screen.getByTestId("snip-expand")).toBeInTheDocument();
    expect(
      within(screen.getByTestId("snip")).getByRole("radio", { name: "2D" }),
    ).toBeInTheDocument();
    for (const id of [
      "snip-preset",
      "snip-filter-domain",
      "snip-layers-menu",
      "snip-open-as-query",
      "snip-detail",
      "snip-lateral",
    ])
      expect(screen.queryByTestId(id)).not.toBeInTheDocument();
    // It turns and draws no grid, whatever the whole ontology's settings say.
    expect(last().setRotate).toHaveBeenLastCalledWith(true);
    expect(last().setGrid).toHaveBeenLastCalledWith(false);
  });

  it("reads the depth the reader picks", async () => {
    snippet(productPreset("c360"), "customer_360");
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    fireEvent.click(within(screen.getByTestId("snip-depth")).getByRole("radio", { name: "3" }));
    await waitFor(() => expect(reads.scopes.at(-1)).toMatchObject({ focus: "p:c360", depth: 3 }));
  });

  it("opens a node clicked in it on that node's own page", async () => {
    snippet(productPreset("c360"), "customer_360");
    await waitFor(() => expect(made.callbacks).toHaveLength(1));
    await waitFor(() => expect(nodeIds(drawn())).toContain("term:customer"));
    act(() => made.callbacks[0].onSelect({ kind: "node", id: "term:customer" }));
    expect(nav.to).toEqual(["/admin/ontology?term=customer"]);
  });
});

describe("expanding a snippet", () => {
  it("opens the whole ontology with the snippet's focus selected", async () => {
    snippet(productPreset("c360"), "customer_360");
    fireEvent.click(await screen.findByTestId("snip-expand"));
    const full = await screen.findByTestId("graph3d");
    // The whole ontology is read: no focus.
    await waitFor(() => expect(focuses().map(String)).toContain(",2"));
    // Its own controls, and the product selected in its detail panel.
    expect(within(full).getByRole("radio", { name: "Ontology" })).toBeInTheDocument();
    expect(within(full).getByRole("radio", { name: "Taxonomy" })).toBeInTheDocument();
    await waitFor(() =>
      expect(screen.getByTestId("graph3d-detail")).toHaveTextContent("customer_360"),
    );
    expect(screen.getByTestId("graph3d-title-link")).toHaveAttribute(
      "href",
      "/data-products?product=c360",
    );
    // Selected, not a focus: nothing is pruned around it.
    expect(screen.queryByTestId("graph3d-depth")).not.toBeInTheDocument();
  });

  it("from a term's page selects the term", async () => {
    snippet({ focus: "term:customer", depth: 2 }, "Customer");
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(new Set(focuses().map(String))).toEqual(new Set(["term:customer,2"]));
    fireEvent.click(screen.getByTestId("snip-expand"));
    await screen.findByTestId("graph3d");
    await waitFor(() => expect(screen.getByTestId("graph3d-detail")).toHaveTextContent("Customer"));
  });
});

describe("the Ontology page", () => {
  it("shows the ontology whole, focused by the page's search", async () => {
    render(<OntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(new Set(focuses().map(String))).toEqual(new Set([",2"]));
    // No focus, no depth to choose.
    expect(screen.queryByTestId("ontology-depth")).not.toBeInTheDocument();
  });

  it("chooses the depth of a search's neighbourhood", async () => {
    render(<OntologyView search="customer" />);
    fireEvent.click(
      within(await screen.findByTestId("ontology-depth")).getByRole("radio", { name: "1" }),
    );
    await waitFor(() =>
      expect(reads.scopes.at(-1)).toMatchObject({ focus: null, depth: 1, search: ["customer"] }),
    );
  });

  it("draws lateral term edges on asking, in 3D", async () => {
    reads.data = {
      nodes: [...FACTS.nodes, { id: "term:client", type: "term", label: "Client" }],
      edges: [
        ...FACTS.edges,
        { id: "rel", a: "term:client", b: "term:customer", kind: "term_edge", label: "SYNONYM_OF" },
      ],
    };
    render(<OntologyView search="" />);
    await waitFor(() => expect(drawn().edges.some((e) => e.label === "SYNONYM_OF")).toBe(true));
    fireEvent.click(screen.getByTestId("ontology-lateral"));
    await waitFor(() => expect(drawn().edges.some((e) => e.label === "SYNONYM_OF")).toBe(false));
    // A rank edge stays.
    expect(drawn().edges.some((e) => e.label === "KIND_OF")).toBe(true);
  });
});

describe("the Product map entry point", () => {
  it("opens the whole ontology, every product among it", async () => {
    render(<ProductMapButton />);
    expect(reads.scopes).toEqual([]);
    fireEvent.click(screen.getByTestId("product-map-open"));
    await screen.findByTestId("graph3d");
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(new Set(focuses().map(String))).toEqual(new Set([",2"]));
    // Its Data layer is on: every product is drawn.
    expect(reads.scopes.at(-1)?.layers).toContain("data");
    await waitFor(() =>
      expect(nodeIds(drawn())).toEqual(expect.arrayContaining(["p:c360", "p:billing"])),
    );
  });
});
