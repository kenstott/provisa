// Copyright (c) 2026 Kenneth Stott
// Canary: 4f1c8e27-9b3a-4d6e-a0c5-7e2b9d81f364
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended, A GRAPH BEING LAID OUT SAYS SO): while a layout runs the canvas draws no
// node stacked at one point; it says it is laying the graph out, and shows the nodes once they
// have their places. The maintainer saw all 96 nodes of the Ontology page's graph painted at the
// canvas centre for a moment, then moved to their places.
//
// The real cytoscape, headless (jsdom has no 2D context), with every layout held until the test
// answers it: a layout asked is pending, as ELK's is while it computes.

import { Component, type ReactNode } from "react";
import { act } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import type { GraphSpec } from "../model";

type Pt = { x: number; y: number };
interface HeldCore {
  nodes(sel?: string): { map<T>(fn: (n: { id(): string; position(): Pt }) => T): T[] };
  fit(...args: unknown[]): unknown;
  destroyed(): boolean;
}

const held = vi.hoisted(() => ({
  /** The layouts asked and not yet answered, in order. */
  runs: [] as (() => void)[],
  /** The cores made, in order. */
  cores: [] as unknown[],
  /** How many layouts were asked to start. */
  asked: 0,
  /** A layout asked while this is set throws as it starts. */
  failStart: null as Error | null,
}));

vi.mock("cytoscape", async (importOriginal) => {
  type Real = ((...a: unknown[]) => unknown) & { use: (e: unknown) => void };
  const real = (await importOriginal<{ default: Real }>()).default;
  const make = (...args: unknown[]) => {
    // cytoscape("layout", "elk"): an extension's registration.
    if (typeof args[0] === "string") return real(...args);
    const o = args[0] as Record<string, unknown>;
    const cy = real({ ...o, container: undefined, headless: true, styleEnabled: true }) as {
      layout: (opts: unknown) => { run: () => unknown };
    };
    const layout = cy.layout.bind(cy);
    cy.layout = (opts: unknown) => {
      const l = layout(opts);
      const run = l.run.bind(l);
      l.run = () => {
        held.asked += 1;
        if (held.failStart) throw held.failStart;
        held.runs.push(() => run());
        return l;
      };
      return l;
    };
    held.cores.push(cy);
    return cy;
  };
  return { default: Object.assign(make, { use: real.use.bind(real) }) };
});

import { ProvisaGraph, type LayoutPreset } from "../ProvisaGraph";

/** The ``nth`` layout asked: each starts once the frame saying so has been painted. */
const started = (nth = 1) =>
  act(async () => {
    await vi.waitFor(() => expect(held.asked).toBeGreaterThanOrEqual(nth));
  });

/** Answers every layout asked so far (and those their answers ask). */
const answerAll = () => {
  while (held.runs.length > 0) (held.runs.shift() as () => void)();
};

const lastCore = () => held.cores[held.cores.length - 1] as HeldCore;
const placesOf = (cy: HeldCore) =>
  cy.nodes('[kind = "node"]').map((n) => ({ id: n.id(), ...n.position() }));

const GRAPH: GraphSpec = {
  nodes: [
    { id: "a", type: "term", name: "Shelter", lane: "terms" },
    { id: "b", type: "term", name: "Housing", lane: "terms" },
    { id: "c", type: "table", name: "beds", lane: "tables" },
    { id: "d", type: "column", name: "bed_count", lane: "columns" },
  ],
  edges: [
    { id: "ab", source: "a", target: "b" },
    { id: "bc", source: "b", target: "c" },
    { id: "cd", source: "c", target: "d" },
  ],
};
const FILTERED: GraphSpec = {
  nodes: GRAPH.nodes.slice(0, 3),
  edges: GRAPH.edges.slice(0, 2),
};
const LANES = {
  lanes: [
    { id: "columns", title: "Columns" },
    { id: "tables", title: "Tables" },
    { id: "terms", title: "Terms" },
  ],
};

/** The page's own boundary: a graph that throws while rendering is said in its place. */
class Boundary extends Component<{ children: ReactNode }, { error: Error | null }> {
  state: { error: Error | null } = { error: null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    return this.state.error ? (
      <div data-testid="page-error">{this.state.error.message}</div>
    ) : (
      this.props.children
    );
  }
}

const graphOf = (graph: GraphSpec, preset: LayoutPreset) => (
  <Boundary>
    <ProvisaGraph
      graph={graph}
      preset={preset}
      lanes={preset === "lanes" ? LANES : undefined}
      testId="g"
      exportName="g"
    />
  </Boundary>
);

/** The div the core draws in: everything the graph draws (nodes, lines, lane boxes) is in it. */
const drawing = () => {
  const box = screen.getByTestId("g").querySelector(".pg-canvas") as HTMLElement;
  expect(box.children).toHaveLength(1);
  return box.firstElementChild as HTMLElement;
};
const indicator = () => screen.queryByTestId("g-laying-out");

beforeEach(() => {
  held.runs = [];
  held.asked = 0;
  held.cores = [];
  held.failStart = null;
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});
afterEach(() => vi.unstubAllGlobals());

describe.each<LayoutPreset>(["force", "layered", "lanes"])(
  "a %s graph being laid out",
  (preset) => {
    it("says it is laying the graph out, showing no node, before the layout starts", () => {
      render(graphOf(GRAPH, preset));
      expect(held.runs).toHaveLength(0);
      expect(drawing()).not.toBeVisible();
      expect(indicator()).toHaveTextContent("Laying out…");
    });

    it("shows no node and says it is laying the graph out until the layout answers", async () => {
      render(graphOf(GRAPH, preset));
      await started();
      expect(drawing()).not.toBeVisible();
      expect(indicator()).toBeInTheDocument();
      expect(indicator()).toHaveTextContent("Laying out…");
    });

    it("shows the nodes at their places, and no indicator, once it answers", async () => {
      render(graphOf(GRAPH, preset));
      await started();
      await act(async () => answerAll());
      // ELK answers in a promise of its own; the lanes and fCoSE at once.
      await vi.waitFor(() => expect(indicator()).not.toBeInTheDocument());
      expect(drawing()).toBeVisible();
      const places = placesOf(lastCore());
      expect(places).toHaveLength(4);
      // Not stacked: no two nodes share a place.
      const at = new Set(places.map((p) => `${Math.round(p.x)},${Math.round(p.y)}`));
      expect(at.size).toBe(4);
    });

    it("hides the drawing and says so again when a filter lays it out again", async () => {
      const { rerender } = render(graphOf(GRAPH, preset));
      await started();
      await act(async () => answerAll());
      await vi.waitFor(() => expect(indicator()).not.toBeInTheDocument());
      rerender(graphOf(FILTERED, preset));
      expect(drawing()).not.toBeVisible();
      expect(indicator()).toHaveTextContent("Laying out…");
      await started(2);
      await act(async () => answerAll());
      await vi.waitFor(() => expect(indicator()).not.toBeInTheDocument());
      expect(drawing()).toBeVisible();
      expect(placesOf(lastCore())).toHaveLength(3);
    });

    it("drops the indicator and shows the page's error when the layout fails to start", async () => {
      vi.spyOn(console, "error").mockImplementation(() => undefined);
      held.failStart = new Error("layout refused the graph");
      render(graphOf(GRAPH, preset));
      await started();
      expect(indicator()).not.toBeInTheDocument();
      expect(screen.getByTestId("page-error")).toHaveTextContent("layout refused the graph");
    });
  },
);

describe("a graph whose placement fails once the layout answers", () => {
  it("drops the indicator and shows the page's error", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    // The lanes' layout places nothing itself (no fit of its own): their placement, after it, does.
    render(graphOf(GRAPH, "lanes"));
    await started();
    expect(indicator()).toBeInTheDocument();
    // The placement after the layout (its opening fit) throws.
    lastCore().fit = () => {
      throw new Error("placement failed");
    };
    await act(async () => answerAll());
    expect(indicator()).not.toBeInTheDocument();
    expect(screen.getByTestId("page-error")).toHaveTextContent("placement failed");
  });
});
