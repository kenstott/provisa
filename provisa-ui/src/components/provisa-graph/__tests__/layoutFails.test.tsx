// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2e9d14-7c3a-4f85-b1e0-a94d2c7f3e58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (A GRAPH BEING LAID OUT SAYS SO): a layout that fails ends the laying out and goes to
// the page's error path. ELK answers in a promise; one that rejects after the layout has started
// was never said (cytoscape-elk drops the rejection), so the indicator stayed up and the canvas
// stayed hidden. A layout that is slow is not one that failed: there is no time limit.
//
// The real cytoscape, headless (jsdom has no 2D context), with elkjs's answer held until the test
// gives it: resolved with every node placed, or rejected.

import { Component, type ReactNode } from "react";
import { act } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";
import type { GraphSpec } from "../model";

type Pt = { x: number; y: number };
interface ElkGraph {
  id: string;
  x?: number;
  y?: number;
  children?: ElkGraph[];
}
interface HeldCore {
  nodes(sel?: string): { map<T>(fn: (n: { id(): string; position(): Pt }) => T): T[] };
}

const elk = vi.hoisted(() => ({
  /** The graphs ELK was asked to lay out, each with its answer. */
  asked: [] as {
    graph: unknown;
    resolve: (g: unknown) => void;
    reject: (e: unknown) => void;
  }[],
  /** The cores made, in order. */
  cores: [] as unknown[],
}));

vi.mock("elkjs/lib/elk.bundled.js", () => ({
  default: class {
    layout(graph: unknown) {
      return new Promise((resolve, reject) => elk.asked.push({ graph, resolve, reject }));
    }
  },
}));

vi.mock("cytoscape", async (importOriginal) => {
  type Real = ((...a: unknown[]) => unknown) & { use: (e: unknown) => void };
  const real = (await importOriginal<{ default: Real }>()).default;
  const make = (...args: unknown[]) => {
    // cytoscape("layout", name): an extension's registration.
    if (typeof args[0] === "string") return real(...args);
    const o = args[0] as Record<string, unknown>;
    const cy = real({ ...o, container: undefined, headless: true, styleEnabled: true });
    elk.cores.push(cy);
    return cy;
  };
  return { default: Object.assign(make, { use: real.use.bind(real) }) };
});

import { ProvisaGraph } from "../ProvisaGraph";

const GRAPH: GraphSpec = {
  nodes: [
    { id: "a", type: "table", name: "pets" },
    { id: "b", type: "table", name: "owners" },
    { id: "c", type: "table", name: "visits" },
  ],
  edges: [
    { id: "ab", source: "a", target: "b" },
    { id: "bc", source: "b", target: "c" },
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

const layered = () => (
  <Boundary>
    <ProvisaGraph graph={GRAPH} preset="layered" testId="g" exportName="g" />
  </Boundary>
);

/** ELK has been asked: the layout has started. */
const started = () =>
  act(async () => {
    await vi.waitFor(() => expect(elk.asked).toHaveLength(1));
  });

/** ELK's answer: every node at a place of its own, 200 apart. */
const placed = (graph: ElkGraph): ElkGraph => ({
  ...graph,
  x: graph.x ?? 0,
  y: graph.y ?? 0,
  children: graph.children?.map((c, i) => ({ ...placed(c), x: i * 200, y: i * 200 })),
});

const drawing = () => {
  const box = screen.getByTestId("g").querySelector(".pg-canvas") as HTMLElement;
  return box.firstElementChild as HTMLElement;
};
const indicator = () => screen.queryByTestId("g-laying-out");
const lastCore = () => elk.cores[elk.cores.length - 1] as HeldCore;

beforeEach(() => {
  elk.asked = [];
  elk.cores = [];
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("an ELK layout", () => {
  it("whose answer is a failure, after it started, drops the indicator and shows the page's error", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    render(layered());
    await started();
    expect(indicator()).toBeInTheDocument();
    expect(drawing()).not.toBeVisible();
    await act(async () => elk.asked[0].reject(new Error("ELK could not lay the graph out")));
    await vi.waitFor(() => expect(indicator()).not.toBeInTheDocument());
    expect(screen.getByTestId("page-error")).toHaveTextContent("ELK could not lay the graph out");
  });

  it("whose answer is a failure that is not an Error says ELK's message", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    render(layered());
    await started();
    // elkjs's worker answers a failure as a plain object carrying the message.
    await act(async () => elk.asked[0].reject({ message: "UnsupportedGraphException" }));
    await vi.waitFor(() => expect(indicator()).not.toBeInTheDocument());
    expect(screen.getByTestId("page-error")).toHaveTextContent("UnsupportedGraphException");
  });

  it("that answers draws every node at its place, with no indicator and no error", async () => {
    render(layered());
    await started();
    await act(async () => elk.asked[0].resolve(placed(elk.asked[0].graph as ElkGraph)));
    await vi.waitFor(() => expect(indicator()).not.toBeInTheDocument());
    expect(screen.queryByTestId("page-error")).not.toBeInTheDocument();
    expect(drawing()).toBeVisible();
    const places = lastCore()
      .nodes('[kind = "node"]')
      .map((n) => `${Math.round(n.position().x)},${Math.round(n.position().y)}`);
    expect(places).toHaveLength(3);
    expect(new Set(places).size).toBe(3);
  });

  it("that is slow is not taken for one that failed: it is laid out when it answers", async () => {
    render(layered());
    await started();
    vi.useFakeTimers();
    // Ten minutes with no answer: a large graph's layout may take that long.
    await act(async () => {
      vi.advanceTimersByTime(10 * 60 * 1000);
    });
    expect(indicator()).toBeInTheDocument();
    expect(drawing()).not.toBeVisible();
    expect(screen.queryByTestId("page-error")).not.toBeInTheDocument();
    vi.useRealTimers();
    await act(async () => elk.asked[0].resolve(placed(elk.asked[0].graph as ElkGraph)));
    await vi.waitFor(() => expect(indicator()).not.toBeInTheDocument());
    expect(screen.queryByTestId("page-error")).not.toBeInTheDocument();
    expect(drawing()).toBeVisible();
  });
});
