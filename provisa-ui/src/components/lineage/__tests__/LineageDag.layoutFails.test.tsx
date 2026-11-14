// Copyright (c) 2026 Kenneth Stott
// Canary: fe992e04-6a50-4041-8790-81c577b117b1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a lineage DAG whose ELK layout fails after it started goes to the page's error path,
// as the graph component's does (provisa-graph/__tests__/layoutFails.test.tsx). ELK answers in a
// promise; cytoscape-elk dropped its rejection, so the DAG stayed unplaced with nothing said.
//
// The real cytoscape, headless (jsdom has no 2D context), with elkjs's answer held until the test
// gives it: resolved with every node placed, or rejected.

import { Component, type ReactNode } from "react";
import { act } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";

interface ElkGraph {
  id: string;
  x?: number;
  y?: number;
  children?: ElkGraph[];
}

const elk = vi.hoisted(() => ({
  /** The graphs ELK was asked to lay out, each with its answer. */
  asked: [] as {
    graph: unknown;
    resolve: (g: unknown) => void;
    reject: (e: unknown) => void;
  }[],
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
    return real({ ...o, container: undefined, headless: true, styleEnabled: true });
  };
  return { default: Object.assign(make, { use: real.use.bind(real) }) };
});

import { LineageDag } from "../LineageDag";

const GRAPH = {
  nodes: [
    { id: "raw.a.x", column: "x", relation: "raw.a", kind: "source", materialized: false },
    { id: "t.b.x", column: "x", relation: "t.b", kind: "source", materialized: false },
  ],
  edges: [{ source: "raw.a.x", target: "t.b.x", kind: "copy" }],
  outputs: [],
  cycles: [],
} as never;

/** The page's own boundary: a DAG that throws while rendering is said in its place. */
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

const dag = () => (
  <Boundary>
    <LineageDag graph={GRAPH} />
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

beforeEach(() => {
  elk.asked = [];
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("a lineage DAG's ELK layout", () => {
  it("whose answer is a failure, after it started, shows the page's error in place of the DAG", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    render(dag());
    await started();
    expect(screen.getByTestId("lineage-dag")).toBeInTheDocument();
    await act(async () => elk.asked[0].reject(new Error("ELK could not lay the DAG out")));
    await vi.waitFor(() =>
      expect(screen.getByTestId("page-error")).toHaveTextContent("ELK could not lay the DAG out"),
    );
    expect(screen.queryByTestId("lineage-dag")).not.toBeInTheDocument();
  });

  it("whose answer is a failure that is not an Error says ELK's message", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    render(dag());
    await started();
    // elkjs's worker answers a failure as a plain object carrying the message.
    await act(async () => elk.asked[0].reject({ message: "UnsupportedGraphException" }));
    await vi.waitFor(() =>
      expect(screen.getByTestId("page-error")).toHaveTextContent("UnsupportedGraphException"),
    );
  });

  it("that answers draws the DAG, with no error", async () => {
    render(dag());
    await started();
    await act(async () => elk.asked[0].resolve(placed(elk.asked[0].graph as ElkGraph)));
    expect(screen.queryByTestId("page-error")).not.toBeInTheDocument();
    expect(screen.getByTestId("lineage-dag")).toBeInTheDocument();
  });
});
