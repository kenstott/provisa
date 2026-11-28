// Copyright (c) 2026 Kenneth Stott
// Canary: e925e983-0297-4759-a241-c1eaee52e225
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Pointing at a collapsed dataset shows what a click does and previews its columns, without
// throwing: the hint's Tooltip once mounted before its target existed, and Mantine's refusal
// blanked the whole app. REQ-2239: the hint is the one graph component's card for the node.
//
// The real cytoscape, headless (jsdom has no 2D context), with elkjs's answer held until the test
// gives it.

import { act } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "../../../test-utils/render";

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

import { LineageDag } from "../LineageDag";

interface ElkGraph {
  id: string;
  x?: number;
  y?: number;
  children?: ElkGraph[];
}

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
const answer = () => act(async () => elk.asked[0].resolve(placed(elk.asked[0].graph as ElkGraph)));

interface HeldCore {
  getElementById(id: string): { emit(event: string): void };
}

const GRAPH = {
  nodes: [
    { id: "t.a", column: "a", relation: "t", kind: "source", materialized: false },
    { id: "t.b", column: "b", relation: "t", kind: "source", materialized: false },
  ],
  edges: [],
  outputs: [],
  cycles: [],
} as never;

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
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("a dataset's hint", () => {
  it("shows at the dataset pointed at, without throwing", async () => {
    render(
      <LineageDag graph={GRAPH} collapsedRelations={new Set(["t"])} onToggleRelation={vi.fn()} />,
    );
    await started();
    await answer();
    const core = elk.cores[elk.cores.length - 1] as HeldCore;
    act(() => core.getElementById("rel:t").emit("mouseover"));
    const hint = await screen.findByTestId("lineage-box-hint");
    expect(hint).toHaveTextContent("Click to expand");
    expect(hint).toHaveTextContent("2 columns");
    // Its columns, previewed.
    expect(hint).toHaveTextContent(/a\s*b/);
  });

  it("a click on the collapsed dataset expands it", async () => {
    const toggle = vi.fn();
    render(
      <LineageDag graph={GRAPH} collapsedRelations={new Set(["t"])} onToggleRelation={toggle} />,
    );
    await started();
    await answer();
    const core = elk.cores[elk.cores.length - 1] as HeldCore;
    act(() => core.getElementById("rel:t").emit("tap"));
    expect(toggle).toHaveBeenCalledWith("t");
  });
});
