// Copyright (c) 2026 Kenneth Stott
// Canary: 6d2a9f41-3c87-4e15-b0d9-8a7e5c2f1b63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2161, REQ-2239: the flow preset routes its lines orthogonally when its view asks (the role
// graph's lines run between the role boxes as they did before the one graph component drew it);
// lineage asks nothing and its lines stay bezier.
//
// The real cytoscape, headless (jsdom has no 2D context), with elkjs's answer given by the test.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act } from "react";
import { render, screen } from "../../../test-utils/render";
import type { GraphSpec } from "../model";

interface Core {
  edges(): { map<T>(fn: (e: { style(name: string): string }) => T): T[] };
}

const made = vi.hoisted(() => ({
  cores: [] as unknown[],
  asked: [] as { graph: unknown; resolve: (g: unknown) => void }[],
}));

vi.mock("elkjs/lib/elk.bundled.js", () => ({
  default: class {
    layout(graph: unknown) {
      return new Promise((resolve) => made.asked.push({ graph, resolve }));
    }
  },
}));

interface ElkGraph {
  x?: number;
  y?: number;
  children?: ElkGraph[];
}
/** ELK's answer: every node at a place of its own, 300 apart. */
const placed = (graph: ElkGraph): ElkGraph => ({
  ...graph,
  x: graph.x ?? 0,
  y: graph.y ?? 0,
  children: graph.children?.map((c, i) => ({ ...placed(c), x: i * 300, y: i * 300 })),
});

vi.mock("cytoscape", async (importOriginal) => {
  type Real = ((...a: unknown[]) => unknown) & { use: (e: unknown) => void };
  const real = (await importOriginal<{ default: Real }>()).default;
  const make = (...args: unknown[]) => {
    if (typeof args[0] === "string") return real(...args);
    const o = args[0] as Record<string, unknown>;
    const cy = real({ ...o, container: undefined, headless: true, styleEnabled: true });
    made.cores.push(cy);
    return cy;
  };
  return { default: Object.assign(make, { use: real.use.bind(real) }) };
});

import { ProvisaGraph, type LayeredOptions } from "../ProvisaGraph";

const ROLES: GraphSpec = {
  nodes: [
    { id: "viewer", type: "role", name: "viewer", lines: [{ text: "standard role" }] },
    { id: "analyst", type: "role", name: "analyst", lines: [{ text: "standard role" }] },
    { id: "lead", type: "role", name: "lead", lines: [{ text: "custom role" }] },
  ],
  edges: [
    { id: "va", source: "viewer", target: "analyst", arrow: "target" },
    { id: "al", source: "analyst", target: "lead", arrow: "target" },
  ],
};
const TAXI: LayeredOptions = { detail: "all", routing: "taxi", gridSnap: 0, showOrphans: true };

const curves = async (options?: LayeredOptions) => {
  render(<ProvisaGraph graph={ROLES} preset="flow" options={options} testId="g" exportName="g" />);
  await act(async () => {
    await vi.waitFor(() => expect(made.asked).toHaveLength(1));
  });
  await act(async () => made.asked[0].resolve(placed(made.asked[0].graph as ElkGraph)));
  await act(async () => {
    await vi.waitFor(() =>
      expect(screen.queryByTestId("g-laying-out")).not.toBeInTheDocument(),
    );
  });
  const cy = made.cores[made.cores.length - 1] as Core;
  return cy.edges().map((e) => e.style("curve-style"));
};

beforeEach(() => {
  made.cores = [];
  made.asked = [];
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});
afterEach(() => vi.unstubAllGlobals());

describe("the flow preset's lines", () => {
  it("run orthogonally when the view routes them so", async () => {
    // Each line in its own lane (taxi.ts applyTaxiLanes: orthogonal segments, or taxi where its
    // nodes overlap along the corridor).
    const drawn = await curves(TAXI);
    expect(drawn).toHaveLength(2);
    for (const c of drawn) expect(["segments", "taxi"]).toContain(c);
  });

  it("stay bezier when the view asks nothing, as lineage's do", async () => {
    expect(await curves()).toEqual(["bezier", "bezier"]);
  });
});
