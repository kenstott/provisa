// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9f7a21-84de-4b6a-a0e5-5d1b72c4e9f8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Reported 2026-11-22 (Explore > Cypher): after the caption field changed, some nodes showed the
// new caption and others kept the old one, and its place (inside the circle or beneath it). The
// canvas cleared a "label" bypass no node ever had, which cytoscape skips, so a node was only
// restyled when something else (a hover, a selection) touched it. A caption change, and a size
// change, restyle every node the same way.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, render } from "../../../test-utils/render";

// The real cytoscape, without a canvas renderer: jsdom has no 2D context.
vi.mock("cytoscape", async (importOriginal) => {
  const real = (await importOriginal<{ default: (o: Record<string, unknown>) => unknown }>())
    .default as ((o: Record<string, unknown>) => unknown) & { use: (e: unknown) => void };
  const headless = Object.assign(
    (o: Record<string, unknown>) =>
      real({ ...o, container: undefined, headless: true, styleEnabled: true }),
    { use: real.use.bind(real) },
  );
  return { default: headless };
});

import { GraphCanvas } from "../GraphCanvas";
import type { CanvasProps } from "../canvas/canvas-types";
import type { CyInstance } from "../cytoscape-types";
import type { GEdge, GNode } from "../graph-model";
import { captionLines } from "../graph-stylesheet";

const node = (id: number, name: string): GNode => ({
  id,
  label: "Neighborhood",
  tableLabel: "neighborhoods",
  properties: { name, code: `N${id}`, title: `District number ${id} of the old town` },
});
const NODES = [node(1, "Eastgate"), node(2, "Riverside"), node(3, "Westlake Heights")];
const key = (n: GNode) => `${n.label}:${n.id}`;
const EDGES: GEdge[] = [
  {
    identity: "e1",
    start: 1,
    end: 2,
    type: "BORDERS",
    properties: {},
    startNode: NODES[0],
    endNode: NODES[1],
  },
];

const BASE_PROPS: CanvasProps = {
  nodes: new Map(NODES.map((n) => [key(n), n])),
  edges: new Map(EDGES.map((e) => [e.identity, e])),
  overlayNodes: new Map(),
  overlayEdges: new Map(),
  onSelect: vi.fn(),
  colorOverrides: {},
  sizeOverrides: {},
  labelProperty: {},
  sizeByProperty: {},
  sizeMultiplier: {},
  relLineOverrides: {},
  onExcludeNode: vi.fn(),
  pkMap: {},
  labelToTableLabel: {},
  relationships: [],
  showingChildrenNatural: new Set(),
  onToggleChildren: vi.fn(),
  onToggleChildrenBatch: vi.fn(),
  showingChildrenCircular: new Set(),
  onToggleChildrenCircular: vi.fn(),
  showingParents: new Set(),
  onToggleParents: vi.fn(),
  onToggleParentsBatch: vi.fn(),
  showingParentsCircular: new Set(),
  onToggleParentsCircular: vi.fn(),
  clusterLevel: "none",
};

beforeEach(() => {
  vi.stubGlobal(
    "ResizeObserver",
    class {
      observe() {}
      disconnect() {}
    },
  );
});
afterEach(() => vi.unstubAllGlobals());

const settle = () => act(() => new Promise((r) => setTimeout(r, 1500)));

interface Styled {
  style(name: string): string;
}
const look = (cy: CyInstance, n: GNode) => {
  const ele = cy.$id(key(n)) as unknown as Styled;
  return {
    label: ele.style("label"),
    valign: ele.style("text-valign"),
    maxWidth: ele.style("text-max-width"),
  };
};

describe("a caption-field change redraws every node", () => {
  it("captions every node from the new field, each placed by whether it fits", async () => {
    let cy: CyInstance | null = null;
    const ready = (c: CyInstance | null) => {
      if (c) cy = c;
    };
    const { rerender } = render(<GraphCanvas {...BASE_PROPS} onCyReady={ready} />);
    await settle();
    // Short codes fit inside the 44px circle.
    rerender(
      <GraphCanvas {...BASE_PROPS} labelProperty={{ Neighborhood: "code" }} onCyReady={ready} />,
    );
    await settle();
    for (const n of NODES)
      expect(look(cy!, n)).toEqual({ label: `N${n.id}`, valign: "center", maxWidth: "36px" });

    // Long titles go beneath the node, wrapped to two lines.
    rerender(
      <GraphCanvas {...BASE_PROPS} labelProperty={{ Neighborhood: "title" }} onCyReady={ready} />,
    );
    await settle();
    for (const n of NODES)
      expect(look(cy!, n)).toEqual({
        label: captionLines(`District number ${n.id} of the old town`, 68),
        valign: "bottom",
        maxWidth: "68px",
      });
  }, 15000);
});
