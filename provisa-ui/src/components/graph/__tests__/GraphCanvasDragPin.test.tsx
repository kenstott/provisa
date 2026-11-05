// Copyright (c) 2026 Kenneth Stott
// Canary: 7b3e91d4-25ac-4f0e-9d61-c8a4f2e07b53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A node dragged on a graph is pinned where it is dropped. In the SPARQL explorer (keepPositions)
// the rest of the drawing stays as it is, also when the drag's DESCRIBE merges new nodes in and
// the drawing is rebuilt; the Cypher explorer keeps its pin through its nudge.

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

const node = (id: number): GNode => ({
  id,
  label: "ex:Thing",
  tableLabel: "ex:Thing",
  properties: { name: String(id), iri: `https://x/${id}` },
});
const key = (n: GNode) => `${n.label}:${n.id}`;
const edge = (a: GNode, b: GNode): GEdge => ({
  identity: `e${a.id}-${b.id}`,
  start: a.id,
  end: b.id,
  type: "ex:rel",
  properties: {},
  startNode: a,
  endNode: b,
});
const graph = (ns: GNode[], es: GEdge[]) => ({
  nodes: new Map(ns.map((n) => [key(n), n])),
  edges: new Map(es.map((e) => [e.identity, e])),
});

const [A, B, C, D] = [1, 2, 3, 4].map(node);

const BASE_PROPS: Omit<CanvasProps, "nodes" | "edges"> = {
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

type Pos = { x: number; y: number };
const positions = (cy: CyInstance) => {
  const m = new Map<string, Pos>();
  cy.nodes().forEach((n) => m.set(n.id() as string, { ...n.position() }));
  return m;
};
// Long enough for a drag's nudges (600 ms each, three in a row) and their safety timers.
const settle = () => act(() => new Promise((r) => setTimeout(r, 2500)));

const pinned = (cy: CyInstance, id: string) =>
  (cy.$id(id) as unknown as { hasClass: (c: string) => boolean }).hasClass("pinned");

function drag(cy: CyInstance, id: string, to: Pos) {
  const n = cy.$id(id) as unknown as {
    emit: (e: string) => void;
    position: (p: Pos) => void;
  };
  n.emit("grab");
  n.position(to);
  n.emit("free");
}

describe("dragging a node pins it where it is dropped", () => {
  it("SPARQL: the rest stays where it was, with no nudge", async () => {
    let cy: CyInstance | null = null;
    const g = graph([A, B, C], [edge(A, B), edge(B, C)]);
    render(<GraphCanvas {...BASE_PROPS} {...g} keepPositions onCyReady={(c) => c && (cy = c)} />);
    await settle();
    const before = positions(cy!);
    const to = { x: before.get(key(B))!.x + 300, y: before.get(key(B))!.y + 200 };
    drag(cy!, key(B), to);
    await settle();
    const after = positions(cy!);
    expect(after.get(key(B))).toEqual(to);
    expect(pinned(cy!, key(B))).toBe(true);
    for (const k of [key(A), key(C)]) expect(after.get(k)).toEqual(before.get(k));
  }, 15000);

  it("SPARQL: the drag's DESCRIBE merged in keeps the pin and every other node's place", async () => {
    let cy: CyInstance | null = null;
    const first = graph([A, B, C], [edge(A, B), edge(B, C)]);
    const merged = graph([A, B, C, D], [edge(A, B), edge(B, C), edge(B, D)]);
    const onNodeDragged = vi.fn();
    const { rerender } = render(
      <GraphCanvas
        {...BASE_PROPS}
        {...first}
        keepPositions
        onNodeDragged={onNodeDragged}
        onCyReady={(c) => c && (cy = c)}
      />,
    );
    await settle();
    const before = positions(cy!);
    const to = { x: before.get(key(B))!.x + 300, y: before.get(key(B))!.y + 200 };
    drag(cy!, key(B), to);
    expect(onNodeDragged).toHaveBeenCalledWith(key(B));
    // The DESCRIBE answers while whatever the drag started is still running.
    rerender(
      <GraphCanvas
        {...BASE_PROPS}
        {...merged}
        keepPositions
        onNodeDragged={onNodeDragged}
        onCyReady={(c) => c && (cy = c)}
      />,
    );
    await settle();
    const after = positions(cy!);
    expect(after.get(key(B))).toEqual(to);
    expect(pinned(cy!, key(B))).toBe(true);
    for (const k of [key(A), key(C)]) expect(after.get(k)).toEqual(before.get(k));
    expect(after.has(key(D))).toBe(true);
  }, 15000);

  it("SPARQL: a second merge while the first one's new node is placed keeps the drawing", async () => {
    let cy: CyInstance | null = null;
    const E = node(5);
    const first = graph([A, B, C], [edge(A, B), edge(B, C)]);
    const once = graph([A, B, C, D], [edge(A, B), edge(B, C), edge(B, D)]);
    const twice = graph([A, B, C, D, E], [edge(A, B), edge(B, C), edge(B, D), edge(C, E)]);
    const canvas = (g: ReturnType<typeof graph>) => (
      <GraphCanvas {...BASE_PROPS} {...g} keepPositions onCyReady={(c) => c && (cy = c)} />
    );
    const { rerender } = render(canvas(first));
    await settle();
    const before = positions(cy!);
    const to = { x: before.get(key(B))!.x + 300, y: before.get(key(B))!.y + 200 };
    drag(cy!, key(B), to);
    rerender(canvas(once));
    // The nudge that places D is still running when the next DESCRIBE answers.
    rerender(canvas(twice));
    await settle();
    const after = positions(cy!);
    expect(after.get(key(B))).toEqual(to);
    expect(pinned(cy!, key(B))).toBe(true);
    for (const k of [key(A), key(C)]) expect(after.get(k)).toEqual(before.get(k));
    expect(after.has(key(E))).toBe(true);
  }, 15000);

  it("Cypher: the dragged node stays where it was dropped through the nudge", async () => {
    let cy: CyInstance | null = null;
    const g = graph([A, B, C], [edge(A, B), edge(B, C)]);
    render(<GraphCanvas {...BASE_PROPS} {...g} onCyReady={(c) => c && (cy = c)} />);
    await settle();
    const before = positions(cy!);
    const to = { x: before.get(key(B))!.x + 300, y: before.get(key(B))!.y + 200 };
    drag(cy!, key(B), to);
    await settle();
    const after = positions(cy!).get(key(B))!;
    expect(after.x).toBeCloseTo(to.x, 0);
    expect(after.y).toBeCloseTo(to.y, 0);
    expect(pinned(cy!, key(B))).toBe(true);
  }, 15000);
});
