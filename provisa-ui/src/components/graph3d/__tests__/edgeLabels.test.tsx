// Copyright (c) 2026 Kenneth Stott
// Canary: 8d1f4a27-3c6e-4b95-a0d2-7e5b9c1f6a38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: a line's name sits on its own line in the 3D graph, frame after frame, in either
// layout, after a re-layout and as the camera moves; no two line names, nor a line name and a
// node's name, are drawn over each other. jsdom has no WebGL, so the scene draws through a
// stand-in renderer.

import * as THREE from "three";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { GraphData } from "../graphModel";

vi.mock("three", async (actual) => {
  const real = await actual<typeof import("three")>();
  class Renderer {
    shadowMap = { enabled: false, type: 0 };
    outputColorSpace = "";
    domElement = document.createElement("canvas");
    setPixelRatio() {}
    getPixelRatio() {
      return 1;
    }
    setSize() {}
    setClearColor() {}
    render() {}
    dispose() {}
  }
  return { ...real, WebGLRenderer: Renderer };
});

const THEME = {
  canvas: "#ffffff",
  ground: "#eeeeee",
  grid: "#cccccc",
  ramp: ["#000000", "#333333", "#666666", "#999999", "#cccccc"] as [
    string,
    string,
    string,
    string,
    string,
  ],
  wallA: "#eeeeee",
  wallB: "#dddddd",
  wallLine: "#bbbbbb",
  nul: "#999999",
  qTail: "#aa0000",
  qMid: "#aaaa00",
  hl: "#0000aa",
  accent: "#00aa00",
  warm: "#aa5500",
  cool: "#0055aa",
  family: {
    numeric: "#111111",
    temporal: "#222222",
    text: "#333333",
    boolean: "#444444",
    other: "#555555",
    foreign: "#666666",
  },
  dark: false,
};

const W = 800;
const H = 500;

/** The maintainer's chain: test3 -PART_OF-> test2 -PART_OF-> test -RELATED_TO-> animal breed name. */
function chain(): GraphData {
  return {
    nodes: [
      { id: "t3", type: "term", label: "test3" },
      { id: "t2", type: "term", label: "test2" },
      { id: "t1", type: "term", label: "test" },
      { id: "abn", type: "term", label: "animal breed name" },
    ],
    edges: [
      { id: "e1", a: "t3", b: "t2", kind: "term_edge", label: "PART_OF" },
      { id: "e2", a: "t2", b: "t1", kind: "term_edge", label: "PART_OF" },
      { id: "e3", a: "t1", b: "abn", kind: "term_edge", label: "RELATED_TO" },
    ],
  };
}

const PLACES = new Map<string, [number, number, number]>([
  ["t3", [-6, 4, 1]],
  ["t2", [-1, 0, -2]],
  ["t1", [4, -3, 2]],
  ["abn", [7, 3, -1]],
]);

type Inside = {
  camera: THREE.Camera;
  nodes: { node: { id: string }; mesh: THREE.Mesh }[];
};

async function stage() {
  const { GraphScene } = await import("../graphScene");
  const wrap = document.createElement("div");
  Object.defineProperty(wrap, "clientWidth", { value: W });
  Object.defineProperty(wrap, "clientHeight", { value: H });
  const canvas = document.createElement("canvas");
  wrap.appendChild(canvas);
  document.body.appendChild(wrap);
  const labels = document.createElement("div");
  const scene = new GraphScene(
    canvas,
    labels,
    { onHover: () => {}, onSelect: () => {}, onUserMove: () => {} },
    THEME,
  );
  return { scene, inside: scene as unknown as Inside, labels };
}

/** Where a pinned label was put on the stage, from its transform. */
function placedAt(el: HTMLElement): { x: number; y: number } {
  const m = /translate\(([-\d.]+)px,([-\d.]+)px\)/.exec(el.style.transform);
  if (!m) throw new Error(`label ${el.textContent} was never placed`);
  return { x: parseFloat(m[1]), y: parseFloat(m[2]) };
}

/** A world point on the 800×500 stage, as the camera now sees it. */
function onStage(inside: Inside, p: THREE.Vector3): { x: number; y: number } {
  inside.camera.updateMatrixWorld();
  const v = p.clone().project(inside.camera);
  return { x: ((v.x + 1) / 2) * W, y: ((1 - v.y) / 2) * H };
}

function meshAt(inside: Inside, id: string): THREE.Vector3 {
  const n = inside.nodes.find((x) => x.node.id === id);
  if (!n) throw new Error(`no node ${id}`);
  return n.mesh.position.clone();
}

function edgeLabels(labels: HTMLElement): HTMLElement[] {
  return [...labels.querySelectorAll<HTMLElement>(".g3d-edge-label")];
}

/** The point ``t`` along the arc the layered layout draws a lateral edge on (graphScene). */
function arcPoint(a: THREE.Vector3, b: THREE.Vector3, t: number): THREE.Vector3 {
  const unit = b.clone().sub(a).normalize();
  const side = new THREE.Vector3(-unit.y, unit.x, 0);
  const ctrl = a
    .clone()
    .add(b)
    .multiplyScalar(0.5)
    .add(side.multiplyScalar(a.distanceTo(b) * 0.3));
  return new THREE.QuadraticBezierCurve3(a, ctrl, b).getPoint(t);
}

/** Each edge label sits, on the stage, on its own edge's point ``t`` (the midpoint, or the arc's),
 * within a pixel: a turn moves the camera on frames it skips drawing. */
function expectOnEdges(
  inside: Inside,
  labels: HTMLElement,
  graph: GraphData,
  point: (e: GraphData["edges"][number], a: THREE.Vector3, b: THREE.Vector3) => THREE.Vector3,
) {
  const els = edgeLabels(labels);
  expect(els).toHaveLength(graph.edges.length);
  const seen: { x: number; y: number }[] = [];
  graph.edges.forEach((e, i) => {
    const want = onStage(inside, point(e, meshAt(inside, e.a), meshAt(inside, e.b)));
    const got = placedAt(els[i]);
    expect(Math.abs(got.x - want.x)).toBeLessThan(1);
    expect(Math.abs(got.y - want.y)).toBeLessThan(1);
    seen.push(got);
  });
  // No two on one spot.
  for (let i = 0; i < seen.length; i++)
    for (let j = i + 1; j < seen.length; j++)
      expect(Math.hypot(seen[i].x - seen[j].x, seen[i].y - seen[j].y)).toBeGreaterThan(5);
}

const straightMid = (_e: unknown, a: THREE.Vector3, b: THREE.Vector3) =>
  a.clone().add(b).multiplyScalar(0.5);

describe("a line's name in the 3D graph", () => {
  beforeEach(() => {
    vi.useFakeTimers({
      toFake: [
        "setTimeout",
        "clearTimeout",
        "requestAnimationFrame",
        "cancelAnimationFrame",
        "performance",
        "Date",
      ],
    });
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
    document.body.textContent = "";
  });

  it("sits at its own line's midpoint, frame after frame, as the view turns (Force)", async () => {
    const { scene, inside, labels } = await stage();
    const graph = chain();
    scene.setData(graph, PLACES, true);
    vi.advanceTimersByTime(100);
    expectOnEdges(inside, labels, graph, straightMid);
    // Many frames as the view turns: each one places every label from the line as it is now.
    scene.setRotate(true);
    for (let s = 0; s < 5; s++) {
      vi.advanceTimersByTime(400);
      expectOnEdges(inside, labels, graph, straightMid);
    }
    scene.dispose();
  });

  it("the shared stage never moves a point a pin hands it (profile3d baseScene)", async () => {
    const { scene, inside, labels } = await stage();
    scene.setData(chain(), PLACES, true);
    const held = new THREE.Vector3(2, 1, 0);
    const pin = (
      scene as unknown as { pin: (t: string, c: string, at: () => THREE.Vector3) => HTMLElement }
    ).pin.bind(scene);
    const el = pin("held", "p3d-label", () => held);
    scene.setRotate(true);
    for (let s = 0; s < 4; s++) {
      vi.advanceTimersByTime(400);
      expect(held.toArray()).toEqual([2, 1, 0]);
      const want = onStage(inside, held);
      const got = placedAt(el);
      expect(Math.abs(got.x - want.x)).toBeLessThan(1);
      expect(Math.abs(got.y - want.y)).toBeLessThan(1);
    }
    expect(labels.contains(el)).toBe(true);
    scene.dispose();
  });

  it("sits on its own arc in the layered layout, after a re-layout and a layout change", async () => {
    const { scene, inside, labels } = await stage();
    const graph = chain();
    // Force first, then Layered: PART_OF is a rank edge (straight), RELATED_TO lateral (an arc).
    scene.setData(graph, PLACES, true);
    vi.advanceTimersByTime(100);
    scene.setArcs(true);
    const flat = new Map<string, [number, number, number]>([
      ["t3", [0, 6, 0]],
      ["t2", [0, 2, 0]],
      ["t1", [0, -2, 0]],
      ["abn", [5, -2, 0]],
    ]);
    scene.setData(graph, flat, true);
    const point = (e: GraphData["edges"][number], a: THREE.Vector3, b: THREE.Vector3) =>
      e.label === "RELATED_TO" ? arcPoint(a, b, 0.5) : straightMid(e, a, b);
    for (let s = 0; s < 4; s++) {
      vi.advanceTimersByTime(300);
      expectOnEdges(inside, labels, graph, point);
    }
    // A re-layout (a hop change brings the same nodes back elsewhere), the camera kept.
    const moved = new Map<string, [number, number, number]>([
      ["t3", [-3, 5, 0]],
      ["t2", [-3, 0, 0]],
      ["t1", [-3, -5, 0]],
      ["abn", [4, -5, 0]],
    ]);
    scene.setData(graph, moved, false);
    scene.setRotate(true);
    for (let s = 0; s < 4; s++) {
      vi.advanceTimersByTime(400);
      expectOnEdges(inside, labels, graph, point);
    }
    scene.dispose();
  });

  it("two lines between one pair of nodes name themselves apart along the line", async () => {
    const { scene, inside, labels } = await stage();
    const graph: GraphData = {
      nodes: [
        { id: "x", type: "term", label: "x" },
        { id: "y", type: "term", label: "y" },
      ],
      edges: [
        { id: "p", a: "x", b: "y", kind: "term_edge", label: "PART_OF" },
        { id: "r", a: "y", b: "x", kind: "term_edge", label: "RELATED_TO" },
      ],
    };
    scene.setData(
      graph,
      new Map([
        ["x", [-6, 0, 0]],
        ["y", [6, 0, 0]],
      ]),
      true,
    );
    vi.advanceTimersByTime(500);
    // A third of the way from x, then two thirds: each on the line, not on one midpoint.
    const at = (t: number) => meshAt(inside, "x").lerp(meshAt(inside, "y"), t);
    expectOnEdges(inside, labels, graph, (e) => at(e.id === "p" ? 1 / 3 : 2 / 3));
    scene.dispose();
  });

  it("hides a line name that collides with a node name or a line name ranked ahead", async () => {
    // jsdom lays nothing out: a label's box is its placed point and its text's width.
    vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockImplementation(function (
      this: HTMLElement,
    ) {
      const m = /translate\(([-\d.]+)px,([-\d.]+)px\)/.exec(this.style.transform);
      const x = m ? parseFloat(m[1]) : 0;
      const y = m ? parseFloat(m[2]) : 0;
      const w = (this.textContent ?? "").length * 6;
      return {
        left: x,
        right: x + w,
        top: y,
        bottom: y + 12,
        x,
        y,
        width: w,
        height: 12,
      } as DOMRect;
    });
    const { scene, labels } = await stage();
    // Two lines crossing at one point (their midpoints coincide), a node away from both.
    const graph: GraphData = {
      nodes: [
        { id: "a", type: "term", label: "a" },
        { id: "b", type: "term", label: "b" },
        { id: "c", type: "term", label: "c" },
        { id: "d", type: "term", label: "d" },
      ],
      edges: [
        { id: "ab", a: "a", b: "b", kind: "term_edge", label: "PART_OF" },
        { id: "cd", a: "c", b: "d", kind: "term_edge", label: "RELATED_TO" },
      ],
    };
    scene.setData(
      graph,
      new Map([
        ["a", [-6, 0, 0]],
        ["b", [6, 0, 0]],
        ["c", [0, -6, 0]],
        ["d", [0, 6, 0]],
      ]),
      true,
    );
    vi.advanceTimersByTime(500);
    const shown = () =>
      edgeLabels(labels)
        .filter((el) => el.style.visibility !== "hidden")
        .map((el) => el.textContent);
    expect(shown()).toHaveLength(1);
    // The selected node's line goes ahead of the other.
    scene.setSelected("c");
    vi.advanceTimersByTime(500);
    expect(shown()).toEqual(["RELATED_TO"]);
    scene.setSelected("a");
    vi.advanceTimersByTime(500);
    expect(shown()).toEqual(["PART_OF"]);
    // A line name never hides a node's name: every node label is drawn.
    const nodeShown = [...labels.querySelectorAll<HTMLElement>(".p3d-node-label")].filter(
      (el) => el.style.visibility !== "hidden",
    );
    expect(nodeShown).toHaveLength(4);
    scene.dispose();
  });
});
