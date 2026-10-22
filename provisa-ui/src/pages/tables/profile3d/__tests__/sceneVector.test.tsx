// Copyright (c) 2026 Kenneth Stott
// Canary: 63259246-5261-43d8-b231-285a0a506221
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A 3D network graph drawn flat for an SVG export (BaseScene.vector): each mesh as the outline of
// its projected vertices, on the stage, in its colour; the floor grid's lines while it is drawn.
// jsdom has no WebGL, so the scene draws through a stand-in renderer.

import { beforeEach, describe, expect, it, vi } from "vitest";
import type { GraphData } from "../../../../components/graph3d/graphModel";

vi.mock("three", async (actual) => {
  const real = await actual<typeof import("three")>();
  class Renderer {
    shadowMap = { enabled: false, type: 0 };
    outputColorSpace = "";
    domElement = document.createElement("canvas");
    setPixelRatio() {}
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

const G: GraphData = {
  nodes: [
    { id: "term:1", type: "term", label: "Customer" },
    { id: "term:2", type: "term", label: "Party" },
  ],
  edges: [{ id: "e", a: "term:1", b: "term:2", kind: "term_edge", label: "KIND_OF" }],
};

const AT = new Map<string, [number, number, number]>([
  ["term:1", [0, 0, 0]],
  ["term:2", [2, 0, 0]],
]);

describe("a 3D network graph drawn flat for an SVG", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        disconnect() {}
      },
    );
  });

  it("outlines every node, edge and arrowhead in its colour, over the floor's lines", async () => {
    const { GraphScene } = await import("../../../../components/graph3d/graphScene");
    const wrap = document.createElement("div");
    Object.defineProperty(wrap, "clientWidth", { value: 800 });
    Object.defineProperty(wrap, "clientHeight", { value: 600 });
    const canvas = document.createElement("canvas");
    wrap.appendChild(canvas);
    const scene = new GraphScene(
      canvas,
      document.createElement("div"),
      { onHover: () => {}, onSelect: () => {}, onUserMove: () => {} },
      THEME,
    );
    scene.setData(G, AT, true);
    // Where the camera settles, turned to its target as a frame turns it (jsdom runs no frames).
    scene.resetView(true);
    (scene as unknown as { controls: { update: () => void } }).controls.update();
    expect(scene.canVector).toBe(true);
    const flat = scene.vector();
    expect([flat.width, flat.height]).toEqual([800, 600]);
    // Two terms, the KIND_OF edge's rod and its arrowhead (a term edge runs one way).
    expect(flat.shapes).toHaveLength(4);
    for (const s of flat.shapes) {
      expect(s.points.length).toBeGreaterThanOrEqual(3);
      // On the stage; a shape reaching past its edge is clipped by the SVG's own box.
      expect(s.points.some(([x, y]) => x >= 0 && x <= 800 && y >= 0 && y <= 600)).toBe(true);
      expect(s.color).toMatch(/^#[0-9a-f]{6}$/);
    }
    expect(flat.floor.length).toBeGreaterThan(0);
    scene.setGrid(false);
    expect(scene.vector().floor).toEqual([]);
    scene.dispose();
  });
});
