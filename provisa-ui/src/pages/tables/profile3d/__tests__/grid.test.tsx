// Copyright (c) 2026 Kenneth Stott
// Canary: 4d0a37de-8bc5-488f-a1a7-930ba0bfcdd4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A 3D network graph's floor grid: drawn at first, left out of the scene when the Grid checkbox is
// cleared. jsdom has no WebGL, so the scene draws through a stand-in renderer; everything it adds to
// the scene is three.js's own.

import * as THREE from "three";
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

describe("the floor grid in a 3D network graph's scene", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        disconnect() {}
      },
    );
  });

  it("is drawn at first, left out when the grid is turned off, and back when it is on", async () => {
    const { GraphScene } = await import("../../../../components/graph3d/graphScene");
    const wrap = document.createElement("div");
    const canvas = document.createElement("canvas");
    wrap.appendChild(canvas);
    const labels = document.createElement("div");
    const scene = new GraphScene(
      canvas,
      labels,
      { onHover: () => {}, onSelect: () => {}, onUserMove: () => {} },
      THEME,
    );
    scene.setData(G, AT, true);
    const root = (scene as unknown as { scene: THREE.Scene }).scene;
    const drawn = () => {
      const grids: THREE.GridHelper[] = [];
      root.traverseVisible((o) => {
        if (o instanceof THREE.GridHelper) grids.push(o);
      });
      return grids.length;
    };
    expect(drawn()).toBe(1);
    scene.setGrid(false);
    expect(drawn()).toBe(0);
    // A graph laid out again while the grid is off stays without it.
    scene.setData(G, AT, false);
    expect(drawn()).toBe(0);
    scene.setGrid(true);
    expect(drawn()).toBe(1);
    scene.dispose();
  });
});
