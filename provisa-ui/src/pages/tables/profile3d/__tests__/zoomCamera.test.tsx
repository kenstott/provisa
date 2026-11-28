// Copyright (c) 2026 Kenneth Stott
// Canary: 6cd3b5ee-e75d-4480-add9-144783aab692
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): a 3D view's Zoom in and Zoom out move its camera: in 3D nearer to
// or farther from what it looks at, within the orbit's bounds; in 2D by the orthographic zoom. jsdom
// has no WebGL, so the scene draws through a stand-in renderer; its cameras are three.js's own.

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

type Inside = {
  camera: THREE.PerspectiveCamera | THREE.OrthographicCamera;
  controls: { target: THREE.Vector3; minDistance: number; maxDistance: number };
};

async function made() {
  const { GraphScene } = await import("../../../../components/graph3d/graphScene");
  const canvas = document.createElement("canvas");
  document.createElement("div").appendChild(canvas);
  const scene = new GraphScene(
    canvas,
    document.createElement("div"),
    { onHover: () => {}, onSelect: () => {}, onUserMove: () => {} },
    THEME,
  );
  scene.setData(G, AT, true);
  return { scene, inside: scene as unknown as Inside };
}

describe("a 3D scene's zoom step", () => {
  beforeEach(() => {
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        disconnect() {}
      },
    );
  });

  it("brings the 3D camera nearer by the factor and back out, looking at the same point", async () => {
    const { scene, inside } = await made();
    inside.controls.target.set(1, 0, 0);
    inside.camera.position.set(1, 0, 20);
    const distance = () => inside.camera.position.distanceTo(inside.controls.target);
    scene.zoom(2);
    expect(distance()).toBeCloseTo(10);
    expect(inside.camera.position.x).toBeCloseTo(1);
    scene.zoom(0.5);
    expect(distance()).toBeCloseTo(20);
    scene.dispose();
  });

  it("stops at the orbit's nearest and farthest distance", async () => {
    const { scene, inside } = await made();
    inside.controls.target.set(0, 0, 0);
    inside.camera.position.set(0, 0, 20);
    scene.zoom(1000);
    expect(inside.camera.position.length()).toBeCloseTo(inside.controls.minDistance);
    scene.zoom(1 / 100000);
    expect(inside.camera.position.length()).toBeCloseTo(inside.controls.maxDistance);
    scene.dispose();
  });

  it("scales the 2D camera's zoom", async () => {
    const { scene, inside } = await made();
    scene.setFlat(true);
    const ortho = inside.camera as THREE.OrthographicCamera;
    expect(ortho).toBeInstanceOf(THREE.OrthographicCamera);
    const before = ortho.zoom;
    scene.zoom(1.3);
    expect(ortho.zoom).toBeCloseTo(before * 1.3);
    scene.zoom(1 / 1.3);
    expect(ortho.zoom).toBeCloseTo(before);
    scene.dispose();
  });
});
