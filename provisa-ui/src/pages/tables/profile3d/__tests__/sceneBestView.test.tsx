// Copyright (c) 2026 Kenneth Stott
// Canary: 3b994b82-1098-444f-bb33-6700aade4b58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 on a real stage: a network graph's scene opens on its best view, turns about the axis that
// keeps its spread plane facing the viewer, eases back to the best view when turning pauses on idle,
// sizes its nodes and labels against that view, and in 2D locks a face-on orthographic camera.
// jsdom has no WebGL, so the scene draws through a stand-in renderer.

import * as THREE from "three";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { GraphData } from "../../../../components/graph3d/graphModel";
import { TURN_IDLE_MS } from "../turnPause";

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

/** A 5×5 grid of terms joined to their neighbours, lying in ``u``/``v``'s plane with a little
 * depth along its normal, ``step`` apart. */
function planeGraph(u: THREE.Vector3, v: THREE.Vector3, step = 4) {
  const normal = u.clone().cross(v).normalize();
  const nodes: GraphData["nodes"] = [];
  const edges: GraphData["edges"] = [];
  const at = new Map<string, [number, number, number]>();
  for (let i = 0; i < 5; i++)
    for (let j = 0; j < 5; j++) {
      const id = `term:${i}-${j}`;
      nodes.push({ id, type: "term", label: `T${i}${j}` });
      const p = u
        .clone()
        .multiplyScalar((i - 2) * step)
        .add(v.clone().multiplyScalar((j - 2) * step))
        .add(normal.clone().multiplyScalar(((i * 7 + j * 3) % 5) * 0.08 - 0.16));
      at.set(id, [p.x, p.y, p.z]);
      if (j > 0) edges.push({ id: `h${i}${j}`, a: `term:${i}-${j - 1}`, b: id, kind: "term_edge" });
      if (i > 0) edges.push({ id: `v${i}${j}`, a: `term:${i - 1}-${j}`, b: id, kind: "term_edge" });
    }
  return { graph: { nodes, edges }, at, normal };
}

type Inside = {
  camera: THREE.Camera & { up: THREE.Vector3 };
  controls: { target: THREE.Vector3; object: THREE.Camera; enableRotate: boolean };
  turning: boolean;
  best: { dir: THREE.Vector3; up: THREE.Vector3; axis: THREE.Vector3 } | null;
};

async function stage(width = 800, height = 500) {
  const { GraphScene } = await import("../../../../components/graph3d/graphScene");
  const wrap = document.createElement("div");
  Object.defineProperty(wrap, "clientWidth", { value: width });
  Object.defineProperty(wrap, "clientHeight", { value: height });
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
  const inside = scene as unknown as Inside;
  const looking = () =>
    inside.camera.position.clone().sub(inside.controls.target).normalize();
  return { scene, inside, looking, labels };
}

const degrees = (a: THREE.Vector3, b: THREE.Vector3) =>
  (Math.acos(Math.min(1, Math.abs(a.clone().normalize().dot(b.clone().normalize())))) * 180) /
  Math.PI;

describe("a network graph's best view", () => {
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
    document.body.textContent = "";
  });

  it.each([
    ["the floor", new THREE.Vector3(1, 0, 0), new THREE.Vector3(0, 0, 1)],
    ["a tilted plane", new THREE.Vector3(1, 0.6, 0).normalize(), new THREE.Vector3(0, 0.2, 1).normalize()],
  ])("opens facing a graph that lies mostly in %s", async (_name, u, v) => {
    const { scene, looking } = await stage();
    const { graph, at, normal } = planeGraph(u, v);
    scene.setData(graph, at, true);
    expect(degrees(looking(), normal)).toBeLessThan(15);
    scene.dispose();
  });

  it("turns about the plane's normal, the plane facing the viewer all the way round", async () => {
    const { scene, inside, looking } = await stage();
    const u = new THREE.Vector3(1, 0.6, 0).normalize();
    const v = new THREE.Vector3(0, 0.2, 1).normalize();
    const { graph, at, normal } = planeGraph(u, v);
    scene.setData(graph, at, true);
    const facing = degrees(looking(), normal);
    const upright = inside.camera.up.clone();
    scene.setRotate(true);
    for (let s = 0; s < 8; s++) {
      vi.advanceTimersByTime(1000);
      expect(Math.abs(degrees(looking(), normal) - facing)).toBeLessThan(1);
    }
    // It did turn: up on screen has gone round with it.
    expect(degrees(inside.camera.up, upright)).toBeGreaterThan(10);
    scene.dispose();
  });

  it("eases back to the best view when turning pauses on idle, and draws nothing after", async () => {
    const { scene, inside, looking } = await stage();
    const { graph, at } = planeGraph(
      new THREE.Vector3(1, 0.6, 0).normalize(),
      new THREE.Vector3(0, 0.2, 1).normalize(),
    );
    scene.setData(graph, at, true);
    const best = inside.best as NonNullable<Inside["best"]>;
    scene.setRotate(true);
    vi.advanceTimersByTime(TURN_IDLE_MS - 500);
    expect(degrees(inside.camera.up, best.up)).toBeGreaterThan(10);
    // The pause, then the ease and the orbit settling.
    vi.advanceTimersByTime(500 + 4000);
    expect(inside.turning).toBe(false);
    expect(degrees(looking(), best.dir)).toBeLessThan(1);
    expect(degrees(inside.camera.up, best.up)).toBeLessThan(1);
    scene.dispose();
  });

  it("sizes nodes and labels down until the smallest gap holds a label", async () => {
    const { scene, labels } = await stage();
    const roomy = planeGraph(new THREE.Vector3(1, 0, 0), new THREE.Vector3(0, 1, 0), 6);
    scene.setData(roomy.graph, roomy.at, true);
    const full = (labels.firstElementChild as HTMLElement).style.fontSize;
    // The same graph drawn tight: its labels would run into each other at full size.
    const tight = planeGraph(new THREE.Vector3(1, 0, 0), new THREE.Vector3(0, 1, 0), 0.9);
    scene.setData(tight.graph, tight.at, true);
    const small = (labels.firstElementChild as HTMLElement).style.fontSize;
    expect(parseFloat(small)).toBeLessThan(parseFloat(full));
    scene.dispose();
  });

  it("in 2D looks face-on through a locked orthographic camera that never turns", async () => {
    const { scene, inside, looking } = await stage();
    const { graph, at } = planeGraph(new THREE.Vector3(1, 0, 0), new THREE.Vector3(0, 1, 0));
    const flatAt = new Map([...at].map(([id, p]) => [id, [p[0], p[1], 0] as [number, number, number]]));
    scene.setFlat(true);
    scene.setData(graph, flatAt, true);
    expect(inside.camera).toBeInstanceOf(THREE.OrthographicCamera);
    expect(inside.controls.object).toBe(inside.camera);
    expect(inside.controls.enableRotate).toBe(false);
    expect(degrees(looking(), new THREE.Vector3(0, 0, 1))).toBeLessThan(1e-6);
    scene.setRotate(true);
    expect(inside.turning).toBe(false);
    vi.advanceTimersByTime(3000);
    expect(degrees(looking(), new THREE.Vector3(0, 0, 1))).toBeLessThan(1e-6);
    // Back to 3D: the perspective camera, free to turn.
    scene.setFlat(false);
    expect(inside.camera).toBeInstanceOf(THREE.PerspectiveCamera);
    expect(inside.controls.enableRotate).toBe(true);
    expect(inside.turning).toBe(true);
    scene.dispose();
  });
});
