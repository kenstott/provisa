// Copyright (c) 2026 Kenneth Stott
// Canary: f162bb51-98a1-49a3-89fc-496ec8537465
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A turning 3D stage stops turning after TURN_IDLE_MS with no pointer or keyboard input on the
// page, draws nothing while stopped, and turns again on the next input. jsdom has no WebGL, so
// the scene draws through a stand-in renderer that counts its frames.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { GraphData } from "../../../../components/graph3d/graphModel";
import { TURN_IDLE_MS } from "../turnPause";

const rendered = { frames: 0 };

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
    render() {
      rendered.frames++;
    }
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

async function turningStage() {
  const { GraphScene } = await import("../../../../components/graph3d/graphScene");
  const wrap = document.createElement("div");
  Object.defineProperty(wrap, "clientWidth", { value: 400 });
  Object.defineProperty(wrap, "clientHeight", { value: 300 });
  const canvas = document.createElement("canvas");
  wrap.appendChild(canvas);
  document.body.appendChild(wrap);
  const scene = new GraphScene(
    canvas,
    document.createElement("div"),
    { onHover: () => {}, onSelect: () => {}, onUserMove: () => {} },
    THEME,
  );
  scene.setData(G, AT, true);
  scene.setRotate(true);
  const turning = () =>
    (scene as unknown as { controls: { autoRotate: boolean } }).controls.autoRotate;
  return { scene, turning };
}

/** Frames drawn over ``ms`` of animation frames. */
function framesOver(ms: number): number {
  const before = rendered.frames;
  vi.advanceTimersByTime(ms);
  return rendered.frames - before;
}

describe("a turning 3D stage with no input on the page", () => {
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

  it("turns until TURN_IDLE_MS pass without input, then draws nothing", async () => {
    const { scene, turning } = await turningStage();
    expect(framesOver(TURN_IDLE_MS - 1000)).toBeGreaterThan(0);
    expect(turning()).toBe(true);
    // The pause, then the orbit's damping settling once the turn stops.
    vi.advanceTimersByTime(1000 + 5000);
    expect(turning()).toBe(false);
    expect(framesOver(5000)).toBe(0);
    scene.dispose();
  });

  it.each([
    ["pointermove", () => new Event("pointermove", { bubbles: true })],
    ["pointerdown", () => new Event("pointerdown", { bubbles: true })],
    ["keydown", () => new KeyboardEvent("keydown", { key: "a", bubbles: true })],
    ["wheel", () => new WheelEvent("wheel", { bubbles: true })],
  ])("turns again on the next %s anywhere on the page", async (_name, event) => {
    const { scene, turning } = await turningStage();
    vi.advanceTimersByTime(TURN_IDLE_MS + 5000);
    expect(turning()).toBe(false);
    document.body.dispatchEvent(event());
    expect(turning()).toBe(true);
    expect(framesOver(1000)).toBeGreaterThan(0);
    scene.dispose();
  });

  it("keeps turning while input keeps coming", async () => {
    const { scene, turning } = await turningStage();
    for (let i = 0; i < 4; i++) {
      vi.advanceTimersByTime(TURN_IDLE_MS - 1000);
      document.body.dispatchEvent(new KeyboardEvent("keydown", { key: "a", bubbles: true }));
    }
    expect(turning()).toBe(true);
    scene.dispose();
  });

  it("stays still on input when the turn is off", async () => {
    const { scene, turning } = await turningStage();
    scene.setRotate(false);
    vi.advanceTimersByTime(TURN_IDLE_MS + 5000);
    document.body.dispatchEvent(new Event("pointermove", { bubbles: true }));
    expect(turning()).toBe(false);
    scene.dispose();
  });

  it("listens to the page no longer once it is gone", async () => {
    const { scene, turning } = await turningStage();
    vi.advanceTimersByTime(TURN_IDLE_MS + 5000);
    scene.dispose();
    document.body.dispatchEvent(new Event("pointermove", { bubbles: true }));
    expect(turning()).toBe(false);
    expect(framesOver(1000)).toBe(0);
  });
});
