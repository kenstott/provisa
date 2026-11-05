// Copyright (c) 2026 Kenneth Stott
// Canary: 99c4a74d-675e-489f-8d12-5acaa0c40161
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A 3D view's export (exportImage.ts): images of the view at its size times the chosen scale with
// its labels and legend painted in, an SVG with nodes and labels as text, and a recorded turn of
// so many frames, paced to its length, in the format the browser records. jsdom has no canvas or
// MediaRecorder: a recording 2D context, toBlob, captureStream and MediaRecorder stand in.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  exportFileName,
  exportImage,
  rasterFormats,
  recordOrbit,
  videoFormats,
  type ExportableScene,
} from "../exportImage";
import type { VectorScene } from "../baseScene";

type Ctx = Record<string, unknown> & {
  fills: string[];
  texts: string[];
  canvas: HTMLCanvasElement;
};

function recordingContext(canvas: HTMLCanvasElement): Ctx {
  const ctx = {
    canvas,
    fills: [] as string[],
    texts: [] as string[],
    fillStyle: "",
    font: "",
    globalAlpha: 1,
    textBaseline: "",
    strokeStyle: "",
    lineWidth: 1,
    save: vi.fn(),
    restore: vi.fn(),
    setTransform: vi.fn(),
    beginPath: vi.fn(),
    roundRect: vi.fn(),
    moveTo: vi.fn(),
    lineTo: vi.fn(),
    closePath: vi.fn(),
    fill: vi.fn(),
    stroke: vi.fn(),
    drawImage: vi.fn(),
    measureText: (s: string) => ({ width: s.length * 6 }),
    fillRect: vi.fn(function (this: Ctx) {
      this.fills.push(String(this.fillStyle));
    }),
    fillText: vi.fn(function (this: Ctx, text: string) {
      this.texts.push(text);
    }),
    createLinearGradient: () => ({ addColorStop: vi.fn() }),
  };
  return ctx as unknown as Ctx;
}

let made: Ctx[] = [];
let blobType: string | null = null;

beforeEach(() => {
  made = [];
  blobType = null;
  // jsdom lays nothing out: a legend entry's text sits at the stage's corner, 12px high.
  (Range.prototype as unknown as { getBoundingClientRect: () => DOMRect }).getBoundingClientRect =
    () =>
      ({
        left: 10,
        top: 280,
        width: 60,
        height: 12,
        right: 70,
        bottom: 292,
        x: 10,
        y: 280,
        toJSON: () => ({}),
      }) as DOMRect;
  vi.spyOn(HTMLCanvasElement.prototype, "getContext").mockImplementation(function (
    this: HTMLCanvasElement,
  ) {
    const ctx = recordingContext(this);
    made.push(ctx);
    return ctx as unknown as CanvasRenderingContext2D;
  } as never);
  vi.spyOn(HTMLCanvasElement.prototype, "toBlob").mockImplementation(function (
    cb: BlobCallback,
    type?: string,
  ) {
    blobType = type ?? null;
    cb(new Blob(["x"], { type }));
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  document.body.innerHTML = "";
});

/** A stage 400×300 on screen, with a pinned label and a legend entry over its canvas. */
function stageWith(): { stage: HTMLDivElement; scene: ExportableScene; frames: number[] } {
  const stage = document.createElement("div");
  stage.getBoundingClientRect = () => ({
    left: 0,
    top: 0,
    width: 400,
    height: 300,
    right: 400,
    bottom: 300,
    x: 0,
    y: 0,
    toJSON: () => ({}),
  });
  const label = document.createElement("div");
  label.textContent = "Customer";
  stage.appendChild(label);
  const legend = document.createElement("div");
  legend.className = "p3d-legend";
  const entry = document.createElement("span");
  entry.textContent = "concrete term";
  legend.appendChild(entry);
  stage.appendChild(legend);
  document.body.appendChild(stage);
  const frames: number[] = [];
  const src = document.createElement("canvas");
  const vector: VectorScene = {
    width: 400,
    height: 300,
    floor: [{ x1: 0, y1: 250, x2: 400, y2: 250, color: "#cccccc", opacity: 0.9 }],
    shapes: [
      {
        points: [
          [10, 10],
          [40, 10],
          [25, 40],
        ],
        color: "#2266aa",
        opacity: 1,
      },
      {
        points: [
          [100, 100],
          [130, 100],
          [130, 130],
          [100, 130],
        ],
        color: "#228855",
        opacity: 1,
      },
    ],
  };
  const scene: ExportableScene = {
    background: "#f4f6fa",
    snapshot: (_scale, draw) => draw(src),
    orbit: () => ({
      frame: (fraction, draw) => {
        frames.push(fraction);
        draw(src);
      },
      end: vi.fn(),
    }),
    vector: () => vector,
    canVector: true,
    placedLabels: () => [{ el: label, x: 120, y: 80, rotate: 0, alignEnd: false }],
  };
  return { stage, scene, frames };
}

describe("a 3D view's export", () => {
  it("names a file after the view and the day", () => {
    expect(exportFileName("Ontology", "png", new Date(2026, 9, 22))).toBe(
      "ontology-2026-10-22.png",
    );
    expect(exportFileName("customers terrain!", "webm", new Date(2026, 0, 5))).toBe(
      "customers-terrain-2026-01-05.webm",
    );
  });

  it("offers WebP only where a canvas really encodes it", () => {
    const toDataURL = vi.spyOn(HTMLCanvasElement.prototype, "toDataURL");
    toDataURL.mockReturnValue("data:image/png;base64,x");
    expect(rasterFormats()).toEqual(["png", "jpeg"]);
    toDataURL.mockReturnValue("data:image/webp;base64,x");
    expect(rasterFormats()).toEqual(["png", "jpeg", "webp"]);
  });

  it("offers each video container the browser records, with its first codec", () => {
    vi.stubGlobal("MediaRecorder", {
      isTypeSupported: (m: string) => m === "video/webm;codecs=vp9" || m === "video/mp4",
    });
    expect(videoFormats()).toEqual([
      { ext: "mp4", mime: "video/mp4" },
      { ext: "webm", mime: "video/webm;codecs=vp9" },
    ]);
    vi.stubGlobal("MediaRecorder", undefined);
    expect(videoFormats()).toEqual([]);
  });

  it("draws an image at the view's size times the scale, background, frame, labels and legend", async () => {
    const { stage, scene } = stageWith();
    const blob = await exportImage(scene, stage, "jpeg", 2);
    expect(blob.type).toBe("image/jpeg");
    expect(blobType).toBe("image/jpeg");
    const ctx = made[made.length - 1];
    expect([ctx.canvas.width, ctx.canvas.height]).toEqual([800, 600]);
    expect(ctx.fills[0]).toBe("#f4f6fa");
    expect(ctx.drawImage).toHaveBeenCalledWith(expect.any(HTMLCanvasElement), 0, 0, 800, 600);
    expect(ctx.texts).toEqual(expect.arrayContaining(["Customer", "concrete term"]));
  });

  it("writes an SVG with the scene's shapes and its labels and legend as text", async () => {
    const { stage, scene } = stageWith();
    const svg = await (await exportImage(scene, stage, "svg", 1)).text();
    expect(svg).toMatch(/^<svg xmlns="http:\/\/www.w3.org\/2000\/svg" width="400" height="300"/);
    expect(svg.match(/<polygon /g)).toHaveLength(2);
    expect(svg).toContain("<line ");
    expect(svg).toMatch(/<text [^>]*>Customer<\/text>/);
    expect(svg).toMatch(/<text [^>]*>concrete term<\/text>/);
  });
});

describe("a legend glyph clipped to its outline", () => {
  it("is painted as that polygon, in an SVG and on a canvas", async () => {
    const { stage, scene } = stageWith();
    // A data product's glyph: a hexagon, 10 by 10 at the legend's corner.
    const glyph = document.createElement("i");
    glyph.dataset.glyph = "0.5,0 0.933,0.25 0.933,0.75 0.5,1 0.067,0.75 0.067,0.25";
    glyph.style.background = "rgb(1, 2, 3)";
    glyph.getBoundingClientRect = () =>
      ({
        left: 20,
        top: 30,
        width: 10,
        height: 10,
        right: 30,
        bottom: 40,
        x: 20,
        y: 30,
      }) as DOMRect;
    stage.querySelector(".p3d-legend")!.prepend(glyph);

    const svg = await (await exportImage(scene, stage, "svg", 1)).text();
    expect(svg).toContain(
      '<polygon transform="matrix(1 0 0 1 20 30)" points="5,0 9.33,2.5 9.33,7.5 5,10 0.67,7.5 0.67,2.5" fill="rgb(1, 2, 3)"',
    );
    // Not also as the box it is clipped from.
    expect(svg).not.toMatch(/<rect [^>]*fill="rgb\(1, 2, 3\)"/);

    await exportImage(scene, stage, "png", 1);
    const ctx = made[made.length - 1];
    expect(ctx.lineTo).toHaveBeenCalledTimes(5);
    expect(ctx.closePath).toHaveBeenCalled();
  });
});

describe("a recorded turn", () => {
  function recorderStub() {
    const instances: { mime: string; started: boolean }[] = [];
    class Recorder {
      ondataavailable: ((e: { data: Blob }) => void) | null = null;
      onstop: (() => void) | null = null;
      constructor(_stream: unknown, o: { mimeType: string }) {
        instances.push({ mime: o.mimeType, started: false });
      }
      start() {
        instances[instances.length - 1].started = true;
      }
      stop() {
        this.ondataavailable?.({ data: new Blob(["frames"]) });
        this.onstop?.();
      }
    }
    vi.stubGlobal("MediaRecorder", Recorder);
    const requestFrame = vi.fn();
    (HTMLCanvasElement.prototype as unknown as { captureStream: unknown }).captureStream = () => ({
      getVideoTracks: () => [{ requestFrame }],
      getTracks: () => [{ stop: vi.fn() }],
    });
    return { instances, requestFrame };
  }
  /** A clock that moves only as the recording sleeps. */
  function fakeClock() {
    let t = 0;
    const slept: number[] = [];
    return {
      slept,
      clock: {
        now: () => t,
        sleep: async (ms: number) => {
          slept.push(ms);
          t += ms;
        },
      },
    };
  }

  it("draws seconds × fps frames of one full turn, paced to its length, in the format chosen", async () => {
    const { instances, requestFrame } = recorderStub();
    const { stage, scene, frames } = stageWith();
    const { clock } = fakeClock();
    const progress: number[] = [];
    const blob = await recordOrbit(scene, stage, {
      seconds: 2,
      fps: 5,
      format: { ext: "webm", mime: "video/webm;codecs=vp9" },
      scale: 1,
      onProgress: (f) => progress.push(f),
      signal: new AbortController().signal,
      clock,
    });
    expect(frames).toHaveLength(10);
    expect(frames[0]).toBe(0);
    expect(frames[9]).toBeCloseTo(0.9);
    expect(requestFrame).toHaveBeenCalledTimes(10);
    expect(clock.now()).toBe(2000);
    expect(progress[progress.length - 1]).toBe(1);
    expect(instances).toEqual([{ mime: "video/webm;codecs=vp9", started: true }]);
    expect(blob.type).toBe("video/webm");
    // Every frame has the labels painted in.
    expect(made[made.length - 1].texts.filter((t) => t === "Customer")).toHaveLength(10);
  });

  it("puts the camera back when cancelled, and says it was cancelled", async () => {
    recorderStub();
    const { stage, scene } = stageWith();
    const { clock } = fakeClock();
    const end = vi.fn();
    const controller = new AbortController();
    const turning: ExportableScene = {
      ...scene,
      orbit: () => ({
        frame: (fraction, draw) => {
          if (fraction >= 0.5) controller.abort();
          draw(document.createElement("canvas"));
        },
        end,
      }),
    };
    await expect(
      recordOrbit(turning, stage, {
        seconds: 1,
        fps: 4,
        format: { ext: "mp4", mime: "video/mp4" },
        scale: 1,
        onProgress: () => {},
        signal: controller.signal,
        clock,
      }),
    ).rejects.toMatchObject({ name: "AbortError" });
    expect(end).toHaveBeenCalledTimes(1);
  });
});
