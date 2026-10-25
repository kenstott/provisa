// Copyright (c) 2026 Kenneth Stott
// Canary: d0307a77-33cd-4799-92a4-ae8a309b2880
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A 3D view exported as a file: an image of the current view (PNG, JPEG, WebP; SVG where the scene
// can be drawn flat), or a video of one full turn about it (MP4 where the browser encodes H.264,
// WebM otherwise). The node labels and the legend are DOM over the canvas, not in it, so every
// image and video frame paints them too, from the elements as they stand.

import type { Orbit, PlacedLabel, VectorScene } from "./baseScene";

/** What a 3D scene offers an export (BaseScene). */
export interface ExportableScene {
  background: string;
  snapshot: (scale: number, draw: (src: HTMLCanvasElement) => void) => void;
  orbit: (scale: number) => Orbit;
  vector: () => VectorScene | null;
  canVector: boolean;
  placedLabels: () => PlacedLabel[];
}

export type ImageFormat = "png" | "jpeg" | "webp" | "svg";

const IMAGE_TYPES: Record<Exclude<ImageFormat, "svg">, string> = {
  png: "image/png",
  jpeg: "image/jpeg",
  webp: "image/webp",
};

/** The raster formats this browser can encode a canvas as: PNG always, JPEG always, WebP where a
 * canvas actually produces it (a browser that cannot answers PNG instead). */
export function rasterFormats(): Exclude<ImageFormat, "svg">[] {
  const probe = document.createElement("canvas");
  probe.width = probe.height = 1;
  const webp = probe.toDataURL("image/webp").startsWith("data:image/webp");
  return webp ? ["png", "jpeg", "webp"] : ["png", "jpeg"];
}

export interface VideoFormat {
  ext: "mp4" | "webm";
  mime: string;
}

/** Each container this browser's MediaRecorder can encode, with its first supported codec: MP4
 * (H.264) and WebM (VP9, else VP8). None where MediaRecorder is missing. */
export function videoFormats(): VideoFormat[] {
  if (typeof MediaRecorder === "undefined") return [];
  const candidates: VideoFormat[] = [
    { ext: "mp4", mime: "video/mp4;codecs=avc1.42E01E" },
    { ext: "mp4", mime: "video/mp4" },
    { ext: "webm", mime: "video/webm;codecs=vp9" },
    { ext: "webm", mime: "video/webm;codecs=vp8" },
    { ext: "webm", mime: "video/webm" },
  ];
  const out: VideoFormat[] = [];
  for (const c of candidates)
    if (!out.some((o) => o.ext === c.ext) && MediaRecorder.isTypeSupported(c.mime)) out.push(c);
  return out;
}

/** ``name``-yyyy-mm-dd.``ext``, the name lower-cased with anything not a letter or digit as a dash. */
export function exportFileName(name: string, ext: string, on: Date = new Date()): string {
  const slug = name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
  const day = `${on.getFullYear()}-${String(on.getMonth() + 1).padStart(2, "0")}-${String(on.getDate()).padStart(2, "0")}`;
  return `${slug}-${day}.${ext}`;
}

// ---------- the overlay: labels and legend, as paint ----------

/** A 2D affine transform, as canvas and SVG take it: [a, b, c, d, e, f]. */
type Matrix = [number, number, number, number, number, number];

interface Box {
  kind: "box";
  m: Matrix;
  w: number;
  h: number;
  fill: string | null;
  gradient: string[] | null;
  stroke: string | null;
  strokeWidth: number;
  radius: number;
  opacity: number;
}

interface Text {
  kind: "text";
  m: Matrix;
  x: number;
  y: number;
  text: string;
  font: string;
  family: string;
  size: number;
  weight: string;
  style: string;
  color: string;
  opacity: number;
  strike: boolean;
}

/** A legend glyph clipped to an outline (data-glyph): its points in the element's own box. */
interface Polygon {
  kind: "polygon";
  m: Matrix;
  points: [number, number][];
  fill: string;
  opacity: number;
}

export type Paint = Box | Text | Polygon;

const translate = (x: number, y: number): Matrix => [1, 0, 0, 1, x, y];
const multiply = (p: Matrix, q: Matrix): Matrix => [
  p[0] * q[0] + p[2] * q[1],
  p[1] * q[0] + p[3] * q[1],
  p[0] * q[2] + p[2] * q[3],
  p[1] * q[2] + p[3] * q[3],
  p[0] * q[4] + p[2] * q[5] + p[4],
  p[1] * q[4] + p[3] * q[5] + p[5],
];
const rotate = (deg: number): Matrix => {
  const r = (deg * Math.PI) / 180;
  return [Math.cos(r), Math.sin(r), -Math.sin(r), Math.cos(r), 0, 0];
};

const transparent = (c: string) =>
  c === "" || c === "transparent" || /rgba\([^)]*,\s*0\)$/.test(c) || /\/\s*0\)$/.test(c);

function textOf(
  cs: CSSStyleDeclaration,
  m: Matrix,
  x: number,
  y: number,
  text: string,
  opacity: number,
): Text {
  const size = parseFloat(cs.fontSize) || 11;
  return {
    kind: "text",
    m,
    x,
    y,
    text,
    font: `${cs.fontStyle} ${cs.fontWeight} ${size}px ${cs.fontFamily}`,
    family: cs.fontFamily,
    size,
    weight: cs.fontWeight,
    style: cs.fontStyle,
    color: cs.color,
    opacity,
    strike: (cs.textDecorationLine || cs.textDecoration || "").includes("line-through"),
  };
}

/** The pinned labels, where the scene placed them. */
function labelPaint(labels: PlacedLabel[]): Paint[] {
  const out: Paint[] = [];
  for (const l of labels) {
    const cs = getComputedStyle(l.el);
    if (cs.visibility === "hidden" || cs.display === "none") continue;
    const text = l.el.textContent ?? "";
    if (!text) continue;
    const w = l.el.offsetWidth;
    const h = l.el.offsetHeight || parseFloat(cs.fontSize) * 1.3;
    let m = multiply(translate(l.x, l.y), rotate(l.rotate));
    if (l.alignEnd) m = multiply(m, translate(-w, -h / 2));
    out.push(textOf(cs, m, 0, h / 2, text, parseFloat(cs.opacity) || 1));
  }
  return out;
}

/** An element's computed transform as a matrix about its centre, or null when it has none. */
function ownTransform(cs: CSSStyleDeclaration): Matrix | null {
  const t = cs.transform;
  if (!t || t === "none") return null;
  const m = /^matrix\(([^)]+)\)$/.exec(t);
  if (!m) return null;
  const v = m[1].split(",").map(Number);
  return [v[0], v[1], v[2], v[3], v[4], v[5]];
}

/** The legend (or any small overlay), element by element: its boxes, swatches and text. */
function overlayPaint(root: HTMLElement, origin: DOMRect, inherited = 1): Paint[] {
  const out: Paint[] = [];
  const cs = getComputedStyle(root);
  if (cs.display === "none" || cs.visibility === "hidden") return out;
  const opacity = inherited * (parseFloat(cs.opacity) || 1);
  const r = root.getBoundingClientRect();
  const own = ownTransform(cs);
  const w = own ? root.offsetWidth : r.width;
  const h = own ? root.offsetHeight : r.height;
  const m: Matrix = own
    ? multiply(
        multiply(translate(r.left - origin.left + r.width / 2, r.top - origin.top + r.height / 2), [
          own[0],
          own[1],
          own[2],
          own[3],
          0,
          0,
        ]),
        translate(-w / 2, -h / 2),
      )
    : translate(r.left - origin.left, r.top - origin.top);
  const gradient = /linear-gradient/.test(cs.backgroundImage)
    ? (cs.backgroundImage.match(/(rgba?\([^)]+\)|color\([^)]+\)|#[0-9a-f]{3,8})/gi) ?? null)
    : null;
  const fill = transparent(cs.backgroundColor) ? null : cs.backgroundColor;
  const bw = parseFloat(cs.borderTopWidth) || 0;
  const stroke = bw > 0 && !transparent(cs.borderTopColor) ? cs.borderTopColor : null;
  const rr = cs.borderTopLeftRadius;
  const radius = rr.endsWith("%") ? (Math.min(w, h) * parseFloat(rr)) / 100 : parseFloat(rr) || 0;
  const glyph = root.dataset.glyph;
  if (glyph && fill && w > 0 && h > 0) {
    // Clipped to its outline on screen (clip-path), painted as that polygon here.
    const points = glyph.split(" ").map((xy) => {
      const [x, y] = xy.split(",").map(Number);
      return [x * w, y * h] as [number, number];
    });
    out.push({ kind: "polygon", m, points, fill, opacity });
  } else if ((fill || gradient || stroke) && w > 0 && h > 0)
    out.push({ kind: "box", m, w, h, fill, gradient, stroke, strokeWidth: bw, radius, opacity });
  for (const child of root.childNodes) {
    if (child.nodeType === Node.TEXT_NODE) {
      const text = child.textContent?.trim() ?? "";
      if (!text) continue;
      const range = document.createRange();
      range.selectNodeContents(child);
      const tr = range.getBoundingClientRect();
      out.push(
        textOf(
          cs,
          translate(tr.left - origin.left, tr.top - origin.top),
          0,
          tr.height / 2,
          text,
          opacity,
        ),
      );
    } else if (child instanceof HTMLElement) {
      out.push(...overlayPaint(child, origin, opacity));
    }
  }
  return out;
}

/** Everything over the canvas an export paints: the placed labels, then the legend. */
export function overlayOf(stage: HTMLElement, scene: ExportableScene): Paint[] {
  const origin = stage.getBoundingClientRect();
  const legend = stage.querySelector<HTMLElement>(".p3d-legend");
  return [...labelPaint(scene.placedLabels()), ...(legend ? overlayPaint(legend, origin) : [])];
}

/** Paints ``paint`` onto ``ctx`` at ``scale`` (CSS pixels to canvas pixels). */
export function paintCanvas(ctx: CanvasRenderingContext2D, paint: Paint[], scale: number) {
  for (const p of paint) {
    ctx.save();
    ctx.setTransform(
      scale * p.m[0],
      scale * p.m[1],
      scale * p.m[2],
      scale * p.m[3],
      scale * p.m[4],
      scale * p.m[5],
    );
    ctx.globalAlpha = p.opacity;
    if (p.kind === "box") {
      ctx.beginPath();
      ctx.roundRect(0, 0, p.w, p.h, p.radius);
      if (p.gradient && p.gradient.length > 1) {
        const g = ctx.createLinearGradient(0, 0, p.w, 0);
        p.gradient.forEach((c, i) => g.addColorStop(i / (p.gradient!.length - 1), c));
        ctx.fillStyle = g;
        ctx.fill();
      } else if (p.fill) {
        ctx.fillStyle = p.fill;
        ctx.fill();
      }
      if (p.stroke) {
        ctx.strokeStyle = p.stroke;
        ctx.lineWidth = p.strokeWidth;
        ctx.stroke();
      }
    } else if (p.kind === "polygon") {
      ctx.beginPath();
      p.points.forEach(([x, y], i) => (i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y)));
      ctx.closePath();
      ctx.fillStyle = p.fill;
      ctx.fill();
    } else {
      ctx.font = p.font;
      ctx.fillStyle = p.color;
      ctx.textBaseline = "middle";
      ctx.fillText(p.text, p.x, p.y);
      if (p.strike) {
        const width = ctx.measureText(p.text).width;
        ctx.fillRect(p.x, p.y - 0.5, width, 1);
      }
    }
    ctx.restore();
  }
}

const esc = (s: string) =>
  s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
const mat = (m: Matrix) => `matrix(${m.map((v) => +v.toFixed(3)).join(" ")})`;
const pt = (v: number) => +v.toFixed(2);

/** The scene flat, with its labels and legend as text, as an SVG document. */
export function svgOf(vector: VectorScene, paint: Paint[], background: string): string {
  const parts: string[] = [];
  const defs: string[] = [];
  parts.push(`<rect width="100%" height="100%" fill="${esc(background)}"/>`);
  for (const l of vector.floor)
    parts.push(
      `<line x1="${pt(l.x1)}" y1="${pt(l.y1)}" x2="${pt(l.x2)}" y2="${pt(l.y2)}" stroke="${l.color}" stroke-opacity="${pt(l.opacity)}" stroke-width="0.6"/>`,
    );
  for (const s of vector.shapes)
    parts.push(
      `<polygon points="${s.points.map(([x, y]) => `${pt(x)},${pt(y)}`).join(" ")}" fill="${s.color}" fill-opacity="${pt(s.opacity)}"/>`,
    );
  paint.forEach((p, i) => {
    if (p.kind === "box") {
      let fill = p.fill ? esc(p.fill) : "none";
      if (p.gradient && p.gradient.length > 1) {
        const stops = p.gradient
          .map(
            (c, j) => `<stop offset="${pt(j / (p.gradient!.length - 1))}" stop-color="${esc(c)}"/>`,
          )
          .join("");
        defs.push(`<linearGradient id="g${i}">${stops}</linearGradient>`);
        fill = `url(#g${i})`;
      }
      const stroke = p.stroke
        ? ` stroke="${esc(p.stroke)}" stroke-width="${pt(p.strokeWidth)}"`
        : "";
      parts.push(
        `<rect transform="${mat(p.m)}" width="${pt(p.w)}" height="${pt(p.h)}" rx="${pt(p.radius)}" fill="${fill}"${stroke} opacity="${pt(p.opacity)}"/>`,
      );
    } else if (p.kind === "polygon") {
      parts.push(
        `<polygon transform="${mat(p.m)}" points="${p.points.map(([x, y]) => `${pt(x)},${pt(y)}`).join(" ")}" fill="${esc(p.fill)}" opacity="${pt(p.opacity)}"/>`,
      );
    } else {
      parts.push(
        `<text transform="${mat(p.m)}" x="${pt(p.x)}" y="${pt(p.y)}" dominant-baseline="middle" font-family="${esc(p.family)}" font-size="${pt(p.size)}" font-weight="${esc(p.weight)}" font-style="${esc(p.style)}" fill="${esc(p.color)}" opacity="${pt(p.opacity)}"${p.strike ? ' text-decoration="line-through"' : ""}>${esc(p.text)}</text>`,
      );
    }
  });
  return (
    `<svg xmlns="http://www.w3.org/2000/svg" width="${pt(vector.width)}" height="${pt(vector.height)}" viewBox="0 0 ${pt(vector.width)} ${pt(vector.height)}">` +
    (defs.length ? `<defs>${defs.join("")}</defs>` : "") +
    parts.join("") +
    `</svg>`
  );
}

// ---------- images and video ----------

function canvasOf(
  stage: HTMLElement,
  scale: number,
): [HTMLCanvasElement, CanvasRenderingContext2D] {
  const r = stage.getBoundingClientRect();
  const out = document.createElement("canvas");
  out.width = Math.round(r.width * scale);
  out.height = Math.round(r.height * scale);
  const ctx = out.getContext("2d");
  if (!ctx) throw new Error("This browser cannot draw a 2D canvas to export the view into.");
  return [out, ctx];
}

/** One frame: the background, the scene's frame scaled to fit, the labels and legend over it. */
function compose(
  ctx: CanvasRenderingContext2D,
  src: HTMLCanvasElement,
  stage: HTMLElement,
  scene: ExportableScene,
  scale: number,
) {
  const { width, height } = ctx.canvas;
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.globalAlpha = 1;
  ctx.fillStyle = scene.background;
  ctx.fillRect(0, 0, width, height);
  ctx.drawImage(src, 0, 0, width, height);
  paintCanvas(ctx, overlayOf(stage, scene), scale);
}

/** The view as an image file at ``scale`` times its size on screen. */
export async function exportImage(
  scene: ExportableScene,
  stage: HTMLElement,
  format: ImageFormat,
  scale: number,
): Promise<Blob> {
  if (format === "svg") {
    const vector = scene.vector();
    if (!vector) throw new Error("This view cannot be drawn as an SVG.");
    return new Blob([svgOf(vector, overlayOf(stage, scene), scene.background)], {
      type: "image/svg+xml",
    });
  }
  const [out, ctx] = canvasOf(stage, scale);
  scene.snapshot(scale, (src) => compose(ctx, src, stage, scene, scale));
  return new Promise((resolve, reject) =>
    out.toBlob(
      (blob) =>
        blob
          ? resolve(blob)
          : reject(new Error(`This browser could not encode the view as ${format}.`)),
      IMAGE_TYPES[format],
      0.92,
    ),
  );
}

export interface RecordOptions {
  seconds: number;
  fps: number;
  format: VideoFormat;
  scale: number;
  onProgress: (fraction: number) => void;
  signal: AbortSignal;
  /** The clock frames are paced by (performance.now and a timeout in the browser). */
  clock?: { now: () => number; sleep: (ms: number) => Promise<void> };
}

const realClock = {
  now: () => performance.now(),
  sleep: (ms: number) => new Promise<void>((r) => setTimeout(r, ms)),
};

/**
 * One full turn about the current view, recorded: ``seconds`` × ``fps`` frames, each the scene's
 * frame with its labels and legend composited onto a canvas the recorder captures, paced in real
 * time (the recorder stamps frames by the clock). The camera is back where it was afterwards,
 * finished or cancelled.
 */
export async function recordOrbit(
  scene: ExportableScene,
  stage: HTMLElement,
  o: RecordOptions,
): Promise<Blob> {
  const clock = o.clock ?? realClock;
  const [out, ctx] = canvasOf(stage, o.scale);
  const stream = out.captureStream(0);
  const track = stream.getVideoTracks()[0] as CanvasCaptureMediaStreamTrack;
  const recorder = new MediaRecorder(stream, {
    mimeType: o.format.mime,
    videoBitsPerSecond: 8_000_000,
  });
  const chunks: Blob[] = [];
  recorder.ondataavailable = (e) => {
    if (e.data.size > 0) chunks.push(e.data);
  };
  const stopped = new Promise<void>((resolve) => {
    recorder.onstop = () => resolve();
  });
  const frames = Math.round(o.seconds * o.fps);
  const orbit = scene.orbit(o.scale);
  recorder.start();
  const start = clock.now();
  try {
    for (let i = 0; i < frames; i++) {
      if (o.signal.aborted) throw new DOMException("The recording was cancelled.", "AbortError");
      orbit.frame(i / frames, (src) => compose(ctx, src, stage, scene, o.scale));
      track.requestFrame();
      o.onProgress((i + 1) / frames);
      await clock.sleep(Math.max(0, start + ((i + 1) * 1000) / o.fps - clock.now()));
    }
  } finally {
    orbit.end();
    recorder.stop();
    await stopped;
    stream.getTracks().forEach((t) => t.stop());
  }
  return new Blob(chunks, { type: o.format.mime.split(";")[0] });
}

/** Hands ``blob`` to the browser as a download named ``name``. */
export function saveFile(blob: Blob, name: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
