// Copyright (c) 2026 Kenneth Stott
// Canary: 8b3e41d2-6c07-4f59-a1e8-2d94c7b05e63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: how the plan diagram's canvas looks at its drawing, as the one graph's viewport does
// (provisa-graph/viewport.ts): it opens at no less than the readable zoom with its root in view;
// "fit all" fits the whole image however small; scrolling moves it only as far as it runs past
// the canvas.

import { FIT_PADDING, READABLE_ZOOM } from "./provisa-graph/viewport";

/** A diagram opens no larger than this multiple of its natural size, however much room it has. */
export const MAX_DIAGRAM_SCALE = 1.5;
/** The zoom limits of the controls and the wheel: the one graph's largest. */
export const MAX_ZOOM = 4;
export const MIN_ZOOM = 0.02;
/** The space between the charts of a batch, stacked top to bottom. */
export const CHART_GAP = 24;
/** The band at the canvas's top the hint line sits in: a view starts the drawing below it. */
export const HINT_BAND = 28;
/** The node a plan is drawn from: the route every operator hangs below. */
export const ROOT_NODE = "route";

/** The charts as drawn at their natural size: their extent and where each node's centre is. */
export interface Drawing {
  width: number;
  height: number;
  nodes: Map<string, { x: number; y: number }>;
}

/** How the canvas shows the drawing: its scale and where its top left is, in canvas px. */
export interface View {
  zoom: number;
  x: number;
  y: number;
}

export interface Size {
  width: number;
  height: number;
}

const clampZoom = (z: number) => Math.min(MAX_ZOOM, Math.max(MIN_ZOOM, z));

/** Where a span of ``span`` px starts in a room of ``size`` with ``lead`` px kept clear before it
 * (FIT_PADDING after it): centred when it fits; otherwise with ``focus`` (a point of it, in the
 * same px) as near the middle as the span allows, or from its start when there is none. */
function place(span: number, size: number, focus: number | null, lead = FIT_PADDING): number {
  const room = size - lead - FIT_PADDING;
  if (span <= room) return lead + (room - span) / 2;
  if (focus === null) return lead;
  return Math.min(lead, Math.max(size - FIT_PADDING - span, size / 2 - focus));
}

/** How the diagram opens (and its canvas's height, when ``size.height`` is unbounded): the
 * whole image while that keeps the zoom readable, never above MAX_DIAGRAM_SCALE; past that the
 * readable zoom, with the plan's root in view. */
export function openingView(d: Drawing, size: Size): View {
  const fit = Math.min(
    (size.width - 2 * FIT_PADDING) / d.width,
    (size.height - HINT_BAND - FIT_PADDING) / d.height,
  );
  const zoom = Math.min(MAX_DIAGRAM_SCALE, Math.max(READABLE_ZOOM, fit));
  const root = d.nodes.get(ROOT_NODE) ?? null;
  return {
    zoom,
    x: place(d.width * zoom, size.width, root ? root.x * zoom : null),
    y: Number.isFinite(size.height)
      ? place(d.height * zoom, size.height, root ? root.y * zoom : null, HINT_BAND)
      : HINT_BAND,
  };
}

/** The fit control: the whole image in the canvas, however small that makes it (REQ-2239, FIT
 * MEANS THE WHOLE IMAGE). */
export function fitWhole(d: Drawing, size: Size): View {
  const zoom = clampZoom(
    Math.min(
      (size.width - 2 * FIT_PADDING) / d.width,
      (size.height - HINT_BAND - FIT_PADDING) / d.height,
    ),
  );
  return {
    zoom,
    x: place(d.width * zoom, size.width, null),
    y: place(d.height * zoom, size.height, null, HINT_BAND),
  };
}

/** Fit the width: the image as wide as the canvas, read from its top. */
export function fitWidth(_v: View, d: Drawing, size: Size): View {
  const zoom = clampZoom((size.width - 2 * FIT_PADDING) / d.width);
  return { zoom, x: FIT_PADDING, y: place(d.height * zoom, size.height, null, HINT_BAND) };
}

/** Zoom by ``factor`` about ``at`` (canvas px): what is under it stays there. */
export function zoomAt(v: View, factor: number, at: { x: number; y: number }): View {
  const zoom = clampZoom(v.zoom * factor);
  return {
    zoom,
    x: at.x - ((at.x - v.x) * zoom) / v.zoom,
    y: at.y - ((at.y - v.y) * zoom) / v.zoom,
  };
}

/** Scroll by (``dx``, ``dy``) screen px, as far as the drawing runs past the canvas and no
 * further. The same view when it does not move: a drawing that fits does not scroll. */
export function scrollView(v: View, d: Drawing, size: Size, dx: number, dy: number): View {
  const shift = (delta: number, at: number, span: number, room: number, lead: number) => {
    if (span <= room - lead - FIT_PADDING) return 0;
    const most = lead - at;
    const least = room - FIT_PADDING - (at + span);
    return Math.min(most, Math.max(least, -delta));
  };
  const mx = shift(dx, v.x, d.width * v.zoom, size.width, FIT_PADDING);
  const my = shift(dy, v.y, d.height * v.zoom, size.height, HINT_BAND);
  if (Math.abs(mx) < 0.5 && Math.abs(my) < 0.5) return v;
  return { ...v, x: v.x + mx, y: v.y + my };
}

/** The view with the node at ``at`` (drawing px) in it: the same view when it already shows it
 * at a readable zoom; otherwise the node in the middle of the canvas at no less than that zoom. */
export function centredOn(v: View, at: { x: number; y: number }, size: Size): View {
  const sx = v.x + at.x * v.zoom;
  const sy = v.y + at.y * v.zoom;
  const margin = 4 * FIT_PADDING;
  const inside =
    sx >= margin && sx <= size.width - margin && sy >= margin && sy <= size.height - margin;
  if (inside && v.zoom >= READABLE_ZOOM) return v;
  const zoom = Math.max(v.zoom, READABLE_ZOOM);
  return { zoom, x: size.width / 2 - at.x * zoom, y: size.height / 2 - at.y * zoom };
}
