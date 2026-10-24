// Copyright (c) 2026 Kenneth Stott
// Canary: c154db38-6378-40d2-9868-18ee76fcc957
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A node shape's legend glyph: the outline of the solid as it is seen (an icosahedron a hexagon, a
// dodecahedron a pentagon, a cone and a tetrahedron triangles, an octahedron a diamond). The
// outline clips the glyph on screen and is carried on it (data-glyph), so an exported image paints
// the same polygon (exportImage.ts). A sphere, a cylinder, a cube and a torus are boxes with radii
// and borders (graph3d.css), which an export paints as they are.

import type { Shape } from "./nodeLook";

/** The outline in the glyph's own box, each point a fraction of its width and height. */
export const GLYPH_OUTLINE: Partial<Record<Shape, [number, number][]>> = {
  icosahedron: [
    [0.5, 0],
    [0.933, 0.25],
    [0.933, 0.75],
    [0.5, 1],
    [0.067, 0.75],
    [0.067, 0.25],
  ],
  dodecahedron: [
    [0.5, 0],
    [0.976, 0.345],
    [0.794, 0.905],
    [0.206, 0.905],
    [0.024, 0.345],
  ],
  cone: [
    [0.5, 0],
    [0.9, 1],
    [0.1, 1],
  ],
  tetrahedron: [
    [0.5, 0.067],
    [1, 0.933],
    [0, 0.933],
  ],
  diamond: [
    [0.5, 0],
    [1, 0.5],
    [0.5, 1],
    [0, 0.5],
  ],
};

/** The outline as data-glyph carries it: "x,y x,y …". */
export const glyphPoints = (outline: [number, number][]): string =>
  outline.map(([x, y]) => `${x},${y}`).join(" ");

/** The outline as a CSS clip-path. */
export const glyphClip = (outline: [number, number][]): string =>
  `polygon(${outline.map(([x, y]) => `${x * 100}% ${y * 100}%`).join(", ")})`;
