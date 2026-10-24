// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A legend glyph is the shape its nodes are drawn with, as seen: a polygon for a solid with flat
// faces, a box with radii or a border for the rest (graph3d.css).

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { GLYPH_OUTLINE, glyphClip, glyphPoints } from "../glyph";
import { nodeLook, type Shape } from "../nodeLook";
import type { GraphNode } from "../graphModel";

const CSS = readFileSync(resolve(__dirname, "../graph3d.css"), "utf8");
const node = (type: GraphNode["type"], over: Partial<GraphNode> = {}) =>
  ({ id: `${type}:1`, type, label: type, ...over }) as GraphNode;

describe("a legend glyph", () => {
  it.each([
    ["icosahedron", 6],
    ["dodecahedron", 5],
    ["cone", 3],
    ["tetrahedron", 3],
    ["diamond", 4],
  ] as const)("of a %s is its outline, %i corners", (shape, corners) => {
    expect(GLYPH_OUTLINE[shape]).toHaveLength(corners);
  });

  it.each(["sphere", "cylinder", "torus"] as const)(
    "of a %s is a box with its own rule",
    (shape) => {
      expect(GLYPH_OUTLINE[shape]).toBeUndefined();
      expect(CSS).toContain(`.g3d-shape-${shape} {`);
    },
  );

  it("of every node kind is the shape its node is drawn with", () => {
    const kinds: [GraphNode["type"], Shape][] = [
      ["product", "icosahedron"],
      ["domain", "sphere"],
      ["table", "cylinder"],
      ["command", "cone"],
      ["checker", "tetrahedron"],
      ["profile", "torus"],
      ["tag", "dodecahedron"],
      ["term", "diamond"],
    ];
    for (const [type, shape] of kinds) expect(nodeLook(node(type)).shape).toBe(shape);
    // A column is a cube: a square, the box itself.
    expect(nodeLook(node("column")).shape).toBe("cube");
    expect(GLYPH_OUTLINE.cube).toBeUndefined();
  });

  it("is carried as points and clipped by the same outline", () => {
    const triangle = GLYPH_OUTLINE.cone!;
    expect(glyphPoints(triangle)).toBe("0.5,0 0.9,1 0.1,1");
    expect(glyphClip(triangle)).toBe("polygon(50% 0%, 90% 100%, 10% 100%)");
  });
});
