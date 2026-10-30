// Copyright (c) 2026 Kenneth Stott
// Canary: 8b4d2f6a-17c3-4e95-b0a8-5d6e9c3f2174
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: a graph label keeps a readable size, following the zoom between bounds; a crowded
// pair of nodes shrinks the nodes (labelFit), never the labels, and the label that would overlap
// one ranked ahead of it is hidden.

import { describe, expect, it } from "vitest";
import * as THREE from "three";
import { labelFit } from "../../../pages/tables/profile3d/bestView";
import {
  hiddenLabels,
  labelPx,
  LABEL_FONT_PX,
  MAX_LABEL_PX,
  MIN_LABEL_PX,
  type Box,
} from "../labelSize";

describe("a graph label's size", () => {
  it("never drops below the minimum in a crowded graph", () => {
    // Two nodes nearly on one spot, with long names: the nodes are sized down for them.
    const nodes = [
      { at: new THREE.Vector3(0, 0, 0), radius: 0.36, label: 4 },
      { at: new THREE.Vector3(0.1, 0, 0), radius: 0.36, label: 4 },
      { at: new THREE.Vector3(6, 3, 0), radius: 0.36, label: 4 },
    ];
    const { scale } = labelFit(nodes, new THREE.Vector3(0, 0, 1), new THREE.Vector3(0, 1, 0));
    expect(scale).toBeLessThan(1);
    // The label at the resting view is the readable minimum, whatever the nodes' scale.
    expect(labelPx(1)).toBeGreaterThanOrEqual(MIN_LABEL_PX);
    expect(MIN_LABEL_PX).toBeGreaterThanOrEqual(11);
  });

  it("follows the zoom, within its bounds", () => {
    expect(labelPx(1.5)).toBeCloseTo(LABEL_FONT_PX * 1.5);
    expect(labelPx(1.5)).toBeGreaterThan(labelPx(1.2));
    expect(labelPx(0.2)).toBe(MIN_LABEL_PX);
    expect(labelPx(10)).toBe(MAX_LABEL_PX);
  });

  it("hides the label that collides with one ranked ahead of it, at full size", () => {
    const px = labelPx(1);
    const at = (x: number, y: number, chars: number): Box => ({
      left: x,
      right: x + chars * px * 0.6,
      top: y,
      bottom: y + px,
    });
    // The crowded pair's labels, at the same size: the second overlaps the first and is hidden;
    // a label well apart is drawn.
    const boxes = [at(100, 100, 20), at(104, 102, 20), at(400, 300, 20)];
    expect(hiddenLabels(boxes)).toEqual([false, true, false]);
    // Hiding changes no size: every box is still the full label.
    for (const b of boxes) expect(b.bottom - b.top).toBe(px);
  });

  it("keeps two labels a word's gap apart, not run together", () => {
    expect(hiddenLabels([at2(0, 0, 50), at2(55, 0, 50)])).toEqual([false, true]);
    expect(hiddenLabels([at2(0, 0, 50), at2(60, 0, 50)])).toEqual([false, false]);
  });
});

function at2(x: number, y: number, w: number): Box {
  return { left: x, right: x + w, top: y, bottom: y + 12 };
}
