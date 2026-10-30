// Copyright (c) 2026 Kenneth Stott
// Canary: 3e9c1a74-58b2-4d06-a7f3-91c2e5d80b16
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174: a graph label's size and which labels are drawn. A label never shrinks below a
// readable size to fit a crowded spot: it follows the zoom, between a minimum and a maximum, and a
// label that would overlap one ranked ahead of it is hidden (its node's full name stays in the
// hover card). The nodes are sized on their own (labelFit), apart from the labels.

/** A label's font size at the resting view (profile3d.css .p3d-label). */
export const LABEL_FONT_PX = 10.5;
/** The smallest and largest a label is drawn, whatever the zoom. */
export const MIN_LABEL_PX = 11.5;
export const MAX_LABEL_PX = 20;

/** A label's font size at ``zoom`` (1 at the resting view, more when nearer). */
export function labelPx(zoom: number): number {
  return Math.min(MAX_LABEL_PX, Math.max(MIN_LABEL_PX, LABEL_FONT_PX * zoom));
}

export interface Box {
  left: number;
  right: number;
  top: number;
  bottom: number;
}

/** Of ``boxes`` (labels on screen, the most important first), which are hidden: each one that
 * overlaps one kept before it, with a gap that reads as one name, not two run together. */
export function hiddenLabels(boxes: Box[]): boolean[] {
  const kept: Box[] = [];
  return boxes.map((r) => {
    const hit = kept.some(
      (k) =>
        r.left < k.right + 8 && r.right > k.left - 8 && r.top < k.bottom + 3 && r.bottom > k.top - 3,
    );
    if (!hit) kept.push(r);
    return hit;
  });
}
