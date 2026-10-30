// Copyright (c) 2026 Kenneth Stott
// Canary: 8b3e41d2-6c07-4f59-a1e8-2d94c7b05e63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** Text is never drawn larger than this multiple of its natural size, however wide the pane. */
export const MAX_DIAGRAM_SCALE = 1.5;

/** Sizes a rendered chart to the pane's width, its height following the aspect ratio. Mermaid's
 * own inline max-width (the natural width) would shrink a tall chain to the pane; here that
 * width, times MAX_DIAGRAM_SCALE, is the cap instead. The pane scrolls for the height. */
export function fitSvgToWidth(svg: SVGSVGElement): void {
  const naturalWidth = Number(svg.getAttribute("viewBox")?.trim().split(/\s+/)[2]);
  if (!(naturalWidth > 0)) {
    throw new Error("Mermaid SVG has no viewBox width");
  }
  svg.style.width = "100%";
  svg.style.height = "auto";
  svg.style.maxWidth = `${naturalWidth * MAX_DIAGRAM_SCALE}px`;
  svg.removeAttribute("width");
  svg.removeAttribute("height");
}
