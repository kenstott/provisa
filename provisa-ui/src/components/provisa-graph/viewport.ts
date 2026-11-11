// Copyright (c) 2026 Kenneth Stott
// Canary: 0b7d2e95-4c18-4a3f-9e61-8f5c3a2d7b10
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a graph's camera moves and downloads, as its controls call them.

import type { CyInstance } from "../graph/cytoscape-types";

/** The margin a fit leaves round the diagram. */
export const FIT_PADDING = 10;

export interface GraphExports {
  svg: () => void;
  png: () => void;
  jpeg: () => void;
  json?: () => void;
}

/** How far down the canvas the legend reaches (it sits over the canvas's top right); 0 with none. */
function legendBand(cy: CyInstance): number {
  const box = cy.container();
  const legend = box?.closest(".pg-stage")?.querySelector(".pg-legend");
  if (!box || !legend) return 0;
  return Math.max(0, legend.getBoundingClientRect().bottom - box.getBoundingClientRect().top);
}

/** Fit the whole diagram, `padding` round it, below the legend: a fit that lets the legend cover
 * the diagram's top (a lane's or a group's title) fits again into the canvas under the legend. */
export function fitClear(cy: CyInstance, padding: number = FIT_PADDING): void {
  cy.fit(undefined, padding);
  const band = legendBand(cy);
  if (band === 0) return;
  const all = cy.elements();
  if (all.renderedBoundingBox({}).y1 >= band) return;
  const box = cy.container();
  const bb = all.boundingBox({});
  const width = box.clientWidth - 2 * padding;
  const height = box.clientHeight - band - padding;
  if (bb.w <= 0 || bb.h <= 0 || width <= 0 || height <= 0) return;
  cy.zoom(Math.min(cy.zoom(), width / bb.w, height / bb.h));
  // The zoom the instance accepted: it holds its own limits.
  const z = cy.zoom();
  cy.pan({
    x: (box.clientWidth - bb.w * z) / 2 - bb.x1 * z,
    y: band + (height - bb.h * z) / 2 - bb.y1 * z,
  });
}

/** Zoom by `factor` about the middle of the canvas, as a pinch at its centre would. */
export function zoomAboutCentre(cy: CyInstance, factor: number): void {
  const box = cy.container();
  const cx = box.clientWidth / 2;
  const cyMid = box.clientHeight / 2;
  const before = cy.zoom();
  const pan = cy.pan();
  cy.zoom(before * factor);
  // The zoom the instance accepted: it holds its own limits.
  const after = cy.zoom();
  cy.pan({
    x: cx - ((cx - pan.x) * after) / before,
    y: cyMid - ((cyMid - pan.y) * after) / before,
  });
}
