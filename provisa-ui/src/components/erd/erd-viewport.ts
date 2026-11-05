// Copyright (c) 2026 Kenneth Stott
// Canary: 0b7d2e95-4c18-4a3f-9e61-8f5c3a2d7b10
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The ERD's viewport moves and downloads, as its canvas bar calls them.

import type { CyInstance } from "../graph/cytoscape-types";

/** The margin a fit leaves round the diagram. */
export const FIT_PADDING = 10;

export interface ErdExports {
  svg: () => void;
  png: () => void;
  jpeg: () => void;
  json: () => void;
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
