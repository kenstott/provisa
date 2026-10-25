// Copyright (c) 2026 Kenneth Stott
// Canary: daad4537-5bdd-4300-9eea-d66d0faea0a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { CyInstance } from "../cytoscape-types";
import type { CanvasHighlight } from "./canvas-types";

/** An Overview pill's highlight on a drawn graph: its label's nodes, or its type's edges and their
 *  ends, stand out (gc-lit) and everything else dims (gc-dim); none clears both. */
export function applyHighlight(cy: CyInstance, highlight: CanvasHighlight | null): void {
  cy.batch(() => {
    cy.elements().removeClass("gc-dim gc-lit");
    if (!highlight) return;
    const lit =
      highlight.kind === "label"
        ? cy.nodes().filter((n) => n.data("label") === highlight.value)
        : cy.edges().filter((e) => e.data("label") === highlight.value);
    lit.addClass("gc-lit");
    const shown = new Set<string>(lit.map((el) => el.id() as string));
    if (highlight.kind === "type")
      lit.forEach((e) => {
        shown.add(e.data("source") as string);
        shown.add(e.data("target") as string);
      });
    cy.elements()
      .filter((el) => !shown.has(el.id() as string))
      .addClass("gc-dim");
  });
}
