// Copyright (c) 2026 Kenneth Stott
// Canary: 4c9b2e7f-3a16-4d58-8f0b-a2d6e1c5b973
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2187, REQ-2239: what a view marks on its graph -- the things its focus names (fitted into
// view), and what its filter matches (the rest dimmed) -- as classes on the drawn elements, so a
// keystroke moves and rebuilds nothing. And the one hover and selection state.

import type { CyInstance } from "../graph/cytoscape-types";
import { MARK } from "./stylesheet";

/** The margin round what a focus fits into view. */
const FOCUS_PADDING = 40;
/** A focus on one small thing is not blown up to the whole canvas. */
const MOST_ZOOM = 1.5;

/** Mark the elements ``ids`` names that the graph holds, and fit them into view; the ids held. */
export function markFocused(cy: CyInstance, ids: string[]): string[] {
  cy.$(`.${MARK.focused}`).removeClass(MARK.focused);
  const held = ids.filter((id) => {
    const el = cy.getElementById(id);
    if (el.empty()) return false;
    el.addClass(MARK.focused);
    return true;
  });
  const focused = cy.$(`.${MARK.focused}`);
  if (focused.empty()) return held;
  cy.fit(focused, FOCUS_PADDING);
  if (cy.zoom() > MOST_ZOOM) {
    cy.zoom(MOST_ZOOM);
    cy.center(focused);
  }
  return held;
}

/** A view's filter: what it matches (``marked``) and what stays lit beside it (``lit``: the
 * matches and what they bring with them). Null: no filter. */
export interface GraphFilter {
  marked: string[];
  lit: string[];
}

export function markFilter(cy: CyInstance, filter: GraphFilter | null): void {
  const marked = new Set(filter?.marked ?? []);
  const lit = new Set(filter?.lit ?? []);
  cy.batch(() =>
    cy.elements().forEach((el) => {
      el.removeClass(MARK.dim);
      el.removeClass(MARK.match);
      if (filter === null) return;
      if (!lit.has(el.id())) el.addClass(MARK.dim);
      if (marked.has(el.id())) el.addClass(MARK.match);
    }),
  );
}

/** What hovering or picking brings forward: the nodes and the lines between them. */
export interface Neighbourhood {
  nodes: Set<string>;
  links: Set<string>;
}

/** A node's own lines and the nodes at their other ends. */
export function nearOf(cy: CyInstance, id: string): Neighbourhood {
  const nodes = new Set([id]);
  const links = new Set<string>();
  cy.edges().forEach((e) => {
    const s = String(e.data("source"));
    const t = String(e.data("target"));
    if (s !== id && t !== id) return;
    links.add(e.id());
    nodes.add(s).add(t);
  });
  return { nodes, links };
}

/** The hover and selection state: ``picked`` bordered, what ``near`` holds lit and everything else
 * faded; no ``near``, nothing faded. A group, a lane's box and a frame are never faded. */
export function markHover(
  cy: CyInstance,
  picked: string | null,
  near: Neighbourhood | null,
): void {
  cy.batch(() =>
    cy.elements().forEach((ele) => {
      const id = ele.id();
      const kind = ele.data("kind");
      const drawn = kind !== "group" && kind !== "lane" && kind !== "frame";
      ele.removeClass(MARK.picked);
      ele.removeClass(MARK.lit);
      ele.removeClass(MARK.faded);
      if (id === picked) ele.addClass(MARK.picked);
      if (near?.links.has(id)) ele.addClass(MARK.lit);
      if (near && drawn && !near.nodes.has(id) && !near.links.has(id)) ele.addClass(MARK.faded);
    }),
  );
}
