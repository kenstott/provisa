// Copyright (c) 2026 Kenneth Stott
// Canary: 2f8c5a71-d94e-4b36-8e0a-c13b7f6d92e4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (A GRAPH BEING LAID OUT SAYS SO): ProvisaGraph's ELK layout, a cytoscape layout of its
// own around elkjs's promise. cytoscape-elk drops the promise's rejection, so a layout that fails
// after it has started never ends: the graph stayed hidden behind "Laying out…". This one says a
// failure as ``layoutfail`` (``LAYOUT_FAIL``), with the error; an answer places the nodes as
// cytoscape-elk does (the same graph sent, the same places read back) and ends in ``layoutstop``.
// No time limit: a slow layout of a large graph is not a failure.

import ELK from "elkjs/lib/elk.bundled.js";
import type { ElkExtendedEdge, ElkNode } from "elkjs/lib/elk.bundled.js";

/** The layout's name, as ``cy.layout({ name })`` takes it. */
export const PROVISA_ELK = "provisa-elk";
/** The event a failed layout emits, with its error. */
export const LAYOUT_FAIL = "layoutfail";

/** What of a cytoscape element the layout reads. */
interface El {
  id(): string;
  data(k: string): unknown;
  isParent(): boolean;
  isChild(): boolean;
  parent(): El & { nonempty(): boolean };
  position(): { x: number; y: number };
  layoutDimensions(o: unknown): { w: number; h: number };
}
interface Els {
  nodes(): Els;
  edges(): Els;
  filter(fn: (n: El) => boolean): Els;
  forEach(fn: (n: El) => void): void;
  layoutPositions(layout: unknown, o: unknown, at: (n: El) => { x: number; y: number }): void;
}
interface LayoutOptions {
  cy: { width(): number; height(): number };
  eles: Els;
  elk?: Record<string, unknown>;
  [k: string]: unknown;
}

// cytoscape-elk's defaults: the same options give the same drawing.
const DEFAULTS = {
  nodeDimensionsIncludeLabels: false,
  fit: true,
  padding: 20,
  animate: false,
  animateFilter: () => true,
  animationDuration: 500,
  animationEasing: undefined,
  transform: (_n: unknown, pos: unknown) => pos,
  ready: undefined,
  stop: undefined,
  nodeLayoutOptions: undefined,
  priority: () => null,
};

/** A failure as an Error: elkjs's worker answers one as a plain object with its message. */
function asError(e: unknown): Error {
  if (e instanceof Error) return e;
  const message = (e as { message?: unknown } | null)?.message;
  return new Error(typeof message === "string" ? message : String(e));
}

/** The graph ELK is sent: each node with its size (top-left at its current place), children
 * under their parents, every edge at the top. */
function elkGraph(eles: Els, options: LayoutOptions): ElkNode {
  const byId = new Map<string, ElkNode>();
  const nodes: [El, ElkNode][] = [];
  eles.nodes().forEach((n) => {
    const k: ElkNode = { id: n.id() };
    const per = options.nodeLayoutOptions as ((n: El) => ElkNode["layoutOptions"]) | undefined;
    if (per) k.layoutOptions = per(n);
    if (!n.isParent()) {
      const d = n.layoutDimensions(options);
      const p = n.position();
      Object.assign(k, { x: p.x - d.w / 2, y: p.y - d.h / 2, width: d.w, height: d.h });
    }
    byId.set(k.id, k);
    nodes.push([n, k]);
  });
  const graph: ElkNode = { id: "root", children: [], edges: [] };
  for (const [n, k] of nodes) {
    const parent = n.isChild() ? byId.get(n.parent().id()) : undefined;
    const into = parent ?? graph;
    (into.children ??= []).push(k);
  }
  eles.edges().forEach((e) => {
    const edge = { id: e.id(), source: e.data("source"), target: e.data("target") };
    (graph.edges as ElkExtendedEdge[]).push(edge as unknown as ElkExtendedEdge);
  });
  graph.layoutOptions = options.elk as ElkNode["layoutOptions"];
  return graph;
}

/** Each node's top-left as ELK answered it, in the graph's frame (a child's is its parent's
 * plus its own). */
function placesOf(graph: ElkNode): Map<string, { x: number; y: number }> {
  const at = new Map<string, { x: number; y: number }>();
  const walk = (n: ElkNode, x0: number, y0: number) => {
    for (const c of n.children ?? []) {
      const p = { x: x0 + (c.x ?? 0), y: y0 + (c.y ?? 0) };
      at.set(c.id, p);
      walk(c, p.x, p.y);
    }
  };
  walk(graph, 0, 0);
  return at;
}

/** The layout, registered with cytoscape, which adds its events (``one``, ``emit``). A function,
 * not a class: cytoscape calls it on the layout it makes (``registrant.call(this, options)``). */
interface ProvisaElkLayout {
  options: LayoutOptions;
  emit(event: string, params?: unknown[]): unknown;
  run(): ProvisaElkLayout;
  stop(): ProvisaElkLayout;
  destroy(): ProvisaElkLayout;
}

function ProvisaElkLayout(this: ProvisaElkLayout, options: LayoutOptions): void {
  const { cy } = options;
  this.options = { ...DEFAULTS, ...options };
  this.options.elk = { aspectRatio: cy.width() / cy.height(), ...options.elk };
}

ProvisaElkLayout.prototype.run = function (this: ProvisaElkLayout) {
  const { options } = this;
  const nodes = options.eles.nodes();
  const graph = elkGraph(options.eles, options);
  new ELK().layout(graph).then(
    (answered) => {
      const at = placesOf(answered);
      nodes
        .filter((n) => !n.isParent())
        .layoutPositions(this, options, (n) => {
          const d = n.layoutDimensions(options);
          const p = at.get(n.id()) ?? { x: 0, y: 0 };
          return { x: p.x + d.w / 2, y: p.y + d.h / 2 };
        });
    },
    (error: unknown) => this.emit(LAYOUT_FAIL, [asError(error)]),
  );
  return this;
};
ProvisaElkLayout.prototype.stop = function (this: ProvisaElkLayout) {
  return this;
};
ProvisaElkLayout.prototype.destroy = function (this: ProvisaElkLayout) {
  return this;
};

/** The extension, for ``cytoscape.use``. */
export default function provisaElk(cytoscape: (type: string, name: string, ext: unknown) => void) {
  cytoscape("layout", PROVISA_ELK, ProvisaElkLayout);
}
