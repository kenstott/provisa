// Copyright (c) 2026 Kenneth Stott
// Canary: 171ead8c-86ae-4452-86ee-8652beb3c49a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A graph drawn as the UI's graph page draws it, to a PNG: the page's own Cytoscape stylesheet
// and layouts, on the page's background for the theme. Cytoscape's canvas renderer draws into a
// jsdom document backed by node-canvas; nothing here is a renderer of its own.

import { JSDOM } from "jsdom";
import { buildGraphStylesheet } from "../../src/components/graph/graph-stylesheet";
import provisaElk, { LAYOUT_FAIL } from "../../src/components/provisa-graph/elkLayout";
import { BACKGROUND, LAYOUTS, type LayoutName, type Theme } from "./look";

export interface DrawRequest {
  elements: {
    nodes: { data: Record<string, unknown> }[];
    edges: { data: Record<string, unknown> }[];
  };
  layout: LayoutName;
  theme: Theme;
  width: number;
  height: number;
}

/** The node's data as the page's stylesheet reads it: the node itself under `_node`, its id the
 *  part of the element id after its label (run_cypher's ids are ``<label>:<id>``). */
function pageNode(data: Record<string, unknown>) {
  const label = String(data.label);
  const elementId = String(data.id);
  if (!elementId.startsWith(`${label}:`)) {
    throw new Error(
      `node ${JSON.stringify(elementId)} is not <label>:<id> for label ${JSON.stringify(label)}`,
    );
  }
  const id = elementId.slice(label.length + 1);
  return { ...data, _node: { id, label, properties: data.properties ?? {} } };
}

export async function drawPng(req: DrawRequest): Promise<Buffer> {
  if (!(req.layout in LAYOUTS)) throw new Error(`unknown layout ${JSON.stringify(req.layout)}`);
  if (!(req.theme in BACKGROUND)) throw new Error(`unknown theme ${JSON.stringify(req.theme)}`);
  const dom = new JSDOM(`<!doctype html><div id="cy"></div>`, { pretendToBeVisual: true });
  const g = globalThis as Record<string, unknown>;
  for (const k of [
    "window",
    "document",
    "HTMLElement",
    "HTMLCanvasElement",
    "Image",
    "getComputedStyle",
    "requestAnimationFrame",
    "cancelAnimationFrame",
  ]) {
    g[k] = k === "window" ? dom.window : (dom.window as unknown as Record<string, unknown>)[k];
  }
  try {
    const { default: cytoscape } = await import("cytoscape");
    const { default: fcose } = await import("cytoscape-fcose");
    // ELK through Provisa's own adapter (the UI's), which says a failed layout: the drawing fails
    // with ELK's error rather than waiting forever on a layout that never stops.
    for (const ext of [fcose, provisaElk]) {
      try {
        // The UI's type shims and the package's own types name the extension type differently.
        cytoscape.use(ext as never);
      } catch {
        /* already registered in this process */
      }
    }
    const container = dom.window.document.getElementById("cy") as HTMLElement;
    // jsdom lays nothing out: the container is given the size asked for, as a browser gives it.
    Object.defineProperty(container, "clientWidth", { value: req.width });
    Object.defineProperty(container, "clientHeight", { value: req.height });
    container.getBoundingClientRect = () =>
      ({
        x: 0,
        y: 0,
        left: 0,
        top: 0,
        width: req.width,
        height: req.height,
        right: req.width,
        bottom: req.height,
      }) as DOMRect;
    const none = { current: {} };
    const cy = cytoscape({
      container,
      elements: [
        ...req.elements.nodes.map((n) => ({ group: "nodes" as const, data: pageNode(n.data) })),
        ...req.elements.edges.map((e) => ({ group: "edges" as const, data: e.data })),
      ],
      // The page's stylesheet with nothing the user has set on it: no colour, size, label or
      // line overrides, as a graph page opened fresh.
      style: buildGraphStylesheet({
        colorOverridesRef: none,
        sizeOverridesRef: none,
        labelPropertyRef: none,
        relLineOverridesRef: none,
      }) as never,
      layout: { name: "null" },
    });
    await new Promise<void>((done, failed) => {
      const layout = cy.layout({
        ...(LAYOUTS[req.layout] as object),
        // The canvas's own box, which a browser container reports and jsdom's does not.
        boundingBox: { x1: 0, y1: 0, w: req.width, h: req.height },
      } as never);
      layout.one(LAYOUT_FAIL as never, (_evt: unknown, error?: unknown) =>
        failed(new Error(`the ${req.layout} layout failed: ${(error as Error).message}`)),
      );
      layout.one("layoutstop", () => done());
      layout.run();
    });
    const png = cy.png({
      output: "base64",
      bg: BACKGROUND[req.theme],
      full: true,
      scale: 2,
    }) as string;
    cy.destroy();
    return Buffer.from(png, "base64");
  } finally {
    // Its animation-frame timer would keep the process alive.
    dom.window.close();
  }
}
