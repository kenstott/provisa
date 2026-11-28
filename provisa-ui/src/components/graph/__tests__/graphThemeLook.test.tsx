// Copyright (c) 2026 Kenneth Stott
// Canary: 5e2b9c17-84d3-4f0a-a6c1-7d93e0b4f528
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Cypher graph's labels follow the theme. Reported 2026-11-17: in the light theme, the label
// on the edge between two collapsed clusters was drawn on a dark box. The stylesheet draws from the
// graph's one token set (provisa-graph/tokens.ts): every label box on the scheme's canvas, captions
// and edge labels in its text colours, in light and in dark, and again after the theme is switched
// without a reload.

import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter } from "react-router-dom";
import { I18nextProvider } from "react-i18next";
import { MockedProvider } from "@apollo/client/testing/react";
import { theme } from "../../../theme/theme";
import i18n from "../../../i18n";

const { cy, cytoscape } = vi.hoisted(() => {
  const collection: Record<string, unknown> = {};
  Object.assign(collection, {
    on: vi.fn(),
    forEach: vi.fn(),
    length: 0,
    removeClass: () => collection,
    // A caption-field or size change restyles every node (graph-stylesheet restyleCaptions).
    updateStyle: () => collection,
    addClass: () => collection,
    filter: () => collection,
    map: () => [],
  });
  const cy = {
    on: vi.fn(),
    resize: vi.fn(),
    fit: vi.fn(),
    destroy: vi.fn(),
    batch: (fn: () => void) => fn(),
    nodes: () => collection,
    edges: () => collection,
    elements: () => collection,
    style: vi.fn(),
    container: () => document.createElement("div"),
    getElementById: () => ({ position: vi.fn() }),
    layout: () => ({ run: vi.fn(), on: vi.fn(), one: vi.fn(), stop: vi.fn() }),
    userZoomingEnabled: vi.fn(),
  };
  return {
    cy,
    cytoscape: Object.assign(
      vi.fn((_options: unknown) => cy),
      { use: vi.fn() },
    ),
  };
});
vi.mock("cytoscape", () => ({ default: cytoscape }));

import { GraphCanvas } from "../GraphCanvas";
import { buildGraphStylesheet } from "../graph-stylesheet";
import { graphTokens } from "../../provisa-graph/tokens";
import type { CanvasProps } from "../canvas/canvas-types";

type Rule = { selector: string; style: Record<string, unknown> };
type Scheme = "light" | "dark";

const look = (scheme: Scheme) => graphTokens(scheme === "dark");

const none = { current: {} };
const sheet = (scheme: Scheme) =>
  buildGraphStylesheet(
    {
      colorOverridesRef: none,
      sizeOverridesRef: none,
      labelPropertyRef: none,
      relLineOverridesRef: none,
    },
    look(scheme),
  ) as Rule[];

const rule = (rules: Rule[], selector: string) => {
  const r = rules.find((x) => x.selector === selector);
  expect(r, selector).toBeDefined();
  return r!.style;
};

/** The colour a node's caption takes when it runs beneath its node (it does not fit inside). */
const outsideCaption = (rules: Rule[]) => {
  const colour = rule(rules, "node").color as (e: unknown) => string;
  const ele = {
    data: (k: string) => (k === "label" ? "A label far too long to fit" : undefined),
    width: () => 20,
  };
  return colour(ele);
};

describe("the Cypher graph's look follows the theme", () => {
  it.each(["light", "dark"] as const)(
    "draws every label box on the %s canvas, never a fixed colour",
    (scheme) => {
      const rules = sheet(scheme);
      const boxed = rules.filter((r) => "text-background-color" in r.style);
      expect(boxed.map((r) => r.selector)).toContain("edge[?_metaEdge]");
      for (const r of boxed) {
        expect(r.style["text-background-color"], r.selector).toBe(look(scheme).canvas);
      }
    },
  );

  it.each(["light", "dark"] as const)(
    "captions and edge labels in the %s text colours",
    (scheme) => {
      const rules = sheet(scheme);
      expect(outsideCaption(rules)).toBe(look(scheme).text);
      expect(rule(rules, "edge").color).toBe(look(scheme).textMuted);
      expect(rule(rules, "edge[?_metaEdge]").color).toBe(look(scheme).accent);
    },
  );

  it("the light cluster-edge label is not drawn on a dark box (the 2026-11-17 report)", () => {
    const box = rule(sheet("light"), "edge[?_metaEdge]")["text-background-color"];
    expect(box).toBe(graphTokens(false).canvas);
    expect(box).not.toBe(graphTokens(true).canvas);
  });
});

const BASE_PROPS: CanvasProps = {
  nodes: new Map(),
  edges: new Map(),
  overlayNodes: new Map(),
  overlayEdges: new Map(),
  onSelect: vi.fn(),
  colorOverrides: {},
  sizeOverrides: {},
  labelProperty: {},
  sizeByProperty: {},
  sizeMultiplier: {},
  relLineOverrides: {},
  onExcludeNode: vi.fn(),
  pkMap: {},
  labelToTableLabel: {},
  relationships: [],
  showingChildrenNatural: new Set(),
  onToggleChildren: vi.fn(),
  onToggleChildrenBatch: vi.fn(),
  showingChildrenCircular: new Set(),
  onToggleChildrenCircular: vi.fn(),
  showingParents: new Set(),
  onToggleParents: vi.fn(),
  onToggleParentsBatch: vi.fn(),
  showingParentsCircular: new Set(),
  onToggleParentsCircular: vi.fn(),
  clusterLevel: "none",
  exportName: "cypher-graph",
};

/** The app's providers with the colour scheme set from outside, as the theme toggle sets it. */
function Providers({ scheme, children }: { scheme: Scheme; children: ReactNode }) {
  return (
    <MockedProvider mocks={[]}>
      <MantineProvider theme={theme} forceColorScheme={scheme}>
        <I18nextProvider i18n={i18n}>
          <MemoryRouter>{children}</MemoryRouter>
        </I18nextProvider>
      </MantineProvider>
    </MockedProvider>
  );
}

const metaBox = (style: unknown) =>
  rule(style as Rule[], "edge[?_metaEdge]")["text-background-color"];

describe("a theme switch restyles the drawn graph without a reload", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        disconnect() {}
      },
    );
  });
  afterEach(() => vi.unstubAllGlobals());

  it("light -> dark -> light", () => {
    const { rerender } = render(
      <Providers scheme="light">
        <GraphCanvas {...BASE_PROPS} />
      </Providers>,
    );
    const created = cytoscape.mock.calls.at(-1)![0] as { style: unknown };
    expect(metaBox(created.style)).toBe(look("light").canvas);
    const builds = cytoscape.mock.calls.length;

    rerender(
      <Providers scheme="dark">
        <GraphCanvas {...BASE_PROPS} />
      </Providers>,
    );
    expect(cy.style).toHaveBeenCalled();
    expect(metaBox(cy.style.mock.calls.at(-1)![0])).toBe(look("dark").canvas);

    rerender(
      <Providers scheme="light">
        <GraphCanvas {...BASE_PROPS} />
      </Providers>,
    );
    expect(metaBox(cy.style.mock.calls.at(-1)![0])).toBe(look("light").canvas);
    // Restyled in place: the drawing (and its layout) is not rebuilt for a theme switch.
    expect(cytoscape.mock.calls.length).toBe(builds);
  });
});

/** jsdom reports an inline colour as rgb(r, g, b). */
const rgb = (hex: string) => {
  const n = parseInt(hex.slice(1), 16);
  return `rgb(${n >> 16}, ${(n >> 8) & 255}, ${n & 255})`;
};

describe("the canvas is the colour the label boxes are drawn in", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.stubGlobal(
      "ResizeObserver",
      class {
        observe() {}
        disconnect() {}
      },
    );
  });
  afterEach(() => vi.unstubAllGlobals());

  it.each(["light", "dark"] as const)(
    "paints the %s canvas in the tokens' canvas colour",
    (scheme) => {
      render(
        <Providers scheme={scheme}>
          <GraphCanvas {...BASE_PROPS} />
        </Providers>,
      );
      const created = cytoscape.mock.calls.at(-1)![0] as { style: unknown };
      const canvas = screen.getByTestId("graph-canvas-wrap").style.backgroundColor;
      expect(canvas).toBe(rgb(look(scheme).canvas));
      expect(canvas).toBe(rgb(metaBox(created.style) as string));
    },
  );

  it("repaints the canvas on a theme switch", () => {
    const { rerender } = render(
      <Providers scheme="light">
        <GraphCanvas {...BASE_PROPS} />
      </Providers>,
    );
    rerender(
      <Providers scheme="dark">
        <GraphCanvas {...BASE_PROPS} />
      </Providers>,
    );
    expect(screen.getByTestId("graph-canvas-wrap").style.backgroundColor).toBe(
      rgb(look("dark").canvas),
    );
  });
});
