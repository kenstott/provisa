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
// on the edge between two collapsed clusters was drawn on a dark box. Every label the stylesheet
// draws a box behind sits on the page's own --surface (the frame's background), and its text is
// the page's text colour, in light and in dark, and again after the theme is switched without
// a reload.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import type { ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render } from "@testing-library/react";
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
import { buildGraphStylesheet, GRAPH_LOOK } from "../graph-stylesheet";
import type { CanvasProps } from "../canvas/canvas-types";

type Rule = { selector: string; style: Record<string, unknown> };
type Scheme = "light" | "dark";

const tokens = readFileSync(resolve(__dirname, "../../../theme/tokens.css"), "utf8");

function pageToken(scheme: Scheme, name: string): string {
  const at = tokens.indexOf(`:root[data-mantine-color-scheme="${scheme}"] {`);
  expect(at, scheme).toBeGreaterThan(-1);
  const block = tokens.slice(at, tokens.indexOf("}", at));
  const m = new RegExp(`--${name}:\\s*(#[0-9a-fA-F]{6});`).exec(block);
  expect(m, `${scheme} --${name}`).not.toBeNull();
  return m![1].toLowerCase();
}

const none = { current: {} };
const sheet = (scheme: Scheme) =>
  buildGraphStylesheet(
    {
      colorOverridesRef: none,
      sizeOverridesRef: none,
      labelPropertyRef: none,
      relLineOverridesRef: none,
    },
    scheme,
  ) as Rule[];

const rule = (rules: Rule[], selector: string) => {
  const r = rules.find((x) => x.selector === selector);
  expect(r, selector).toBeDefined();
  return r!.style;
};

/** The colour a node's caption takes when it runs beneath its node (it does not fit inside). */
const outsideCaption = (rules: Rule[]) => {
  const colour = rule(rules, "node").color as (e: unknown) => string;
  const ele = { data: (k: string) => (k === "label" ? "A label far too long to fit" : undefined), width: () => 20 };
  return colour(ele);
};

describe("the Cypher graph's look follows the theme", () => {
  it("takes the page's surface and text colours for each theme", () => {
    for (const scheme of ["light", "dark"] as const) {
      expect(GRAPH_LOOK[scheme].surface, scheme).toBe(pageToken(scheme, "surface"));
    }
    expect(GRAPH_LOOK.light.caption).toBe(pageToken("light", "text"));
    expect(GRAPH_LOOK.light.edgeText).toBe(pageToken("light", "text-muted"));
  });

  it.each(["light", "dark"] as const)(
    "draws every label box on the %s surface, never a fixed colour",
    (scheme) => {
      const rules = sheet(scheme);
      const boxed = rules.filter((r) => "text-background-color" in r.style);
      expect(boxed.map((r) => r.selector)).toContain("edge[?_metaEdge]");
      for (const r of boxed) {
        expect(r.style["text-background-color"], r.selector).toBe(GRAPH_LOOK[scheme].surface);
      }
    },
  );

  it.each(["light", "dark"] as const)("captions and edge labels in the %s text colours", (scheme) => {
    const rules = sheet(scheme);
    expect(outsideCaption(rules)).toBe(GRAPH_LOOK[scheme].caption);
    expect(rule(rules, "edge").color).toBe(GRAPH_LOOK[scheme].edgeText);
    expect(rule(rules, "edge[?_metaEdge]").color).toBe(GRAPH_LOOK[scheme].metaText);
  });

  it("the light cluster-edge label is not drawn on a dark box (the 2026-11-17 report)", () => {
    expect(rule(sheet("light"), "edge[?_metaEdge]")["text-background-color"]).toBe("#ffffff");
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
    expect(metaBox(created.style)).toBe(GRAPH_LOOK.light.surface);
    const builds = cytoscape.mock.calls.length;

    rerender(
      <Providers scheme="dark">
        <GraphCanvas {...BASE_PROPS} />
      </Providers>,
    );
    expect(cy.style).toHaveBeenCalled();
    expect(metaBox(cy.style.mock.calls.at(-1)![0])).toBe(GRAPH_LOOK.dark.surface);

    rerender(
      <Providers scheme="light">
        <GraphCanvas {...BASE_PROPS} />
      </Providers>,
    );
    expect(metaBox(cy.style.mock.calls.at(-1)![0])).toBe(GRAPH_LOOK.light.surface);
    // Restyled in place: the drawing (and its layout) is not rebuilt for a theme switch.
    expect(cytoscape.mock.calls.length).toBe(builds);
  });
});
