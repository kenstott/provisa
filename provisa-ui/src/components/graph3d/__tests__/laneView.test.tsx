// Copyright (c) 2026 Kenneth Stott
// Canary: a1d84e3f-5b29-4c7e-9f60-3e8c2b7d5a14
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28): the 2D view is the lane diagram, drawn by cytoscape: the product
// panel's card and the full graph's presets in lanes, a "+N columns" node expanding in place, a
// click picking a node whose page the detail panel opens, and the diagram copied as Mermaid.
// jsdom draws no canvas: cytoscape is replaced by a recorder, three.js by a stub.

import { act } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import { nodeHref } from "../nodeHref";
import { DATA } from "./fixture";

type Handler = { event: string; selector: string; fn: (evt: unknown) => void };
type Element = { group: string; data: Record<string, unknown> };
const made = vi.hoisted(() => ({ cys: [] as { elements: Element[]; handlers: Handler[] }[] }));
vi.mock("cytoscape", () => {
  const cytoscape = (options: unknown) => {
    // cytoscape("layout", "elk"): the ELK layout is registered.
    if (typeof options === "string") return true;
    const record = {
      elements: (options as { elements: Element[] }).elements,
      handlers: [] as Handler[],
    };
    made.cys.push(record);
    const none = { empty: () => true, nonempty: () => false, forEach: () => undefined };
    return {
      on: (event: string, selector: string, fn: (evt: unknown) => void) =>
        record.handlers.push({ event, selector, fn }),
      layout: () => {
        let done: (() => void) | null = null;
        return {
          one: (_e: string, fn: () => void) => {
            done = fn;
          },
          run: () => done?.(),
        };
      },
      nodes: () => none,
      elements: () => ({ ...none, filter: () => none }),
      // Every node at the origin: the second pass's order is ELK's to keep, not the test's.
      getElementById: () => ({ ...none, position: () => 0 }),
      add: vi.fn(),
      fit: vi.fn(),
      resize: vi.fn(),
      zoom: () => 1,
      center: vi.fn(),
      batch: (fn: () => void) => fn(),
      destroy: vi.fn(),
    };
  };
  return { default: Object.assign(cytoscape, { use: vi.fn() }) };
});
vi.mock("cytoscape-elk", () => ({ default: vi.fn() }));
vi.mock("../graphScene", () => ({
  GraphScene: class {
    constructor() {
      for (const m of [
        "setTheme",
        "setRotate",
        "setGrid",
        "dispose",
        "resetView",
        "setHover",
        "setData",
        "setSelected",
        "focus",
        "setFlat",
        "setArcs",
      ])
        (this as Record<string, unknown>)[m] = vi.fn();
    }
  },
}));

const observed = vi.hoisted(() => ({ fire: [] as (() => void)[] }));
class FakeObserver {
  private cb: IntersectionObserverCallback;
  constructor(cb: IntersectionObserverCallback) {
    this.cb = cb;
  }
  observe() {
    observed.fire.push(() =>
      this.cb(
        [{ isIntersecting: true } as IntersectionObserverEntry],
        this as unknown as IntersectionObserver,
      ),
    );
  }
  disconnect() {}
}
vi.stubGlobal("IntersectionObserver", FakeObserver);
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));
vi.mock("../useProductGraph", () => ({
  useProductGraph: (_product: string | null, enabled: boolean) => ({
    data: enabled ? DATA : null,
    error: null,
    scoped: enabled ? { data: DATA, query: "query DataProductGraph { x }" } : null,
    onScope: () => undefined,
  }),
}));

import { GraphPanel } from "../GraphPanel";

const last = () => made.cys[made.cys.length - 1];
const ids = (kind: string) =>
  last()
    .elements.filter((e) => e.group === "nodes" && e.data.kind === kind)
    .map((e) => e.data.id as string)
    .sort();
const laneOf = (id: string) => last().elements.find((e) => e.data.id === id)?.data.lane;
const tap = (id: string) => {
  const el = last().elements.find((e) => e.data.id === id) as Element;
  const h = last().handlers.find(
    (x) => x.event === "tap" && x.selector === `node[kind = "${el.data.kind as string}"]`,
  ) as Handler;
  act(() => h.fn({ target: { id: () => id, data: (k: string) => el.data[k] } }));
};

beforeEach(() => {
  made.cys.length = 0;
  observed.fire.length = 0;
});

function panel() {
  render(<GraphPanel productId="c360" productName="customer_360" graph={DATA} />);
  act(() => observed.fire.forEach((f) => f()));
}

async function explorer() {
  panel();
  fireEvent.click(screen.getByTestId("graph-panel-expand"));
  const full = await screen.findByTestId("graph3d");
  fireEvent.click(within(full).getByRole("radio", { name: "2D" }));
  await screen.findByTestId("graph3d-lanes");
  return full;
}

describe("the product panel in 2D", () => {
  it("draws the product's lanes: terms, columns, tables, the product", async () => {
    panel();
    fireEvent.click(within(screen.getByTestId("graph-panel")).getByRole("radio", { name: "2D" }));
    await screen.findByTestId("graph-panel-lanes");
    expect(ids("node")).toEqual([
      "c:customers.email",
      "p:c360",
      "t:customers",
      "t:orders",
      "term:customer",
    ]);
    expect(laneOf("term:customer")).toBe("term");
    expect(laneOf("c:customers.email")).toBe("column");
    expect(laneOf("t:orders")).toBe("table");
    expect(laneOf("p:c360")).toBe("product");
    // A 2D graph has one layout: no Force / Layered choice.
    expect(screen.queryByTestId("graph-panel-layout")).not.toBeInTheDocument();
    // A glance: no copy control on the card.
    expect(screen.queryByTestId("graph-panel-lanes-mermaid")).not.toBeInTheDocument();
  });

  it("expands a table's +N columns node in place", async () => {
    panel();
    fireEvent.click(within(screen.getByTestId("graph-panel")).getByRole("radio", { name: "2D" }));
    await screen.findByTestId("graph-panel-lanes");
    expect(ids("more")).toEqual(["more:t:orders"]);
    tap("more:t:orders");
    await waitFor(() => expect(ids("more")).toEqual([]));
    expect(laneOf("c:orders.total")).toBe("column");
  });
});

describe("the full graph in 2D", () => {
  it("draws the All preset in lanes, with no 3D controls", async () => {
    const full = await explorer();
    expect(laneOf("term:party")).toBe("term");
    expect(laneOf("p:c360")).toBe("product");
    expect(ids("node")).not.toContain("d:sales");
    for (const id of ["graph3d-layout", "graph3d-rotate", "graph3d-grid", "graph3d-attraction"])
      expect(within(full).queryByTestId(id)).not.toBeInTheDocument();
  });

  it("draws the Ontology preset's tables from their columns, and every product they belong to", async () => {
    const full = await explorer();
    fireEvent.click(within(full).getByRole("radio", { name: "Ontology" }));
    await waitFor(() => expect(laneOf("t:customers")).toBe("table"));
    expect(ids("node").filter((id) => id.startsWith("p:"))).toEqual(["p:c360"]);
  });

  it("opens a clicked node in the detail panel, whose title links to its page", async () => {
    await explorer();
    tap("t:customers");
    const link = await screen.findByTestId("graph3d-title-link");
    const table = DATA.nodes.find((n) => n.id === "t:customers");
    expect(link).toHaveAttribute("href", nodeHref(table as (typeof DATA.nodes)[number]));
  });

  it("expands +N columns in place, and folds them back from the detail panel", async () => {
    await explorer();
    tap("more:t:orders");
    await waitFor(() => expect(laneOf("c:orders.total")).toBe("column"));
    tap("t:orders");
    fireEvent.click(await screen.findByTestId("graph3d-columns"));
    await waitFor(() => expect(ids("more")).toEqual(["more:t:orders"]));
  });

  it("copies the diagram as Mermaid flowchart text", async () => {
    const writeText = vi.fn(() => Promise.resolve());
    Object.assign(navigator, { clipboard: { writeText } });
    await explorer();
    fireEvent.click(screen.getByTestId("graph3d-lanes-mermaid"));
    await screen.findByText("Copied");
    const text = (writeText.mock.calls[0] as unknown as [string])[0];
    expect(text.split("\n")[0]).toBe("flowchart LR");
    expect(text).toContain('subgraph lane_term["Terms"]');
    expect(text).toContain('["+1 column"]');
  });
});
