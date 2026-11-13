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
import { DATA, FACTS } from "./fixture";
import { ASSOCIATION_CHAIN, KIN, TEST2, TEST3 } from "./associationChain";
import { reach } from "../graphModel";
import { NEIGHBOURHOOD } from "../neighbourhood";

type Handler = { event: string; selector: string; fn: (evt: unknown) => void };
type Element = { group: string; data: Record<string, unknown> };
const made = vi.hoisted(() => ({
  cys: [] as { elements: Element[]; handlers: Handler[]; classes: Map<string, Set<string>> }[],
}));
vi.mock("cytoscape", () => {
  const cytoscape = (options: unknown) => {
    // cytoscape("layout", "elk"): the ELK layout is registered.
    if (typeof options === "string") return true;
    const record = {
      elements: (options as { elements: Element[] }).elements,
      handlers: [] as Handler[],
      classes: new Map<string, Set<string>>(),
    };
    made.cys.push(record);
    const none = { empty: () => true, nonempty: () => false, forEach: () => undefined };
    const drawnElement = (e: Element) => {
      const id = e.data.id as string;
      const cls = record.classes.get(id) ?? new Set<string>();
      record.classes.set(id, cls);
      const self = {
        id: () => id,
        data: (k: string) => e.data[k],
        isNode: () => e.group === "nodes",
        renderedBoundingBox: () => ({ x1: 0, y1: 0, x2: 1, y2: 1 }),
        source: () => ({ renderedPosition: () => ({ x: 0, y: 0 }) }),
        target: () => ({ renderedPosition: () => ({ x: 1, y: 1 }) }),
        removeClass: (c: string) => (cls.delete(c), self),
        addClass: (c: string) => (cls.add(c), self),
      };
      return self;
    };
    return {
      // The element it draws in, as cytoscape's own container() gives it (a fit reads it).
      container: () => (options as { container: HTMLElement }).container,
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
      // The lanes preset reads the lines to place the term lane: none drawn here.
      edges: () => none,
      elements: () => ({
        ...none,
        filter: () => none,
        forEach: (f: (e: unknown) => void) => record.elements.forEach((e) => f(drawnElement(e))),
      }),
      // Every node at the origin: where the lanes preset places them is lanes.test's to check.
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
// The ontology as read: the fixture, with Amount bound to orders.total and orders' placed column
// bound to nothing (its "+N columns" node).
const LANES_DATA = vi.hoisted(() => ({ data: null as unknown }));
vi.mock("../ontologyData", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../ontologyData")>()),
  // The query stands for its scope; the server below answers it with the graph read.
  ontologyQuery: (_m: unknown, s: { layers: Set<string> }) =>
    JSON.stringify({ ...s, layers: [...s.layers] }),
  ontologyCountQuery: () => "count",
  // The graph read; a test's own reader answers by the scope asked (its depth).
  ontologyGraph: (_rows: unknown, s: { depth: number }) =>
    typeof LANES_DATA.data === "function"
      ? (LANES_DATA.data as (s: { depth: number }) => unknown)(s)
      : LANES_DATA.data,
}));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => ({ schema: null, roots: new Map() }),
  dataGraphql: async (query: string) => {
    void query;
    return { _terms: [] };
  },
}));

import { OntologySnippet } from "../OntologyGraph";
import { productPreset, termPreset } from "../ontologyPresets";

const DATA_READ = {
  nodes: [
    ...FACTS.nodes,
    { id: "term:amount", type: "term" as const, label: "Amount" },
    {
      id: "c:orders.placed",
      type: "column" as const,
      label: "placed",
      parent: "t:orders",
      domains: ["sales"],
    },
  ],
  edges: [
    ...FACTS.edges,
    { id: "b2", a: "term:amount", b: "c:orders.total", kind: "term_binding" as const },
    { id: "h2", a: "t:orders", b: "c:orders.placed", kind: "has_column" as const },
  ],
};
LANES_DATA.data = DATA_READ;

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
  render(
    <OntologySnippet preset={productPreset("c360")} name="customer_360" testId="graph-panel" />,
  );
  act(() => observed.fire.forEach((f) => f()));
}

async function explorer() {
  panel();
  fireEvent.click(await screen.findByTestId("graph-panel-expand"));
  const full = await screen.findByTestId("graph3d");
  fireEvent.click(within(full).getByRole("radio", { name: "2D" }));
  await screen.findByTestId("graph3d-lanes");
  return full;
}

describe("the product panel in 2D", () => {
  it("draws the product's lanes: terms with their hierarchy, columns, tables, the product", async () => {
    panel();
    fireEvent.click(
      within(await screen.findByTestId("graph-panel-dimension")).getByRole("radio", { name: "2D" }),
    );
    await screen.findByTestId("graph-panel-lanes");
    // REQ-2174 (amended 2026-11-12, A HOP IS A STEP BETWEEN TERMS): it opens at 1 hop -- the
    // product's tables, their columns tied to terms, those terms, and Party a step across a term
    // link.
    await waitFor(() => expect(ids("node")).toContain("term:party"));
    expect(ids("node")).toEqual([
      "c:customers.email",
      "c:orders.total",
      "p:c360",
      "t:customers",
      "t:orders",
      "term:amount",
      "term:customer",
      "term:party",
    ]);
    expect(laneOf("term:customer")).toBe("term");
    expect(laneOf("c:customers.email")).toBe("column");
    expect(laneOf("t:orders")).toBe("table");
    expect(laneOf("p:c360")).toBe("product");
    // Related tables is the 3D view's (REQ-2174, amended 2026-11-07).
    expect(screen.queryByTestId("graph-panel-related")).not.toBeInTheDocument();
    // A 2D graph has one layout: no Force / Layered choice.
    expect(screen.queryByTestId("graph-panel-layout")).not.toBeInTheDocument();
    // A glance: no copy control on the card.
    expect(screen.queryByTestId("graph-panel-lanes-mermaid")).not.toBeInTheDocument();
  });

  it("expands a table's +N columns node in place", async () => {
    panel();
    fireEvent.click(
      within(await screen.findByTestId("graph-panel-dimension")).getByRole("radio", { name: "2D" }),
    );
    await screen.findByTestId("graph-panel-lanes");
    fireEvent.click(
      within(screen.getByTestId("graph-panel-depth")).getByRole("radio", { name: "3" }),
    );
    await waitFor(() => expect(ids("more")).toEqual(["more:t:orders"]));
    tap("more:t:orders");
    await waitFor(() => expect(ids("more")).toEqual([]));
    expect(laneOf("c:orders.placed")).toBe("column");
  });
});

describe("the full graph in 2D", () => {
  it("draws the whole ontology in lanes, with no 3D controls", async () => {
    const full = await explorer();
    // A product's graph opens on the product: the whole ontology is the rest of the model.
    fireEvent.click(within(full).getByTestId("graph3d-rest"));
    await waitFor(() => expect(laneOf("term:party")).toBe("term"));
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
    await waitFor(() => expect(laneOf("c:orders.placed")).toBe("column"));
    tap("t:orders");
    fireEvent.click(await screen.findByTestId("graph3d-columns"));
    await waitFor(() => expect(ids("more")).toEqual(["more:t:orders"]));
  });

  it("copies the diagram as Mermaid flowchart text", async () => {
    const writeText = vi.fn(() => Promise.resolve());
    Object.assign(navigator, { clipboard: { writeText } });
    const full = await explorer();
    // The term link it names is past the product's own: Party is in the rest of the model.
    fireEvent.click(within(full).getByTestId("graph3d-rest"));
    await waitFor(() => expect(laneOf("term:party")).toBe("term"));
    fireEvent.click(screen.getByTestId("graph3d-lanes-mermaid"));
    await screen.findByText("Copied");
    const text = (writeText.mock.calls[0] as unknown as [string])[0];
    expect(text.split("\n")[0]).toBe("flowchart LR");
    expect(text).toContain('subgraph lane_term["Terms"]');
    expect(text).toContain('["1 column with no term in this view"]');
    // A term link in its own direction, named by its type: Customer is a kind of Party.
    const node = (name: string) =>
      (new RegExp(`(n\\d+)\\["${name}"\\]`).exec(text) as RegExpExecArray)[1];
    expect(text).toContain(`${node("Customer")} -.->|"KIND_OF"| ${node("Party")}`);
  });
});

const hover = (id: string) => {
  const h = last().handlers.find(
    (x) => x.event === "mouseover" && x.selector.includes('node[kind = "more"]'),
  ) as Handler;
  const box = () => ({ x1: 10, y1: 10, x2: 40, y2: 36 });
  act(() =>
    h.fn({
      target: { id: () => id, renderedBoundingBox: box },
      renderedPosition: { x: 10, y: 10 },
    }),
  );
};

describe("the 2D view's controls (2D SHOWS ONLY 2D CONTROLS)", () => {
  it("shows no Layers control in 2D, and shows it again in 3D", async () => {
    const full = await explorer();
    expect(within(full).queryByTestId("graph3d-layers-menu")).not.toBeInTheDocument();
    fireEvent.click(within(full).getByRole("radio", { name: "3D" }));
    expect(await within(full).findByTestId("graph3d-layers-menu")).toBeInTheDocument();
  });

  it("keeps the Term relationships filter in 2D, whose term links it filters, and in 3D", async () => {
    const full = await explorer();
    expect(within(full).getByTestId("graph3d-filter-edge-type")).toHaveTextContent(
      "All term relationships",
    );
    fireEvent.click(within(full).getByRole("radio", { name: "3D" }));
    expect(await within(full).findByTestId("graph3d-filter-edge-type")).toHaveTextContent(
      "All term relationships",
    );
  });
});

describe("what each lane node says (EVERY LANE NODE INFORMS AND LINKS)", () => {
  it("gives a term's hover card its definition, status and stewards", async () => {
    await explorer();
    hover("term:customer");
    const facts = await screen.findByTestId("graph3d-lanes-tip-facts");
    expect(facts).toHaveTextContent("A person or company that has bought from us.");
    expect(facts).toHaveTextContent("Approved");
    expect(facts).toHaveTextContent("ada");
  });

  it("gives a column's card its type, its mask and a link to its table", async () => {
    await explorer();
    hover("c:customers.email");
    const facts = await screen.findByTestId("graph3d-lanes-tip-facts");
    expect(facts).toHaveTextContent("varchar");
    expect(facts).toHaveTextContent("Masked for some readers");
    const table = FACTS.nodes.find((n) => n.id === "t:customers") as (typeof FACTS.nodes)[number];
    expect(within(facts).getByRole("link", { name: "sales.customers" })).toHaveAttribute(
      "href",
      nodeHref(table),
    );
  });

  it("names a +N columns node's columns, each a link to its page", async () => {
    await explorer();
    hover("more:t:orders");
    const facts = await screen.findByTestId("graph3d-lanes-tip-facts");
    const column = DATA_READ.nodes.find(
      (n) => n.id === "c:orders.placed",
    ) as (typeof FACTS.nodes)[number];
    expect(within(facts).getByRole("link", { name: "placed" })).toHaveAttribute(
      "href",
      nodeHref(column),
    );
  });

  it("gives a table's detail panel its domain, source and quality, the names linked", async () => {
    await explorer();
    tap("t:customers");
    const facts = await screen.findByTestId("graph3d-facts");
    expect(within(facts).getByRole("link", { name: "sales" })).toHaveAttribute(
      "href",
      "/admin/domains?domain=sales",
    );
    expect(within(facts).getByRole("link", { name: "sales_pg" })).toHaveAttribute(
      "href",
      "/sources?expanded=sales_pg",
    );
    expect(facts).toHaveTextContent("Passing");
    expect(facts).toHaveTextContent("Every customer, one row each.");
  });

  it("gives a product's detail panel its status, owner and member count", async () => {
    await explorer();
    tap("p:c360");
    const facts = await screen.findByTestId("graph3d-facts");
    expect(facts).toHaveTextContent("Live");
    expect(facts).toHaveTextContent("sales_steward");
    // Its member tables: customers and orders (its checker is no table).
    expect(within(facts).getByText("2")).toBeInTheDocument();
  });
});

describe("the detail panel's names (NAMES WRAP, NEVER CUT)", () => {
  it("lists a connection at the row's whole width, breaking after _ | . with no ellipsis", async () => {
    await explorer();
    tap("p:c360");
    const detail = await screen.findByTestId("graph3d-detail");
    const row = within(detail)
      .getAllByRole("link")
      .find((a) => a.classList.contains("p3d-row") && a.textContent?.includes("sales.customers"));
    expect(row).toBeDefined();
    const link = row as HTMLElement;
    // No fixed columns beside the name: it takes the row's width.
    expect(link).toHaveClass("p3d-row-compact");
    expect(link).not.toHaveClass("p3d-row-link");
    const name = link.querySelector(".p3d-row-name") as HTMLElement;
    expect(name).toHaveClass("is-wrap");
    expect((name.querySelector(".name-breakable") as HTMLElement).innerHTML).toBe(
      "sales.<wbr>customers",
    );
  });

  it("wraps the names of what is shown the same way", async () => {
    await explorer();
    const shown = screen.getByText("Shown").closest("section") as HTMLElement;
    const names = shown.querySelectorAll(".p3d-row-name");
    expect(names.length).toBeGreaterThan(0);
    for (const n of names) {
      expect(n).toHaveClass("is-wrap");
      expect(n.querySelector(".name-breakable")).not.toBeNull();
    }
  });
});

describe("the hover card and the stage (what the frame does to it)", () => {
  it("draws the card outside the stage, so the frame clips nothing and the legend stays behind it", async () => {
    await explorer();
    hover("term:customer");
    const tip = await screen.findByTestId("graph3d-lanes-tip");
    const stage = screen.getByTestId("graph3d-lanes");
    expect(stage.contains(tip)).toBe(false);
    expect(tip.closest(".mantine-Tooltip-tooltip")).not.toBeNull();
  });
});

describe("focusing a term (the rest of its graph with it)", () => {
  const faded = (id: string) => last().classes.get(id)?.has("faded") ?? false;

  it("lights the term's columns, tables and products on a click and fades the rest", async () => {
    await explorer();
    tap("term:amount");
    await waitFor(() => expect(faded("t:customers")).toBe(true));
    expect(faded("term:amount")).toBe(false);
    expect(faded("c:orders.total")).toBe(false);
    expect(faded("t:orders")).toBe(false);
    const lit = [...last().classes].filter(([, c]) => c.has("lit")).map(([id]) => id);
    expect(lit.length).toBeGreaterThanOrEqual(2);
  });

  it("does the same on hover, and gives the focus back when the pointer leaves", async () => {
    await explorer();
    hover("term:amount");
    await waitFor(() => expect(faded("t:customers")).toBe(true));
    expect(faded("t:orders")).toBe(false);
  });
});

describe("a term's graph in 2D, its hops raised", () => {
  // J1206: Pet's graph drew the same lanes at 1 and 3 hops -- nothing is read past 1 hop of Pet.
  // The depth control does redraw the lane diagram: Medication's graph is a chain, Visit at 1 hop,
  // Visit's other part at 2.
  const CHAIN = {
    nodes: [
      { id: "term:1", type: "term" as const, label: "Medication" },
      { id: "term:2", type: "term" as const, label: "Visit" },
      { id: "term:3", type: "term" as const, label: "Diagnosis" },
    ],
    edges: [
      { id: "r1", a: "term:1", b: "term:2", kind: "term_edge" as const, label: "PART_OF" },
      { id: "r2", a: "term:3", b: "term:2", kind: "term_edge" as const, label: "PART_OF" },
    ],
  };
  // The server reads a term's neighbourhood to the depth asked: each hop a step along the chain.
  const readTo = (s: { depth: number }) => {
    const keep = new Set(CHAIN.nodes.slice(0, Math.min(3, s.depth + 1)).map((n) => n.id));
    return {
      nodes: CHAIN.nodes.filter((n) => keep.has(n.id)),
      edges: CHAIN.edges.filter((e) => keep.has(e.a) && keep.has(e.b)),
    };
  };

  it("redraws the lanes with what the hops reach", async () => {
    LANES_DATA.data = readTo;
    try {
      render(<OntologySnippet preset={termPreset(1)} name="Medication" testId="term-graph" />);
      act(() => observed.fire.forEach((f) => f()));
      await screen.findByTestId("term-graph-lanes");
      await waitFor(() => expect(ids("node")).toEqual(["term:1", "term:2"]));
      fireEvent.click(
        within(screen.getByTestId("term-graph-depth")).getByRole("radio", { name: "2" }),
      );
      await waitFor(() => expect(ids("node")).toEqual(["term:1", "term:2", "term:3"]));
      expect(laneOf("term:3")).toBe("term");
    } finally {
      LANES_DATA.data = DATA_READ;
    }
  });
});

describe("a term's graph in 2D, its focus selected (associations lit)", () => {
  // J1230: test3's graph lit only its ranking path; a term reached by RELATED_TO, and that term's
  // column, table and product, were drawn faded.
  const readTo = (s: { depth: number }) => {
    const byId = new Map(ASSOCIATION_CHAIN.nodes.map((n) => [n.id, n]));
    const keep = reach([TEST3], ASSOCIATION_CHAIN.edges, byId, { ...NEIGHBOURHOOD, depth: s.depth });
    return {
      nodes: ASSOCIATION_CHAIN.nodes.filter((n) => keep.has(n.id)),
      edges: ASSOCIATION_CHAIN.edges.filter((e) => keep.has(e.a) && keep.has(e.b)),
    };
  };
  const faded = (id: string) => last().classes.get(id)?.has("faded") ?? false;

  it("lights the associated term, its column, table and product when the focus is picked", async () => {
    LANES_DATA.data = readTo;
    try {
      render(<OntologySnippet preset={termPreset(11)} name="test3" testId="term-graph" />);
      act(() => observed.fire.forEach((f) => f()));
      await screen.findByTestId("term-graph-lanes");
      await waitFor(() => expect(ids("node")).toContain(KIN));
      tap(TEST3);
      await waitFor(() => expect(faded(TEST3)).toBe(false));
      // Something is faded once the pick is painted: the lines not followed.
      await waitFor(() =>
        expect([...last().classes].some(([, c]) => c.has("faded") || c.has("lit"))).toBe(true),
      );
      for (const id of [KIN, "c:203:kin", "t:203", "p:kin", TEST2])
        expect(faded(id), `${id} faded`).toBe(false);
    } finally {
      LANES_DATA.data = DATA_READ;
    }
  });
});
