// Copyright (c) 2026 Kenneth Stott
// Canary: c5e81a3d-2f7b-4d96-8a10-6b4e9f27d3c5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174, REQ-2244: a table's columns button in a term's graph does what it says. A term's graph
// reads only the columns its terms reach, so a picked table is offered "Expand columns" only when
// its read counts more columns than are drawn; expanding reads that table's columns alone and
// draws them, as the lane diagram draws a table's unbound columns, and "Collapse columns" takes
// them away. Answered by a server holding a small term world, against the meta model's real
// schema (tests/fixtures/graphql_golden/meta_schema.json); jsdom draws no canvas, so cytoscape is
// a recorder (as laneView.test's) and three.js a stub.

import { act } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import {
  ONTOLOGY_SCOPE,
  NO_FILTERS,
  visibleGraph,
  type LayerId,
  type QueryScope,
} from "../graphModel";
import { laneDiagram } from "../laneDiagram";
import { tableColumnsQuery, withTableColumns, type TableColumnsRow } from "../tableColumns";
import { loadOntology } from "../useOntology";
import { snippetLayers, termGraph, termPreset } from "../ontologyPresets";
import { parse, reader, server, world, type World } from "./termServer";

type Handler = { event: string; selector: string; fn: (evt: unknown) => void };
type Element = { group: string; data: Record<string, unknown> };
const made = vi.hoisted(() => ({
  cys: [] as { elements: Element[]; handlers: Handler[]; classes: Map<string, Set<string>> }[],
}));
vi.mock("cytoscape", () => {
  const cytoscape = (options: unknown) => {
    // cytoscape("layout", name): an extension is registered.
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
// Every read goes to the term world's server, as the meta model's schema.
const wire = vi.hoisted(() => ({
  read: null as null | ((q: string) => Promise<unknown>),
  reader: null as null | (() => unknown),
}));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => wire.reader!(),
  dataGraphql: async (q: string) => wire.read!(q),
}));

import { OntologySnippet } from "../OntologyGraph";

// Term 7 (Breed) IS shelter.animal_breed's breed column, one of its five; term 8 (Owner name) IS
// shelter.owner's only column. Breed is a kind of Owner name, so the term graph of 7 reaches both.
const WORLD: World = world({
  products: [{ id: "p1", name: "shelter" }],
  tables: [
    {
      id: 101,
      name: "animal_breed",
      product: "p1",
      columns: ["breed", "origin", "size", "coat", "lifespan"],
    },
    { id: 102, name: "owner", product: "p1", columns: ["name"] },
  ],
  terms: [
    { id: 7, name: "Breed", columns: [[101, "breed"]] },
    { id: 8, name: "Owner name", columns: [[102, "name"]] },
  ],
  links: [[7, 8, "KIND_OF"]],
});

const sent: string[] = [];
beforeEach(() => {
  made.cys.length = 0;
  observed.fire.length = 0;
  sent.length = 0;
  const s = server(WORLD);
  wire.reader = reader;
  wire.read = (q) => {
    sent.push(q);
    return s.read(q);
  };
});

/** The term graph's scope, as it opens: its layers, no filter. */
const termScope = (): QueryScope => {
  const p = termGraph(termPreset(7));
  return {
    layers: new Set<LayerId>(snippetLayers(p)),
    domains: [],
    edgeTypes: [],
    hiddenStatuses: [],
    orphans: true,
    tags: [],
    sensitivities: [],
    search: [],
    focus: p.focus,
    depth: p.depth,
    products: p.products,
  };
};

describe("a term's graph counts each table's columns, and reads the rest of one on asking", () => {
  it("reads how many columns each table has, beside the columns its terms reach", async () => {
    const read = await loadOntology("analyst", termScope());
    if (read === null || read.over !== undefined) throw new Error("the term graph was not read");
    const node = (id: string) => read.data.nodes.find((n) => n.id === id);
    expect(node("t:101")?.columns).toBe(5);
    expect(node("t:102")?.columns).toBe(1);
    // Only the bound columns are read as columns.
    expect(read.data.nodes.filter((n) => n.parent === "t:101").map((n) => n.id)).toEqual([
      "c:101:breed",
    ]);
  });

  it("asks for that table alone, and draws its columns as the lane diagram draws a table's", async () => {
    const read = await loadOntology("analyst", termScope());
    if (read === null || read.over !== undefined) throw new Error("the term graph was not read");
    const q = tableColumnsQuery(reader(), "t:101") as string;
    const [root] = parse(q);
    expect(root.name).toBe("m__registeredTables");
    expect(root.where).toEqual({ id: { eq: 101 } });
    const rows = (await server(WORLD).read(q)) as unknown as {
      registered_tables: TableColumnsRow[];
    };
    expect(rows.registered_tables.map((t) => t.id)).toEqual([101]);
    const data = withTableColumns(read.data, rows.registered_tables);
    const shown = visibleGraph(
      data,
      new Set(snippetLayers(termPreset(7))),
      NO_FILTERS,
      new Set(),
      ONTOLOGY_SCOPE,
    );
    const columnsOf = (expanded: Set<string>) =>
      laneDiagram(shown, data, expanded)
        .graph.nodes.filter((n) => n.parent === "t:101")
        .map((n) => n.label)
        .sort();
    expect(columnsOf(new Set(["t:101"]))).toEqual([
      "animal_breed.breed",
      "animal_breed.coat",
      "animal_breed.lifespan",
      "animal_breed.origin",
      "animal_breed.size",
    ]);
    // Collapsed, the bound column alone, the rest one node.
    expect(columnsOf(new Set())).toEqual(["animal_breed.breed"]);
  });
});

const last = () => made.cys[made.cys.length - 1];
const laneOf = (id: string) => last().elements.find((e) => e.data.id === id)?.data.lane;
const tap = (id: string) => {
  const el = last().elements.find((e) => e.data.id === id) as Element;
  const h = last().handlers.find(
    (x) => x.event === "tap" && x.selector === `node[kind = "${el.data.kind as string}"]`,
  ) as Handler;
  act(() => h.fn({ target: { id: () => id, data: (k: string) => el.data[k] } }));
};

/** The term's graph, enlarged, in 2D. */
async function enlarged() {
  render(<OntologySnippet preset={termPreset(7)} name="Breed" testId="term-graph" />);
  act(() => observed.fire.forEach((f) => f()));
  fireEvent.click(await screen.findByTestId("term-graph-expand"));
  const full = await screen.findByTestId("graph3d");
  fireEvent.click(within(full).getByRole("radio", { name: "2D" }));
  await screen.findByTestId("graph3d-lanes");
  await waitFor(() => expect(laneOf("t:101")).toBe("table"));
  return full;
}

const columnsDrawn = (table: string) =>
  last()
    .elements.filter((e) => e.group === "nodes" && e.data.lane === "column")
    .map((e) => e.data.id as string)
    .filter((id) => id.startsWith(`c:${table.slice(2)}:`))
    .sort();

describe("the picked table's columns button in a term's enlarged 2D graph", () => {
  it("offers Expand columns, reads that table's columns alone, draws them, and collapses them", async () => {
    await enlarged();
    expect(columnsDrawn("t:101")).toEqual(["c:101:breed"]);
    tap("t:101");
    const button = await screen.findByTestId("graph3d-columns");
    expect(button).toHaveTextContent("Expand columns");
    const before = sent.length;
    fireEvent.click(button);
    await waitFor(() => expect(columnsDrawn("t:101")).toHaveLength(5));
    const asked = sent.slice(before).filter((q) => q.startsWith("query TableColumns"));
    expect(asked).toHaveLength(1);
    expect(parse(asked[0])[0].where).toEqual({ id: { eq: 101 } });
    expect(screen.getByTestId("graph3d-columns")).toHaveTextContent("Collapse columns");
    fireEvent.click(screen.getByTestId("graph3d-columns"));
    await waitFor(() => expect(columnsDrawn("t:101")).toEqual(["c:101:breed"]));
    expect(screen.getByTestId("graph3d-columns")).toHaveTextContent("Expand columns");
  });

  it("offers no button for a table whose every column is drawn", async () => {
    await enlarged();
    tap("t:102");
    await screen.findByTestId("graph3d-detail");
    expect(screen.queryByTestId("graph3d-columns")).not.toBeInTheDocument();
  });
});
