// Copyright (c) 2026 Kenneth Stott
// Canary: 7a4c1e93-2d58-4f06-9b37-c8e5a2f1d649
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130: the Glossary page's Ontology View. It reads the glossary from the meta domain and draws
// it once opened, with the status filters' defaults, the Taxonomy preset and the expand button.
// jsdom has no WebGL, so the three.js scene is replaced by a recorder.

import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor } from "../../../test-utils/render";
import type { GraphData } from "../graphModel";

type Calls = Record<string, ReturnType<typeof vi.fn>>;
const made = vi.hoisted(() => ({ scenes: [] as Calls[] }));
vi.mock("../graphScene", () => ({
  GraphScene: class {
    constructor() {
      const calls: Calls = {};
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
      ])
        calls[m] = vi.fn();
      Object.assign(this, calls);
      made.scenes.push(calls);
    }
  },
}));
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));
const reads = vi.hoisted(() => ({ roles: [] as string[] }));
vi.mock("../useProductGraph", () => ({
  metaReader: async (role: string) => {
    reads.roles.push(role);
    return { schema: null, roots: new Map() };
  },
}));
// The view's one query (REQ-2136) stands for its scope here; the server below answers it as the
// query's where would (ontologyQuery.test.ts holds the query text itself).
const asked = vi.hoisted(() => ({ scopes: [] as Record<string, unknown>[] }));
vi.mock("../glossaryGraphData", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../glossaryGraphData")>()),
  ontologyQuery: (_m: unknown, s: { layers: Set<string> }) =>
    JSON.stringify({ ...s, layers: [...s.layers] }),
  // The count read before it (REQ-2143): the same scope, answered with the terms' ids.
  ontologyCountQuery: (_m: unknown, s: { layers: Set<string> }) =>
    JSON.stringify({ ...s, layers: [...s.layers], count: true }),
}));
const term = (id: number, name: string, over: object = {}) => ({
  id,
  name,
  definition: `${name}.`,
  is_abstract: false,
  deprecated: false,
  retired: false,
  live: true,
  grounded: true,
  domains: [{ domain_id: "sales" }],
  refs: [] as { domain_id: string }[],
  ...over,
});
const TERMS = [
  term(1, "Customer"),
  term(2, "Party", { is_abstract: true }),
  term(3, "Old customer", { deprecated: true, live: false }),
  term(4, "Ancient", { retired: true, live: false }),
  // No edge of any type: an orphan.
  term(5, "Lonely", { domains: [{ domain_id: "finance" }] }),
  term(7, "shift coverage"),
  term(8, "shift start"),
  term(9, "clinic"),
  term(10, "referral"),
  // No term edge, but a column it IS.
  term(6, "Email address", {
    refs: [
      {
        column_key: "10:email",
        column_name: "email",
        table_id: 10,
        table_name: "customers",
        domain_id: "sales",
        table: { id: 10, product: null },
      },
    ],
  }),
];
const EDGES = [
  { id: 1, from_term_id: 1, to_term_id: 2, rel_type: "KIND_OF" },
  { id: 2, from_term_id: 3, to_term_id: 1, rel_type: "REPLACES" },
  { id: 3, from_term_id: 10, to_term_id: 9, rel_type: "RELATED_TO" },
];
type Term = ReturnType<typeof term>;
const standing = (t: Term) =>
  t.retired ? "retired" : t.deprecated ? "deprecated" : t.live ? "live" : "draft";
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  dataGraphql: async (query: string) => {
    const s = JSON.parse(query) as {
      layers: string[];
      domains: string[];
      edgeTypes: string[];
      hiddenStatuses: string[];
      orphans: boolean;
      count?: boolean;
    };
    if (!s.count) asked.scopes.push(s);
    const inDomains = (ds: string[]) =>
      s.domains.length === 0 || ds.some((d) => d === "*" || s.domains.includes(d));
    const kept = TERMS.filter(
      (t) =>
        !s.hiddenStatuses.includes(standing(t)) && inDomains(t.domains.map((d) => d.domain_id)),
    );
    const keptIds = new Set(kept.map((t) => t.id));
    const edges = EDGES.filter((e) => s.edgeTypes.length === 0 || s.edgeTypes.includes(e.rel_type));
    const refsOf = (t: Term) => t.refs.filter((r) => inDomains([r.domain_id]));
    // Orphans off: a term with an edge kept to a term kept, or a column it IS under Columns.
    const related = (t: Term) =>
      (s.layers.includes("ontology") &&
        edges.some(
          (e) =>
            (e.from_term_id === t.id && keptIds.has(e.to_term_id)) ||
            (e.to_term_id === t.id && keptIds.has(e.from_term_id)),
        )) ||
      (s.layers.includes("columns") && refsOf(t).length > 0);
    const terms = kept
      .filter((t) => s.orphans || related(t))
      .map((t) => ({
        ...t,
        domains: s.layers.includes("domains")
          ? t.domains.filter((d) => inDomains([d.domain_id]))
          : undefined,
        refs: s.layers.includes("columns") ? refsOf(t) : undefined,
        // Under Ontology, each term is read with the edges leaving it.
        edges_out: s.layers.includes("ontology")
          ? edges.filter((e) => e.from_term_id === t.id)
          : undefined,
      }));
    if (s.count) return { _terms: terms.map((t) => ({ id: t.id, domains: t.domains })) };
    return { _terms: terms };
  },
}));

import { GlossaryOntologyView, OntologyToggle } from "../GlossaryOntologyView";
import { GraphExplorer } from "../GraphExplorer";
import { GLOSSARY_SCOPE } from "../graphModel";

const drawn = () => made.scenes[made.scenes.length - 1].setData.mock.calls.at(-1)?.[0] as GraphData;
const ids = (g: GraphData) => g.nodes.map((n) => n.id).sort();
const node = (g: GraphData, id: string) => g.nodes.find((n) => n.id === id);

function Page() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <OntologyToggle open={open} onChange={setOpen} />
      {open && <GlossaryOntologyView search="" />}
    </>
  );
}

beforeEach(() => {
  made.scenes.length = 0;
  reads.roles.length = 0;
  asked.scopes.length = 0;
});

/** The Ontology View's Layers or Terms dropdown, opened, and the option asked for in it. */
async function option(menu: "layers" | "terms", value: string): Promise<HTMLElement> {
  const id = `ontology-${menu}-menu-option-${value}`;
  if (!screen.queryByTestId(id)) fireEvent.click(screen.getByTestId(`ontology-${menu}-menu`));
  return screen.findByTestId(id);
}
/** Ticks or unticks a choice in one of the dropdowns. */
const choose = async (menu: "layers" | "terms", value: string) =>
  fireEvent.click(await option(menu, value));
/** Whether a choice is ticked. */
const ticked = async (menu: "layers" | "terms", value: string) =>
  (await option(menu, value)).hasAttribute("data-combobox-active");

describe("the Glossary page's Ontology View", () => {
  it("reads and draws nothing until it is opened", async () => {
    render(<Page />);
    expect(reads.roles).toEqual([]);
    expect(made.scenes).toHaveLength(0);
    fireEvent.click(screen.getByTestId("glossary-ontology-toggle"));
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    // As the user's role: what the controls choose from, then what they keep.
    expect(new Set(reads.roles)).toEqual(new Set(["analyst"]));
    expect(asked.scopes.length).toBeGreaterThanOrEqual(2);
  });

  it("runs its layers, domains, edge types and statuses in its query, not after it", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    // Every layer and no filter, for the controls' choices; then the defaults: retired hidden.
    expect(asked.scopes[0]).toEqual({
      layers: ["ontology", "domains", "data", "columns", "ownerTables"],
      domains: [],
      edgeTypes: [],
      hiddenStatuses: [],
      orphans: true,
      tags: [],
      sensitivities: [],
      search: [],
    });
    await waitFor(() =>
      expect(asked.scopes.at(-1)).toEqual({
        layers: ["columns", "domains", "ontology"],
        domains: [],
        edgeTypes: [],
        hiddenStatuses: ["retired"],
        orphans: true,
        tags: [],
        sensitivities: [],
        search: [],
      }),
    );
    // The Taxonomy preset's edge types and layers are the query's.
    fireEvent.click(screen.getByRole("radio", { name: "Taxonomy" }));
    await waitFor(() =>
      expect(asked.scopes.at(-1)).toMatchObject({
        layers: ["ontology"],
        edgeTypes: ["KIND_OF", "PART_OF"],
      }),
    );
    // A domain left out is left out of the query's where.
    fireEvent.click(screen.getByRole("radio", { name: "Ontology" }));
    fireEvent.click(screen.getByTestId("ontology-filter-domain"));
    fireEvent.click(await screen.findByTestId("ontology-filter-domain-option-finance"));
    await waitFor(() => expect(asked.scopes.at(-1)).toMatchObject({ domains: ["sales"] }));
    await waitFor(() => expect(ids(drawn())).not.toContain("term:5"));
  });

  it("shows draft and deprecated terms and hides retired ones, until asked", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(ids(made.scenes[0].setData.mock.calls[0][0])).toEqual([
      "c:10:email",
      "d:finance",
      "d:sales",
      "term:1",
      "term:10",
      "term:2",
      "term:3",
      "term:5",
      "term:6",
      "term:7",
      "term:8",
      "term:9",
    ]);
    expect(screen.getByTestId("ontology-terms-menu")).toHaveTextContent("Terms: 4 of 5");
    expect(await ticked("terms", "retired")).toBe(false);
    expect(await ticked("terms", "deprecated")).toBe(true);
    await choose("terms", "retired");
    await waitFor(() => expect(ids(drawn())).toContain("term:4"));
  });

  it("first draws the whole graph: every edge type and every domain chosen", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes.length).toBeGreaterThan(0));
    await waitFor(() => expect(drawn()).toBeDefined());
    // Both of the graph's edge types (KIND_OF, REPLACES) and both of its domains (sales, finance).
    expect(screen.getByTestId("ontology-filter-edge-type")).toHaveTextContent("All edge types");
    expect(screen.getByTestId("ontology-filter-domain")).toHaveTextContent("All domains");
    expect(ids(drawn())).toEqual(expect.arrayContaining(["term:1", "term:2", "term:3", "term:5"]));
  });

  it("hides the terms left with no edge when Unconnected is unticked, and shows them again", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(await ticked("terms", "unconnected")).toBe(true);
    expect(ids(drawn())).toContain("term:5");
    await choose("terms", "unconnected");
    await waitFor(() => expect(ids(drawn())).not.toContain("term:5"));
    expect(ids(drawn())).toEqual(expect.arrayContaining(["term:1", "term:2", "term:3"]));
    await choose("terms", "unconnected");
    await waitFor(() => expect(ids(drawn())).toContain("term:5"));
  });

  it("draws the columns a term IS, and takes them away with Columns", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const isEdges = () =>
      drawn()
        .edges.filter((e) => e.kind === "term_binding")
        .map((e) => [e.a, e.label, e.b]);
    expect(screen.getByTestId("ontology-layers-menu")).toHaveTextContent(
      "Layers: Domains, Columns",
    );
    expect(isEdges()).toEqual([["term:6", "IS", "c:10:email"]]);
    expect(node(drawn(), "c:10:email")).toMatchObject({ type: "column", label: "customers.email" });
    await choose("layers", "columns");
    await waitFor(() => expect(isEdges()).toEqual([]));
    expect(ids(drawn())).not.toContain("c:10:email");
    await choose("layers", "columns");
    await waitFor(() => expect(isEdges()).toEqual([["term:6", "IS", "c:10:email"]]));
  });

  it("does not count a term joined only by IS as unconnected while Columns is on", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    await choose("terms", "unconnected");
    await waitFor(() => expect(ids(drawn())).not.toContain("term:5"));
    expect(ids(drawn())).toContain("term:6");
    await choose("layers", "columns");
    await waitFor(() => expect(ids(drawn())).not.toContain("term:6"));
  });

  it("focuses on what the page's search matches, any of its items, with their neighbourhoods", async () => {
    const { rerender } = render(<GlossaryOntologyView search="shift clinic" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    // Both shift terms and the clinic, each with what it reaches; nothing else.
    await waitFor(() =>
      expect(ids(drawn()).filter((id) => id.startsWith("term:"))).toEqual([
        "term:10",
        "term:7",
        "term:8",
        "term:9",
      ]),
    );
    expect(screen.queryByTestId("ontology-filter-focus")).toBeNull();
    // A quoted phrase is one item beside a bare word.
    rerender(<GlossaryOntologyView search={'"shift coverage" clinic'} />);
    await waitFor(() =>
      expect(ids(drawn()).filter((id) => id.startsWith("term:"))).toEqual([
        "term:10",
        "term:7",
        "term:9",
      ]),
    );
    // An empty search is the whole graph.
    rerender(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(ids(drawn())).toContain("term:1"));
  });

  it("says a picked term's definition under its title, clamped, and nothing when it has none", () => {
    const g: GraphData = {
      nodes: [
        { id: "term:1", type: "term", label: "Customer", definition: "A person who buys." },
        { id: "term:2", type: "term", label: "Draft" },
      ],
      edges: [],
    };
    const { unmount } = render(
      <GraphExplorer
        data={g}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        focus="term:1"
        settingsKey={{ kind: "test", subject: null }}
        exportName="test"
        testId="x"
      />,
    );
    expect(screen.getByTestId("x-definition-text")).toHaveTextContent("A person who buys.");
    unmount();
    render(
      <GraphExplorer
        data={g}
        scope={GLOSSARY_SCOPE}
        preset="ontology"
        focus="term:2"
        settingsKey={{ kind: "test", subject: null }}
        exportName="test"
        testId="x"
      />,
    );
    expect(screen.queryByTestId("x-definition-text")).toBeNull();
  });

  it("the Taxonomy preset redraws the KIND_OF and PART_OF edges alone", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    fireEvent.click(screen.getByRole("radio", { name: "Taxonomy" }));
    await waitFor(() => expect(drawn().edges.map((e) => e.label)).toEqual(["KIND_OF"]));
  });

  it("expands to 95% of the viewport with a scene of its own", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("ontology-expand"));
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    expect(await screen.findByTestId("graph3d-modal")).toBeInTheDocument();
  });
});
