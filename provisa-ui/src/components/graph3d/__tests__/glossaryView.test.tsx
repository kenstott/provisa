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
        "setFrontal",
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
vi.mock("../glossaryGraphData", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../glossaryGraphData")>()),
  termsQuery: () => "query GlossaryTerms { x }",
  termEdgesQuery: () => "query GlossaryTermEdges { x }",
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
  refs: [],
  ...over,
});
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  dataGraphql: async (query: string) =>
    query.includes("GlossaryTermEdges")
      ? {
          glossary_term_edges: [
            { id: 1, from_term_id: 1, to_term_id: 2, rel_type: "KIND_OF" },
            { id: 2, from_term_id: 3, to_term_id: 1, rel_type: "REPLACES" },
            { id: 3, from_term_id: 10, to_term_id: 9, rel_type: "RELATED_TO" },
          ],
        }
      : {
          glossary_terms: [
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
          ],
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
});

describe("the Glossary page's Ontology View", () => {
  it("reads and draws nothing until it is opened", async () => {
    render(<Page />);
    expect(reads.roles).toEqual([]);
    expect(made.scenes).toHaveLength(0);
    fireEvent.click(screen.getByTestId("glossary-ontology-toggle"));
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(reads.roles).toEqual(["analyst"]);
  });

  it("shows proposed and deprecated terms and hides retired ones, until asked", async () => {
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
    expect(screen.getByTestId("ontology-status-retired")).not.toBeChecked();
    expect(screen.getByTestId("ontology-status-deprecated")).toBeChecked();
    fireEvent.click(screen.getByTestId("ontology-status-retired"));
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

  it("hides the terms left with no edge when Orphans is unchecked, and shows them again", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(screen.getByTestId("ontology-orphans")).toBeChecked();
    expect(ids(drawn())).toContain("term:5");
    fireEvent.click(screen.getByTestId("ontology-orphans"));
    await waitFor(() => expect(ids(drawn())).not.toContain("term:5"));
    expect(ids(drawn())).toEqual(expect.arrayContaining(["term:1", "term:2", "term:3"]));
    fireEvent.click(screen.getByTestId("ontology-orphans"));
    await waitFor(() => expect(ids(drawn())).toContain("term:5"));
  });

  it("draws the columns a term IS, and takes them away with Columns", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    const isEdges = () =>
      drawn()
        .edges.filter((e) => e.kind === "term_binding")
        .map((e) => [e.a, e.label, e.b]);
    expect(screen.getByTestId("ontology-columns")).toBeChecked();
    expect(isEdges()).toEqual([["term:6", "IS", "c:10:email"]]);
    expect(node(drawn(), "c:10:email")).toMatchObject({ type: "column", label: "customers.email" });
    fireEvent.click(screen.getByTestId("ontology-columns"));
    await waitFor(() => expect(isEdges()).toEqual([]));
    expect(ids(drawn())).not.toContain("c:10:email");
    fireEvent.click(screen.getByTestId("ontology-columns"));
    await waitFor(() => expect(isEdges()).toEqual([["term:6", "IS", "c:10:email"]]));
  });

  it("does not count a term joined only by IS as an orphan while Columns is on", async () => {
    render(<GlossaryOntologyView search="" />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("ontology-orphans"));
    await waitFor(() => expect(ids(drawn())).not.toContain("term:5"));
    expect(ids(drawn())).toContain("term:6");
    fireEvent.click(screen.getByTestId("ontology-columns"));
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
        zero={[]}
        settingsKey={{ kind: "test", subject: null }}
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
        zero={[]}
        settingsKey={{ kind: "test", subject: null }}
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
