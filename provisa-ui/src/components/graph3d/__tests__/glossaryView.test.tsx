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
        "dispose",
        "resetView",
        "setHover",
        "setData",
        "setSelected",
        "focus",
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
          ],
        }
      : {
          glossary_terms: [
            term(1, "Customer"),
            term(2, "Party", { is_abstract: true }),
            term(3, "Old customer", { deprecated: true, live: false }),
            term(4, "Ancient", { retired: true, live: false }),
          ],
        },
}));

import { GlossaryOntologyView, OntologyToggle } from "../GlossaryOntologyView";

const drawn = () => made.scenes[made.scenes.length - 1].setData.mock.calls.at(-1)?.[0] as GraphData;
const ids = (g: GraphData) => g.nodes.map((n) => n.id).sort();

function Page() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <OntologyToggle open={open} onChange={setOpen} />
      {open && <GlossaryOntologyView />}
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
    render(<GlossaryOntologyView />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    expect(ids(made.scenes[0].setData.mock.calls[0][0])).toEqual([
      "d:sales",
      "term:1",
      "term:2",
      "term:3",
    ]);
    expect(screen.getByTestId("ontology-status-retired")).not.toBeChecked();
    expect(screen.getByTestId("ontology-status-deprecated")).toBeChecked();
    fireEvent.click(screen.getByTestId("ontology-status-retired"));
    await waitFor(() => expect(ids(drawn())).toContain("term:4"));
  });

  it("the Taxonomy preset redraws the KIND_OF and PART_OF edges alone", async () => {
    render(<GlossaryOntologyView />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    fireEvent.click(screen.getByRole("radio", { name: "Taxonomy" }));
    await waitFor(() => expect(drawn().edges.map((e) => e.label)).toEqual(["KIND_OF"]));
  });

  it("expands to 95% of the viewport with a scene of its own", async () => {
    render(<GlossaryOntologyView />);
    await waitFor(() => expect(made.scenes).toHaveLength(1));
    fireEvent.click(screen.getByTestId("ontology-expand"));
    await waitFor(() => expect(made.scenes).toHaveLength(2));
    expect(await screen.findByTestId("graph3d-modal")).toBeInTheDocument();
  });
});
