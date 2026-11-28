// Copyright (c) 2026 Kenneth Stott
// Canary: 0b7e4a19-6c25-4d83-9f1e-2a5c8d7b3e60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2136, REQ-2174 (amended 2026-11-26): a drawn graph always opens as the query that drew it. A
// product's expanded graph with the rest of the model shown and a node focused at All hops drew a
// graph, yet every Open as query item was disabled: the focus was drawn as a neighbourhood of the
// whole model read, which the read's query did not state. The focus is the query's own now, and All
// opens as the Cypher variable-length query from it.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../../test-utils/render";
import "../../../i18n";
import { reader, server, world, type World } from "./termServer";

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
vi.mock("../../../context/AuthContext", () => ({ useAuth: () => ({ actingRoles: "analyst" }) }));
const nav = vi.hoisted(() => ({ went: [] as { to: string; state: unknown }[] }));
vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof import("react-router-dom")>()),
  useNavigate: () => (to: string, opts?: { state?: unknown }) =>
    nav.went.push({ to, state: opts?.state }),
}));
const wire = vi.hoisted(() => ({ read: null as null | ((q: string) => Promise<unknown>) }));
vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: async () => reader(),
  dataGraphql: async (q: string) => wire.read!(q),
}));
vi.mock("../ontologyCypher", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../ontologyCypher")>()),
  termsCypherGraph: async () => ({
    label: "Ontology:Terms",
    links: ["KIND_OF", "PART_OF", "RELATED_TO"],
  }),
}));
vi.mock("../../../api/lineage", () => ({
  fetchProductLineage: async () => ({ nodes: [], edges: [] }),
}));

import { OntologyModal } from "../OntologyGraph";
import { PRODUCT_GRAPH_LAYERS, PRODUCT_MAP, productGraph, productPreset } from "../ontologyPresets";

// Shelter holds animal; species and breed are its columns' terms; breed KIND_OF species, and
// species RELATED_TO habitat, a term of the clinic product's table.
const WORLD: World = world({
  products: [
    { id: "p1", name: "shelter" },
    { id: "p2", name: "clinic" },
  ],
  tables: [
    { id: 101, name: "animal", product: "p1", columns: ["species", "breed"] },
    { id: 102, name: "visit", product: "p2", columns: ["habitat"] },
  ],
  terms: [
    { id: 15, name: "species", columns: [[101, "species"]] },
    { id: 16, name: "breed", columns: [[101, "breed"]] },
    { id: 17, name: "habitat", columns: [[102, "habitat"]] },
  ],
  links: [
    [16, 15, "KIND_OF"],
    [15, 17, "RELATED_TO"],
  ],
});

beforeEach(() => {
  nav.went.length = 0;
  window.localStorage.clear();
  wire.read = server(WORLD).read;
});

describe("a product's graph with the rest of the model and a node focused, at All hops", () => {
  it("opens as the Cypher query from the focus, its menu enabled", async () => {
    render(
      <OntologyModal
        opened
        onClose={() => {}}
        title="shelter"
        selected="p:p1"
        preset={PRODUCT_MAP}
        home={productGraph(productPreset("p1"))}
        layers={PRODUCT_GRAPH_LAYERS}
      />,
    );
    await screen.findByTestId("graph3d-filter-focus");
    // Focus breed too: the rest of the model is shown, the product and breed both focused.
    fireEvent.click(screen.getByTestId("graph3d-filter-focus"));
    fireEvent.click(await screen.findByTestId("graph3d-filter-focus-option-term:16"));
    await waitFor(() => expect(screen.getByTestId("graph3d-rest")).toBeChecked());
    fireEvent.click(within(screen.getByTestId("graph3d-depth")).getByText("All"));
    const open = screen.getByTestId("graph3d-open-as-query");
    await waitFor(() => expect(open).not.toBeDisabled());
    fireEvent.click(open);
    const cypher = await screen.findByTestId("graph3d-open-as-cypher");
    await waitFor(() => expect(cypher).not.toHaveAttribute("data-disabled"));
    fireEvent.click(cypher);
    expect(nav.went.at(-1)?.to).toBe("/graph");
    const query = (nav.went.at(-1)?.state as { query: string }).query;
    // From the focus's terms -- the product's own (species, breed) and breed -- over the term
    // links to the end of the connected part: habitat, one step from species.
    expect(query).toBe(
      "MATCH (t:Ontology:Terms)-[:KIND_OF|PART_OF|RELATED_TO*0..1]-(a:Ontology:Terms) " +
        "WHERE t.id IN [15, 16] " +
        "OPTIONAL MATCH (a)-[r:KIND_OF|PART_OF|RELATED_TO]->(b:Ontology:Terms) RETURN DISTINCT a, r, b",
    );
  });

  it("opens a fixed hop count as the GraphQL that read it, the focus in it", async () => {
    render(
      <OntologyModal
        opened
        onClose={() => {}}
        title="shelter"
        selected="p:p1"
        preset={PRODUCT_MAP}
        home={productGraph(productPreset("p1"))}
        layers={PRODUCT_GRAPH_LAYERS}
      />,
    );
    await screen.findByTestId("graph3d-filter-focus");
    fireEvent.click(screen.getByTestId("graph3d-filter-focus"));
    fireEvent.click(await screen.findByTestId("graph3d-filter-focus-option-term:16"));
    await waitFor(() => expect(screen.getByTestId("graph3d-rest")).toBeChecked());
    const open = screen.getByTestId("graph3d-open-as-query");
    await waitFor(() => expect(open).not.toBeDisabled());
    fireEvent.click(open);
    const graphql = await screen.findByTestId("graph3d-open-as-graphql");
    await waitFor(() => expect(graphql).not.toHaveAttribute("data-disabled"));
    fireEvent.click(graphql);
    const query = (nav.went.at(-1)?.state as { query: string }).query;
    expect(query).toContain("id: { eq: 16 }");
    expect(query).toContain('productId: { eq: "p1" }');
  });
});
