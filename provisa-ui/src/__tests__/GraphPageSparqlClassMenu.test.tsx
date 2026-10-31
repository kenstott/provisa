// Copyright (c) 2026 Kenneth Stott
// Canary: d3f60a18-7c25-4e91-a8b4-1f6e92c7d054
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A class pill's right click on the SPARQL tab is the menu a label pill's is on Cypher's: colour,
// size, what a node is captioned by and what it is sized by, each kept by the class's drawn label
// under the tab's own settings. The panel opens wider than Cypher's, its sections closed.

import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter } from "react-router-dom";
import "../i18n";
import type { GNode } from "../components/graph/graph-model";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "analyst" }, selectedRoles: [{ id: "analyst" }], loading: false }),
}));
vi.mock("../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({ checkedDomains: new Set<string>() }),
}));
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useRelationships: () => ({ relationships: [], refetch: vi.fn() }),
  useAllRelationships: () => ({ relationships: [], refetch: vi.fn() }),
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useTables: () => ({ tables: [], loading: false }),
}));
const seen = vi.hoisted(() => ({
  colors: {} as Record<string, string>,
  sizes: {} as Record<string, number>,
  captions: {} as Record<string, string>,
  sizedBy: {} as Record<string, string>,
  multipliers: {} as Record<string, number>,
  onEffectiveDataChange: undefined as
    | undefined
    | ((frameId: string, nodes: Map<string, GNode>, edges: Map<string, never>) => void),
}));
vi.mock("../components/graph/GraphFrame", () => ({
  GraphFrame: (p: {
    colorOverrides: Record<string, string>;
    sizeOverrides: Record<string, number>;
    labelProperty: Record<string, string>;
    sizeByProperty: Record<string, string>;
    sizeMultiplier: Record<string, number>;
  }) => {
    seen.colors = p.colorOverrides;
    seen.sizes = p.sizeOverrides;
    seen.captions = p.labelProperty;
    seen.sizedBy = p.sizeByProperty;
    seen.multipliers = p.sizeMultiplier;
    return null;
  },
}));

import { GraphPage } from "../pages/GraphPage";
import { DEFAULT_QUERY, graphStateOf } from "../components/graph/graph-persistence";
import { getStored, setStored } from "../lib/browserStorage";

const BASE = "https://provisa.example";
const XSD = "http://www.w3.org/2001/XMLSchema#";
const PETS = `${BASE}/vocab/pet-store/pets`;
const VOCABULARY = {
  base: BASE,
  prefixes: {
    rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    term: `${BASE}/vocab/term/`,
    rel: `${BASE}/vocab/rel/`,
    "pet-store": `${BASE}/vocab/pet-store/`,
  },
  classes: [{ iri: PETS, kind: "table", labels: ["pets"], superclasses: [] }],
  predicates: [
    {
      iri: `${PETS}#breedName`,
      kind: "column",
      label: "pets",
      property: "breedName",
      datatype: `${XSD}string`,
    },
    {
      iri: `${PETS}#price`,
      kind: "column",
      label: "pets",
      property: "price",
      datatype: `${XSD}decimal`,
    },
  ],
  edges: [],
  concepts: [],
};
const LABEL = "pet-store:pets";

function page(language: "sparql" | "cypher" = "sparql") {
  return render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/sparql"]}>
        <GraphPage language={language} key={language} />
      </MemoryRouter>
    </MantineProvider>,
  );
}
const stored = (name: string) => JSON.parse(getStored(name) ?? "null");
const openMenu = async () => {
  fireEvent.contextMenu(await screen.findByTestId(`sparql-class-${LABEL}`), {
    clientX: 40,
    clientY: 80,
  });
  return (await screen.findByText(LABEL, { selector: ".node-ctx-title" })).closest(
    ".node-ctx-menu",
  ) as HTMLElement;
};
/** A Select's own options: each of the menu's two lists is in the page while it is closed. */
const optionsOf = async (name: string) => {
  const input = screen.getByRole("textbox", { name });
  fireEvent.click(input);
  await waitFor(() => expect(input).toHaveAttribute("aria-controls"));
  return within(document.getElementById(input.getAttribute("aria-controls") as string) as HTMLElement);
};
const choose = async (name: string, option: string) => {
  fireEvent.click((await optionsOf(name)).getByRole("option", { name: option, hidden: true }));
};

describe("the SPARQL class menu", () => {
  beforeEach(() => {
    localStorage.clear();
    Object.assign(graphStateOf("sparql"), {
      frames: [
        { id: "f", query: "x", status: "done", nodes: new Map(), edges: new Map(), rows: [], columns: [] },
      ],
      history: [],
      currentQuery: DEFAULT_QUERY.sparql,
    });
    Object.assign(graphStateOf("cypher"), {
      frames: [],
      history: [],
      currentQuery: DEFAULT_QUERY.cypher,
    });
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) =>
        String(url).endsWith("/data/sparql/vocabulary")
          ? new Response(JSON.stringify(VOCABULARY), {
              headers: { "content-type": "application/json" },
            })
          : new Response("{}", { headers: { "content-type": "application/json" } }),
      ),
    );
  });

  it("opens on a class pill's right click", async () => {
    page();
    expect(await openMenu()).toBeInTheDocument();
  });

  it("sets a class's colour by the label its nodes are drawn with", async () => {
    page();
    const menu = await openMenu();
    const [swatch] = menu.querySelectorAll<HTMLElement>(".node-ctx-swatch");
    fireEvent.click(swatch);
    const color = stored("provisa.sparql.colorOverrides")?.[LABEL];
    expect(color).toBeTruthy();
    await waitFor(() => expect(seen.colors[LABEL]).toBe(color));
    expect(getStored("provisa.graph.colorOverrides")).toBeNull();
    // The pill shows the colour its nodes now have.
    expect(screen.getByTestId(`sparql-class-${LABEL}`).style.background).not.toBe("");
  });

  it("sets a class's size", async () => {
    page();
    const menu = await openMenu();
    const slider = menu.querySelector('[role="slider"]') as HTMLElement;
    act(() => slider.focus());
    fireEvent.keyDown(slider, { key: "ArrowRight" });
    await waitFor(() => expect(seen.sizes[LABEL]).toBe(45));
    expect(stored("provisa.sparql.sizeOverrides")).toEqual({ [LABEL]: 45 });
    expect(getStored("provisa.graph.sizeOverrides")).toBeNull();
  });

  it("captions a class by a property, named plainly and kept as the node holds it", async () => {
    page();
    await openMenu();
    await choose("Label by", "breedName");
    await waitFor(() => expect(seen.captions[LABEL]).toBe(`${LABEL}#breedName`));
    expect(stored("provisa.sparql.labelProperty")).toEqual({ [LABEL]: `${LABEL}#breedName` });
    expect(getStored("provisa.graph.labelProperty")).toBeNull();
  });

  it("sizes a class by a numeric property, with its multiplier", async () => {
    page();
    await openMenu();
    await choose("Size by", "price");
    await waitFor(() => expect(seen.sizedBy[LABEL]).toBe(`${LABEL}#price`));
    expect(stored("provisa.sparql.sizeByProperty")).toEqual({ [LABEL]: `${LABEL}#price` });
    expect(getStored("provisa.graph.sizeByProperty")).toBeNull();
    await openMenu();
    fireEvent.change(screen.getByRole("textbox", { name: "Size multiplier" }), {
      target: { value: "4" },
    });
    await waitFor(() => expect(seen.multipliers[LABEL]).toBe(4));
  });

  it("offers a node's degrees to size by, and a string property no size", async () => {
    page();
    await openMenu();
    const sizes = await optionsOf("Size by");
    expect(sizes.getByRole("option", { name: "degTotal", hidden: true })).toBeInTheDocument();
    expect(sizes.queryByRole("option", { name: "breedName", hidden: true })).toBeNull();
  });

  it("remembers the choices on the tab, and Cypher's stay its own", async () => {
    const first = page();
    await openMenu();
    await choose("Label by", "breedName");
    await waitFor(() => expect(seen.captions[LABEL]).toBe(`${LABEL}#breedName`));
    first.unmount();
    seen.captions = {};
    const second = page();
    await screen.findByTestId(`sparql-class-${LABEL}`);
    expect(seen.captions[LABEL]).toBe(`${LABEL}#breedName`);
    second.unmount();
    setStored("provisa.graph.labelProperty", JSON.stringify({ Pets: "breed" }));
    seen.captions = {};
    page();
    await screen.findByTestId(`sparql-class-${LABEL}`);
    expect(stored("provisa.sparql.labelProperty")).toEqual({ [LABEL]: `${LABEL}#breedName` });
  });
});

describe("the SPARQL panel's first look", () => {
  beforeEach(() => {
    localStorage.clear();
    Object.assign(graphStateOf("sparql"), { frames: [], history: [], currentQuery: "" });
    Object.assign(graphStateOf("cypher"), { frames: [], history: [], currentQuery: "" });
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) =>
        String(url).endsWith("/data/sparql/vocabulary")
          ? new Response(JSON.stringify(VOCABULARY), {
              headers: { "content-type": "application/json" },
            })
          : new Response("{}", { headers: { "content-type": "application/json" } }),
      ),
    );
  });
  const sidebarWidth = (container: HTMLElement) =>
    (container.querySelector(".graph-sidebar") as HTMLElement).style.width;

  it("opens wider than Cypher's", async () => {
    const { container } = page();
    await screen.findByTestId("sparql-vocabulary");
    expect(parseInt(sidebarWidth(container), 10)).toBeGreaterThan(240);
  });

  it("leaves Cypher's width as it was", () => {
    const { container } = page("cypher");
    expect(sidebarWidth(container)).toBe("240px");
  });

  it("keeps a saved width", async () => {
    setStored("provisa.sparql.sidebarWidth", "200");
    const { container } = page();
    await screen.findByTestId("sparql-vocabulary");
    expect(sidebarWidth(container)).toBe("200px");
  });

  it("opens with every section closed", async () => {
    page();
    await screen.findByTestId("sparql-vocabulary");
    for (const section of ["domains", "classes", "properties", "relationships", "concepts", "prefixes"])
      expect(screen.getByTestId(`sparql-vocab-${section}-toggle`)).toHaveAttribute(
        "aria-expanded",
        "false",
      );
    expect(screen.getByTestId("sparql-class-prefix-pet-store")).toHaveAttribute(
      "aria-expanded",
      "false",
    );
  });

  it("keeps a section the viewer left open", async () => {
    setStored(
      "provisa.collapsibles",
      JSON.stringify({ "sparql.vocabulary": { classes: true } }),
    );
    page();
    await screen.findByTestId("sparql-vocabulary");
    expect(screen.getByTestId("sparql-vocab-classes-toggle")).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    expect(screen.getByTestId("sparql-vocab-domains-toggle")).toHaveAttribute(
      "aria-expanded",
      "false",
    );
  });
});
