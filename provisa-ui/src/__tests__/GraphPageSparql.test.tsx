// Copyright (c) 2026 Kenneth Stott
// Canary: 6d4a9e17-2b83-4f05-a1c6-8e3b7d2f5a90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135 phase 5: the SPARQL tab is the graph visualizer with SPARQL's vocabulary. Its sidebar is
// the role's vocabulary, a pill adds to the query in the bar, a query is POSTed to /data/sparql as
// application/sparql-query, and a CONSTRUCT's triples are drawn; its frames and history are its
// own, apart from Cypher's.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";
import "../i18n";
import type { FrameData } from "../components/graph/graph-model";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "analyst" },
    selectedRoles: [{ id: "analyst" }],
    loading: false,
  }),
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
const framesSeen: FrameData[] = [];
const languagesSeen: (string | undefined)[] = [];
const frameProps = vi.hoisted(() => ({
  expand: undefined as
    | undefined
    | ((frameId: string, iri: string, toggle: boolean) => void),
  drop: undefined as undefined | ((frameId: string, iri: string) => void),
  describe: undefined as undefined | ((frameId: string, iri: string) => void),
  prefixes: undefined as undefined | Record<string, string>,
}));
vi.mock("../components/graph/GraphFrame", () => ({
  GraphFrame: ({
    frame,
    language,
    onExpandResource,
    onDescribeResource,
    onTableDrop,
    sparqlPrefixes,
  }: {
    frame: FrameData;
    language?: string;
    onExpandResource?: (frameId: string, iri: string, toggle: boolean) => void;
    onDescribeResource?: (frameId: string, iri: string) => void;
    onTableDrop?: (frameId: string, iri: string) => void;
    sparqlPrefixes?: Record<string, string>;
  }) => {
    framesSeen.push(frame);
    languagesSeen.push(language);
    frameProps.expand = onExpandResource;
    frameProps.drop = onTableDrop;
    frameProps.describe = onDescribeResource;
    frameProps.prefixes = sparqlPrefixes;
    return null;
  },
}));
vi.mock("../components/graph/Neo4jExportModal", () => ({
  Neo4jExportModal: () => <div data-testid="neo4j-export-modal" />,
}));

import { GraphPage } from "../pages/GraphPage";
import { DEFAULT_QUERY, graphStateOf } from "../components/graph/graph-persistence";

const BASE = "https://provisa.example";
const VOCABULARY = {
  base: BASE,
  prefixes: {
    rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    term: `${BASE}/vocab/term/`,
    rel: `${BASE}/vocab/rel/`,
    sales: `${BASE}/vocab/sales/`,
  },
  // As GET /data/sparql/vocabulary answers (provisa.sparql_query.vocabulary.vocabulary_document):
  // a table's own class, or a Class term's ("term"), with the labels it types and the terms it is
  // a KIND_OF.
  classes: [
    { iri: `${BASE}/vocab/sales/orders`, kind: "table", labels: ["orders"], superclasses: [] },
    {
      iri: `${BASE}/vocab/sales/customers`,
      kind: "table",
      labels: ["customers"],
      superclasses: [],
    },
    {
      iri: `${BASE}/vocab/term/Customer`,
      kind: "term",
      labels: ["customers"],
      superclasses: [`${BASE}/vocab/term/Party`],
    },
  ],
  predicates: [
    {
      iri: `${BASE}/vocab/sales/amount`,
      kind: "column",
      label: "orders",
      property: "amount",
      datatype: "http://www.w3.org/2001/XMLSchema#decimal",
    },
    {
      // A column predicate as the server names it: its table's class IRI and '#'.
      iri: `${BASE}/vocab/sales/orders#total`,
      kind: "column",
      label: "orders",
      property: "total",
      datatype: "http://www.w3.org/2001/XMLSchema#decimal",
    },
  ],
  // An edge type's predicate between two labels.
  edges: [
    {
      iri: `${BASE}/vocab/rel/PLACED_BY`,
      type: "PLACED_BY",
      source: "orders",
      target: "customers",
    },
  ],
  concepts: [{ iri: `${BASE}/vocab/term/Revenue`, name: "Revenue", definition: "Money in" }],
};
const NT = [
  `<${BASE}/id/sales/orders/1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <${BASE}/vocab/sales/orders> .`,
  `<${BASE}/id/sales/orders/1> <${BASE}/vocab/sales/amount> "12.50" .`,
].join("\n");
const sent: { url: string; init?: RequestInit }[] = [];

const CONSTRUCT = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o } LIMIT 5";
function Run() {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/sparql", { state: { query: CONSTRUCT, autoRun: true } })}>
      run
    </button>
  );
}

function page() {
  render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/sparql"]}>
        <Run />
        <GraphPage language="sparql" />
      </MemoryRouter>
    </MantineProvider>,
  );
}

describe("the SPARQL tab", () => {
  beforeEach(() => {
    localStorage.clear();
    // The tab's state outlives a page, as it does a navigation.
    Object.assign(graphStateOf("sparql"), {
      frames: [],
      history: [],
      currentQuery: DEFAULT_QUERY.sparql,
    });
    framesSeen.length = 0;
    languagesSeen.length = 0;
    sent.length = 0;
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        sent.push({ url: String(url), init });
        if (String(url).endsWith("/data/sparql/vocabulary"))
          return new Response(JSON.stringify(VOCABULARY), {
            headers: { "content-type": "application/json" },
          });
        if (String(url).endsWith("/data/sparql"))
          return new Response(NT, { headers: { "content-type": "application/n-triples" } });
        return new Response("{}", { headers: { "content-type": "application/json" } });
      }),
    );
  });

  it("shows the role's vocabulary where Cypher's labels are", async () => {
    page();
    expect(await screen.findByTestId("sparql-vocabulary")).toBeInTheDocument();
    const read = sent.find((s) => s.url.endsWith("/data/sparql/vocabulary"));
    expect((read?.init?.headers as Record<string, string>)["X-Provisa-Role"]).toBe("analyst");
    // No Cypher schema is read.
    expect(sent.some((s) => s.url.includes("/data/graph-schema"))).toBe(false);
    expect(screen.getByTestId("sparql-class-sales:orders")).toHaveTextContent("sales:orders");
    // A Class term with the class it is a KIND_OF.
    expect(screen.getByTestId("sparql-class-term:Customer")).toHaveTextContent("⊂ term:Party");
    // An edge type, from its source label's class to its target's.
    expect(screen.getByTestId("sparql-edge-rel:PLACED_BY")).toBeInTheDocument();
    expect(screen.getByTestId("sparql-concept-Revenue")).toHaveTextContent("Revenue");
    // A column's pill is named by its local name, under its class.
    expect(screen.getByTestId("sparql-predicate-orders.total")).toHaveTextContent("total");
  });

  it("shows the refusal's own words when the vocabulary is refused", async () => {
    // The refusal body is the app's ApiError body (cypher_error): its words are in ``detail``.
    const told =
      "server.public_url is not set. Set Public address (server.public_url) in Deployment settings";
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ detail: told, code: "sparql.refused", params: {} }), {
            status: 400,
            statusText: "Bad Request",
            headers: { "content-type": "application/json" },
          }),
      ),
    );
    page();
    expect(await screen.findByText(told)).toBeInTheDocument();
    expect(screen.queryByText("Bad Request")).toBeNull();
  });

  const posted = () =>
    sent
      .filter((s) => s.url.endsWith("/data/sparql") && s.init?.method === "POST")
      .map((s) => s.init?.body as string);

  it("runs a class pill as its own query, its instances with a limit", async () => {
    page();
    fireEvent.click(await screen.findByTestId("sparql-class-sales:orders"));
    await waitFor(() => expect(posted()).toHaveLength(1));
    expect(posted()[0]).toBe(
      `PREFIX sales: <${BASE}/vocab/sales/>\nSELECT * WHERE {\n  ?s a sales:orders .\n}\nLIMIT 25`,
    );
    await waitFor(() => expect(framesSeen.some((f) => f.status === "done")).toBe(true));
    // The query in the bar is left as it was.
    const bar = document.querySelector(".gf-header-query-collapsed") as HTMLElement;
    expect(bar.textContent).toBe(DEFAULT_QUERY.sparql.replace(/\s*\n\s*/g, " "));
  });

  it("runs a property pill as its own query, the triples using it with a limit", async () => {
    page();
    fireEvent.click(await screen.findByTestId("sparql-predicate-orders.amount"));
    await waitFor(() => expect(posted()).toHaveLength(1));
    expect(posted()[0]).toBe(
      `PREFIX sales: <${BASE}/vocab/sales/>\nSELECT * WHERE {\n  ?s sales:amount ?o .\n}\nLIMIT 25`,
    );
  });

  it("adds a domain pill's PREFIX line to the query in the bar without running it", async () => {
    page();
    fireEvent.click(await screen.findByTestId("sparql-domain-sales"));
    const bar = document.querySelector(".gf-header-query-collapsed") as HTMLElement;
    await waitFor(() =>
      expect(bar.textContent).toBe(
        `PREFIX sales: <${BASE}/vocab/sales/> ${DEFAULT_QUERY.sparql.replace(/\s*\n\s*/g, " ")}`,
      ),
    );
    expect(posted()).toHaveLength(0);
  });

  it("POSTs a query to /data/sparql and draws a CONSTRUCT's triples", async () => {
    page();
    await screen.findByTestId("sparql-vocabulary");
    act(() => screen.getByText("run").click());
    await waitFor(() => expect(framesSeen.some((f) => f.status === "done")).toBe(true));
    const post = sent.find((s) => s.url.endsWith("/data/sparql") && s.init?.method === "POST");
    expect(post?.init?.body).toBe(CONSTRUCT);
    const headers = post?.init?.headers as Record<string, string>;
    expect(headers["Content-Type"]).toBe("application/sparql-query");
    expect(headers.Accept).toContain("application/n-triples");
    const frame = framesSeen.filter((f) => f.status === "done").at(-1)!;
    const labels = [...frame.nodes.values()].map((n) => n.label).sort();
    // The order typed by its class, its amount a property of it (a literal node on the toggle).
    expect(labels).toEqual(["sales", "sales:orders"]);
    expect(frame.edges.size).toBe(1);
    // Each frame is drawn as a SPARQL query's frame.
    expect(new Set(languagesSeen)).toEqual(new Set(["sparql"]));
    // Kept apart from Cypher's: the SPARQL history holds the query, Cypher's does not.
    expect(localStorage.getItem("test-cp:test-org:provisa.sparql.state")).toContain("CONSTRUCT");
    expect(localStorage.getItem("test-cp:test-org:provisa.graph.state")).toBeNull();
  });

  const lastFrame = () => framesSeen.at(-1)!;
  const runConstruct = async () => {
    page();
    await screen.findByTestId("sparql-vocabulary");
    act(() => screen.getByText("run").click());
    await waitFor(() => expect(lastFrame()?.status).toBe("done"));
  };
  const ORDER = `${BASE}/id/sales/orders/1`;

  it("runs a relationship and a concept pill as a new frame too", async () => {
    page();
    fireEvent.click(await screen.findByTestId("sparql-edge-rel:PLACED_BY"));
    await waitFor(() => expect(posted()).toHaveLength(1));
    expect(posted()[0]).toContain("?s rel:PLACED_BY ?o");
    fireEvent.click(screen.getByTestId("sparql-concept-Revenue"));
    await waitFor(() => expect(posted()).toHaveLength(2));
    expect(posted()[1]).toContain("term:Revenue ?p ?o");
  });

  it("gives each query its own frame, in its history", async () => {
    page();
    fireEvent.click(await screen.findByTestId("sparql-class-sales:orders"));
    fireEvent.click(screen.getByTestId("sparql-class-sales:customers"));
    await waitFor(() => expect(posted()).toHaveLength(2));
    const held = localStorage.getItem("test-cp:test-org:provisa.sparql.state")!;
    const { frames, history } = JSON.parse(held);
    expect(frames).toHaveLength(2);
    expect(history).toHaveLength(2);
  });

  it("shows the endpoint's URL for other tools", async () => {
    page();
    expect(await screen.findByTestId("sparql-endpoint-url")).toHaveTextContent(
      `${BASE}/data/sparql`,
    );
  });

  it("shows the prefixes, and takes an edit into what a pill writes and shows", async () => {
    page();
    fireEvent.click(await screen.findByTestId("sparql-prefixes-toggle"));
    const ns = screen.getByTestId("sparql-prefix-namespace-sales");
    expect(ns).toHaveValue(`${BASE}/vocab/sales/`);
    const name = screen.getByTestId("sparql-prefix-name-sales");
    fireEvent.change(name, { target: { value: "shop" } });
    fireEvent.blur(name);
    fireEvent.click(await screen.findByTestId("sparql-class-shop:orders"));
    await waitFor(() => expect(posted()).toHaveLength(1));
    expect(posted()[0]).toContain(`PREFIX shop: <${BASE}/vocab/sales/>`);
    expect(posted()[0]).toContain("?s a shop:orders");
    await waitFor(() => expect(frameProps.prefixes?.shop).toBe(`${BASE}/vocab/sales/`));
    expect(frameProps.prefixes).not.toHaveProperty("sales");
  });

  it("refuses a prefix that could not be declared", async () => {
    page();
    fireEvent.click(await screen.findByTestId("sparql-prefixes-toggle"));
    const name = screen.getByTestId("sparql-prefix-name-sales");
    fireEvent.change(name, { target: { value: "not valid" } });
    fireEvent.blur(name);
    expect(await screen.findByText("Letters, digits, - and _ only")).toBeInTheDocument();
    expect(screen.getByTestId("sparql-class-sales:orders")).toBeInTheDocument();
  });

  it("adds a prefix, and restores the vocabulary's", async () => {
    page();
    fireEvent.click(await screen.findByTestId("sparql-prefixes-toggle"));
    fireEvent.change(screen.getByTestId("sparql-prefix-name-"), { target: { value: "ex" } });
    const ns = screen.getByTestId("sparql-prefix-namespace-");
    fireEvent.change(ns, { target: { value: "http://x/" } });
    fireEvent.blur(ns);
    expect(await screen.findByTestId("sparql-prefix-row-ex")).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("sparql-prefixes-restore"));
    await waitFor(() => expect(screen.queryByTestId("sparql-prefix-row-ex")).toBeNull());
  });

  it("runs the query exactly as written, whatever the prefixes", async () => {
    await runConstruct();
    expect(posted().at(-1)).toBe(CONSTRUCT);
  });

  it("merges a DESCRIBE of an expanded node into its frame, and takes it back", async () => {
    await runConstruct();
    const before = posted().length;
    const { id } = lastFrame();
    act(() => frameProps.expand?.(id, ORDER, true));
    await waitFor(() => expect(lastFrame().sparqlExpansions).toBeDefined());
    expect(posted().slice(before)).toEqual([`DESCRIBE <${ORDER}>`]);
    const post = sent.at(-1)!;
    expect((post.init?.headers as Record<string, string>).Accept).toBe("application/n-triples");
    expect((post.init?.headers as Record<string, string>)["X-Provisa-Role"]).toBe("analyst");
    expect(lastFrame().sparqlExpansions?.[ORDER]).toHaveLength(2);
    // The frame kept its own answer.
    expect(lastFrame().query).toBe(CONSTRUCT);
    act(() => frameProps.expand?.(id, ORDER, true));
    await waitFor(() => expect(lastFrame().sparqlExpansions).toEqual({}));
    expect(posted().slice(before)).toHaveLength(1);
  });

  it("merges once for a dragged node, and a drag never takes it back", async () => {
    await runConstruct();
    const before = posted().length;
    const { id } = lastFrame();
    act(() => frameProps.expand?.(id, ORDER, false));
    await waitFor(() => expect(lastFrame().sparqlExpansions?.[ORDER]).toBeDefined());
    act(() => frameProps.expand?.(id, ORDER, false));
    expect(posted().slice(before)).toHaveLength(1);
    expect(lastFrame().sparqlExpansions?.[ORDER]).toBeDefined();
  });

  it("describes a selected node once, keeping the drawing as it is", async () => {
    await runConstruct();
    const before = posted().length;
    const { id } = lastFrame();
    const drawn = lastFrame();
    act(() => frameProps.describe?.(id, ORDER));
    act(() => frameProps.describe?.(id, ORDER));
    await waitFor(() => expect(lastFrame().sparqlDescribed?.[ORDER]).toBeDefined());
    expect(posted().slice(before)).toEqual([`DESCRIBE <${ORDER}>`]);
    const post = sent.at(-1)!;
    expect((post.init?.headers as Record<string, string>)["X-Provisa-Role"]).toBe("analyst");
    expect(lastFrame().sparqlDescribed?.[ORDER]).toHaveProperty("literals");
    // Selection fills the panel only: nothing merged, nothing redrawn.
    expect(lastFrame().sparqlExpansions).toBeUndefined();
    expect(lastFrame().sparqlMerge).toBeUndefined();
    expect(lastFrame().nodes).toBe(drawn.nodes);
    expect(lastFrame().edges).toBe(drawn.edges);
    act(() => frameProps.describe?.(id, ORDER));
    expect(posted().slice(before)).toHaveLength(1);
  });

  it("keeps a refused DESCRIBE's error for the panel, with no merge notice", async () => {
    await runConstruct();
    const { id } = lastFrame();
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ detail: "Not for you", code: "sparql.refused" }), {
            status: 403,
          }),
      ),
    );
    act(() => frameProps.describe?.(id, ORDER));
    await waitFor(() =>
      expect(lastFrame().sparqlDescribed?.[ORDER]).toEqual({ error: "Not for you" }),
    );
    expect(lastFrame().sparqlMerge).toBeUndefined();
  });

  it("merges a CONSTRUCT around a class dropped on a frame", async () => {
    await runConstruct();
    const { id } = lastFrame();
    const orders = `${BASE}/vocab/sales/orders`;
    act(() => frameProps.drop?.(id, orders));
    await waitFor(() => expect(lastFrame().sparqlExpansions?.[orders]).toBeDefined());
    expect(posted().at(-1)).toBe(
      `CONSTRUCT { ?s ?p ?o } WHERE { ?s a <${orders}> ; ?p ?o } LIMIT 25`,
    );
  });

  it("says a refused expansion in its own words, keeping the frame", async () => {
    await runConstruct();
    const { id } = lastFrame();
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ detail: "Not for you", code: "sparql.refused" }), {
            status: 403,
          }),
      ),
    );
    act(() => frameProps.expand?.(id, ORDER, true));
    await waitFor(() => expect(lastFrame().sparqlMerge?.error).toBe("Not for you"));
    expect(lastFrame().status).toBe("done");
  });

  it("says when a resource has nothing more to say", async () => {
    await runConstruct();
    const { id } = lastFrame();
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () => new Response("", { headers: { "content-type": "application/n-triples" } }),
      ),
    );
    act(() => frameProps.expand?.(id, ORDER, true));
    await waitFor(() => expect(lastFrame().sparqlMerge?.nothingMore).toBe(ORDER));
    expect(lastFrame().sparqlExpansions).toBeUndefined();
  });

  it("forgets what was merged when the frame's query runs again", async () => {
    await runConstruct();
    const { id } = lastFrame();
    act(() => frameProps.expand?.(id, ORDER, true));
    await waitFor(() => expect(lastFrame().sparqlExpansions).toBeDefined());
    act(() => screen.getByText("run").click());
    await waitFor(() => expect(framesSeen.filter((f) => f.status === "done").length).toBeGreaterThan(1));
    const fresh = framesSeen.filter((f) => f.id !== id).at(-1)!;
    expect(fresh.sparqlExpansions).toBeUndefined();
  });

  it("exports its results in SPARQL's own formats, never to Neo4j", async () => {
    page();
    await screen.findByTestId("sparql-vocabulary");
    act(() => screen.getByText("run").click());
    await waitFor(() => expect(framesSeen.some((f) => f.status === "done")).toBe(true));
    const tab = screen.getByTestId("graph-sidebar-tab-export");
    expect(tab).toHaveAttribute("aria-label", "Export RDF or SPARQL results");
    fireEvent.click(tab);
    const formats = await screen.findByTestId("sparql-export-format");
    // The CONSTRUCT's graph: RDF, Turtle first.
    expect(formats).toHaveTextContent("Turtle");
    expect(screen.queryByTestId("neo4j-export-modal")).toBeNull();
  });
});
