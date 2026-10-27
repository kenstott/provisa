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
import { LITERAL_LABEL } from "../components/graph/sparql";

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
vi.mock("../components/graph/GraphFrame", () => ({
  GraphFrame: ({ frame, language }: { frame: FrameData; language?: string }) => {
    framesSeen.push(frame);
    languagesSeen.push(language);
    return null;
  },
}));
vi.mock("../components/graph/Neo4jExportModal", () => ({
  Neo4jExportModal: () => <div data-testid="neo4j-export-modal" />,
}));

import { GraphPage } from "../pages/GraphPage";
import { DEFAULT_QUERY } from "../components/graph/graph-persistence";

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
    { iri: `${BASE}/vocab/sales/customers`, kind: "table", labels: ["customers"], superclasses: [] },
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
      label: "orders.amount",
      property: "amount",
      datatype: "http://www.w3.org/2001/XMLSchema#decimal",
    },
    {
      // A column predicate as the server names it: its table's class IRI and '#'.
      iri: `${BASE}/vocab/sales/orders#total`,
      kind: "column",
      label: "orders.total",
      property: "total",
      datatype: "http://www.w3.org/2001/XMLSchema#decimal",
    },
  ],
  // An edge type's predicate between two labels.
  edges: [
    { iri: `${BASE}/vocab/rel/PLACED_BY`, type: "PLACED_BY", source: "orders", target: "customers" },
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
    // A column predicate's IRI has no prefixed name: its pill shows its column.
    expect(screen.getByTestId("sparql-predicate-orders.total")).toHaveTextContent("orders.total");
  });

  it("adds a pill's triple pattern to the query in the bar, with its PREFIX line", async () => {
    page();
    fireEvent.click(await screen.findByTestId("sparql-class-sales:orders"));
    fireEvent.click(screen.getByTestId("sparql-predicate-sales:amount"));
    const bar = document.querySelector(".gf-header-query-collapsed") as HTMLElement;
    await waitFor(() =>
      expect(bar.textContent).toBe(
        `PREFIX sales: <${BASE}/vocab/sales/> ` +
          `${DEFAULT_QUERY.sparql.replace(/\s*\n\s*/g, " ").replace(/ }/, " . ?s a sales:orders . ?s sales:amount ?o2 . }")}`,
      ),
    );
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
    // The order typed by its class, its amount a literal drawn apart.
    expect(labels).toEqual([LITERAL_LABEL, "sales", "sales:orders"]);
    expect(frame.edges.size).toBe(2);
    // Each frame is drawn as a SPARQL query's frame.
    expect(new Set(languagesSeen)).toEqual(new Set(["sparql"]));
    // Kept apart from Cypher's: the SPARQL history holds the query, Cypher's does not.
    expect(localStorage.getItem("test-cp:test-org:provisa.sparql.state")).toContain("CONSTRUCT");
    expect(localStorage.getItem("test-cp:test-org:provisa.graph.state")).toBeNull();
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
