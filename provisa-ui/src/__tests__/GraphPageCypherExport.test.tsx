// Each tab offers its own language's exports: the Cypher tab's is the Neo4j export, never the
// SPARQL tab's RDF and SPARQL results.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter } from "react-router-dom";
import "../i18n";

vi.mock("../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "all" }, selectedRoles: [{ id: "all" }], loading: false }),
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
vi.mock("../components/graph/GraphCanvas", () => ({ GraphCanvas: () => null }));
vi.mock("../components/graph/Neo4jExportModal", () => ({
  Neo4jExportModal: () => <div data-testid="neo4j-export-modal" />,
}));

import { GraphPage } from "../pages/GraphPage";
import { graphStateOf } from "../components/graph/graph-persistence";

const json = (body: unknown) =>
  new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });

describe("the Cypher tab's export", () => {
  beforeEach(() => {
    localStorage.clear();
    graphStateOf("cypher").frames = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) =>
        String(url).includes("/data/graph-schema")
          ? json({ node_labels: [], relationship_types: [] })
          : json({}),
      ),
    );
  });

  it("is the Neo4j export", async () => {
    render(
      <MantineProvider>
        <MemoryRouter initialEntries={["/graph"]}>
          <GraphPage />
        </MemoryRouter>
      </MantineProvider>,
    );
    const tab = screen.getByTestId("graph-sidebar-tab-export");
    expect(tab).toHaveAttribute("aria-label", "Export to Neo4j");
    fireEvent.click(tab);
    expect(await screen.findByTestId("neo4j-export-modal")).toBeInTheDocument();
    expect(screen.queryByTestId("sparql-export-modal")).toBeNull();
  });
});
