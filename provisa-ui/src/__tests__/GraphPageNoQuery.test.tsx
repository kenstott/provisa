// Copyright (c) 2026 Kenneth Stott
// Canary: a3f60b28-9d14-4e7c-b5a2-61c8d07e4f93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A Cypher or SPARQL editor run with no statement in it (blank, or only comments) says so in the
// stream, before anything is sent. A real query still runs.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter } from "react-router-dom";
import "../i18n";

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
vi.mock("../components/graph/GraphFrame", () => ({ GraphFrame: () => null }));
let typed = "";
vi.mock("../components/graph/QueryBar", () => ({
  QueryBar: ({ onRun }: { onRun: (q: string) => void }) => (
    <button data-testid="run" onClick={() => onRun(typed.trim())} />
  ),
}));
vi.mock("../components/graph/Neo4jExportModal", () => ({
  Neo4jExportModal: () => <div data-testid="neo4j-export-modal" />,
}));

import { GraphPage } from "../pages/GraphPage";

function serve() {
  const answer = (body: unknown) =>
    new Response(JSON.stringify(body), { headers: { "content-type": "application/json" } });
  const f = vi.fn(async (url: string) =>
    String(url).endsWith("/vocabulary")
      ? answer({ base: "https://p.example", prefixes: {}, classes: [], predicates: [], edges: [], concepts: [] })
      : answer({}),
  );
  vi.stubGlobal("fetch", f);
  return f;
}
const queries = (f: ReturnType<typeof serve>) =>
  f.mock.calls.filter(([u]) => /\/data\/(cypher|sparql)$/.test(String(u)));

describe.each([
  ["cypher", "// nothing here", "MATCH (n) RETURN n"],
  ["sparql", "# nothing here", "SELECT * WHERE { ?s ?p ?o }"],
] as const)("%s editor with no statement", (language, comment, real) => {
  beforeEach(() => {
    localStorage.clear();
    sessionStorage.clear();
  });

  const mount = () =>
    render(
      <MantineProvider>
        <MemoryRouter>
          <GraphPage language={language} key={language} />
        </MemoryRouter>
      </MantineProvider>,
    );

  it.each([
    ["blank", ""],
    ["comment-only", comment],
  ])("%s: says there is no query and sends no request", async (_n, text) => {
    const f = serve();
    typed = text;
    mount();
    fireEvent.click(await screen.findByTestId("run"));
    expect(await screen.findByTestId("graph-no-query")).toHaveTextContent(
      "There is no query to run.",
    );
    expect(queries(f)).toHaveLength(0);
  });

  it("a real query still runs", async () => {
    const f = serve();
    typed = real;
    mount();
    fireEvent.click(await screen.findByTestId("run"));
    await waitFor(() =>
      expect(queries(f).length, JSON.stringify(f.mock.calls.map((c) => String(c[0])))).toBeGreaterThan(0),
    );
    expect(screen.queryByTestId("graph-no-query")).toBeNull();
  });
});
