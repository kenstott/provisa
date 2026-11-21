// Copyright (c) 2026 Kenneth Stott
// Canary: 5c1e8a42-7d93-4b06-9f2a-e3b6d0c41a97
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A query running in a Cypher or SPARQL frame can be stopped. While it runs, Stop stands where the
// query bar's Run is, and in the frame's header. Stop aborts the request, and the frame says
// "Stopped": not an error, and no longer running. An answer that comes after Stop is dropped.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MantineProvider } from "@mantine/core";
import { MemoryRouter, useNavigate } from "react-router-dom";
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
vi.mock("../components/graph/Neo4jExportModal", () => ({ Neo4jExportModal: () => null }));

import { GraphPage } from "../pages/GraphPage";
import { graphStateOf, type GraphLanguage } from "../components/graph/graph-persistence";

const QUERIES: Record<GraphLanguage, string> = {
  cypher: "MATCH (n:PetStore:Pets) RETURN n.name LIMIT 25",
  sparql: "SELECT ?s WHERE { ?s ?p ?o } LIMIT 25",
};
const ENDPOINT: Record<GraphLanguage, string> = { cypher: "/data/cypher", sparql: "/data/sparql" };

const json = (body: unknown, status = 200, type = "application/json") =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": type } });

/** What each language answers, late: one row named "late". */
const LATE: Record<GraphLanguage, () => Response> = {
  cypher: () =>
    json({
      results: [
        {
          statement: QUERIES.cypher,
          status: 200,
          body: { rows: [{ "n.name": "late" }], columns: ["n.name"] },
        },
      ],
    }),
  sparql: () =>
    json(
      { head: { vars: ["s"] }, results: { bindings: [{ s: { type: "literal", value: "late" } }] } },
      200,
      "application/sparql-results+json",
    ),
};

interface Held {
  signal?: AbortSignal;
  answer: (r: Response) => void;
}

/** Serves the page; each query request is held until the test answers it. */
function serve(language: GraphLanguage, honorAbort = true): Held[] {
  const held: Held[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string, init?: RequestInit) => {
      const u = String(url);
      if (u.endsWith(ENDPOINT[language]) && init?.method === "POST") {
        return new Promise<Response>((resolve, reject) => {
          held.push({ signal: init.signal ?? undefined, answer: resolve });
          // As fetch does, unless the answer is to come regardless (one already on its way).
          if (honorAbort) init.signal?.addEventListener("abort", () => reject(init.signal?.reason));
        });
      }
      if (u.includes("/data/graph-schema"))
        return json({ node_labels: [], relationship_types: [] });
      if (u.endsWith("/data/sparql/vocabulary")) return json({ detail: "none" }, 404);
      return json({});
    }),
  );
  return held;
}

function Run({ query }: { query: string }) {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/graph", { state: { query, autoRun: true } })}>run</button>
  );
}

function runQuery(language: GraphLanguage) {
  render(
    <MantineProvider>
      <MemoryRouter initialEntries={["/graph"]}>
        <Run query={QUERIES[language]} />
        <GraphPage language={language} />
      </MemoryRouter>
    </MantineProvider>,
  );
  fireEvent.click(screen.getByText("run"));
}

describe.each<GraphLanguage>(["cypher", "sparql"])("stopping a %s query", (language) => {
  beforeEach(() => {
    localStorage.clear();
    graphStateOf(language).frames = [];
  });

  it("shows Stop in place of Run, in the bar and the frame, while the query runs", async () => {
    const held = serve(language);
    runQuery(language);
    expect(await screen.findByTestId("graph-stop-btn")).toBeInTheDocument();
    expect(screen.getByTestId("graph-frame-stop-btn")).toBeInTheDocument();
    expect(document.querySelector(".graph-run-btn")).toBeNull();
    await waitFor(() => expect(held).toHaveLength(1));
    await act(async () => held[0].answer(LATE[language]()));
    await waitFor(() => expect(screen.queryByTestId("graph-stop-btn")).toBeNull());
    expect(screen.queryByTestId("graph-frame-stop-btn")).toBeNull();
    expect(document.querySelector(".graph-run-btn")).not.toBeNull();
  });

  it("aborts the request, and the frame says Stopped, not an error and not running", async () => {
    const held = serve(language);
    runQuery(language);
    fireEvent.click(await screen.findByTestId("graph-stop-btn"));
    await waitFor(() => expect(held).toHaveLength(1));
    expect(held[0].signal?.aborted).toBe(true);
    expect(await screen.findByTestId("graph-frame-stopped")).toHaveTextContent("Stopped");
    expect(screen.queryByTestId("graph-frame-error")).toBeNull();
    expect(screen.queryByText("Running…")).toBeNull();
    expect(screen.queryByTestId("graph-stop-btn")).toBeNull();
  });

  it("stops one frame from its own header", async () => {
    const held = serve(language);
    runQuery(language);
    fireEvent.click(await screen.findByTestId("graph-frame-stop-btn"));
    await waitFor(() => expect(held).toHaveLength(1));
    expect(held[0].signal?.aborted).toBe(true);
    expect(await screen.findByTestId("graph-frame-stopped")).toBeInTheDocument();
    expect(screen.queryByTestId("graph-frame-error")).toBeNull();
  });

  it("drops an answer that comes after Stop", async () => {
    const held = serve(language, false);
    runQuery(language);
    fireEvent.click(await screen.findByTestId("graph-stop-btn"));
    await waitFor(() => expect(held).toHaveLength(1));
    await act(async () => held[0].answer(LATE[language]()));
    await act(async () => new Promise((r) => setTimeout(r, 20)));
    expect(screen.getByTestId("graph-frame-stopped")).toBeInTheDocument();
    expect(screen.queryByText("late")).toBeNull();
    expect(screen.queryByTestId("graph-frame-error")).toBeNull();
  });
});
